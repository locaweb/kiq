defmodule Kiq.Client do
  @moduledoc false

  use GenServer

  import Kiq.Logger, only: [log: 1]

  alias Kiq.{Batch, Config, Pool, Job}
  alias Kiq.Client.{Batching, Cleanup, Queueing}

  @type client :: GenServer.server()
  @type options :: [config: Config.t(), name: GenServer.name()]
  @type scoping :: :sandbox | :shared

  defmodule State do
    @moduledoc false

    defstruct pool: nil,
              table: nil,
              test_mode: :disabled,
              flush_interval: 10,
              flush_maximum: 5_000,
              start_interval: 10
  end

  @spec start_link(opts :: options()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec store(client(), Batch.t() | Job.t()) :: {:ok, Batch.t() | Job.t()}
  def store(client, %Batch{} = batch) do
    GenServer.call(client, {:store, batch})
  end

  def store(client, %Job{queue: queue} = job) when is_binary(queue) do
    job =
      job
      |> Job.apply_unique()
      |> Job.apply_expiry()

    GenServer.call(client, {:store, job})
  end

  @spec fetch(client(), scoping()) :: list(Job.t())
  def fetch(client, scoping \\ :sandbox) when scoping in [:sandbox, :shared] do
    GenServer.call(client, {:fetch, scoping})
  end

  @spec clear(client()) :: :ok
  def clear(client) do
    GenServer.call(client, :clear)
  end

  # Server

  @impl GenServer
  def init(config: config) do
    %Config{flush_interval: interval, pool_name: pool, test_mode: test_mode} = config

    Process.flag(:trap_exit, true)

    opts =
      [table: :ets.new(:jobs, [:duplicate_bag, :compressed])]
      |> Keyword.put(:flush_interval, interval)
      |> Keyword.put(:start_interval, interval)
      |> Keyword.put(:pool, pool)
      |> Keyword.put(:test_mode, test_mode)

    state =
      State
      |> struct(opts)
      |> schedule_flush()

    {:ok, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    try do
      perform_flush(state)
    rescue
      _error -> :ok
    catch
      :exit, _value -> :ok
    end

    :ok
  end

  @impl GenServer
  def handle_info(:flush, state) do
    state
    |> perform_flush()
    |> schedule_flush()

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:clear, _from, %State{pool: pool, table: table} = state) do
    true = :ets.delete_all_objects(table)

    :ok =
      pool
      |> Pool.checkout()
      |> Cleanup.clear()

    {:reply, :ok, state}
  end

  def handle_call({:store, struct}, {pid, _tag}, %State{table: table} = state) do
    :ets.insert(table, {pid, struct})

    {:reply, {:ok, struct}, state}
  end

  def handle_call({:fetch, scoping}, {pid, _tag}, %State{table: table} = state) do
    pid_match = if scoping == :sandbox, do: pid, else: :_
    jobs = :ets.select(table, [{{pid_match, :"$1"}, [], [:"$1"]}])

    {:reply, jobs, state}
  end

  # Helpers

  defp schedule_flush(%State{flush_interval: interval} = state) do
    Process.send_after(self(), :flush, interval)

    state
  end

  defp schedule_flush(%State{} = state) do
    state
  end

  defp perform_flush(%State{pool: pool, table: table, test_mode: :disabled} = state) do
    try do
      conn = Pool.checkout(pool)

      {jobs, batches} = :ets.foldl(&entry_to_lists/2, {[], []}, table)

      :ok = Queueing.enqueue(conn, jobs)
      :ok = Batching.enqueue(conn, batches)
      true = :ets.delete_all_objects(table)

      transition_to_success(state)
    rescue
      error in [Redix.ConnectionError] ->
        transition_to_failed(state, Exception.message(error))
    catch
      :exit, reason ->
        transition_to_failed(state, reason)
    end
  end

  defp perform_flush(state) do
    state
  end

  defp entry_to_lists({_key, %Job{} = job}, {jobs, batches}), do: {[job | jobs], batches}
  defp entry_to_lists({_key, %Batch{} = batch}, {jobs, batches}), do: {jobs, [batch | batches]}

  defp transition_to_success(%State{flush_interval: interval, start_interval: interval} = state) do
    state
  end

  defp transition_to_success(%State{start_interval: interval} = state) do
    log(%{event: "enqueueing_restored", details: "enqueueing has been restored"})

    %{state | flush_interval: interval}
  end

  defp transition_to_failed(state, reason) do
    if state.flush_interval == state.start_interval do
      log(%{event: "enqueueing_failed", details: inspect(reason)})
    end

    interval = Enum.min([trunc(state.flush_interval * 1.5), state.flush_maximum])

    %{state | flush_interval: interval}
  end
end
