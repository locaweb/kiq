defmodule Kiq.Reporter.Batcher do
  @moduledoc false

  use Kiq.Reporter

  alias Kiq.{Client, Job, Pool}
  alias Kiq.Batch.{Status, CallbackWorker}
  alias Kiq.Client.Batching

  defmodule State do
    @moduledoc false

    defstruct [:client, :pool]
  end

  # Callbacks

  @impl GenStage
  def init(opts) do
    {conf, opts} = Keyword.pop(opts, :config)

    {:consumer, %State{client: conf.client_name, pool: conf.pool_name}, opts}
  end

  @impl Reporter
  def handle_success(%Job{bid: bid, jid: jid}, _meta, state) when is_binary(bid) do
    conn = Pool.checkout(state.pool)
    status = Batching.add_success(conn, bid, jid)

    maybe_enqueue_complete(status, conn, state.client)
    maybe_enqueue_success(status, conn, state.client)

    state
  end

  def handle_success(_job, _meta, state), do: state

  @impl Reporter
  def handle_failure(%Job{bid: bid, jid: jid}, error, _stack, state) when is_binary(bid) do
    conn = Pool.checkout(state.pool)
    status = Batching.add_failure(conn, bid, jid, error)

    maybe_enqueue_complete(status, conn, state.client)

    state
  end

  def handle_failure(_job, _error, _stack, state), do: state

  # Helpers

  defp maybe_enqueue_complete(status, conn, client) do
    if Status.complete?(status) and Batching.locked?(conn, status.bid, :complete) do
      enqueue_callbacks(client, status, "complete")
    end
  end

  defp maybe_enqueue_success(status, conn, client) do
    if Status.success?(status) and Batching.locked?(conn, status.bid, :success) do
      enqueue_callbacks(client, status, "success")
    end
  end

  defp enqueue_callbacks(client, %Status{callbacks: callbacks, queue: queue} = status, event) do
    callbacks
    |> Map.get(event, [])
    |> Enum.each(fn callback ->
      [{module, args}] = Map.to_list(callback)

      job =
        [Map.from_struct(status), module, event, args]
        |> CallbackWorker.new()
        |> maybe_put_queue(queue)

      Client.store(client, job)
    end)
  end

  defp maybe_put_queue(job, ""), do: job
  defp maybe_put_queue(job, queue), do: %{job | queue: queue}
end
