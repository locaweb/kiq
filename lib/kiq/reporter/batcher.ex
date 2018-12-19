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
    status =
      state.pool
      |> Pool.checkout()
      |> Batching.add_success(bid, jid)

    # TODO: Use a distributed lock to prevent duplicates
    if Status.complete?(status), do: enqueue_callbacks(state.client, status, "complete")
    if Status.success?(status), do: enqueue_callbacks(state.client, status, "success")

    state
  end

  def handle_success(_job, _meta, state), do: state

  # Helpers

  defp enqueue_callbacks(client, %Status{callbacks: callbacks, queue: queue} = status, event) do
    callbacks
    |> Map.get(event, [])
    |> Enum.each(fn callback ->
      [{module, args}] = Map.to_list(callback)

      job =
        [Map.from_struct(status), module, event, args]
        |> CallbackWorker.new()
        |> Map.put(:queue, queue)

      Client.store(client, job)
    end)
  end
end
