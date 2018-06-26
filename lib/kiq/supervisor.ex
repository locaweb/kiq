defmodule Kiq.Supervisor do
  @moduledoc false

  use Supervisor

  alias Kiq.{Client, Config}
  alias Kiq.Queue.Scheduler
  alias Kiq.Queue.Supervisor, as: QueueSupervisor
  alias Kiq.Reporter.Supervisor, as: ReporterSupervisor

  @type options :: [config: Config.t(), name: identifier()]

  @doc false
  @spec start_link(opts :: options()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    conf = Keyword.get(opts, :config, Config.new())

    Supervisor.start_link(__MODULE__, conf, name: name)
  end

  @impl Supervisor
  def init(%Config{} = config) do
    children = client_children(config) ++ server_children(config)

    Supervisor.init(children, strategy: :rest_for_one)
  end

  ## Helpers

  defp client_children(config) do
    [{Client, config: config, name: config.client}]
  end

  defp server_children(%Config{server?: false}) do
    []
  end

  defp server_children(config) do
    reporters = [{ReporterSupervisor, config: config}]
    schedulers = Enum.map(config.schedulers, &scheduler_spec(&1, config))
    queues = Enum.map(config.queues, &queue_spec(&1, config))

    reporters ++ schedulers ++ queues
  end

  defp scheduler_spec(set, config) do
    name = Module.concat(["Kiq", "Scheduler", String.capitalize(set)])
    opts = [client: config.client, set: set, name: name]

    Supervisor.child_spec({Scheduler, opts}, id: name)
  end

  defp queue_spec({queue, limit}, config) do
    queue = maybe_to_string(queue)
    name = Module.concat(["Kiq", "Queue", String.capitalize(queue)])
    opts = [config: config, queue: queue, limit: limit, name: name]

    Supervisor.child_spec({QueueSupervisor, opts}, id: name)
  end

  defp maybe_to_string(queue) when is_atom(queue), do: Atom.to_string(queue)
  defp maybe_to_string(queue) when is_binary(queue), do: queue
end
