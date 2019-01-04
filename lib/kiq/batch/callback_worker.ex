defmodule Kiq.Batch.CallbackWorker do
  @moduledoc false

  use Kiq.Worker, queue: "default"

  alias Kiq.Batch.Status

  def perform([status_map, module_string, event_string, args]) do
    module = to_module(module_string)
    function = to_function(event_string)
    status = to_status(status_map)

    apply(module, function, [status, args])
  end

  defp to_module(module_string) do
    module_string
    |> String.split(".")
    |> Module.safe_concat()
  end

  defp to_function("complete"), do: :handle_complete
  defp to_function("success"), do: :handle_success

  defp to_status(status_map) do
    opts = Map.new(status_map, fn {key, val} -> {String.to_existing_atom(key), val} end)

    struct!(Status, opts)
  end
end
