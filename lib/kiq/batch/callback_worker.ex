defmodule Kiq.Batch.CallbackWorker do
  use Kiq.Worker, queue: "default"

  def perform([status, module_string, event_string, args]) do
    module = to_module(module_string)
    function = to_function(event_string)

    apply(module, function, [status, args])
  end

  defp to_module(module_string) do
    module_string
    |> String.split(".")
    |> Module.safe_concat()
  end

  defp to_function("complete"), do: :handle_complete
  defp to_function("success"), do: :handle_success
end
