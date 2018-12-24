defmodule Bench.BatchWorker do
  use Kiq.Worker, queue: "bench"

  def perform([index, total]) do
    index * total
  end
end

defmodule Bench.BatchCallbacks do
  import Bench.Kiq, only: [bin_to_pid: 1]

  def handle_success(_status, %{pid: pid}) do
    send(bin_to_pid(pid), :batch_success)
  end
end

enqueue_and_wait = fn total ->
  pid_bin = Bench.Kiq.pid_to_bin(self())

  batch =
    [queue: "bench"]
    |> Kiq.Batch.new()
    |> Kiq.Batch.add_callback(:success, Bench.BatchCallbacks, pid: pid_bin)

  batch =
    Enum.reduce(0..total, batch, fn index, batch ->
      job = Bench.BatchWorker.new([index, total])

      Kiq.Batch.add_job(batch, job)
    end)

  Bench.Kiq.enqueue(batch)

  receive do
    :batch_success -> :ok
  after
    5_000 -> IO.puts("No message received")
  end
end

Benchee.run(
  %{"Batch Callbacks" => enqueue_and_wait},
  inputs: %{"100" => 100},
  time: 10
)
