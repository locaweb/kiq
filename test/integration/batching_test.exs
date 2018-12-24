defmodule Kiq.Integration.BatchingTest do
  use Kiq.Case

  alias Kiq.{Batch, Integration}
  alias Kiq.Integration.Worker

  defmodule BatchCallbackHandler do
    alias Kiq.Integration.Worker

    def handle_complete(status, %{pid: pid}) do
      send(Worker.bin_to_pid(pid), {:batch_complete, status})
    end

    def handle_success(status, %{pid: pid}) do
      send(Worker.bin_to_pid(pid), {:batch_success, status})
    end
  end

  setup do
    {:ok, _pid} = start_supervised({Integration, fetch_interval: 10})

    :ok = Integration.clear()
  end

  test "batch jobs are enqueued and monitored as a group" do
    pid_bin = Worker.pid_to_bin()

    # Enqueueing within a batch must maintain pairity with enqueueing a job by
    # itself. We can verify with a scheduled job, which should count toward the
    # batch status.
    scheduled_job =
      [pid_bin, 4]
      |> Worker.new()
      |> Map.put(:at, unix_in(10, :millisecond))

    Batch.new(description: "Special Jobs", queue: "integration")
    |> Batch.add_job(Worker.new([pid_bin, 1]))
    |> Batch.add_job(Worker.new([pid_bin, 2]))
    |> Batch.add_job(Worker.new([pid_bin, 3]))
    |> Batch.add_job(scheduled_job)
    |> Batch.add_callback(:success, BatchCallbackHandler, pid: pid_bin)
    |> Batch.add_callback(:complete, BatchCallbackHandler, pid: pid_bin)
    |> Integration.enqueue()

    assert_receive {:batch_success, %{total: 4, failures: 0, pending: 0}}
    assert_receive {:batch_complete, %{total: 4, failures: 0, pending: 0}}

    assert_received {:processed, 1}
    assert_received {:processed, 2}
    assert_received {:processed, 3}
    assert_received {:processed, 4}
  end

  test "batch job failures are recorded and trigger completion callbacks" do
    pid_bin = Worker.pid_to_bin()

    Batch.new(queue: "integration")
    |> Batch.add_job(Worker.new([pid_bin, "FAIL"]))
    |> Batch.add_job(Worker.new([pid_bin, "FAIL"]))
    |> Batch.add_callback(:complete, BatchCallbackHandler, pid: pid_bin)
    |> Integration.enqueue()

    assert_receive {:batch_complete, %{total: 2, failures: 2, pending: 2}}

    assert_received :failed
  end
end
