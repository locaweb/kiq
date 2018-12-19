defmodule Kiq.Integration.BatchingTest do
  use Kiq.Case

  alias Kiq.{Batch, Integration}
  alias Kiq.Integration.Worker

  test "a collection of jobs are enqueued and monitored as a group" do
    defmodule BatchCallbackHandler do
      def handle_complete(status, %{pid: pid}) do
        send(Worker.bin_to_pid(pid), {:batch_complete, status})
      end

      def handle_success(status, %{pid: pid}) do
        send(Worker.bin_to_pid(pid), {:batch_success, status})
      end
    end

    {:ok, _pid} = start_supervised(Integration)

    pid_bin = Worker.pid_to_bin()

    Batch.new(description: "Special Jobs", queue: "integration")
    |> Batch.add_job(Worker.new([pid_bin, 1]))
    |> Batch.add_job(Worker.new([pid_bin, 2]))
    |> Batch.add_job(Worker.new([pid_bin, 3]))
    |> Batch.add_callback(:success, BatchCallbackHandler, pid: pid_bin)
    |> Batch.add_callback(:complete, BatchCallbackHandler, pid: pid_bin)
    |> Integration.enqueue()

    assert_receive {:batch_success, %{total: 3}}
    assert_receive {:batch_complete, %{total: 3}}

    assert_received {:processed, 1}
    assert_received {:processed, 2}
    assert_received {:processed, 3}
  end
end
