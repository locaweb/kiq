describe "Batches" do
  test "a collection of jobs are enqueued and monitored as a group" do
    defmodule BatchCallbackHandler do
      def handle_success(%{total: total, pending: pending, failures: failures}, opts) do
        send(self(), {:batch_success, total, pending, failures, opts})
      end
    end

    pid_bin = Worker.pid_to_bin()

    Batch.new(description: "Special Jobs")
    |> Batch.add_job(Worker.new([pid_bin, 1]))
    |> Batch.add_job(Worker.new([pid_bin, 2]))
    |> Batch.add_job(Worker.new([pid_bin, 3]))
    |> Batch.add_callback(:success, BatchCallbackHandler, id: 1)
    |> Integration.enqueue()

    assert_receive {:batch_success, 3, 0, 0, %{id: 1}}

    assert_received {:processed, 1}
    assert_received {:processed, 2}
    assert_received {:processed, 3}
  end
end
