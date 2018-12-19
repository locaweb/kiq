defmodule Kiq.Integration.EnqueueingTest do
  use Kiq.Case

  import ExUnit.CaptureLog

  alias Kiq.Integration

  setup do
    {:ok, _pid} = start_supervised(Integration)

    :ok = Integration.clear()
  end

  test "enqueuing and executing jobs successfully" do
    logged =
      capture_log(fn ->
        enqueue_job("OK")

        assert_receive {:processed, "OK"}
      end)

    assert logged =~ ~s("event":"job_started")
    assert logged =~ ~s("event":"job_success")
    refute logged =~ ~s("event":"job_failure")
  end

  test "jobs are reliably enqueued despite network failures" do
    {:ok, redix} = Redix.start_link(redis_url())

    capture_log(fn ->
      {:ok, _} = Redix.command(redix, ["CLIENT", "KILL", "TYPE", "normal"])

      enqueue_job("OK")

      assert_receive {:processed, "OK"}, 3_000
    end)
  end
end
