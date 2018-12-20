defmodule Kiq.Integration.ExpiringTest do
  use Kiq.Case

  import ExUnit.CaptureLog

  alias Kiq.Integration

  test "epxiring jobs are not run past the expiration time" do
    {:ok, _pid} = start_supervised(Integration)

    logged =
      capture_log(fn ->
        enqueue_job("A", expires_in: 1)
        enqueue_job("B", expires_in: 5_000)

        assert_receive {:processed, "B"}
      end)

    assert logged =~ ~s("reason":"expired","source":"kiq")
    assert logged =~ ~s("event":"job_success")
  end
end
