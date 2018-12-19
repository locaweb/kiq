defmodule Kiq.Integration.RetryingTest do
  use Kiq.Case

  alias Kiq.{Integration, Pool}
  alias Kiq.Client.Introspection

  @identity "ident:1234"

  setup do
    {:ok, _pid} = start_supervised({Integration, identity: @identity})

    :ok = Integration.clear()
  end

  test "jobs are pruned from the backup queue after running" do
    conn = Pool.checkout(Integration.Pool)

    enqueue_job("PASS")

    assert_receive {:processed, "PASS"}

    assert Introspection.queue_size(conn, "integration") == 0

    with_backoff(fn ->
      assert Introspection.backup_size(conn, @identity, "integration") == 0
    end)
  end

  test "failed jobs are enqueued for retry" do
    conn = Pool.checkout(Integration.Pool)

    enqueue_job("FAIL")

    assert_receive :failed

    with_backoff(fn ->
      assert [job | _] = Introspection.retries(conn)

      assert job.retry_count == 1
      assert job.error_class == "RuntimeError"
      assert job.error_message == "bad stuff happened"
    end)
  end

  test "jobs are moved to the dead set when retries are exhausted" do
    conn = Pool.checkout(Integration.Pool)

    enqueue_job("FAIL", retry: true, retry_count: 25)
    enqueue_job("FAIL", retry: true, retry_count: 25, dead: false)

    assert_receive :failed

    with_backoff(fn ->
      assert Introspection.set_size(conn, "retry") == 0
      assert Introspection.set_size(conn, "dead") == 1
    end)
  end
end
