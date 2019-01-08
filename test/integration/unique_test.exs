defmodule Kiq.Integration.UniqueTest do
  use Kiq.Case

  alias Kiq.{Integration, Pool, Timestamp}
  alias Kiq.Client.Introspection

  setup do
    {:ok, _pid} = start_supervised(Integration)

    :ok = Integration.clear()
  end

  test "unique jobs with the same arguments are only enqueued once" do
    conn = Pool.checkout(Integration.Pool)

    at = Timestamp.unix_in(10)

    {:ok, job_a} = enqueue_job("PASS", at: at, unique_for: :timer.minutes(1))
    {:ok, job_b} = enqueue_job("PASS", at: at, unique_for: :timer.minutes(1))
    {:ok, job_c} = enqueue_job("PASS", at: at, unique_for: :timer.minutes(1))

    assert job_a.unique_token == job_b.unique_token
    assert job_b.unique_token == job_c.unique_token

    with_backoff(fn ->
      assert Introspection.set_size(conn, "schedule") == 1
      assert Introspection.locked?(conn, job_c)
    end)
  end

  test "successful unique jobs are unlocked after completion" do
    conn = Pool.checkout(Integration.Pool)

    {:ok, job} = enqueue_job("PASS", unique_for: :timer.minutes(1))

    assert job.unique_token
    assert job.unlocks_at

    assert_receive {:processed, _}

    with_backoff(fn ->
      refute Introspection.locked?(conn, job)
    end)
  end

  test "started jobs with :unique_until start are unlocked immediately" do
    conn = Pool.checkout(Integration.Pool)

    {:ok, job} = enqueue_job("FAIL", unique_until: "start", unique_for: :timer.minutes(1))

    assert_receive :failed

    with_backoff(fn ->
      refute Introspection.locked?(conn, job)
    end)
  end
end
