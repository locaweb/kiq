defmodule Kiq.Integration.ResurrectionTest do
  use Kiq.Case

  alias Kiq.{Integration, Pool}
  alias Kiq.Client.Introspection

  test "orphaned jobs in backup queues are resurrected" do
    {:ok, _pid} = start_supervised({Integration, identity: "ident:1234"})

    enqueue_job("SLOW")

    assert_receive :started

    :ok = stop_supervised(Integration)
    {:ok, _pid} = start_supervised({Integration, identity: "ident:4321", pool_size: 1})

    assert_receive :started

    conn = Pool.checkout(Integration.Pool)

    with_backoff(fn ->
      assert Introspection.backup_size(conn, "ident:1234", "integration") == 0
      assert Introspection.backup_size(conn, "ident:4321", "integration") == 0
    end)
  end
end
