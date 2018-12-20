defmodule Kiq.Integration.ResurrectionTest do
  use Kiq.Case

  alias Kiq.{Integration, Pool}
  alias Kiq.Client.Introspection

  defp backup_size(ident) do
    Integration.Pool
    |> Pool.checkout()
    |> Introspection.backup_size(ident, "integration")
  end

  test "orphaned jobs in backup queues are resurrected" do
    {:ok, _pid} = start_supervised({Integration, identity: "ident:1234"})

    :ok = Integration.clear()

    enqueue_job("SLOW")

    assert_receive :started

    assert backup_size("ident:1234") == 1

    :ok = stop_supervised(Integration)
    {:ok, _pid} = start_supervised({Integration, identity: "ident:4321", pool_size: 1})

    assert_receive :started

    with_backoff(fn ->
      assert backup_size("ident:1234") == 0
      assert backup_size("ident:4321") == 0
    end)
  end
end
