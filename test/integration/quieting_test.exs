defmodule Kiq.Integration.QuietingTest do
  use Kiq.Case

  alias Kiq.Integration

  describe "Quieting" do
    test "job processing is paused when quieted" do
      {:ok, _pid} = start_supervised(Integration)

      Integration.configure(quiet: true)

      enqueue_job("OK")

      refute_receive {:processed, "OK"}, 100

      Integration.configure(quiet: false)

      assert_receive {:processed, "OK"}
    end
  end
end
