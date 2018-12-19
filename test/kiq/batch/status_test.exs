defmodule Kiq.Batch.StatusTest do
  use ExUnit.Case, async: true

  alias Kiq.Batch.Status

  describe "complete?/1" do
    test "the status is complete when pending and failures are equal" do
      assert Status.complete?(%Status{pending: 0, failures: 0})
      assert Status.complete?(%Status{pending: 1, failures: 1})
      refute Status.complete?(%Status{pending: 0, failures: 1})
      refute Status.complete?(%Status{pending: 1, failures: 0})
    end
  end

  describe "success?/1" do
    test "the status is successful when there are no more pending jobs" do
      assert Status.success?(%Status{pending: 0})
      refute Status.success?(%Status{pending: 1})
    end
  end
end
