defmodule Kiq.BatchTest do
  use Kiq.Case, async: true

  alias Kiq.{Batch, Job}

  doctest Batch

  describe "new/1" do
    test "dynamic default values are added automatically" do
      batch = Batch.new()

      assert batch.bid =~ ~r/[a-z0-9]+/
      assert batch.created_at
      assert batch.expires_in == 2_592_000
    end
  end

  describe "add_job/2" do
    test "jobs are appended to the internal jobs list" do
      batch =
        Batch.new()
        |> Batch.add_job(job())
        |> Batch.add_job(job())
        |> Batch.add_job(job())

      assert [%Job{}, %Job{}, %Job{}] = batch.jobs
    end
  end

  describe "add_callback/4" do
    test "new module/meta callbacks are added for the event" do
      handler = BatchHandler

      batch =
        Batch.new()
        |> Batch.add_callback(:complete, handler, a: 1, b: 2)
        |> Batch.add_callback(:complete, handler, %{a: 3, b: 4})
        |> Batch.add_callback(:success, handler, c: 1, d: 2)
        |> Batch.add_callback(:success, handler, %{c: 3, d: 4})

      assert %{complete: completes, success: successes} = batch.callbacks

      assert completes == [%{handler => %{a: 3, b: 4}}, %{handler => %{a: 1, b: 2}}]
      assert successes == [%{handler => %{c: 3, d: 4}}, %{handler => %{c: 1, d: 2}}]
    end
  end

  describe "encode/1 decode/1" do
    test "nil and temporary values are omitted" do
      encoded = Batch.encode(Batch.new())

      assert encoded =~ "bid"
      assert encoded =~ "created_at"
      assert encoded =~ "callbacks"
      refute encoded =~ "description"
      refute encoded =~ "jobs"
      refute encoded =~ "parent"
    end

    test "encoding and decoding are complementary" do
      decoded =
        Batch.new(description: "Mega Batch")
        |> Batch.add_job(job())
        |> Batch.add_callback(:success, __MODULE__)
        |> Batch.encode()
        |> Batch.decode()

      assert [] = decoded.jobs
      assert %{success: [%{}]} = decoded.callbacks
    end
  end
end
