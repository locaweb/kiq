defmodule Kiq.Util do
  @moduledoc false

  @spec random_id(size :: pos_integer()) :: binary()
  def random_id(size \\ 14) when size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :lower)
  end
end
