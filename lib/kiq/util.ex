defmodule Kiq.Util do
  @moduledoc false

  @spec random_id(size :: pos_integer()) :: binary()
  def random_id(size \\ 14) when size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :lower)
  end

  @spec error_name(any()) :: binary()
  def error_name(error) do
    %{__struct__: module} = Exception.normalize(:error, error)

    module
    |> Module.split()
    |> Enum.join(".")
  end
end
