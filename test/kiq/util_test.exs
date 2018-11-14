defmodule Kiq.UtilTest do
  use Kiq.Case, async: true

  alias Kiq.Util

  describe "random_id/1" do
    property "a random length id of the requested length is generated" do
      check all size <- integer(1..20) do
        Util.random_id(size) =~ ~r/^[0-9a-z]{#{size}}$/
      end
    end
  end
end
