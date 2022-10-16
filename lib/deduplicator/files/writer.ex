defmodule Deduplicator.Files.Writer do
  @moduledoc """
  Write to files and everything related to it.
  """

  def generate_filename do
    now =
      :os.system_time(:millisecond)
      |> Integer.to_string

    :crypto.hash(:md5, now)
    |> Base.encode16()
  end

#  def write_chuck
end
