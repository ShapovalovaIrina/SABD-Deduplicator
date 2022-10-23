defmodule Deduplicator.Files do
  @moduledoc """
  Files logic.
  """

  alias Deduplicator.Repo
  alias Deduplicator.Schemas.File, as: StoredFile

  alias Deduplicator.BinaryUtils
  
  # DATABASE

  def save_file(filename, bytes, algorithm) do
    StoredFile.create_changeset(filename, bytes, Atom.to_string(algorithm))
    |> Repo.insert()
  end

  def get_input_file(filename) do
    Repo.get_by(StoredFile, filename: filename)
    |> wrap_search()
  end

  def wrap_search(nil), do: {:error, :not_found}
  def wrap_search(res), do: {:ok, res}

  # FILES

  def read_chunks(filename, bytes, hash_type) do
    %{size: size} = File.stat!(filename)

    filename
    |> read_chunked_binary(size, bytes, hash_type)
  end

  def find_chunk(filename, line, bytes, hash_type) do
    %{size: size} = File.stat!(filename)

    filename
    |> read_chunked_binary(size, bytes, hash_type)
    |> Enum.fetch(line)
  end

  def read(filename, bytes) do
    %{size: size} = File.stat!(filename)
    IO.puts("File #{filename}: #{size} bytes")

    read_binary(filename, size, bytes)
  end

  def read_binary(filename, size, bytes) do
    filename
    |> File.stream!([], size)
    |> Stream.flat_map(&BinaryUtils.split_binary_into_chunks(&1, bytes))
  end

  defp read_chunked_binary(filename, size, bytes, hash_type) do
    filename
    |> File.stream!([], size)
    |> Stream.flat_map(&BinaryUtils.read_chunked_file(&1, bytes, hash_type))
  end
end