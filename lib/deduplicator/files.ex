defmodule BitUtils do
  def chunks(binary, n) do
    do_chunks(binary, n, [])
  end

  defp do_chunks(binary, n, acc) when byte_size(binary) <= n do
    Enum.reverse([binary | acc])
  end

  defp do_chunks(binary, n, acc) do
    <<chunk::binary-size(n), rest::bitstring>> = binary
    do_chunks(rest, n, [<<chunk::binary-size(n)>> | acc])
  end
end

defmodule BitUtilsChunk do
  def chunks(binary, chunk_size, hash_type) do
    hash_size = Deduplicator.Hash.hash_size(hash_type)
    do_chunks(binary, {chunk_size, hash_size}, [])
  end

  defp do_chunks(<<"0", rest::bitstring>> = binary, {chunk_size, _}, acc) when byte_size(rest) <= chunk_size do
    Enum.reverse([binary | acc])
  end
  defp do_chunks(<<"1", rest::bitstring>> = binary, {_, hash_size}, acc) when byte_size(rest) <= hash_size do
    Enum.reverse([binary | acc])
  end
  defp do_chunks(<<"0", _::bitstring>> = binary, {chunk_size, _} = n, acc) do
    <<"0", chunk::binary-size(chunk_size), rest::bitstring>> = binary
    #    IO.inspect(rest, binaries: :as_binaries, limit: 8, label: "chunk rest")
    do_chunks(rest, n, [<<"0", chunk::binary-size(chunk_size)>> | acc])
  end
  defp do_chunks(<<"1", _::bitstring>> = binary, {_, hash_size} = n, acc) do
    <<"1", chunk::binary-size(hash_size), rest::bitstring>> = binary
    #    IO.inspect(rest, binaries: :as_binaries, limit: 8, label: "chunk rest")
    do_chunks(rest, n, [<<"1", chunk::binary-size(hash_size)>> | acc])
  end
end

defmodule Deduplicator.Files do
  @moduledoc """
  Files logic.
  """

  alias Deduplicator.Repo
  alias Deduplicator.Schemas.File, as: StoredFile

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

    read_binary_chunk(filename, size, bytes, hash_type)
  end

  def find_chunk(filename, line, bytes, hash_type) do
    %{size: size} = File.stat!(filename)

    read_binary_chunk(filename, size, bytes, hash_type)
    |> Enum.fetch(line)
  end

  def read(filename, bytes, type \\ :binary) do
    %{size: size} = File.stat!(filename)
    IO.puts("File #{filename}: #{size} bytes")

    case type do
      :binary -> read_binary(filename, size, bytes)
      :chunk -> read_chuck(filename, size, bytes)
    end
  end

  def read_binary(filename, size, bytes) do
    filename
    |> File.stream!([], size)
    |> Stream.flat_map(fn
      binary -> BitUtils.chunks(binary, bytes)
    end)
  end

  defp read_binary_chunk(filename, size, bytes, hash_type) do
    filename
    |> File.stream!([], size)
    |> Stream.flat_map(fn
      binary -> BitUtilsChunk.chunks(binary, bytes, hash_type)
    end)
  end

  def read_chuck(filename, size, bytes) do
    filename
    |> File.stream!([], size)
    |> Stream.chunk_every(bytes)
    |> hd()
  end
end
