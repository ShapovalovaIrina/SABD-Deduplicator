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
  def chunks(binary, chunk_size) do
    hash_size = Deduplicator.Hash.hash_size(:sha)
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

defmodule Deduplicator.Files.Reader do
  @moduledoc """
  Read data from file.
  """

  def read_line(filename, bytes \\ nil) do
    bytes = bytes || Application.get_env(:deduplicator, :chuck_size_bytes)
    %{size: size} = File.stat!(filename)

    read_binary_chunk(filename, size, bytes)
  end

  def find_chunk(filename, line, bytes \\ nil) do
    bytes = bytes || Application.get_env(:deduplicator, :chuck_size_bytes)
    %{size: size} = File.stat!(filename)

    read_binary_chunk(filename, size, bytes)
    |> Enum.fetch(line)
  end

  def read(filename, bytes \\ nil, type \\ :binary) do
    bytes = bytes || Application.get_env(:deduplicator, :chuck_size_bytes)
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

  def read_binary_chunk(filename, size, bytes) do
    filename
    |> File.stream!([], size)
    |> Stream.flat_map(fn
      binary -> BitUtilsChunk.chunks(binary, bytes)
    end)
  end

  def read_chuck(filename, size, bytes) do
    filename
    |> File.stream!([], size)
    |> Stream.chunk_every(bytes)
    |> hd()
  end
end
