defmodule Deduplicator.BinaryUtils do
  @moduledoc false

  @chunk_identifier "0"
  @hash_identifier "1"

  def chunk_identifier, do: @chunk_identifier
  def hash_identifier, do: @hash_identifier

  def split_binary_into_chunks(binary, n) do
    split_binary_into_chunks_with_acc(binary, n, [])
  end

  defp split_binary_into_chunks_with_acc(binary, n, acc) when byte_size(binary) <= n do
    Enum.reverse([binary | acc])
  end
  defp split_binary_into_chunks_with_acc(binary, n, acc) do
    <<chunk::binary-size(n), rest::bitstring>> = binary
    split_binary_into_chunks_with_acc(rest, n, [<<chunk::binary-size(n)>> | acc])
  end

  def read_chunked_file(binary, chunk_size, hash_type) do
    hash_size = Deduplicator.Hash.hash_size(hash_type)
    read_chunked_file_with_acc(binary, {chunk_size, hash_size}, [])
  end

  defp read_chunked_file_with_acc(<<@chunk_identifier, rest::bitstring>> = binary, {chunk_size, _}, acc)
       when byte_size(rest) <= chunk_size do
    Enum.reverse([binary | acc])
  end
  defp read_chunked_file_with_acc(<<@hash_identifier, rest::bitstring>> = binary, {_, hash_size}, acc)
       when byte_size(rest) <= hash_size do
    Enum.reverse([binary | acc])
  end
  defp read_chunked_file_with_acc(<<@chunk_identifier, _::bitstring>> = binary, {chunk_size, _} = n, acc) do
    <<@chunk_identifier, chunk::binary-size(chunk_size), rest::bitstring>> = binary
    read_chunked_file_with_acc(rest, n, [<<@chunk_identifier, chunk::binary-size(chunk_size)>> | acc])
  end
  defp read_chunked_file_with_acc(<<@hash_identifier, _::bitstring>> = binary, {_, hash_size} = n, acc) do
    <<@hash_identifier, chunk::binary-size(hash_size), rest::bitstring>> = binary
    read_chunked_file_with_acc(rest, n, [<<@hash_identifier, chunk::binary-size(hash_size)>> | acc])
  end
end
