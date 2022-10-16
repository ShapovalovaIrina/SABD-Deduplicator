defmodule Deduplicator do
  @moduledoc """
  Deduplication logic.
  """

  alias Deduplicator.Hash
  require Logger

  import Deduplicator.Files.Writer, only: [generate_filename: 0]

  @doc """
  Read data from input file.
  Deduplicate data chunks by hash calculating.
  """
  def deduplicate_file(filename, filepath \\ "") do
    output_filename = filepath <> "/" <> generate_filename()
    :ok = File.touch(output_filename)
    {:ok, output_file} = File.open(output_filename, [:append, :write])

    filename
    |> Deduplicator.Files.Reader.read()
    |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, line} ->
        if rem(line, 100) == 0, do: IO.puts("chunk #{line}")
        case handle_chunk(chunk, output_file, output_filename, line) do
          :ok              -> {:cont, {:ok, line + 1,}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
    end)
    |> case do
      {:ok, _} ->
        :ok = File.close(output_file)
        {:ok, output_filename}
      {:error, reason} ->
        Logger.error("Deduplicate file error #{inspect(reason)}")
        :ok = File.close(output_file)
        File.rm!(output_filename)
        {:error, reason}
    end
  end

  def recovery_file(input_filename, output_filename) do
    :ok = File.touch(output_filename)
    {:ok, output_file} = File.open(output_filename, [:write])
    
    input_filename
    |> Deduplicator.Files.Reader.read_line()
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
#      chunk =
#        chunk
#        |> String.split_at(-1)
#        |> elem(0)
      case recovery_chunk(chunk) do
        {:ok, chunk} ->
          :ok = IO.binwrite(output_file, chunk)
          {:cont, :ok}
        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
         :ok ->
           :ok = File.close(output_file)
           {:ok, output_filename}
         {:error, reason} ->
           Logger.error("Recovery file error #{inspect(reason)}")
           :ok = File.close(output_file)
           File.rm!(output_filename)
           {:error, reason}
       end
  end

  defp handle_chunk(chunk, file, filename, line) do
    hash = Hash.binary_hash(chunk)

    with {:ok, hash_link} <- save_hash(hash, filename, line),
          :ok <- IO.binwrite(file, get_hash_str(chunk, hash_link)) do
      :ok
    end
  end

  defp save_hash(hash, filename, line) do
    Hash.hash_link_exists?(hash)
    |> case do
         {:ok, hash} ->
           Hash.inc_links(hash)
         {:error, :not_found} ->
           Hash.save_hash_link(hash, filename, line)
    end
  end

  # 0 and 1 to indicate chunk and hash
  @chunk_identifier "0"
  @hash_identifier "1"
  defp get_hash_str(chunk, %Deduplicator.Schemas.HashLinks{refs_num: 1} = _hash_link),
      do: @chunk_identifier <> chunk
#          |> IO.inspect(label: "chunk_identifier", limit: :infinity)
  defp get_hash_str(_chunk, hash_link),
      do: @hash_identifier <> hash_link.hash
#          |> IO.inspect(label: "hash_identifier")
      
      
  def recovery_chunk(@chunk_identifier <> chunk) do
    {:ok, chunk}
  end
  def recovery_chunk(@hash_identifier <> hash) do
#    IO.puts("hash_identifier")
    case Hash.hash_link_exists?(hash) do
      {:ok, %{filename: filename, line: line}} ->
        get_chunk_from_file(filename, line)
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_chunk_from_file(filename, line) do
    case Deduplicator.Files.Reader.find_chunk(filename, line) do
      {:ok, @chunk_identifier <> chunk} -> {:ok, chunk}
      {:ok, @hash_identifier <> _hash} -> {:error, :not_chunk}
      {:error, reason} ->{:error, reason}
    end
  end
end
