defmodule Deduplicator do
  @moduledoc """
  Deduplication logic.
  """

  @chunk_identifier Deduplicator.BinaryUtils.chunk_identifier()
  @hash_identifier Deduplicator.BinaryUtils.hash_identifier()

  alias Deduplicator.Hash
  require Logger

  @doc """
  Read data from input file.
  Deduplicate data chunks by hash calculating.

  Possible options:
    - output_filepath - where to save output file. Default: current path.
    - chunk_amount - the number of chunks to save to database at a time. Default: 100.
    - bytes - chunk size in bytes. Default: #{Application.get_env(:deduplicator, :chuck_size_bytes)}.
    - hash - hash algorithm. Default: #{Hash.default_algorithm()}. Choices: #{inspect(Hash.algorithms())}.
  """
  def deduplicate_file(input_filename, opts \\ []) do
    output_filepath = Keyword.get(opts, :output_filepath, "")
    chunk_amount    = Keyword.get(opts, :chunk_amount, 100)
    bytes           = Keyword.get(opts, :bytes, Application.get_env(:deduplicator, :chuck_size_bytes))
    algorithm       = Keyword.get(opts, :hash_algorimth) |> Hash.get_algorithm()
    compress?       = Keyword.get(opts, :compress, false)

    # TODO: handle errors
    output_filename = output_filepath <> "/" <> generate_filename() <> ".bin"
    :ok = File.touch(output_filename)
    {:ok, output_file} = File.open(output_filename, [:append, :write])
    {:ok, %{id: file_id}} = Deduplicator.Files.save_file(output_filename, bytes, algorithm)

    state = %{
      line: 0,
      binary: "",
      insert_list: [],
      update_list: [],
      chunk_amount: chunk_amount,
      algorithm: algorithm,
      file_id: file_id,
      file: output_file
    }

    with {:ok, _} <- split_file_and_handle_chunks(input_filename, state, bytes),
         :ok <- File.close(output_file),
         {:ok, compressed_filename} <- Deduplicator.Files.compress_file(output_filename, compress?),
         :ok <- Deduplicator.Files.remove_file(output_filename, compress?) do
      {:ok, compressed_filename}
    else
      {:error, reason} ->
        Logger.error("Deduplicate file error #{inspect(reason)}")
        :ok = File.close(output_file)
        {:error, reason}
    end
  end

  defp split_file_and_handle_chunks(input_filename, state, bytes) do
    input_filename
    |> Deduplicator.Files.read(bytes)
    |> Enum.reduce_while(state, &handle_chunks/2)
    |> save_hash_list_from_state()
  end

  defp handle_chunks(chunk, %{
    insert_list: insert_list,
    update_list: update_list,
    chunk_amount: chunk_amount
  } = state) when length(insert_list) + length(update_list) < chunk_amount do
    {:cont, add_chuck_to_state(chunk, state)}
  end
  defp handle_chunks(chunk, %{line: line} = state) do
    Logger.info("chunk #{line}, time #{inspect(:erlang.localtime())}")

    add_chuck_to_state(chunk, state)
    |> save_hash_list_from_state()
    |> case do
         {:ok, state} -> {:cont, state}
         {:error, reason} -> {:halt, {:error, reason}}
    end
  end
  
  defp add_chuck_to_state(chunk, %{line: line, binary: binary, insert_list: list, algorithm: algorithm} = state) do
    hash = Hash.binary_hash(chunk, algorithm)

    hash_link =
      with {:error, :not_found} <- Hash.get_hash_link(hash) do
        Enum.find(list, & &1.hash == hash)
      else
        {:ok, hash_link} -> hash_link
      end

    state
    |> add_hash_link_to_state_list(hash_link, hash)
    |> Map.merge(%{
      line: line + 1,
      binary: binary <> get_hash_str(chunk, hash_link)
    })
  end

  defp add_hash_link_to_state_list(
         %{insert_list: list, line: line} = state,
         nil = _hash_link,
         hash
       ) do
    entity = %{
      hash: hash,
      line: line
    }
    %{state | insert_list: [entity | list]}
  end
  defp add_hash_link_to_state_list(
         %{update_list: list} = state,
         %{hash: hash} = _hash_link,
         _
       ) do
    %{state | update_list: [hash | list]}
  end
  
  defp save_hash_list_from_state(%{
    binary: binary,
    insert_list: insert_list,
    update_list: update_list,
    file_id: file_id,
    file: file
  } = state) do
    insert_list = Enum.map(insert_list, &Map.put(&1, :file_id, file_id))

    with :ok <- Hash.insert_all_hash_links(insert_list),
         :ok <- Hash.update_all_hash_links(update_list),
         :ok <- IO.binwrite(file, binary) do
      state = %{state |
        binary: "",
        insert_list: [],
        update_list: [],
      }
      {:ok, state}
    else
      error ->
        Logger.error("Handle chunk error: #{inspect(error)}")
        {:error, error}
    end
  end

  @doc """
  Recovery file from deduplicated file.
  """
  def recovery_file(input_filename, output_filename, opts \\ []) do
    compress? = Keyword.get(opts, :compress, false)

    with {:ok, input_filename_unzip} <-
           Deduplicator.Files.unzip_file(input_filename, compress?),
         :ok <-
           Deduplicator.Files.remove_file(input_filename, compress?),
         {:ok, %{bytes: bytes}} <-
           Deduplicator.Files.get_input_file(input_filename_unzip),
         :ok <-
           File.touch(output_filename),
         {:ok, output_file} <-
           File.open(output_filename, [:write]),
         :ok <-
           read_and_recovery_chunk(input_filename_unzip, output_file, bytes),
         :ok <- File.close(output_file) do
      {:ok, output_filename}
    else
      {:error, reason} ->
        Logger.error("Recovery file error #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_and_recovery_chunk(input_filename, output_file, bytes) do
    input_filename
    |> Deduplicator.Files.read_chunks(bytes)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      with {:ok, chunk} <- recovery_chunk(chunk),
           :ok          <- IO.binwrite(output_file, chunk) do
        {:cont, :ok}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @hash_size Deduplicator.BinaryUtils.hash_size()
  defp get_hash_str(chunk, nil),
       do: @chunk_identifier <> chunk
  defp get_hash_str(_chunk, %{line: line} = _hash_link),
       do: @hash_identifier <> String.pad_leading("#{line}", @hash_size, "0")
      
      
  def recovery_chunk(@chunk_identifier <> chunk) do
    {:ok, chunk}
  end
  def recovery_chunk(@hash_identifier <> line) do
    line =
      line
      |> String.trim_leading("0")
      |> String.to_integer()
    case Hash.get_hash_link_by_line(line, preload_file: true) do
      {:ok, %{file: %{filename: filename, bytes: bytes}, line: line}} ->
        get_chunk_from_file(filename, line, bytes)
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_chunk_from_file(filename, line, bytes) do
    case Deduplicator.Files.find_chunk(filename, line, bytes) do
      {:ok, @chunk_identifier <> chunk} -> {:ok, chunk}
      {:ok, @hash_identifier <> _hash} -> {:error, :not_chunk}
      {:error, reason} ->{:error, reason}
    end
  end

  defp generate_filename do
    now =
      :os.system_time(:millisecond)
      |> Integer.to_string

    :crypto.hash(:md5, now)
    |> Base.encode16()
  end
end
