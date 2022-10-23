defmodule Deduplicator do
  @moduledoc """
  Deduplication logic.
  """

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
    chunk_amount = Keyword.get(opts, :chunk_amount, 100)
    bytes = Keyword.get(opts, :bytes, Application.get_env(:deduplicator, :chuck_size_bytes))
    algorithm = opts |> Keyword.get(:hash_algorimth) |> Hash.get_algorithm()

    # TODO: handle errors
    output_filename = output_filepath <> "/" <> generate_filename()
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

    input_filename
    |> Deduplicator.Files.read(bytes)
    |> Enum.reduce_while(state, &handle_chunks/2)
    |> save_hash_list_from_state()
    |> case do
         {:ok, _} ->
           :ok = File.close(output_file)
           {:ok, output_filename}

         {:error, reason} ->
           Logger.error("Deduplicate file error #{inspect(reason)}")
           :ok = File.close(output_file)
           {:error, reason}
    end
  end

  defp handle_chunks(chunk, %{
    insert_list: insert_list,
    update_list: update_list,
    chunk_amount: chunk_amount
  } = state) when length(insert_list) + length(update_list) < chunk_amount do
    {:cont, add_chuck_to_state(chunk, state)}
  end
  defp handle_chunks(chunk, %{line: line} = state) do
    IO.puts("line #{line}, time #{inspect(:erlang.localtime())}")
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
  def recovery_file(input_filename, output_filename) do
    :ok = File.touch(output_filename)
    {:ok, output_file} = File.open(output_filename, [:write])
    {:ok, %{bytes: bytes, algorithm: algorithm}} = Deduplicator.Files.get_input_file(input_filename)
    
    input_filename
    |> Deduplicator.Files.read_chunks(bytes, String.to_existing_atom(algorithm))
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      with {:ok, chunk} <- recovery_chunk(chunk),
           :ok          <- IO.binwrite(output_file, chunk) do
        {:cont, :ok}
      else
        {:error, reason} -> {:halt, {:error, reason}}
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

  # 0 and 1 to indicate chunk and hash
  @chunk_identifier "0"
  @hash_identifier "1"
  defp get_hash_str(chunk, nil),
       do: @chunk_identifier <> chunk
  defp get_hash_str(_chunk, %{hash: hash} = _hash_link),
      do: @hash_identifier <> hash
      
      
  def recovery_chunk(@chunk_identifier <> chunk) do
    {:ok, chunk}
  end
  def recovery_chunk(@hash_identifier <> hash) do
    case Hash.get_hash_link(hash, preload_file: true) do
      {:ok, %{file: %{filename: filename, bytes: bytes, algorithm: algorithm}, line: line}} ->
        get_chunk_from_file(filename, line, bytes, String.to_existing_atom(algorithm))
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp get_chunk_from_file(filename, line, bytes, algorithm) do
    case Deduplicator.Files.find_chunk(filename, line, bytes, algorithm) do
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
