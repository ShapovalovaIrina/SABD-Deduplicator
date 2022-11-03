defmodule Deduplicator.Hash do
  @moduledoc """
  Hash related logic.
  """
  alias Deduplicator.Repo
  alias Deduplicator.Schemas.HashLink

  import Ecto.Query

  require Logger

  @algorithms  ~w(md5 sha)a
  def algorithms, do: @algorithms

  @default_algorithm :sha
  def default_algorithm, do: @default_algorithm

  def binary_hash(binary, type \\ @default_algorithm) when type in @algorithms do
    :crypto.hash(type, binary)
  end

  def hash_size(type) when type in @algorithms do
    :crypto.hash_info(type).size
  end

  def get_algorithm(algorithm) when algorithm in @algorithms, do: algorithm
  def get_algorithm(_), do: @default_algorithm

  def get_hash_link(hash, opts \\ []) do
    preload_file? = Keyword.get(opts, :preload_file, false)

    HashLink
    |> Repo.get(hash)
    |> preload_file(preload_file?)
    |> wrap_search()
  end

  def get_hash_link_by_line(line, opts \\ []) do
    preload_file? = Keyword.get(opts, :preload_file, false)

    HashLink
    |> Repo.get_by(line: line)
    |> preload_file(preload_file?)
    |> wrap_search()
  end

  defp preload_file(hash, true  = _preload?), do: Repo.preload(hash, :file)
  defp preload_file(hash, false = _preload?), do: hash

  def inc_links(hash) do
    hash
    |> Ecto.Changeset.change(%{refs_num: hash.refs_num + 1})
    |> Repo.update()
  end

  def save_all_hash_links(hashes) do
    hashes_count = Enum.frequencies_by(hashes, & &1.hash)

    uniq_hashes = Enum.uniq_by(hashes, & &1.hash)
    uniq_hashes_list = Enum.map(uniq_hashes, & &1.hash)

    stored_hash_links = get_hash_links(uniq_hashes_list)
    stored_hashes = Enum.map(stored_hash_links, & &1.hash)

    {list_to_update, list_to_insert} = Enum.split_with(uniq_hashes, & &1.hash in stored_hashes)

    :ok = insert_hash_links(list_to_insert, hashes_count)
    :ok = update_hash_links(stored_hash_links, hashes_count)

    hashes_to_insert = Enum.map(list_to_insert, & &1.hash)
    hashes_to_update = Enum.map(list_to_update, & &1.hash)
    inserted = get_hash_links(hashes_to_insert)
    updated = get_hash_links(hashes_to_update)

    {:ok, inserted, updated}
  end

  defp insert_hash_links(list, hashes_count) do
    amount = Enum.count(list)

    list
    |> Enum.map(fn h ->
      h
      |> Map.put(:refs_num, hashes_count[h.hash])
      |> Map.delete(:chunk)
    end)
    |> (&Repo.insert_all(HashLink, &1)).()
    |> case do
         {^amount, _} -> 
           :ok
         {saved, _} ->
           Logger.error("Expected to insert #{amount} hash links. Actually save #{saved}")
           {:error, :insert_error}
    end
  end

  defp update_hash_links(list, hashes_count) do
    list
    |> Enum.reduce_while(:ok, fn hash_link, _ ->
      refs_num = hash_link.refs_num + hashes_count[hash_link.hash]
      hash_link
      |> Ecto.Changeset.change(%{refs_num: refs_num})
      |> Repo.update()
      |> case do
           {:ok, _} -> {:cont, :ok}
           {:error, reason} -> {:halt, {:error, reason}}
         end
    end)
  end
  
  defp get_hash_links(hashes) do
    HashLink
    |> where([h], h.hash in ^hashes)
    |> Repo.all()
  end

  def get_duplicated_lines_for_file(file_id) do
    HashLink
    |> where([h], h.refs_num > 1 and h.file_id == ^file_id)
    |> select([h], h.line)
    |> Repo.all()
  end

  def wrap_search(nil), do: {:error, :not_found}
  def wrap_search(res), do: {:ok, res}
end
