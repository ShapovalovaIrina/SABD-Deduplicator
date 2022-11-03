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

  def save_hash_link(hash, filename, line) do
    %{
      hash: hash,
      filename: filename,
      line: line
    }
    |> HashLink.create_changeset()
    |> Repo.insert()
  end

  def insert_all_hash_links([]), do: :ok
  def insert_all_hash_links(links) do
    amount = length(links)

    HashLink
    |> Repo.insert_all(links)
    |> case do
         {^amount, _} ->
           :ok
         error ->
          Logger.error("insert all hash links error: #{inspect(error)}")
          :error
    end
  end

  def update_all_hash_links([]), do: :ok
  def update_all_hash_links(links) do
    links =
      links
      |> Enum.group_by(& &1)
      |> Enum.map(fn {k, v} -> {k, length(v)} end)
      |> Enum.into(%{})

    hash_list = Map.keys(links)

    HashLink
    |> where([h], h.hash in ^hash_list)
    |> Repo.all()
    |> Enum.reduce_while(:ok, fn hash_link, _ ->
      hash_link
      |> Ecto.Changeset.change(%{refs_num: hash_link.refs_num + links[hash_link.hash]})
      |> Repo.update()
      |> case do
           {:ok, _} -> {:cont, :ok}
           {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  def wrap_search(nil), do: {:error, :not_found}
  def wrap_search(res), do: {:ok, res}
end
