defmodule Deduplicator.Hash do
  @moduledoc """
  Hash related logic.
  """
  alias Deduplicator.Repo
  alias Deduplicator.Schemas.HashLinks

  def binary_hash(binary, type \\ :sha) when type in ~w(md5 sha)a do
    :crypto.hash(type, binary)
  end

  def hash_size(type) when type in ~w(md5 sha)a do
    :crypto.hash_info(type).size
  end

  def hash_link_exists?(hash) do
    HashLinks
    |> Repo.get(hash)
    |> wrap_search()
  end

  def inc_links(hash) do
    hash
    |> Ecto.Changeset.change(%{refs_num: hash.refs_num + 1})
    |> Repo.update()
  end

  def save_hash_link(hash, filename, line) do
#    opts = [
#      on_conflict: [inc: [refs_num: 1]],
#      conflict_target: :hash
#    ]

    %{
      hash: hash,
      filename: filename,
      line: line
    }
    |> HashLinks.create_changeset()
    |> Repo.insert()
  end

  def wrap_search(nil), do: {:error, :not_found}
  def wrap_search(res), do: {:ok, res}
end
