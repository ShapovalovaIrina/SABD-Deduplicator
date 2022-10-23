defmodule Deduplicator.Schemas.HashLink do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  schema "hash_links" do
    field :hash,     :binary, primary_key: true
    field :refs_num, :integer, default: 1
    field :line,     :integer

    belongs_to :file, Deduplicator.Schemas.File
  end

  def create_changeset(attrs) do
    %__MODULE__{refs_num: 1}
    |> cast(attrs, ~w(hash file_id line)a)
    |> foreign_key_constraint(:file_id)
    |> unique_constraint(:hash)
  end
end
