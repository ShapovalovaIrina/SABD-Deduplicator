defmodule Deduplicator.Schemas.HashLinks do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  schema "hash_links" do
    field :hash,     :binary, primary_key: true
    field :filename, :string
    field :refs_num, :integer, default: 1
    field :line,     :integer
  end

  def create_changeset(attrs) do
    %__MODULE__{
      refs_num: 1
    }
    |> cast(attrs, ~w(hash filename line)a)
    |> unique_constraint(:hash)
  end
end
