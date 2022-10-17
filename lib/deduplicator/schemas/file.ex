defmodule Deduplicator.Schemas.File do
  use Ecto.Schema
  import Ecto.Changeset

  schema "files" do
    field :filename, :string
    field :bytes, :integer
    field :algorithm, :string
  end

  def create_changeset(filename, bytes, algorithm) do
    %__MODULE__{}
    |> change(%{filename: filename, bytes: bytes, algorithm: algorithm})
  end
end
