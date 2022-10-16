defmodule Deduplicator.Repo.Migrations.AddHashTable do
  use Ecto.Migration

  def change do
    create table("hash_links", primary_key: false) do
      add :hash,     :binary, primary_key: true
      add :filename, :string, size: 64
      add :refs_num, :integer, default: 1
      add :line,     :integer
    end
  end
end
