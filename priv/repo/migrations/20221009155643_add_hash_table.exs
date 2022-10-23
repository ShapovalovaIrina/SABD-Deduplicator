defmodule Deduplicator.Repo.Migrations.AddHashTable do
  use Ecto.Migration

  def change do
    create table("files") do
      add :filename,  :string, size: 64
      add :bytes,     :integer
      add :algorithm, :string, size: 8
    end

    create table("hash_links", primary_key: false) do
      add :hash,     :binary, primary_key: true
      add :file_id,  references("files")
      add :refs_num, :integer, default: 1
      add :line,     :integer
    end
  end
end
