defmodule Deduplicator.Repo do
  use Ecto.Repo,
      otp_app: :deduplicator,
      adapter: Ecto.Adapters.Postgres
end