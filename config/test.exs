import Config

# Configure your database
config :deduplicator, Deduplicator.Repo,
  username: "postgres",
  password: "postgres",
  database: "deduplicatortests",
  hostname: "localhost",
  port: 5432,
  pool_size: 10,
  ownership_timeout: 5 * 60 * 1_000,
  pool: Ecto.Adapters.SQL.Sandbox

config :logger, :console, level: :info

config :deduplicator, chuck_size_bytes: 64
