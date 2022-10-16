# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :deduplicator,
  ecto_repos: [Deduplicator.Repo]

config :deduplicator, Deduplicator.Repo,
  username:  {:system, :string,  "DEDUPLICATOR_USERNAME", "DeduplicatorService"},
  password:  {:system, :string,  "DEDUPLICATOR_PASSWORD", "DeduplicatorPassword"},
  database:  {:system, :string,  "DEDUPLICATOR_DB_NAME",  "DeduplicatorTests"},
  hostname:  {:system, :string,  "DEDUPLICATOR_HOSTNAME", "localhost"},
  port:      {:system, :integer, "FDB_PORT",              5432},
  pool_size: {:system, :integer, "FDB_POOL_SIZE",         10}

config :deduplicator, chuck_size_bytes: 4

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

if Mix.env() == :test do
  import_config "#{Mix.env()}.exs"
end
