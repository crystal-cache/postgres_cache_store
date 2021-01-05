require "cache"
require "pg"

module Cache
  # A cache store implementation which stores everything in the Postgres database
  struct PostgresStore(K, V) < Store(K, V)
    # Creates a new PostgresStore attached to the provided database.
    #
    # `table_name` and `expires_in` are required for your connection.
    def initialize(@expires_in : Time::Span, @pg : DB::Database, @table_name = "cache_entries")
      create_cache_table unless cache_table_exists?
    end

    private def write_impl(key : K, value : V, *, expires_in = @expires_in)
      @pg.exec(
        <<-SQL
          INSERT INTO #{@table_name} (key, value, expires_in, created_at)
          VALUES ('#{key}', '#{value}', '#{expires_in}', '#{Time.utc}')
          ON CONFLICT (key) DO UPDATE
          SET value = EXCLUDED.value::text
        SQL
      )
    end

    private def read_impl(key : K)
      rs = @pg.query_one?(
        "SELECT value, created_at, expires_in FROM #{@table_name} WHERE key = '#{key}'",
        as: {String, Time, PG::Interval}
      )

      return unless rs

      value, created_at, expires_in = rs

      expires_at = created_at + expires_in.to_span

      if expires_at <= Time.utc
        delete(key)

        return
      end

      value
    end

    def delete(key : K) : Bool
      @pg.exec("DELETE from #{@table_name} WHERE key = '#{key}'")
      true
    end

    def exists?(key : K) : Bool
      rs = @pg.query_one?(
        "SELECT created_at, expires_in FROM #{@table_name} WHERE key = '#{key}'",
        as: {Time, PG::Interval}
      )

      return false unless rs

      created_at, expires_in = rs

      expires_at = created_at + expires_in.to_span

      expires_at > Time.utc
    end

    def clear
      @pg.exec("TRUNCATE TABLE #{@table_name}")
    end

    # Preemptively iterates through all stored keys and removes the ones which have expired.
    def cleanup
      @pg.exec("DELETE FROM cache_entries WHERE created_at + expires_in < NOW()")
    end

    private def create_cache_table
      @pg.exec(
        <<-SQL
          CREATE UNLOGGED TABLE #{@table_name} (
            key text PRIMARY KEY,
            value text,
            expires_in interval NOT NULL,
            created_at timestamp NOT NULL
          )
        SQL
      )
    end

    private def cache_table_exists? : Bool
      @pg.query_one?("SELECT 1 FROM pg_class WHERE pg_class.relname = '#{@table_name}'", as: Int32) == 1
    end
  end
end
