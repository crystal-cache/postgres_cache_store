require "cache"
require "pg"

module Cache
  # A cache store implementation which stores everything in the Postgres database
  struct PostgresCacheStore(K, V) < Store(K, V)
    # Creates a new PostgresCacheStore attached to the provided database.
    #
    # `table_name` and `expires_in` are required for your connection.
    def initialize(@expires_in : Time::Span, @pg : DB::Database, @table_name = "cache_entries")
      create_cache_table unless cache_table_exists?
    end

    private def write_impl(key : K, value : V, *, expires_in = @expires_in)
      sql = <<-SQL
        INSERT INTO #{@table_name} (key, value, expires_in, created_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value,
            expires_in = EXCLUDED.expires_in,
            created_at = EXCLUDED.created_at
      SQL

      @pg.exec(sql, key, value, expires_in, Time.utc)
    end

    private def read_impl(key : K)
      sql = "SELECT value, created_at, expires_in FROM #{@table_name} WHERE key = $1"

      rs = @pg.query_one?(sql, key, as: {String, Time, PG::Interval})

      return unless rs

      value, created_at, expires_in = rs

      expires_at = created_at + expires_in.to_span

      if expires_at <= Time.utc
        delete(key)

        return
      end

      value
    end

    private def delete_impl(key : K) : Bool
      sql = "DELETE from #{@table_name} WHERE key = $1"

      result = @pg.exec(sql, key)

      result.rows_affected.zero? ? false : true
    end

    private def exists_impl(key : K) : Bool
      sql = "SELECT created_at, expires_in FROM #{@table_name} WHERE key = $1"

      rs = @pg.query_one?(sql, key, as: {Time, PG::Interval})

      return false unless rs

      created_at, expires_in = rs

      expires_at = created_at + expires_in.to_span

      expires_at > Time.utc
    end

    def clear
      sql = "TRUNCATE TABLE #{@table_name}"

      @pg.exec(sql)
    end

    # Preemptively iterates through all stored keys and removes the ones which have expired.
    def cleanup
      sql = "DELETE FROM #{@table_name} WHERE created_at + expires_in < NOW()"

      @pg.exec(sql)
    end

    private def create_cache_table
      sql = <<-SQL
        CREATE UNLOGGED TABLE #{@table_name} (
          key text PRIMARY KEY,
          value text,
          expires_in interval NOT NULL,
          created_at timestamp NOT NULL
        )
      SQL

      @pg.exec(sql)
    end

    private def cache_table_exists? : Bool
      sql = "SELECT 1 FROM pg_class WHERE pg_class.relname = '#{@table_name}'"

      @pg.query_one?(sql, as: Int32) == 1
    end
  end
end
