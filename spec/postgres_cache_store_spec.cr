require "./spec_helper"

describe Cache::PostgresCacheStore do
  after_each do
    pg.exec("DROP TABLE IF EXISTS #{table_name}")
    pg.exec("DROP TABLE IF EXISTS #{custom_table_name}")
  end

  it "initialize" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    store.should be_a(Cache::Store(String))
  end

  it "creates created_at as timestamp with time zone" do
    Cache::PostgresCacheStore(String).new(12.hours, pg)

    column_type = pg.query_one(
      "SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = 'created_at'",
      table_name,
      as: String
    )

    column_type.should eq("timestamp with time zone")
  end

  it "initializes with a custom table name" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg, custom_table_name)

    store.write("foo", "bar")

    store.read("foo").should eq("bar")
  end

  it "initializes with a schema-qualified table name" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg, "public.#{custom_table_name}")

    store.write("foo", "bar")

    store.read("foo").should eq("bar")
  end

  it "rejects unsafe table names" do
    unsafe_table_names = [
      "",
      "1cache_entries",
      "cache-entries",
      "public.cache.entries",
      "cache_entries; DROP TABLE cache_entries",
    ]

    unsafe_table_names.each do |unsafe_table_name|
      expect_raises(ArgumentError) do
        Cache::PostgresCacheStore(String).new(12.hours, pg, unsafe_table_name)
      end
    end
  end

  it "write to cache first time" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    value = store.fetch("foo") { "bar" }
    value.should eq("bar")
  end

  it "fetch from cache" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    value = store.fetch("foo") { "bar" }
    value.should eq("bar")

    value = store.fetch("foo") { "baz" }
    value.should eq("bar")
  end

  it "don't fetch from cache if expired" do
    store = Cache::PostgresCacheStore(String).new(1.seconds, pg)

    value = store.fetch("foo") { "bar" }
    value.should eq("bar")

    sleep 2.seconds

    value = store.fetch("foo") { "baz" }
    value.should eq("baz")
  end

  it "fetch with expires_in from cache" do
    store = Cache::PostgresCacheStore(String).new(1.seconds, pg)

    value = store.fetch("foo", expires_in: 1.hours) { "bar" }
    value.should eq("bar")

    sleep 2.seconds

    value = store.fetch("foo") { "baz" }
    value.should eq("bar")
  end

  it "don't fetch with expires_in from cache if expires" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    value = store.fetch("foo", expires_in: 1.seconds) { "bar" }
    value.should eq("bar")

    sleep 2.seconds

    value = store.fetch("foo") { "baz" }
    value.should eq("baz")
  end

  it "write" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)
    store.write("foo", "bar", expires_in: 1.minute)

    value = store.fetch("foo") { "bar" }
    value.should eq("bar")
  end

  it "rewrite value" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)
    store.write("foo", "bar", expires_in: 1.minute)
    store.write("foo", "baz", expires_in: 1.minute)

    value = store.read("foo")

    value.should eq("baz")
  end

  it "read" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)
    store.write("foo", "bar")

    value = store.read("foo")
    value.should eq("bar")
  end

  it "set a custom expires_in value for entry on write" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)
    store.write("foo", "bar", expires_in: 1.second)

    sleep 2.seconds

    value = store.read("foo")
    value.should be_nil
  end

  it "delete from cache" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    value = store.fetch("foo") { "bar" }
    value.should eq("bar")

    result = store.delete("foo")
    result.should be_true

    value = store.read("foo")
    value.should be_nil
    store.keys.should eq(Set(String).new)
  end

  it "deletes all items from the cache" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    value = store.fetch("foo") { "bar" }
    value.should eq("bar")

    store.clear

    value = store.read("foo")
    value.should be_nil
    store.keys.should be_empty
  end

  it "#exists?" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    store.write("foo", "bar")

    store.exists?("foo").should be_true
    store.exists?("foz").should be_false
  end

  it "#exists? expires" do
    store = Cache::PostgresCacheStore(String).new(1.second, pg)

    store.write("foo", "bar")

    sleep 2.seconds

    store.exists?("foo").should be_false
  end

  it "#cleanup deletes expired entries" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    pg.exec(
      "INSERT INTO #{table_name} (key, value, expires_in, created_at) VALUES ($1, $2, $3, NOW() - INTERVAL '2 seconds')",
      "expired",
      "bar",
      1.second
    )

    store.cleanup

    store.read("expired").should be_nil
  end

  it "#cleanup keeps fresh entries" do
    store = Cache::PostgresCacheStore(String).new(12.hours, pg)

    pg.exec(
      "INSERT INTO #{table_name} (key, value, expires_in, created_at) VALUES ($1, $2, $3, NOW())",
      "fresh",
      "bar",
      1.hour
    )

    store.cleanup

    store.read("fresh").should eq("bar")
  end

  context "SQL Injection" do
    it "read" do
      store = Cache::PostgresCacheStore(String).new(12.hours, pg)
      store.write("foo", "bar")

      value = store.read("'foz' OR 1=1")
      value.should be_nil
    end

    it "#exists?" do
      store = Cache::PostgresCacheStore(String).new(12.hours, pg)

      store.write("foo", "bar")

      store.exists?("'foz' OR 1=1").should be_false
    end

    it "delete from cache" do
      store = Cache::PostgresCacheStore(String).new(12.hours, pg)

      value = store.fetch("foo") { "bar" }
      value.should eq("bar")

      result = store.delete("'foz' OR 1=1")
      result.should be_false

      value = store.read("foo")
      value.should eq("bar")
    end
  end
end

def pg
  postgres_user = ENV["POSTGRES_USER"]? || "postgres"
  postgres_password = ENV["POSTGRES_PASSWORD"]? || ""
  postgres_host = ENV["POSTGRES_HOST"]? || "localhost"
  postgres_db = ENV["POSTGRES_DB"]? || "postgres"

  DB.open("postgres://#{postgres_user}:#{postgres_password}@#{postgres_host}/#{postgres_db}")
end

def table_name
  "cache_entries"
end

def custom_table_name
  "custom_cache_entries"
end
