# Cache::PostgresCacheStore

![Crystal CI](https://github.com/crystal-cache/postgres_cache_store/workflows/Crystal%20CI/badge.svg)
[![GitHub release](https://img.shields.io/github/release/crystal-cache/postgres_cache_store.svg)](https://github.com/crystal-cache/postgres_cache_store/releases)

A [cache](https://github.com/crystal-cache/cache) store implementation which stores everything in the Postgres database,
using [crystal-pg](https://github.com/will/crystal-pg) as the backend.

`Cache::PostgresCacheStore` is a Crystal cache backed by a Postgres UNLOGGED table and text column.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     postgres_cache_store:
       github: crystal-cache/postgres_cache_store
   ```

2. Run `shards install`

## Usage

Before using this shard make sure you have created Postgres database. For example `cache_production`:

```console
psql -c 'CREATE DATABASE cache_production;' -U postgres
```

A Postgres database can be opened with:

```crystal
db = DB.open("postgres://postgres@localhost/cache_production")
```

Open and use the new cache instance:

```crystal
require "postgres_cache_store"

cache = Cache::PostgresCacheStore(String).new(1.minute, db)

cache.write("foo", "bar")

cache.read("foo") # => "bar"
```

## Contributing

1. Fork it (<https://github.com/crystal-cache/postgres_cache_store/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Anton Maminov](https://github.com/mamantoha) - creator and maintainer
