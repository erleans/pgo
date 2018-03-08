PGO
=====

PG...Oh god not nother Postgres client in Erlang...

### Why

* No message passing. Clients checkout the socket and use it directly.
* Binary protocol with input oids cached.
* Simple and direct. Tries to limit runtime options as much as possible.
* Mix apps currently too hard to use in a Rebar3 project. 

### Use

Pools defined in the `pgo` application's environment will be started on boot. You can also add pools dynamically with `pgo:start_pool/3`.

To try `pgo` simply modify `config/example.config` by replacing the `host`, `database`, `user` and `password` values for the database you wish to connect to:

```erlang
[
  {pgo, [{pools, [{default, [{size, 10},
                             {postgres, [{host, "127.0.0.1"},
                                         {database, "test"},
                                         {user, "test"}]}]}]}]}
].
```

`default` is the name of the pool, `size` is the number of connections to create for the pool.

Then start a shell with `rebar3`, it will boot the applications which will start the pool automatically:

```shell
$ rebar3 shell 

1> pgo:query("select 1").
#pg_result{command=select, rows=[{1}]}
```

### Running Tests

Pool functionality is tested with common test suites:

```
$ rebar3 ct
```

Postgres query functionality is tested with eunit, create user `test` and database `test`:

```
$ rebar3 eunit
```

### Acknowledgements

Much is owed to https://github.com/semiocast/pgsql (especially for protocol step logic) and https://github.com/epgsql/epgsql/ (especially for some decoding logic).

The pool implementation is owed to James Fish's found in `db_connection` [PR 108](https://github.com/elixir-ecto/db_connection/pull/108). While [db_connection](https://github.com/elixir-ecto/db_connection) and [postgrex](https://github.com/elixir-ecto/postgrex) as a whole were both used as inspiration as well.
