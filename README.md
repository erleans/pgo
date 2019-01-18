# PGO

[![CircleCI](https://circleci.com/gh/erleans/pgo.svg?style=svg)](https://circleci.com/gh/erleans/pgo)
[![codecov](https://codecov.io/gh/erleans/pgo/branch/master/graph/badge.svg)](https://codecov.io/gh/erleans/pgo)
[![Hex.pm](https://img.shields.io/hexpm/v//pgo.svg?maxAge=2592000)](https://hex.pm/packages/pgo)

PG...Oh god not nother Postgres client in Erlang...

## Why

* No message passing. Clients checkout the socket and use it directly.
* Binary protocol with input oids cached.
* Simple and direct. Tries to limit runtime options as much as possible.
* Instrumented with [Telemetry](https://github.com/beam-telemetry/telemetry) and [OpenCensus](https://github.com/census-instrumentation/opencensus-erlang)
* Mix apps currently too hard to use in a Rebar3 project. 

## Requirements

Erlang/OTP 20 and above.

## Use

Pools defined in the `pgo` application's environment will be started on boot. You can also add pools dynamically with `pgo:start_pool/3`.

To try `pgo` simply modify `config/example.config` by replacing the `host`, `database`, `user` and `password` values for the database you wish to connect to:

```erlang
[
  {pgo, [{pools, [{default, [{size, 10},
                             {host, "127.0.0.1"},
                             {database, "test"},
                             {user, "test"}]}]}]}
].
```

`default` is the name of the pool, `size` is the number of connections to create for the pool. Or you can start the pool through `pgo:start_pool/2`:

``` erlang
> pgo:start_pool(default, [{size, 5}, {host, "127.0.0.1"}, {database, "test"}, {user, "test"}]). 
```

Then start a shell with `rebar3 shell`, it will boot the applications which will start the pool automatically if it is configured through `sys.config`.

```erlang
> pgo:query("select 1").
#{command => select, num_rows => 1, rows => [{1}]}
> pgo:transaction(fun() ->
>     pgo:query("INSERT INTO my_table(name) VALUES('Name 1')"),
>     pgo:query("INSERT INTO my_table(name) VALUES('Name 2')")
> end).
#{command => insert,num_rows => 1,rows => []}
```

## Telemetry and Tracing

A [Telemetry](https://github.com/beam-telemetry/telemetry) event `[pgo, query]` can be attached to for receiving the time a query takes as well as other metadata for each query.

[OpenCensus](https://opencensus.io/) spans can be enabled for queries and transactions by either setting the `trace_default` to `true` for the pool:

``` erlang
> pgo:start_pool(default, [{trace_default, true}, {size, 5}, {host, "127.0.0.1"}, 
                           {database, "test"}, {user, "test"}]). 
```

Or by passing `#{trace => true}` in the options for a query or transaction:

```erlang
> pgo:query("select 1", [], #{trace => true}).
#{command => select, num_rows => 1, rows => [{1}]}
> pgo:transaction(fun() ->
>     pgo:query("INSERT INTO my_table(name) VALUES('Name 1')"),
>     pgo:query("INSERT INTO my_table(name) VALUES('Name 2')")
> end, #{trace => true}).
#{command => insert,num_rows => 1,rows => []}
```

Note that since this is optional the `opencensus` application is not included as a dependency of `pgo`. So it must be included as a `rebar3` dependency and runtime dependency (listed in your application's `.app.src` `applications` or the list of applications for `relx` to include in a release).

## Running Tests

Pool functionality is tested with common test suites:

```
$ rebar3 ct
```

Postgres query functionality is tested with eunit, create user `test` and database `test`:

```
$ rebar3 eunit
```

## Acknowledgements

Much is owed to https://github.com/semiocast/pgsql (especially for protocol step logic) and https://github.com/epgsql/epgsql/ (especially for some decoding logic).

The pool implementation is owed to James Fish's found in `db_connection` [PR 108](https://github.com/elixir-ecto/db_connection/pull/108). While [db_connection](https://github.com/elixir-ecto/db_connection) and [postgrex](https://github.com/elixir-ecto/postgrex) as a whole were both used as inspiration as well.
