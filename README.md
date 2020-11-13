# PGO

[![CircleCI](https://circleci.com/gh/erleans/pgo.svg?style=svg)](https://circleci.com/gh/erleans/pgo)
[![codecov](https://codecov.io/gh/erleans/pgo/branch/master/graph/badge.svg)](https://codecov.io/gh/erleans/pgo)
[![Hex.pm](https://img.shields.io/hexpm/v/pgo.svg?style=flat)](https://hex.pm/packages/pgo)

PG...Oh god not nother Postgres client in Erlang...

## Why

* No message passing. Clients checkout the socket and use it directly.
* Binary protocol with input oids cached.
* Simple and direct. Tries to limit runtime options as much as possible.
* Instrumented with [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-erlang) and [Telemetry](https://github.com/beam-telemetry/telemetry)
* Mix apps currently too hard to use in a Rebar3 project. 

## Requirements

Erlang/OTP 21.3 and above.

## Use

Pools defined in the `pgo` application's environment will be started on boot. You can also add pools dynamically with `pgo:start_pool/3`.

To try `pgo` simply modify `config/example.config` by replacing the `host`, `database`, `user` and `password` values for the database you wish to connect to:

```erlang
[
  {pgo, [{pools, [{default, #{pool_size => 10,
                              host => "127.0.0.1",
                              database => "test",
                              user => "test"}}]}]}
].
```

`default` is the name of the pool, `size` is the number of connections to create for the pool. Or you can start the pool through `pgo:start_pool/2` which creates it as a child of `pgo`'s simple one for one:

``` erlang
> pgo:start_pool(default, #{pool_size => 5, host => "127.0.0.1", database => "test", user => "test"}). 
```

Or start a pool as a child of your application's supervisor:

``` erlang
ChildSpec = #{id => pgo_pool,
              start => {pgo_pool, start_link, [Name, PoolConfig]},
              shutdown => 1000},
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

## Options

Pool configuration includes the Postgres connection information, pool configuration like size and defaults for options used at query time. 

``` erlang
#{host => string(),
  port => integer(),
  user => string(),
  password => string(),
  database => string(),

  %% pool specific settings
  pool_size => integer(),
  queue_target => integer(),
  queue_interval => integer(),
  idle_interval => integer(),

  %% defaults for options used at query time
  queue => boolean(),
  trace => boolean(),
  decode_opts => [decode_option()]}
```

The query time options can also be set through options passed to `pgo:query/3`:

``` erlang
decode_fun() :: fun((row(), fields()) -> row()) | undefined.

decode_option() :: return_rows_as_maps | {return_rows_as_maps, boolean()} |
                   column_name_as_atom | {column_name_as_atom, boolean()} |
                   {decode_fun, decode_fun()}.
                         
#{pool => atom(),
  trace => boolean(),
  queue => boolean(),
  decode_opts => [decode_option()]}
```

### Query Options

* `pool` (default: `default`): Name of the pool to use for checking out a connection to the database.
* `return_rows_as_maps` (default: `false`): When `true` each row is returned as a map of column name to value instead of a list of values.
* `column_name_as_atom` (default: `false`): If `true` converts each column name in the result to an atom.
* `decode_fun` (default: `undefined`): Optional function for performing transformations on each row in a result. It must be a 2-arity function returning a list or map for the row and takes the row (as a list or map) and a list of `#row_description_field{}` records.
* `queue` (default: `true`): Whether to wait for a connection from the pool if none are available.
* `trace` (default: `false`): `pgo` is instrumented with [OpenTelemetry](https://opentelemetry.io/) and when this option is `true` a span will be created (if sampled).

### Database Settings

* `host` (default: `127.0.0.1`): Database server hostname.
* `port` (default: 5432): Port the server is listening on.
* `user`: Username to connect to database as.
* `password`: Password for the user.
* `database`: Name of database to use.
* `ssl` (default: `false`): Whether to use SSL or not.
* `ssl_options`: List of SSL options to use if `ssl` is `true`. See the [Erlang SSL connect](http://erlang.org/doc/man/ssl.html#connect-2) options.
* `connection_parameters` (default: `[]`): List of 2-tuples, where key and value must be binary strings. You can include any Postgres connection parameter here, such as `{<<"application_name">>, <<"myappname">>}` and `{<<"timezone">>, <<"GMT">>}`.

### Pool Settings

* `pool_size` (default: 1): Number of connections to keep open with the database
* `queue_target` (default: 50) and `queue_interval` (default: 1000): Checking out connections is handled through a queue. If it takes longer than `queue_target` to get out of the queue for longer than `queue_interval` then the `queue_target` will be doubled and checkouts will start to be dropped if that target is surpassed.
* `idle_interval` (default: 1000): The database is pinged every `idle_interval` when the connection is idle.

## Telemetry and Tracing

A [Telemetry](https://github.com/beam-telemetry/telemetry) event `[pgo, query]` can be attached to for receiving the time a query takes as well as other metadata for each query.

[OpenTelemetry](https://opentelemetry.io/spans) can be enabled for queries and transactions by either setting the `trace_default` to `true` for the pool:

``` erlang
> pgo:start_pool(default, #{host => "127.0.0.1", 
                            database => "test", 
                            user => "test",
                            pool_size => 5,
                            trace => true}]). 
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

Note that since this is optional the `opentelemetry` application is not included as a dependency of `pgo`. So it must be included as a `rebar3` dependency and runtime dependency (listed in your application's `.app.src` `applications` or the list of applications for `relx` to include in a release).

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
