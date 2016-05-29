pgsql_pool
=====

Provider pooling of postgres connections.

Build
-----

    $ rebar3 compile

Use
----

Pools defined in the `pgsql_pool` application's environment will be started on boot. You can also add pools dynamically with `pp:start_pool/3`.

To try `pgsql_pools` simply modify `config/example.config` by replacing the `host`, `database`, `user` and `password` values for the database you wish to connect to:

```erlang
[
  {pgsql_pool, [{pools, [{pool_1, [{size, 10},
                                   {workers, 2},
                                   {postgres, [{host, "127.0.0.1"},
                                               {database, "pgsql_pool"},
                                               {user, "pgsql_pool"},
                                               {password, "pgsql_pool"}]}]}]}]}
].
```

`pool_1` is the name of the pool, `size` is the number of connections to create for the pool and `workers` is the number of processes handling creating and cleaning up the connections in the broker. 2 `workers` with a pool of size 10 means each worker is responsible for 5 postgres connections.

Then start a shell with `rebar3`, it will boot the applications which will start the pool automatically:

```shell
$ rebar3 shell --config config/example.config

===> Booted sbroker
===> Booted pgsql
===> Booted pgsql_pool

1> {ok, C, Ref} = pp:checkout(pool_1).
{ok,{pgsql_connection,<0.140.0>},{<0.117.0>,#Ref<0.0.1.658>}}
2> pgsql_connection:simple_query("SELECT 1", C).
{{select,1},[{1}]}
3> pp:checkin(Ref).
ok
```
