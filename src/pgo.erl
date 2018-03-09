%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <t@crashfast.com>
%%%
%%% @doc Postgres driver and pool for Erlang.
%%%
%%% This module provides functions for interacting with a pool and
%%  querying the database.
%%% @end
%%% ---------------------------------------------------------------------------
-module(pgo).

-export([start_pool/2,
         query/1,
         query/2,
         query/3,
         transaction/1,
         transaction/2,
         checkout/1,
         checkin/2,
         break/1]).

-include("pgo.hrl").

-type error_field() :: severity | code | message | detail | hint | position | internal_position
                     | internal_query | where | file | line | routine
                     | schema | table | column | data_type | constraint | {unknown, byte()}.

-export_type([result/0,
              pool_ref/0,
              conn/0,
              error/0]).
-type conn() :: pgo_pool:conn().
-type pool_ref() :: pgo_pool:ref().
-type result() :: #pg_result{} | {error, error()}.

-type error() :: {pgo_error, #{error_field() => binary()}}.

-type pool() :: atom().

-type pool_opt() :: {size, integer()}
                  | {host, string()}
                  | {port, integer()}
                  | {user, string()}
                  | {password, string()}
                  | {database, string()}.
-type pool_config() :: [pool_opt()].

%% @doc Starts connection pool as a child of `pgo_sup`.
-spec start_pool(pool(), pool_config()) -> {ok, pid()}.
start_pool(Name, PoolConfig) ->
    pgo_sup:start_child(Name, PoolConfig).

%% @equiv query(default, Query).
-spec query(iodata()) -> result().
query(Query) ->
    query(default, Query).

%% @doc Executes a simple query either on a Pool or a provided connection.
-spec query(conn() | pool(), iodata()) -> result().
query(Conn={_, _, _}, Query) ->
    pgo_handler:simple_query(Conn, Query);
query(Pool, Query) when is_atom(Pool) ->
    {ok, Ref, Conn} = checkout(Pool),
    try
        pgo_handler:simple_query(Conn, Query)
    after
        checkin(Ref, Conn)
    end;
query(Query, Params) ->
    query(default, Query, Params).

%% @doc Executes an extended query either on a Pool or a provided connection.
-spec query(conn() | pool(), iodata(), list()) -> result().
query(Conn={_, _, _}, Query, Params) ->
    pgo_handler:extended_query(Conn, Query, Params);
query(Pool, Query, Params) ->
    {ok, Ref, Conn} = checkout(Pool),
    try
        pgo_handler:extended_query(Conn, Query, Params)
    after
        checkin(Ref, Conn)
    end.

%% @equiv transaction(default, Fun).
-spec transaction(fun((conn()) -> any())) -> any().
transaction(Fun) ->
    transaction(default, Fun).

%% @doc Runs a function, passing it a connection, in a SQL transaction.
-spec transaction(pool(), fun((conn()) -> any())) -> any().
transaction(Pool, Fun) ->
    case get(pgo_transaction_connection) of
        undefined ->
            {ok, Ref, Conn} = checkout(Pool),
            try
                #pg_result{command='begin'} = pgo_handler:simple_query(Conn, "BEGIN"),
                put(pgo_transaction_connection, Conn),
                Result = Fun(Conn),
                #pg_result{command='commit'} = pgo_handler:simple_query(Conn, "COMMIT"),
                Result
            catch
                _:_ ->
                    pgo_handler:simple_query(Conn, "ROLLBACK")
            after
                checkin(Ref, Conn),
                erase(pgo_transaction_connection)
            end;
        Conn ->
            %% already in a transaction
            Fun(Conn)
    end.

%% @doc Returns a connection from the pool.
-spec checkout(pool()) -> {ok, pool_ref(), conn()} | {drop, any()}.
checkout(Pool) ->
    pgo_pool:checkout(Pool, [{queue, false}]).

%% @doc Return a checked out connection to its pool
-spec checkin(pool_ref(), pool()) -> ok.
checkin(Ref, Conn) ->
    pgo_pool:checkin(Ref, Conn, []).

%% @doc Disconnects the socket held by this reference.
-spec break(reference()) -> ok.
break(Ref) ->
    pgo_connection:break(Ref).
