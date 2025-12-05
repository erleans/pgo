-module(pgo).
-author("Tristan Sloughter <tristan@sloughter.dev>").
-moduledoc """
Postgres driver and pool for Erlang.
This module provides functions for interacting with a pool and
 querying the database.
""".

-export([start_pool/2,
         query/1,
         query/2,
         query/3,
         query/4,
         transaction/1,
         transaction/2,
         transaction/3,
         with_conn/2,
         checkout/1,
         checkout/2,
         checkin/2,
         break/1,
         format_error/1]).

-include("pgo_internal.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

-export_type([result/0,
              error/0,
              pool_config/0,
              decode_fun/0]).

-type result() :: #{command := atom(),
                    num_rows := integer() | table,
                    rows := list()} | {error, error()} | {error, any()}.

-type error() :: {pgo_error, #{error_field() => binary()}} | pg_types:encoding_error().

-type pool() :: atom().

-type row() :: list() | map().
-type fields() :: [#row_description_field{}].
-type decode_fun() :: fun((row(), fields()) -> row()) | undefined.

-type decode_option() :: return_rows_as_maps | {return_rows_as_maps, boolean()} |
                         column_name_as_atom | {column_name_as_atom, boolean()} |
                         {decode_fun, decode_fun()}.

-type pool_option() :: queue | {queue, boolean()} |
                       {timeout, timeout()} | {deadline, integer()}.
-type options() :: #{pool => atom(),
                     trace => boolean(),
                     include_statement_span_attribute => boolean(),
                     queue => boolean(),
                     decode_opts => [decode_option()],
                     pool_options => [pool_option()]}.

-type pool_config() :: #{host => string(),
                         port => integer(),
                         user => string(),
                         password => string(),
                         database => string(),

                         %% pool specific settings
                         pool_size => integer(),
                         queue_target => integer(),
                         queue_interval => integer(),
                         idle_interval => integer(),

                         %% gen_tcp socket options
                         socket_options => [gen_tcp:connect_option()],

                         %% defaults for options used at query time
                         queue => boolean(),
                         trace => boolean(),
                         decode_opts => [decode_option()]} |
                       list({atom(), string() | integer() | boolean()}).

%% @doc Starts connection pool as a child of pgo_sup.
-spec start_pool(pool(), pool_config()) -> {ok, pid()}.
start_pool(Name, PoolConfig) ->
    pgo_sup:start_child(Name, PoolConfig).

%% @equiv query(Query, [], #{})
-spec query(iodata()) -> result().
query(Query) ->
    query(Query, [], #{}).

-doc #{equiv => query(Query, Params, #{})}.
-spec query(iodata(), list()) -> result().
query(Query, Params) ->
    query(Query, Params, #{}).

%% @doc Executes an extended query either on a Pool or a provided connection.
-spec query(iodata(), list(), options()) -> result().
query(Query, Params, Options) ->
    case get(pgo_transaction_connection) of
        undefined ->
            Pool = maps:get(pool, Options, default),
            PoolOptions = maps:get(pool_options, Options, []),
            case checkout(Pool, PoolOptions) of
                {ok, Ref, Conn} ->
                    try
                        query(Query, Params, Options, Conn)
                    after
                        checkin(Ref, Conn)
                    end;
                {error, _}=E ->
                    E
            end;
        Conn=#conn{pool=Pool} ->
            %% verify we aren't trying to run a query against another pool from a transaction
            case maps:get(pool, Options, Pool) of
                P when P =:= Pool ->
                    query(Query, Params, Options, Conn);
                P ->
                    error({in_other_pool_transaction, P})
            end
    end.

query(Query, Params, Options, Conn=#conn{trace=TraceDefault,
                                         trace_attributes=TraceAttributes,
                                         include_statement_span_attribute=IncludeStatementDefault,
                                         decode_opts=DefaultDecodeOpts}) ->
    DecodeOptions = maps:get(decode_opts, Options, []),
    DoTrace = maps:get(trace, Options, TraceDefault),
    IncludeStatement = maps:get(include_statement_span_attribute,
                                Options,
                                IncludeStatementDefault),

    %% if the SDK (`opentelemetry' application) isn't running then `with_span` is a no-op.
    %% if the SDK is running then `is_recording' is used so the user can disable the span individually.
    ?with_span(<<"pgo:query/3">>, #{is_recording => if DoTrace -> true; true -> false end,
                                    attributes => [{<<"db.statement">>, iolist_to_binary(Query)}
                                                   || IncludeStatement] ++ TraceAttributes},
               fun(_) ->
                       pgo_handler:extended_query(Conn, Query, Params,
                                                  DecodeOptions ++ DefaultDecodeOpts,
                                                  #{queue_time => undefined})
               end).

%% @equiv transaction(default, Fun, [])
-spec transaction(fun(() -> any())) -> any() | {error, any()}.
transaction(Fun) ->
    transaction(default, Fun, #{}).

%% @equiv transaction(default, Fun, Options)
-spec transaction(fun(() -> any()), options()) -> any() | {error, any()}.
transaction(Fun, Options) when is_function(Fun) ->
    Pool = maps:get(pool, Options, default),
    transaction(Pool, Fun, Options).

%% @doc Runs a function, passing it a connection, in a SQL transaction.
-spec transaction(pool(), fun(() -> any()), options()) -> any() | {error, any()}.
transaction(Pool, Fun, Options) ->
    case get(pgo_transaction_connection) of
        undefined ->
            new_transaction(Pool, Fun, Options);
        _Conn ->
            %% already in a transaction
            Fun()
    end.

new_transaction(Pool, Fun, Options) ->
    PoolOptions = maps:get(pool_options, Options, []),
    case checkout(Pool, PoolOptions) of
        {ok, Ref, Conn=#conn{trace=TraceDefault,
                             trace_attributes=TraceAttributes}} ->
            DoTrace = maps:get(trace, Options, TraceDefault),
            ?with_span(<<"pgo:transaction/2">>,
                       #{is_recording => if DoTrace -> true; true -> false end,
                         attributes => TraceAttributes},
                       fun(_) ->
                               try
                                   #{command := 'begin'} = pgo_handler:extended_query(Conn, "BEGIN", [],
                                                                                      #{queue_time => undefined}),
                                   put(pgo_transaction_connection, Conn),
                                   Result = Fun(),
                                   case pgo_handler:extended_query(Conn, "COMMIT", [],
                                                                   #{queue_time => undefined}) of
                                       #{command := commit} -> Result;
                                       #{command := rollback} -> Result
                                   end
                               catch
                                   Type:Reason:Stacktrace ->
                                       pgo_handler:extended_query(Conn, "ROLLBACK", [], #{queue_time => undefined}),
                                       erlang:raise(Type, Reason, Stacktrace)
                               after
                                   checkin(Ref, Conn),
                                   erase(pgo_transaction_connection)
                               end
                       end);
        {error, _}=E ->
            E
    end.

with_conn(Conn, Fun) ->
    case get(pgo_transaction_connection) of
        undefined ->
            put(pgo_transaction_connection, Conn),
            try Fun()
            after
                erase(pgo_transaction_connection)
            end;
        OldConn ->
            try Fun()
            after
                put(pgo_transaction_connection, OldConn)
            end
    end.

-doc """
Returns a connection from the pool.
""".
-spec checkout(atom()) -> {ok, pgo_pool:pool_ref(), pgo_pool:conn()} | {error, any()}.
checkout(Pool) ->
    pgo_pool:checkout(Pool, []).

-spec checkout(atom(), [pool_option()]) -> {ok, pgo_pool:pool_ref(), pgo_pool:conn()} | {error, any()}.
checkout(Pool, Options) ->
    pgo_pool:checkout(Pool, Options).

%% @doc Return a checked out connection to its pool
-spec checkin(pgo_pool:pool_ref(), pgo_pool:conn()) -> ok.
checkin(Ref, Conn) ->
    pgo_pool:checkin(Ref, Conn, []).

%% @doc Disconnects the socket held by this reference.
-spec break(pgo_pool:conn()) -> ok.
break(Conn) ->
    pgo_connection:break(Conn).

format_error(Error=#{module := Module}) ->
    Module:format_error(Error);
format_error(Error) ->
    io_lib:format("Unknown error: ~p", [Error]).
