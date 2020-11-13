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
         transaction/3,
         with_conn/2,
         checkout/1,
         checkout/2,
         checkin/2,
         break/1,
         format_error/1]).

-include("pgo_internal.hrl").

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

-type pool_option() :: queue | {queue, boolean()}.
-type options() :: #{pool => atom(),
                     trace => boolean(),
                     queue => boolean(),
                     decode_opts => [decode_option()]}.

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

%% @equiv query(Query, Params, #{})
-spec query(iodata(), list()) -> result().
query(Query, Params) ->
    query(Query, Params, #{}).

%% @doc Executes an extended query either on a Pool or a provided connection.
-spec query(iodata(), list(), options()) -> result().
query(Query, Params, Options) ->
    DecodeOptions = maps:get(decode_opts, Options, []),
    case get(pgo_transaction_connection) of
        undefined ->
            Pool = maps:get(pool, Options, default),
            PoolOptions = maps:get(pool_options, Options, []),
            case checkout(Pool, PoolOptions) of
                {ok, Ref, Conn=#conn{trace=TraceDefault,
                                     trace_attributes=TraceAttributes,
                                     decode_opts=DefaultDecodeOpts}} ->
                    DoTrace = maps:get(trace, Options, TraceDefault),
                    {SpanCtx, OriginalCtx} = maybe_start_span(DoTrace,
                                                              <<"pgo:query/3">>,
                                                              #{attributes => [{<<"statement">>, Query} |
                                                                               TraceAttributes]}),
                    try
                        pgo_handler:extended_query(Conn, Query, Params,
                                                   DecodeOptions ++ DefaultDecodeOpts,
                                                   #{queue_time => undefined})
                    after
                        maybe_end_span(DoTrace, SpanCtx, OriginalCtx),
                        checkin(Ref, Conn)
                    end;
                {error, _}=E ->
                    E
            end;
        Conn=#conn{pool=Pool,
                   decode_opts=DefaultDecodeOpts} ->
            %% verify we aren't trying to run a query against another pool from a transaction
            case maps:get(pool, Options, Pool) of
                P when P =:= Pool ->
                    pgo_handler:extended_query(Conn, Query, Params,
                                               DecodeOptions ++ DefaultDecodeOpts,
                                               #{queue_time => undefined});
                P ->
                    error({in_other_pool_transaction, P})
            end
    end.

%% @equiv transaction(default, Fun, [])
-spec transaction(fun(() -> any())) -> any() | {error, any()}.
transaction(Fun) ->
    transaction(default, Fun, #{}).

%% @equiv transaction(default, Fun, Options)
-spec transaction(fun(() -> any()), options()) -> any() | {error, any()}.
transaction(Fun, Options) when is_function(Fun) ->
    transaction(Fun, Options).

%% @doc Runs a function, passing it a connection, in a SQL transaction.
-spec transaction(pool(), fun(() -> any()), options()) -> any() | {error, any()}.
transaction(Pool, Fun, Options) ->
    case get(pgo_transaction_connection) of
        undefined ->
            PoolOptions = maps:get(pool_options, Options, []),
            case checkout(Pool, PoolOptions) of
                {ok, Ref, Conn=#conn{trace=TraceDefault}} ->
                    DoTrace = maps:get(trace, Options, TraceDefault),
                    {SpanCtx, OriginalCtx} = maybe_start_span(DoTrace,
                                                              <<"pgo:transaction/2">>,
                                                              #{}),
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
                        ?WITH_STACKTRACE(T, R, S)
                        pgo_handler:extended_query(Conn, "ROLLBACK", [], #{queue_time => undefined}),
                        erlang:raise(T, R, S)
                    after
                        maybe_end_span(DoTrace, SpanCtx, OriginalCtx),
                        checkin(Ref, Conn),
                        erase(pgo_transaction_connection)
                    end;
                {error, _}=E ->
                    E
            end;
        _Conn ->
            %% already in a transaction
            Fun()
    end.


maybe_start_span(false, _, _) ->
    {undefined, undefined};
maybe_start_span(true, Name, Opts) ->
    Tracer = opentelemetry:get_tracer(?MODULE),
    CurrentCtx = otel_ctx:get_current(),

    {SpanCtx, _} = otel_tracer:start_span(CurrentCtx, Tracer, Name, Opts),
    NewCtx = otel_tracer:set_current_span(SpanCtx),
    otel_ctx:attach(NewCtx),

    {SpanCtx, CurrentCtx}.

maybe_end_span(false, _, _) ->
    undefined;
maybe_end_span(true, SpanCtx, OriginalCtx) ->
    _ = otel_span:end_span(SpanCtx),
    otel_ctx:attach(OriginalCtx).

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

%% @doc Returns a connection from the pool.
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
