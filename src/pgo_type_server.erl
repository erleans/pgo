-module(pgo_type_server).

-export([start_link/2,
         reload/1,
         reload_cast/1]).

-export([init/1,
         callback_mode/0,
         ready/3]).

-include("pgo.hrl").

-record(data, {pool        :: atom(),
               db_options  :: list(),
               last_reload :: integer()}).

start_link(Pool, DBOptions) ->
    gen_statem:start_link(?MODULE, [Pool, DBOptions], []).

reload(Pid) ->
    gen_statem:call(Pid, {reload, erlang:monotonic_time()}).

reload_cast(Pid) ->
    gen_statem:cast(Pid, {reload, erlang:monotonic_time()}).

init([Pool, DBOptions]) ->
    ets:new(Pool, [named_table, protected, {read_concurrency, true}]),
    {ok, ready, #data{pool=Pool, db_options=DBOptions},
     {next_event, internal, load}}.

callback_mode() ->
    state_functions.

ready(internal, load, Data=#data{pool=Pool,
                                 db_options=DBOptions}) ->
    load(Pool, -1, 0, DBOptions),
    {keep_state, Data#data{last_reload=erlang:monotonic_time()}};
ready({call, From}, {reload, RequestTime}, Data=#data{pool=Pool,
                                                      db_options=DBOptions,
                                                      last_reload=LastReload}) ->
    load(Pool, LastReload, RequestTime, DBOptions),
    {keep_state, Data#data{last_reload=erlang:monotonic_time()}, [{reply, From, ok}]};
ready(cast, {reload, RequestTime}, Data=#data{pool=Pool,
                                              db_options=DBOptions,
                                              last_reload=LastReload}) ->
    load(Pool, LastReload, RequestTime, DBOptions),
    {keep_state, Data#data{last_reload=erlang:monotonic_time()}}.


load(Pool, LastReload, RequestTime, DBOptions) when LastReload < RequestTime ->
    {ok, Socket} = pgo_handler:pgsql_open(DBOptions),
    #pg_result{rows=Oids} = pgo_handler:simple_query({self(),Socket}, Pool, "SELECT oid, typname FROM pg_type"),
    unlink(Socket),
    pgo_handler:close(Socket),
    [ets:insert(Pool, {Oid, binary_to_atom(Typename, utf8)}) || {Oid, Typename} <- Oids];
load(_, _, _, _) ->
    ok.
