-module(pgo_connection_starter).

-export([start_link/2]).

-export([init/1,
         callback_mode/0,
         connect/3]).

-record(data, {sup  :: pid(),
               pool :: atom(),
               size :: integer()}).

start_link(Pool, Size) ->
    gen_statem:start_link(?MODULE, [self(), Pool, Size], []).

init([Sup, Pool, Size]) ->
    {ok, connect, #data{sup=Sup, pool=Pool, size=Size},
     {next_event, internal, start_connections}}.

callback_mode() ->
    state_functions.

connect(internal, start_connections, #data{sup=Sup, size=Size}) ->
    ConnSup = pgo_pool_sup:whereis_child(Sup, connection_sup),
    [{ok, _} = pgo_connection_sup:start_child(ConnSup) || _ <- lists:seq(1, Size)],

    TypeServer = pgo_pool_sup:whereis_child(Sup, type_server),
    pgo_type_server:reload(TypeServer),
    keep_state_and_data.
