-module(pgo_connection_test).

-include_lib("eunit/include/eunit.hrl").

%% tcp_closed/ssl_closed are informational only -- the real disconnect
%% handling happens via the linked socket's `EXIT` message. Before this fix,
%% `tcp_closed` had no matching clause and crashed the connection process
%% with `function_clause` (only `ssl_closed` was handled), which the
%% supervisor then restarted. Both must be ignored the same way.
tcp_closed_is_ignored_test() ->
    ?assertEqual(keep_state_and_data, pgo_connection:handle_event(info, {tcp_closed, some_port}, undefined)).

ssl_closed_is_ignored_test() ->
    ?assertEqual(keep_state_and_data, pgo_connection:handle_event(info, {ssl_closed, some_port}, undefined)).
