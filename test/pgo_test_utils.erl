-module(pgo_test_utils).

-export([clear_types/1]).

clear_types(Pool) ->
    [persistent_term:erase(K) || {K={pg_types, P, _}, _} <- persistent_term:get(), P =:= Pool].
