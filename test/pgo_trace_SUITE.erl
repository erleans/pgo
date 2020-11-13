-module(pgo_trace_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry/include/otel_span.hrl").

all() ->
    [trace_query, trace_with_parent_query, trace_transaction].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),

    {ok, _} = pgo_sup:start_child(default, #{pool_size => 1,
                                             port => 5432,
                                             database => "test",
                                             user => "test",
                                             trace => true}),

    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    ok.

init_per_testcase(_, Config) ->
    _ = application:load(opentelemetry),
    application:set_env(opentelemetry, processors, [{otel_batch_processor, #{scheduled_delay_ms => 1}}]),
    {ok, _} = application:ensure_all_started(opentelemetry),
    otel_batch_processor:set_exporter(otel_exporter_pid, self()),
    Config.

end_per_testcase(_, _Config) ->
    _ = application:stop(opentelemetry),
    ok.

trace_query(_Config) ->
    ?assertMatch(#{rows := [{empty}]},
                 pgo:query("select '[1,1)'::int4range", [], #{trace => true})),

    receive
        {span, #span{name=Name,
                     parent_span_id=Parent,
                     attributes=_Attributes,
                     events=_TimeEvents}} when Parent =:= undefined ->
            ?assertEqual(<<"pgo:query/3">>, Name)
    after
        5000 ->
            ct:fail(timeout)
    end,
    ok.

trace_with_parent_query(_Config) ->
    SpanCtx = ?start_span(<<"parent-of-query">>),
    ?set_current_span(SpanCtx),

    ?assertMatch(#{rows := [{empty}]},
                 pgo:query("select '[1,1)'::int4range", [], #{trace => true})),

    receive
        {span, #span{name=Name,
                     parent_span_id=Parent,
                     attributes=_Attributes,
                     events=_TimeEvents}} when Parent =/= undefined ->
            ?assertEqual(<<"pgo:query/3">>, Name)
    after
        5000 ->
            ct:fail(timeout)
    end,

    ?end_span(SpanCtx),

    receive
        {span, #span{name=ParentName,
                     parent_span_id=undefined}} ->
            ?assertEqual(<<"parent-of-query">>, ParentName)
    after
        5000 ->
            ct:fail(timeout)
    end,


    ok.

trace_transaction(_Config) ->
    pgo:transaction(fun() ->
                            pgo:query("select '[1,1)'::int4range")
                    end),

    receive
        {span, #span{name=Name,
                     parent_span_id=Parent,
                     attributes=_Attributes,
                     events=_TimeEvents}} when Parent =:= undefined ->
            ?assertEqual(<<"pgo:transaction/2">>, Name)
    after
        5000 ->
            ct:fail(timeout)
    end,

    ok.
