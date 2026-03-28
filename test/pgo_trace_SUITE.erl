-module(pgo_trace_SUITE).

-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry/include/otel_span.hrl").

all() ->
    [trace_query, trace_with_parent_query, trace_transaction, no_statement].

init_per_suite(Config) ->
    application:ensure_all_started(pgo),
    application:ensure_all_started(tls_certificate_check),
    {ok, _} = pgo_sup:start_child(pgo_default, #{pool_size => 1,
                                                 port => 5432,
                                                 database => "test",
                                                 user => "test",
                                                 password => "password",
                                                 trace => true}),
    _ = application:load(opentelemetry),
    application:set_env(opentelemetry, processors, [{otel_simple_processor, #{}}]),
    {ok, _} = application:ensure_all_started(opentelemetry),
    Config.

end_per_suite(_Config) ->
    application:stop(pgo),
    application:stop(opentelemetry),
    application:unload(opentelemetry),
    ok.

init_per_testcase(_, Config) ->
    otel_simple_processor:set_exporter(otel_exporter_pid, self()),
    Config.

end_per_testcase(_, _Config) ->
    otel_simple_processor:set_exporter(none),
    ok.

-define(assertReceive(SpanName),
        receive
            {span, Span=#span{name=SpanName}} ->
                Span
        after
            1000 ->
                ct:fail("Did not receive the span after 1s")
        end).

trace_query(_Config) ->
    ?assertMatch(#{rows := [{1}]},
                 pgo:query("select '1'::int", [], #{})),
    #span{attributes={attributes, 128, infinity, 0, RecievedAttributes}} = 
        ?assertReceive(<<"pgo:query/3">>),
    #{<<"db.name">> := DbName,
      <<"db.statement">> := DbStatement,
      <<"db.system">> := DbSystem,
      <<"db.user">> := DbUser} = RecievedAttributes,
    ?assertMatch(DbName, <<"test">>),
    ?assertMatch(DbStatement, <<"select '1'::int">>),
    ?assertMatch(DbSystem, <<"postgresql">>),
    ?assertMatch(DbUser, <<"test">>),
    ok.

trace_with_parent_query(_Config) ->
    SpanCtx = ?start_span(<<"parent-of-query">>),
    ?set_current_span(SpanCtx),

    ?assertMatch(#{rows := [{1}]},
                 pgo:query("select '1'::int", [], #{trace => true})),

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
        ?assertMatch(#{rows := [{1}]}, 
            pgo:query("select '1'::int"))
    end, #{}),

    receive
        {span, #span{name = <<"pgo:transaction/2">>,
                     parent_span_id=Parent}} when Parent =:= undefined ->
            ok
    after
        5000 ->
            ct:fail(timeout)
    end,

    #span{attributes={attributes, 128, infinity, 0, RecievedAttributes}} = 
        ?assertReceive(<<"pgo:query/3">>),
    #{<<"db.name">> := DbName,
      <<"db.statement">> := DbStatement,
      <<"db.system">> := DbSystem,
      <<"db.user">> := DbUser} = RecievedAttributes,
    ?assertMatch(DbName, <<"test">>),
    ?assertMatch(DbStatement, <<"select '1'::int">>),
    ?assertMatch(DbSystem, <<"postgresql">>),
    ?assertMatch(DbUser, <<"test">>),
    ok.

no_statement(_Config) ->
    pgo:query("select '1'::int", [], #{include_statement_span_attribute => false}),

    #span{attributes={attributes, 128, infinity, 0, RecievedAttributes}} = 
        ?assertReceive(<<"pgo:query/3">>),
    #{<<"db.name">> := DbName,
      <<"db.system">> := DbSystem,
      <<"db.user">> := DbUser} = RecievedAttributes,
    ?assertMatch(DbName, <<"test">>),
    ?assertMatch(DbSystem, <<"postgresql">>),
    ?assertMatch(DbUser, <<"test">>),
    ?assertNot(maps:is_key(<<"db.statement">>, RecievedAttributes)),
    ok.
