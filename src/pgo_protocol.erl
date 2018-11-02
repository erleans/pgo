%% Mostly from the pgsql_protocol module in https://github.com/semiocast/pgsql
-module(pgo_protocol).

-include("pgo_internal.hrl").

-export([encode_startup_message/1,
         encode_ssl_request_message/0,
         encode_password_message/1,
         encode_query_message/1,
         encode_parse_message/3,
         encode_bind_message/6,
         encode_describe_message/2,
         encode_execute_message/2,
         encode_sync_message/0,
         encode_flush_message/0,
         encode_cancel_message/2,
         encode_copy_data_message/1,
         encode_copy_done/0,
         encode_copy_fail/1,

         decode_message/2,
         decode_row/4,

         bind_requires_statement_description/1]).

%%====================================================================
%% Constants
%%====================================================================
-define(PROTOCOL_VERSION_MAJOR, 3).
-define(PROTOCOL_VERSION_MINOR, 0).

-define(POSTGRESQL_GD_EPOCH, 730485). % ?_value(calendar:date_to_gregorian_days({2000,1,1}))).
-define(POSTGRESQL_GS_EPOCH, 63113904000). % ?_value(calendar:datetime_to_gregorian_seconds({{2000,1,1}, {0,0,0}}))).

-define(POSTGRES_EPOC_JDATE, 2451545).
-define(POSTGRES_EPOC_USECS, 946684800000000).

-define(MINS_PER_HOUR, 60).
-define(SECS_PER_MINUTE, 60).

-define(SECS_PER_DAY, 86400.0).

-define(USECS_PER_DAY, 86400000000).
-define(USECS_PER_HOUR, 3600000000).
-define(USECS_PER_MINUTE, 60000000).
-define(USECS_PER_SEC, 1000000).


%%====================================================================
%% Public API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Encode the startup message.
%%
-spec encode_startup_message([{iodata(), iodata()}]) -> binary().
encode_startup_message(Parameters) ->
    EncodedParams = [[iolist_to_binary(Key), 0, iolist_to_binary(Value), 0] || {Key, Value} <- Parameters],
    Packet = list_to_binary([<<?PROTOCOL_VERSION_MAJOR:16/integer, ?PROTOCOL_VERSION_MINOR:16/integer>>, EncodedParams, 0]),
    Size = byte_size(Packet) + 4,
    <<Size:32/integer, Packet/binary>>.

%%--------------------------------------------------------------------
%% @doc Encode the ssl request message.
%%
-spec encode_ssl_request_message() -> binary().
encode_ssl_request_message() ->
    <<8:32/integer, 1234:16/integer, 5679:16/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a password.
%%
-spec encode_password_message(iodata()) -> iolist().
encode_password_message(Password) ->
    encode_string_message($p, Password).

%%--------------------------------------------------------------------
%% @doc Encode a query.
%%
-spec encode_query_message(iodata()) -> iolist().
encode_query_message(Query) ->
    encode_string_message($Q, Query).

%%--------------------------------------------------------------------
%% @doc Encode a data segment of a COPY operation
%%
-spec encode_copy_data_message(iodata()) -> binary().
encode_copy_data_message(Message) ->
    StringBin = iolist_to_binary(Message),
    MessageLen = byte_size(StringBin) + 4,
    <<$d, MessageLen:32/integer, StringBin/binary>>.

%%--------------------------------------------------------------------
%% @doc Encode the end of a COPY operation
%%
-spec encode_copy_done() -> binary().
encode_copy_done() ->
    <<$c, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode the cancellation of a COPY operation with the given
%%      failure message
%%
-spec encode_copy_fail(iodata()) -> iolist().
encode_copy_fail(ErrorMessage) ->
    encode_string_message($f, ErrorMessage).
    
%%--------------------------------------------------------------------
%% @doc Encode a parse message.
%%
-spec encode_parse_message(iodata(), iodata(), [pgsql_oid()]) -> binary().
encode_parse_message(PreparedStatementName, Query, DataTypes) ->
    PreparedStatementNameBin = iolist_to_binary(PreparedStatementName),
    QueryBin = iolist_to_binary(Query),
    DataTypesBin = list_to_binary([<<DataTypeOid:32/integer>> || DataTypeOid <- DataTypes]),
    DataTypesCount = length(DataTypes),
    Packet = <<PreparedStatementNameBin/binary, 0, QueryBin/binary, 0, DataTypesCount:16/integer, DataTypesBin/binary>>,
    PacketLen = byte_size(Packet) + 4,
    <<$P, PacketLen:32/integer, Packet/binary>>.

%%--------------------------------------------------------------------
%% @doc Encode a bind message.
%%
-spec encode_bind_message(iodata(), iodata(), [any()], [pgsql_oid()], atom(), boolean()) -> iolist().
encode_bind_message(PortalName, StatementName, Parameters, ParametersDataTypes, OIDMap, IntegerDateTimes) ->
    ParametersCount = length(Parameters),
    ParametersCountBin = <<ParametersCount:16/integer>>,
    ParametersWithTypes = case ParametersDataTypes of
                              [] -> [{Parameter, undefined} || Parameter <- Parameters];
                              _ -> lists:zip(Parameters, ParametersDataTypes)
                          end,
    ParametersValues = [encode_parameter(Parameter, Type, OIDMap, IntegerDateTimes) || {Parameter, Type} <- ParametersWithTypes],
    ParametersFormatsBin = [ParametersCountBin | [<<1:16/integer>> || _ <- ParametersValues]],
    Results = <<1:16/integer, 1:16/integer>>,   % We want all results in binary format.
    Packet = [PortalName, 0, StatementName, 0, ParametersFormatsBin, ParametersCountBin, ParametersValues, Results],
    PacketLen = iolist_size(Packet) + 4,
    [<<$B, PacketLen:32/integer>> | Packet].

encode_numeric('NaN', _, _) ->
    <<0:16/unsigned, 0:16, 16#C000:16/unsigned, 0:16/unsigned>>;
encode_numeric(Float, _Weight, Scale) ->
    Sign = case Float > 0 of
               true -> 16#0000;
               false -> 16#4000
           end,
    IntegerPart = trunc(Float),
    DecimalPart = trunc((Float - IntegerPart) * math:pow(10, Scale)),

    Weight = length(integer_to_list(IntegerPart)),
    Scale1 = length(integer_to_list(DecimalPart)),

    Digits = [I - 48 || I <- integer_to_list(IntegerPart)] ++
        [I - 48 || I <- integer_to_list(DecimalPart)],
    Bin = [<<I:16/unsigned-integer>> || I <- Digits],
    Len = length(Digits),
    [<<Len:16/unsigned, Weight:16/signed, Sign:16/unsigned, Scale1:16/unsigned>> | Bin].

%%--------------------------------------------------------------------
%% @doc Encode a parameter.
%% All parameters are currently encoded in text format except binaries that are
%% encoded as binaries.
%%
-spec encode_parameter(any(), pgsql_oid() | undefined, atom(), boolean()) -> iodata().
encode_parameter(null, _Type, _OIDMap, _IntegerDateTimes) ->
    <<-1:32/integer>>;
encode_parameter({Numeric, Weight, Scale}, ?NUMERICOID, _OIDMap, _IntegerDateTimes) ->
    D = encode_numeric(Numeric, Weight, Scale),
    [<<(iolist_size(D)):32>> | D];
encode_parameter(Numeric, ?NUMERICOID, OIDMap, IntegerDateTimes) ->
    encode_parameter({Numeric, 8, 5}, ?NUMERICOID, OIDMap, IntegerDateTimes);
encode_parameter(Float, ?FLOAT8OID, _OIDMap, _IntegerDateTimes) ->
    <<8:32/integer, Float:1/big-float-unit:64>>;
encode_parameter(Integer, ?INT2OID, _OIDMap, _IntegerDateTimes) when is_integer(Integer) ->
    <<2:32/integer, Integer:16>>;
encode_parameter(Integer, ?INT4OID, _OIDMap, _IntegerDateTimes) when is_integer(Integer) ->
    <<4:32/integer, Integer:32>>;
encode_parameter(UUID, ?UUIDOID, _OIDMap, _IntegerDateTimes) ->
    encode_uuid(UUID);
encode_parameter(Binary, ?TEXTOID, _OIDMap, _IntegerDateTimes) when is_binary(Binary) ->
    Text = unicode:characters_to_binary(Binary, utf8),
    Size = byte_size(Text),
    <<Size:32/integer, Text/binary>>;
encode_parameter({array, []}, ?JSONBOID, _OIDMap, _IntegerDateTimes) ->
    Binary = <<"{}">>,
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
encode_parameter({array, List}, Type, OIDMap, IntegerDateTimes) ->
    encode_array(List, Type, OIDMap, IntegerDateTimes);
encode_parameter(Binary, ?JSONBOID, _OIDMap, _IntegerDateTimes) when is_binary(Binary) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
encode_parameter({jsonb, Binary}, ?JSONBOID, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
encode_parameter(Binary, ?JSONBOID, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
encode_parameter({json, Binary}, _Type, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<Size:32/integer, Binary/binary>>;
encode_parameter(Binary, ?JSONOID, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<Size:32/integer, Binary/binary>>;
encode_parameter({jsonb, Binary}, _Type, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;

encode_parameter({interval, {T, D, M}}, _, _OIDMap, true) ->
    <<16:32/integer, (encode_time(T, true)):64, D:32, M:32>>;
encode_parameter({T, D, M}, ?INTERVALOID, _OIDMap, true) ->
    <<16:32/integer, (encode_time(T, true)):64, D:32, M:32>>;
encode_parameter({T, D, M}, ?INTERVALOID, _OIDMap, false) ->
    <<16:32/integer, (encode_time(T, false)):1/big-float-unit:64, D:32, M:32>>;

encode_parameter(Binary, _Type, _OIDMap, _IntegerDateTimes) when is_binary(Binary) ->
    Size = byte_size(Binary),
    <<Size:32/integer, Binary/binary>>;
encode_parameter(Float, _Type, _OIDMap, _IntegerDateTimes) when is_float(Float) ->
    <<4:32/integer, Float:1/big-float-unit:32>>;
encode_parameter(Integer, ?INT8OID, _OIDMap, _IntegerDateTimes) ->
    <<8:32/integer, Integer:64>>;
encode_parameter(Integer, _Type, _OIDMap, _IntegerDateTimes) when is_integer(Integer) ->
    <<4:32/integer, Integer:32>>;

encode_parameter(true, _Type, _OIDMap, _IntegerDateTimes) ->
    <<1:32/integer, 1:1/big-signed-unit:8>>;
encode_parameter(false, _Type, _OIDMap, _IntegerDateTimes) ->
    <<1:32/integer, 0:1/big-signed-unit:8>>;

encode_parameter(#{x := X, y := Y}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;
encode_parameter(#{long := X, lat := Y}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;
encode_parameter({point, {X, Y}}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;
encode_parameter({X, Y}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;

encode_parameter(T={{_, _, _}, {_, _, _}}, ?TIMESTAMPOID, _OIDMap, true) ->
    <<8:32/integer, (encode_timestamp(T, true)):64>>;
encode_parameter(T={{_, _, _}, {_, _, _}}, ?TIMESTAMPOID, _OIDMap, false) ->
    <<8:32/integer, (encode_timestamp(T, false)):1/big-float-unit:64>>;

encode_parameter(Time, ?TIMEOID, _OIDMap, true) ->
    <<8:32/integer, (encode_time(Time, true)):64>>;
encode_parameter(Time, ?TIMEOID, _OIDMap, false) ->
    <<8:32/integer, (encode_time(Time, false)):1/big-float-unit:64>>;

encode_parameter(Date, ?DATEOID, _OIDMap, _IntegerDateTimes) ->
    <<4:32/integer, (encode_date(Date) - ?POSTGRES_EPOC_JDATE):32>>;

encode_parameter({Time, Tz}, ?TIMETZOID, _OIDMap, true) ->
    <<12:32/integer, (encode_time(Time, true)):64, Tz:32>>;
encode_parameter({Time, Tz}, ?TIMETZOID, _OIDMap, false) ->
    <<12:32/integer, (encode_time(Time, false)):1/big-float-unit:64, Tz:32>>;

encode_parameter(Timestamp, ?TIMESTAMPTZOID, _OIDMap, true) ->
    <<(encode_timestamp(Timestamp, true)):64>>;
encode_parameter(Timestamp, ?TIMESTAMPTZOID, _OIDMap, false) ->
    <<(encode_timestamp(Timestamp, false)):1/big-float-unit:64>>;

encode_parameter(Binary, _, _OIDMap, _IntegerDateTimes) when is_binary(Binary)
                                                             ; is_list(Binary)->
    Text = unicode:characters_to_binary(Binary, utf8),
    Size = byte_size(Text),
    <<Size:32/integer, Text/binary>>;
encode_parameter(Value, _Type, _OIDMap, _IntegerDateTimes) ->
    throw({badarg, Value}).

encode_timestamp({Date, Time}, true) ->
    D = encode_date(Date) - ?POSTGRES_EPOC_JDATE,
    D * ?USECS_PER_DAY + encode_time(Time, true);
encode_timestamp({Date, Time}, false) ->
    D = encode_date(Date) - ?POSTGRES_EPOC_JDATE,
    D * ?SECS_PER_DAY + encode_time(Time, false).

encode_date({Y, M, D}) ->
    M2 = case M > 2 of
        true ->
            M + 1;
        false ->
            M + 13
    end,
    Y2 = case M > 2 of
        true ->
            Y + 4800;
        false ->
            Y + 4799
    end,
    C = Y2 div 100,
    J1 = Y2 * 365 - 32167,
    J2 = J1 + (Y2 div 4 - C + C div 4),
    J2 + 7834 * M2 div 256 + D.

encode_time(0, _) ->
    0;
encode_time({H, M, S}, true) ->
    US = trunc(round(S * ?USECS_PER_SEC)),
    ((H * ?MINS_PER_HOUR + M) * ?SECS_PER_MINUTE) * ?USECS_PER_SEC + US;
encode_time({H, M, S}, false) ->
    ((H * ?MINS_PER_HOUR + M) * ?SECS_PER_MINUTE) + S.

encode_array(Elements, ArrayType, OIDMap, IntegerDateTimes) ->
    ElementType = array_type_to_element_type(ArrayType, OIDMap),
    ArrayElements = encode_array_elements(Elements, ElementType, OIDMap, IntegerDateTimes, []),
    encode_array_binary(ArrayElements, ElementType).

encode_uuid(<<>>) ->
    <<-1:32/integer>>;
encode_uuid(null) ->
    <<-1:32/integer>>;
encode_uuid(<<U:128>>) ->
    <<16:1/big-signed-unit:32, U:128>>;
encode_uuid(U) when is_integer(U) ->
    <<16:1/big-signed-unit:32, U:128>>;
encode_uuid(U) when is_binary(U) ->
    encode_uuid(binary_to_list(U));
encode_uuid(U) ->
    Hex = [H || H <- U, H =/= $-],
    {ok, [Int], _} = io_lib:fread("~16u", Hex),
    <<16:1/big-signed-unit:32, Int:128>>.

array_type_to_element_type(undefined, _OIDMap) -> undefined;
array_type_to_element_type(?CIDRARRAYOID, _OIDMap) -> ?CIDROID;
array_type_to_element_type(?UUIDARRAYOID, _OIDMap) -> ?UUIDOID;
array_type_to_element_type(?JSONBOID, _OIDMap) -> ?JSONBOID;
array_type_to_element_type(?JSONOID, _OIDMap) -> ?JSONOID;
array_type_to_element_type(?BOOLARRAYOID, _OIDMap) -> ?BOOLOID;
array_type_to_element_type(?BYTEAARRAYOID, _OIDMap) -> ?BYTEAOID;
array_type_to_element_type(?CHARARRAYOID, _OIDMap) -> ?CHAROID;
array_type_to_element_type(?NAMEARRAYOID, _OIDMap) -> ?NAMEOID;
array_type_to_element_type(?INT2ARRAYOID, _OIDMap) -> ?INT2OID;
array_type_to_element_type(?INT2VECTORARRAYOID, _OIDMap) -> ?INT2VECTOROID;
array_type_to_element_type(?INT4ARRAYOID, _OIDMap) -> ?INT4OID;
array_type_to_element_type(?REGPROCARRAYOID, _OIDMap) -> ?REGPROCOID;
array_type_to_element_type(?TEXTARRAYOID, _OIDMap) -> ?TEXTOID;
array_type_to_element_type(?TIDARRAYOID, _OIDMap) -> ?TIDOID;
array_type_to_element_type(?XIDARRAYOID, _OIDMap) -> ?XIDOID;
array_type_to_element_type(?CIDARRAYOID, _OIDMap) -> ?CIDOID;
array_type_to_element_type(?OIDVECTORARRAYOID, _OIDMap) -> ?OIDVECTOROID;
array_type_to_element_type(?BPCHARARRAYOID, _OIDMap) -> ?BPCHAROID;
array_type_to_element_type(?VARCHARARRAYOID, _OIDMap) -> ?VARCHAROID;
array_type_to_element_type(?INT8ARRAYOID, _OIDMap) -> ?INT8OID;
array_type_to_element_type(?POINTARRAYOID, _OIDMap) -> ?POINTOID;
array_type_to_element_type(?LSEGARRAYOID, _OIDMap) -> ?LSEGOID;
array_type_to_element_type(?PATHARRAYOID, _OIDMap) -> ?PATHOID;
array_type_to_element_type(?BOXARRAYOID, _OIDMap) -> ?BOXOID;
array_type_to_element_type(?FLOAT4ARRAYOID, _OIDMap) -> ?FLOAT4OID;
array_type_to_element_type(?FLOAT8ARRAYOID, _OIDMap) -> ?FLOAT8OID;
array_type_to_element_type(?ABSTIMEARRAYOID, _OIDMap) -> ?ABSTIMEOID;
array_type_to_element_type(?RELTIMEARRAYOID, _OIDMap) -> ?RELTIMEOID;
array_type_to_element_type(?TINTERVALARRAYOID, _OIDMap) -> ?TINTERVALOID;
array_type_to_element_type(?POLYGONARRAYOID, _OIDMap) -> ?POLYGONOID;
array_type_to_element_type(?OIDARRAYOID, _OIDMap) -> ?OIDOID;
array_type_to_element_type(?ACLITEMARRAYOID, _OIDMap) -> ?ACLITEMOID;
array_type_to_element_type(?MACADDRARRAYOID, _OIDMap) -> ?MACADDROID;
array_type_to_element_type(?INETARRAYOID, _OIDMap) -> ?INETOID;
array_type_to_element_type(?CSTRINGARRAYOID, _OIDMap) -> ?CSTRINGOID;
array_type_to_element_type(TypeOID, OIDMap) ->
    Type = decode_oid(TypeOID, OIDMap),
    if not is_atom(Type) -> undefined;
        true ->
            case atom_to_list(Type) of
                [$_ | ContentType] -> % Array
                    OIDContentType = type_to_oid(list_to_atom(ContentType), OIDMap),
                    OIDContentType;
                _ -> undefined
            end
    end.

encode_array_elements([{array, SubArray} | Tail], ElementType, OIDMap, IntegerDateTimes, Acc) ->
    SubArrayElements = encode_array_elements(SubArray, ElementType, OIDMap, IntegerDateTimes, []),
    encode_array_elements(Tail, ElementType, OIDMap, IntegerDateTimes, [{array, SubArrayElements} | Acc]);
encode_array_elements([null | Tail], ElementType, OIDMap, IntegerDateTimes, Acc) ->
    encode_array_elements(Tail, ElementType, OIDMap, IntegerDateTimes, [null | Acc]);
encode_array_elements([Element | Tail], ElementType, OIDMap, IntegerDateTimes, Acc) ->
    Encoded = encode_parameter(Element, ElementType, OIDMap, IntegerDateTimes),
    encode_array_elements(Tail, ElementType, OIDMap, IntegerDateTimes, [Encoded | Acc]);
encode_array_elements([], _ElementType, _OIDMap, _IntegerDateTimes, Acc) ->
    lists:reverse(Acc).

encode_array_binary(ArrayElements, ElementTypeOID) ->
    {HasNulls, Rows} = encode_array_binary_row(ArrayElements, false, []),
    Dims = get_array_dims(ArrayElements),
    Header = encode_array_binary_header(Dims, HasNulls, ElementTypeOID),
    Encoded = list_to_binary([Header, Rows]),
    Size = byte_size(Encoded),
    <<Size:32/integer, Encoded/binary>>.

encode_array_binary_row([null | Tail], _HasNull, Acc) ->
    encode_array_binary_row(Tail, true, [<<-1:32/integer>> | Acc]);
encode_array_binary_row([<<_BinarySize:32/integer, _BinaryVal/binary>> = Binary | Tail], HasNull, Acc) ->
    encode_array_binary_row(Tail, HasNull, [Binary | Acc]);
encode_array_binary_row([{array, Elements} | Tail], HasNull, Acc) ->
    {NewHasNull, Row} = encode_array_binary_row(Elements, HasNull, []),
    encode_array_binary_row(Tail, NewHasNull, [Row | Acc]);
encode_array_binary_row([], HasNull, AccRow) ->
    {HasNull, lists:reverse(AccRow)}.

get_array_dims([{array, SubElements} | _] = Row) ->
    Dims0 = get_array_dims(SubElements),
    Dim = length(Row),
    [Dim | Dims0];
get_array_dims(Row) ->
    Dim = length(Row),
    [Dim].

encode_array_binary_header(Dims, HasNulls, ElementTypeOID) ->
    NDims = length(Dims),
    Flags = if
        HasNulls -> 1;
        true -> 0
    end,
    EncodedDimensions = [<<Dim:32/integer, 1:32/integer>> || Dim <- Dims],
    [<<
        NDims:32/integer,
        Flags:32/integer,
        ElementTypeOID:32/integer
    >>,
    EncodedDimensions].

%%--------------------------------------------------------------------
%% @doc Determine if we need the statement description with these parameters.
%% We currently only require statement descriptions if we have arrays of
%% binaries.
-spec bind_requires_statement_description([any()]) -> boolean().
bind_requires_statement_description([]) -> false;
bind_requires_statement_description([{array, [{array, SubArrayElems} | SubArrayT]} | Tail]) ->
    bind_requires_statement_description([{array, SubArrayElems}, {array, SubArrayT} | Tail]);
bind_requires_statement_description([{array, [ArrayElem | _]} | _]) when is_binary(ArrayElem) -> true;
bind_requires_statement_description([{array, [null | ArrayElemsT]} | Tail]) ->
    bind_requires_statement_description([{array, ArrayElemsT} | Tail]);
bind_requires_statement_description([{array, []} | Tail]) ->
    bind_requires_statement_description(Tail);
bind_requires_statement_description([_OtherParam | Tail]) ->
    bind_requires_statement_description(Tail).

%%--------------------------------------------------------------------
%% @doc Encode a describe message.
%%
-spec encode_describe_message(portal | statement, iodata()) -> binary().
encode_describe_message(PortalOrStatement, Name) ->
    NameBin = iolist_to_binary(Name),
    MessageLen = byte_size(NameBin) + 6,
    WhatByte = case PortalOrStatement of
        portal -> $P;
        statement -> $S
    end,
    <<$D, MessageLen:32/integer, WhatByte, NameBin/binary, 0>>.

%%--------------------------------------------------------------------
%% @doc Encode an execute message.
%%
-spec encode_execute_message(iodata(), non_neg_integer()) -> binary().
encode_execute_message(PortalName, MaxRows) ->
    PortalNameBin = iolist_to_binary(PortalName),
    MessageLen = byte_size(PortalNameBin) + 9,
    <<$E, MessageLen:32/integer, PortalNameBin/binary, 0, MaxRows:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a sync message.
%%
-spec encode_sync_message() -> binary().
encode_sync_message() ->
    <<$S, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a flush message.
%%
-spec encode_flush_message() -> binary().
encode_flush_message() ->
    <<$H, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a flush message.
%%
-spec encode_cancel_message(integer(), integer()) -> binary().
encode_cancel_message(ProcID, Secret) ->
    <<16:32/integer, 80877102:32/integer, ProcID:32/integer, Secret:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a string message.
%%
-spec encode_string_message(byte(), iodata()) -> iolist().
encode_string_message(Identifier, String) ->    
    MessageLen = iolist_size(String) + 5,
    [<<Identifier, MessageLen:32/integer>>, String, <<0>>].

%%--------------------------------------------------------------------
%% @doc Decode a message.
%%
-spec decode_message(byte(), binary()) -> {ok, pgsql_backend_message()} | {error, any()}.
decode_message($R, Payload) -> decode_authentication_message(Payload);
decode_message($K, Payload) -> decode_backend_key_data_message(Payload);
decode_message($2, Payload) -> decode_bind_complete_message(Payload);
decode_message($3, Payload) -> decode_close_complete_message(Payload);
decode_message($C, Payload) -> decode_command_complete_message(Payload);
decode_message($d, Payload) -> decode_copy_data_message(Payload);
decode_message($c, Payload) -> decode_copy_done_message(Payload);
decode_message($G, Payload) -> decode_copy_in_response_message(Payload);
decode_message($H, Payload) -> decode_copy_out_response_message(Payload);
decode_message($W, Payload) -> decode_copy_both_response_message(Payload);
decode_message($D, Payload) -> decode_data_row_message(Payload);
decode_message($I, Payload) -> decode_empty_query_response_message(Payload);
decode_message($E, Payload) -> decode_error_response_message(Payload);
decode_message($V, Payload) -> decode_function_call_response_message(Payload);
decode_message($n, Payload) -> decode_no_data_message(Payload);
decode_message($N, Payload) -> decode_notice_response_message(Payload);
decode_message($A, Payload) -> decode_notification_response_message(Payload);
decode_message($t, Payload) -> decode_parameter_description_message(Payload);
decode_message($S, Payload) -> decode_parameter_status_message(Payload);
decode_message($1, Payload) -> decode_parse_complete_message(Payload);
decode_message($s, Payload) -> decode_portal_suspended_message(Payload);
decode_message($Z, Payload) -> decode_ready_for_query_message(Payload);
decode_message($T, Payload) -> decode_row_description_message(Payload);
decode_message(Other, _) ->
    {error, {unknown_message_type, Other}}.

decode_authentication_message(<<0:32/integer>>) ->
    {ok, #authentication_ok{}};
decode_authentication_message(<<2:32/integer>>) ->
    {ok, #authentication_kerberos_v5{}};
decode_authentication_message(<<3:32/integer>>) ->
    {ok, #authentication_cleartext_password{}};
decode_authentication_message(<<5:32/integer, Salt:4/binary>>) ->
    {ok, #authentication_md5_password{salt = Salt}};
decode_authentication_message(<<6:32/integer>>) ->
    {ok, #authentication_scm_credential{}};
decode_authentication_message(<<7:32/integer>>) ->
    {ok, #authentication_gss{}};
decode_authentication_message(<<9:32/integer>>) ->
    {ok, #authentication_sspi{}};
decode_authentication_message(<<8:32/integer, Rest/binary>>) ->
    {ok, #authentication_gss_continue{data = Rest}};
decode_authentication_message(Payload) ->
    {error, {unknown_message, authentication, Payload}}.

decode_backend_key_data_message(<<ProcID:32/integer, Secret:32/integer>>) ->
    {ok, #backend_key_data{procid = ProcID, secret = Secret}};
decode_backend_key_data_message(Payload) ->
    {error, {unknown_message, backend_key_data, Payload}}.

decode_bind_complete_message(<<>>) -> {ok, #bind_complete{}};
decode_bind_complete_message(Payload) ->
    {error, {unknown_message, bind_complete, Payload}}.

decode_close_complete_message(<<>>) -> {ok, #close_complete{}};
decode_close_complete_message(Payload) ->
    {error, {unknown_message, close_complete, Payload}}.

decode_command_complete_message(Payload) ->
    case decode_string(Payload) of
        {ok, String, <<>>} -> {ok, #command_complete{command_tag = String}};
        _ -> {error, {unknown_message, command_complete, Payload}}
    end.

decode_copy_data_message(Payload) -> {ok, #copy_data{data = Payload}}.

decode_copy_done_message(<<>>) -> {ok, #copy_done{}};
decode_copy_done_message(Payload) ->
    {error, {unknown_message, copy_done, Payload}}.

decode_copy_in_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} -> {ok, #copy_in_response{format = OverallFormat, columns = N, column_formats = ColumnFormats}};
        {error, _} -> {error, {unknow_message, copy_in_response, Payload}}
    end.

decode_copy_out_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} -> {ok, #copy_out_response{format = OverallFormat, columns = N, column_formats = ColumnFormats}};
        {error, _} -> {error, {unknow_message, copy_out_response, Payload}}
    end.

decode_copy_both_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} -> {ok, #copy_both_response{format = OverallFormat, columns = N, column_formats = ColumnFormats}};
        {error, _} -> {error, {unknow_message, copy_both_response, Payload}}
    end.

decode_data_row_message(<<N:16/integer, Rest/binary>> = Payload) ->
    case decode_data_row_values(N, Rest) of
        {ok, Values} -> {ok, #data_row{values = Values}};
        {error, _} ->
            {error, {unknow_message, data_row, Payload}}
    end;
decode_data_row_message(Payload) ->
    {error, {unknow_message, data_row, Payload}}.

decode_data_row_values(Columns, Binary) ->
    decode_data_row_values0(Binary, Columns, []).

decode_data_row_values0(<<>>, 0, Acc) -> {ok, lists:reverse(Acc)};
decode_data_row_values0(<<-1:32/signed-integer, Rest/binary>>, N, Acc) when N > 0 ->
    decode_data_row_values0(Rest, N - 1, [null | Acc]);
decode_data_row_values0(<<ValueLen:32/integer, ValueBin:ValueLen/binary, Rest/binary>>, N, Acc) when N > 0 ->
    decode_data_row_values0(Rest, N - 1, [ValueBin | Acc]);
decode_data_row_values0(<<_/binary>>, _N, _Acc) -> {error, invalid_value_len}.
        
decode_empty_query_response_message(<<>>) -> {ok, #empty_query_response{}};
decode_empty_query_response_message(Payload) ->
    {error, {unknown_message, empty_query_response, Payload}}.

decode_error_response_message(Payload) ->
    case decode_error_and_notice_message_fields(Payload) of
        {ok, Fields} -> {ok, #error_response{fields = Fields}};
        {error, _} -> {error, {unknown_message, error_response, Payload}}
    end.

decode_function_call_response_message(<<-1:32/signed-integer>>) -> {ok, #function_call_response{value = null}};
decode_function_call_response_message(<<Len:32/integer, Value:Len/binary>>) -> {ok, #function_call_response{value = Value}};
decode_function_call_response_message(Payload) ->
    {error, {unknown_message, function_call_response, Payload}}.

decode_no_data_message(<<>>) -> {ok, #no_data{}};
decode_no_data_message(Payload) ->
    {error, {unknown_message, no_data, Payload}}.

decode_notice_response_message(Payload) ->
    case decode_error_and_notice_message_fields(Payload) of
        {ok, Fields} -> {ok, #notice_response{fields = Fields}};
        {error, _} -> {error, {unknown_message, notice_response, Payload}}
    end.

decode_notification_response_message(<<ProcID:32/integer, Rest0/binary>> = Payload) ->
    case decode_string(Rest0) of
        {ok, Channel, Rest1} ->
            case decode_string(Rest1) of
                {ok, PayloadStr, <<>>} -> {ok, #notification_response{procid = ProcID, channel = Channel, payload = PayloadStr}};
                {error, _} -> {error, {unknown_message, notification_response, Payload}}
            end;
        {error, _} -> {error, {unknown_message, notification_response, Payload}}
    end;
decode_notification_response_message(Payload) ->
    {error, {unknown_message, notification_response, Payload}}.

decode_parameter_description_message(<<Count:16/integer, Rest/binary>> = Payload) ->
    ParameterDataTypes = decode_parameter_data_types(Rest),
    if
        Count =:= length(ParameterDataTypes) ->
            {ok, #parameter_description{count = Count, data_types = ParameterDataTypes}};
        true ->
            {error, {unknown_message, parameter_description, Payload}}
    end;
decode_parameter_description_message(Payload) ->
    {error, {unknown_message, parameter_description, Payload}}.

decode_parameter_status_message(Payload) ->
    case decode_string(Payload) of
        {ok, Name, Rest0} ->
            case decode_string(Rest0) of
                {ok, Value, <<>>} -> {ok, #parameter_status{name = Name, value = Value}};
                {error, _} -> {error, {unknown_message, parameter_status, Payload}}
            end;
        {error, _} -> {error, {unknown_message, parameter_status, Payload}}
    end.

decode_parse_complete_message(<<>>) -> {ok, #parse_complete{}};
decode_parse_complete_message(Payload) ->
    {error, {unknown_message, parse_complete, Payload}}.

decode_portal_suspended_message(<<>>) -> {ok, #portal_suspended{}};
decode_portal_suspended_message(Payload) ->
    {error, {unknown_message, portal_suspended, Payload}}.

decode_ready_for_query_message(<<$I>>) -> {ok, #ready_for_query{transaction_status = idle}};
decode_ready_for_query_message(<<$T>>) -> {ok, #ready_for_query{transaction_status = transaction}};
decode_ready_for_query_message(<<$E>>) -> {ok, #ready_for_query{transaction_status = error}};
decode_ready_for_query_message(Payload) ->
    {error, {unknown_message, ready_for_query, Payload}}.

decode_row_description_message(<<Count:16/integer, Rest/binary>> = Payload) when Count >= 0 ->
    case decode_row_description_message0(Count, Rest, []) of
        {ok, Fields} ->
            {ok, #row_description{count = Count, fields = Fields}};
        {error, _} ->
            {error, {unknown_message, row_description, Payload}}
    end;
decode_row_description_message(Payload) ->
    {error, {unknown_message, row_description, Payload}}.

decode_row_description_message0(0, <<>>, Acc) -> {ok, lists:reverse(Acc)};
decode_row_description_message0(Count, Binary, Acc) ->
    case decode_string(Binary) of
        {ok, FieldName, <<TableOid:32/integer, AttrNum:16/integer, DataTypeOid:32/integer, DataTypeSize:16/integer, TypeModifier:32/integer, FormatCode:16/integer, Tail/binary>>} ->
            case decode_format_code(FormatCode) of
                {ok, Format} ->
                    Field = #row_description_field{
                        name = FieldName,
                        table_oid = TableOid,
                        attr_number = AttrNum,
                        data_type_oid = DataTypeOid,
                        data_type_size = DataTypeSize,
                        type_modifier = TypeModifier,
                        format = Format},
                    decode_row_description_message0(Count - 1, Tail, [Field | Acc]);
                {error, _} = Error -> Error
            end;
        {error, _} = Error -> Error;
        _ -> {error, unknown_message}
    end.

%%% Helper functions.

decode_copy_response_message(<<Format:8/integer, N:16/integer, Rest/binary>>) when Format =:= 0 orelse Format =:= 1 ->
    {ok, OverallFormat} = decode_format_code(Format),
    if
        byte_size(Rest) =:= N * 2 ->
            case decode_format_codes(Rest) of
                {ok, ColumnFormats} ->
                    {ok, {OverallFormat, N, ColumnFormats}};
                {error, _} -> {error, column_formats}
            end;
        true ->
            {error, column_formats_size}
    end;
decode_copy_response_message(Payload) ->
    {error, {unknown_message, copy_response, Payload}}.

decode_error_and_notice_message_fields(Binary) ->
    decode_error_and_notice_message_fields0(Binary, []).

decode_error_and_notice_message_fields0(<<0>>, Acc) ->
    {ok, maps:from_list(Acc)};

decode_error_and_notice_message_fields0(<<FieldType, Rest0/binary>>, Acc) ->
    case decode_string(Rest0) of
        {ok, FieldString, Rest1} ->
            FieldTypeSym = decode_error_and_mention_field_type(FieldType),
            Field = {FieldTypeSym, FieldString},
            NewAcc = [Field | Acc],
            decode_error_and_notice_message_fields0(Rest1, NewAcc);
        {error, _} = Error -> Error
    end;
decode_error_and_notice_message_fields0(Bin, _Acc) -> {error, {badarg, Bin}}.

-spec decode_error_and_mention_field_type(byte()) -> pgsql_error:pgsql_error_and_mention_field_type().
decode_error_and_mention_field_type($S) -> severity;
decode_error_and_mention_field_type($C) -> code;
decode_error_and_mention_field_type($M) -> message;
decode_error_and_mention_field_type($D) -> detail;
decode_error_and_mention_field_type($H) -> hint;
decode_error_and_mention_field_type($P) -> position;
decode_error_and_mention_field_type($p) -> internal_position;
decode_error_and_mention_field_type($q) -> internal_query;
decode_error_and_mention_field_type($W) -> where;
decode_error_and_mention_field_type($s) -> schema;
decode_error_and_mention_field_type($t) -> table;
decode_error_and_mention_field_type($c) -> column;
decode_error_and_mention_field_type($d) -> data_type;
decode_error_and_mention_field_type($n) -> constraint;
decode_error_and_mention_field_type($F) -> file;
decode_error_and_mention_field_type($L) -> line;
decode_error_and_mention_field_type($R) -> routine;
decode_error_and_mention_field_type(Other) -> {unknown, Other}.

decode_parameter_data_types(Binary) ->
    decode_parameter_data_types0(Binary, []).

decode_parameter_data_types0(<<>>, Acc) -> lists:reverse(Acc);
decode_parameter_data_types0(<<Oid:32/integer, Tail/binary>>, Acc) ->
    decode_parameter_data_types0(Tail, [Oid | Acc]).

-spec decode_format_code(integer()) -> {ok, pgsql_format()} | {error, any()}.
decode_format_code(0) -> {ok, text};
decode_format_code(1) -> {ok, binary};
decode_format_code(_Other) -> {error, unknown_format_code}.

-spec decode_format_codes(binary()) -> {ok, [pgsql_format()]} | {error, any()}.
decode_format_codes(Binary) ->
    decode_format_codes0(Binary, []).

decode_format_codes0(<<FormatCode:16/integer, Tail/binary>>, Acc) ->
    case decode_format_code(FormatCode) of
        {ok, Format} ->
            decode_format_codes0(Tail, [Format | Acc]);
        {error, _} = Error -> Error
    end;
decode_format_codes0(<<>>, Acc) -> {ok, lists:reverse(Acc)}.

-spec decode_string(binary()) -> {ok, binary(), binary()} | {error, not_null_terminated}.
decode_string(Binary) ->
    case binary:match(Binary, <<0>>) of
        nomatch -> {error, not_null_terminated};
        {Position, 1} ->
            {String, <<0, Rest/binary>>} = split_binary(Binary, Position),
            {ok, String, Rest}
    end.

%%--------------------------------------------------------------------
%% @doc Decode a row format.
%%
-spec decode_row([#row_description_field{}], [binary()], atom(), proplists:proplist()) -> tuple().
decode_row(Descs, Values, OIDMap, DecodeOptions) ->
    decode_row0(Descs, Values, OIDMap, DecodeOptions, []).

decode_row0([Desc | DescsT], [Value | ValuesT], OIDMap, DecodeOptions, Acc) ->
    DecodedValue = decode_value(Desc, Value, OIDMap, DecodeOptions),
    decode_row0(DescsT, ValuesT, OIDMap, DecodeOptions, [DecodedValue | Acc]);
decode_row0([], [], _OIDMap, _DecodeOptions, Acc) ->
    list_to_tuple(lists:reverse(Acc)).

decode_value(_Desc, null, _OIDMap, _DecodeOptions) -> null;
decode_value(#row_description_field{data_type_oid = DataTypeOID, format = binary}, Value, OIDMap, DecodeOptions) ->
    decode_value_bin(DataTypeOID, Value, OIDMap, DecodeOptions);
decode_value(#row_description_field{format = text}, _Value, _OIDMap, _DecodeOptions) ->
    throw(no_text_format_support).


cast_datetime_secs(Secs, DecodeOptions) ->
    case proplists:get_value(datetime_float_seconds, DecodeOptions, as_available) of
        round -> round(Secs);
        always -> Secs * 1.0;
        as_available -> Secs
    end.

cast_datetime_usecs(Secs0, USecs, DecodeOptions) ->
    Secs1 = case USecs of
        0 -> Secs0;
        _ -> Secs0 + USecs / 1000000
    end,
    cast_datetime_secs(Secs1, DecodeOptions).

type_to_oid(Type, Pool) ->
    case ets:match_object(Pool, {'_', Type}) of
        [{OIDType, _}] ->
            OIDType;
        [] ->
            undefined
    end.

decode_value_bin(?JSONBOID, <<?JSONB_VERSION_1:8, Value/binary>>, _OIDMap, _IntegerDateTimes) -> Value;
decode_value_bin(?BOOLOID, <<0>>, _OIDMap, _DecodeOptions) -> false;
decode_value_bin(?BOOLOID, <<1>>, _OIDMap, _DecodeOptions) -> true;
decode_value_bin(?BYTEAOID, Value, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?NAMEOID, Value, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?INT8OID, <<Value:64/signed-integer>>, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?INT2OID, <<Value:16/signed-integer>>, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?INT4OID, <<Value:32/signed-integer>>, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?OIDOID, <<Value:32/signed-integer>>, _OIDMap, _DecodeOptions) ->
    Value;
decode_value_bin(?TEXTOID, Value, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?BPCHAROID, Value, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?VARCHAROID, Value, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?FLOAT4OID, <<Value:32/float>>, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?FLOAT4OID, <<127,192,0,0>>, _OIDMap, _DecodeOptions) -> 'NaN';
decode_value_bin(?FLOAT4OID, <<127,128,0,0>>, _OIDMap, _DecodeOptions) -> 'Infinity';
decode_value_bin(?FLOAT4OID, <<255,128,0,0>>, _OIDMap, _DecodeOptions) -> '-Infinity';
decode_value_bin(?FLOAT8OID, <<Value:64/float>>, _OIDMap, _DecodeOptions) -> Value;
decode_value_bin(?FLOAT8OID, <<127,248,0,0,0,0,0,0>>, _OIDMap, _DecodeOptions) -> 'NaN';
decode_value_bin(?FLOAT8OID, <<127,240,0,0,0,0,0,0>>, _OIDMap, _DecodeOptions) -> 'Infinity';
decode_value_bin(?FLOAT8OID, <<255,240,0,0,0,0,0,0>>, _OIDMap, _DecodeOptions) -> '-Infinity';
decode_value_bin(?UUIDOID, Value, _OIDMap, _DecodeOptions) ->
    <<UUID_A:32/integer, UUID_B:16/integer, UUID_C:16/integer, UUID_D:16/integer, UUID_E:48/integer>> = Value,
    UUIDStr = io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [UUID_A, UUID_B, UUID_C, UUID_D, UUID_E]),
    list_to_binary(UUIDStr);
    %% Value;
decode_value_bin(?DATEOID, <<Date:32/signed-integer>>, _OIDMap, _DecodeOptions) -> calendar:gregorian_days_to_date(Date + ?POSTGRESQL_GD_EPOCH);
decode_value_bin(?TIMEOID, TimeBin, _OIDMap, DecodeOptions) -> decode_time(TimeBin, not proplists:get_bool(float_datetimes, DecodeOptions), DecodeOptions);
decode_value_bin(?TIMETZOID, TimeTZBin, _OIDMap, DecodeOptions) -> decode_time_tz(TimeTZBin, proplists:get_bool(integer_datetimes, DecodeOptions), DecodeOptions);
decode_value_bin(?TIMESTAMPOID, TimestampBin, _OIDMap, DecodeOptions) ->
    decode_timestamp(TimestampBin, not proplists:get_bool(float_datetimes, DecodeOptions), DecodeOptions);
decode_value_bin(?TIMESTAMPTZOID, TimestampBin, _OIDMap, DecodeOptions) -> decode_timestamp(TimestampBin, not proplists:get_bool(float_datetimes, DecodeOptions), DecodeOptions);
decode_value_bin(?NUMERICOID, NumericBin, _OIDMap, _DecodeOptions) -> decode_numeric_bin(NumericBin);
decode_value_bin(?POINTOID, <<X:64/float, Y:64/float>>, _OIDMap, _DecodeOptions) ->
    %% TODO: make configurable between this and returning #{x => X, y => Y}
    #{long => X, lat => Y};
decode_value_bin(?LSEGOID, <<P1X:64/float, P1Y:64/float, P2X:64/float, P2Y:64/float>>, _OIDMap, _DecodeOptions) -> {lseg, {P1X, P1Y}, {P2X, P2Y}};
decode_value_bin(?BOXOID, <<P1X:64/float, P1Y:64/float, P2X:64/float, P2Y:64/float>>, _OIDMap, _DecodeOptions) -> {box, {P1X, P1Y}, {P2X, P2Y}};
decode_value_bin(?PATHOID, <<1:8/unsigned-integer, PointsBin/binary>>, _OIDMap, _DecodeOptions) -> {path, closed, decode_points_bin(PointsBin)};
decode_value_bin(?PATHOID, <<0:8/unsigned-integer, PointsBin/binary>>, _OIDMap, _DecodeOptions) -> {path, open, decode_points_bin(PointsBin)};
decode_value_bin(?POLYGONOID, Points, _OIDMap, _DecodeOptions) -> {polygon, decode_points_bin(Points)};
decode_value_bin(?VOIDOID, <<>>, _OIDMap, _DecodeOptions) -> null;
decode_value_bin(TypeOID, Value, OIDMap, DecodeOptions) ->
    Type = decode_oid(TypeOID, OIDMap),
    if not is_atom(Type) -> {Type, Value};
        true ->
            case atom_to_list(Type) of
                [$_ | _] -> % Array
                    decode_array_bin(Value, OIDMap, DecodeOptions);
                _ -> {Type, Value}
            end
    end.

decode_points_bin(<<N:32/unsigned-integer, Points/binary>>) ->
    decode_points_bin(N, Points, []).

decode_points_bin(0, <<>>, Acc) ->
    lists:reverse(Acc);
decode_points_bin(N, <<PX:64/float, PY:64/float, Tail/binary>>, Acc) when N > 0 ->
    decode_points_bin(N - 1, Tail, [{PX, PY}|Acc]).

decode_array_bin(<<Dimensions:32/signed-integer, _Flags:32/signed-integer, ElementOID:32/signed-integer, Remaining/binary>>, OIDMap, DecodeOptions) ->
    {RemainingData, DimsInfo} = lists:foldl(fun(_Pos, {Bin, Acc}) ->
                <<Nbr:32/signed-integer, LBound:32/signed-integer, Next/binary>> = Bin,
                {Next, [{Nbr, LBound} | Acc]}
        end, {Remaining, []}, lists:seq(1, Dimensions)),
    DataList = decode_array_bin_aux(ElementOID, RemainingData, OIDMap, DecodeOptions, []),
    Expanded = expand(DataList, DimsInfo),
    Expanded.

expand([], []) ->
    {array, []};
expand([List], []) ->
    List;
expand(List, [{Nbr,_}|NextDim]) ->
    List2 = expand_aux(List, Nbr, Nbr, [], []),
    expand(List2, NextDim).

expand_aux([], 0, _, Current, Acc) ->
    lists:reverse([{array, lists:reverse(Current)} | Acc]);
expand_aux(List, 0, Nbr, Current, Acc) ->
    expand_aux(List, Nbr, Nbr, [], [ {array, lists:reverse(Current)} | Acc]);
expand_aux([E|Next], Level, Nbr, Current, Acc) ->
    expand_aux(Next, Level-1, Nbr, [E | Current], Acc).


decode_array_bin_aux(_ElementOID, <<>>, _OIDMap, _DecodeOptions, Acc) ->
    lists:reverse(Acc);
decode_array_bin_aux(ElementOID, <<-1:32/signed-integer, Rest/binary>>, OIDMap, DecodeOptions, Acc) ->
    decode_array_bin_aux(ElementOID, Rest, OIDMap, DecodeOptions, [null | Acc]);
decode_array_bin_aux(ElementOID, <<Size:32/signed-integer, Next/binary>>, OIDMap, DecodeOptions, Acc) ->
    {ValueBin, Rest} = split_binary(Next, Size),
    Value = decode_value_bin(ElementOID, ValueBin, OIDMap, DecodeOptions),
    decode_array_bin_aux(ElementOID, Rest, OIDMap, DecodeOptions, [Value | Acc]).

decode_numeric_bin(<<0:16/unsigned, _Weight:16, 16#C000:16/unsigned, 0:16/unsigned>>) -> 'NaN';
decode_numeric_bin(<<Len:16/unsigned, Weight:16/signed, Sign:16/unsigned, DScale:16/unsigned, Tail/binary>>) when Sign =:= 16#0000 orelse Sign =:= 16#4000 ->
    Len = byte_size(Tail) div 2,
    {ValueInt, DecShift} = decode_numeric_bin0(Tail, Weight, 0),
    ValueDec = decode_numeric_bin_scale(ValueInt, DecShift),
    SignedDec = case Sign of
        16#0000 -> ValueDec;
        16#4000 -> -ValueDec
    end,
    % Convert to float if there are digits after the decimal point.
    if
        DScale > 0 andalso is_integer(SignedDec) -> SignedDec * 1.0;
        true -> SignedDec
    end.

-define(NBASE, 10000).

decode_numeric_bin0(<<>>, Weight, Acc) -> {Acc, Weight};
decode_numeric_bin0(<<Digit:16, Tail/binary>>, Weight, Acc) when Digit >= 0 andalso Digit < ?NBASE ->
    NewAcc = (Acc * ?NBASE) + Digit,
    decode_numeric_bin0(Tail, Weight - 1, NewAcc).

decode_numeric_bin_scale(Value, -1) -> Value;
decode_numeric_bin_scale(Value, DecShift) when DecShift < 0 ->
    NewValue = Value / ?NBASE,
    decode_numeric_bin_scale(NewValue, DecShift + 1);
decode_numeric_bin_scale(Value, DecShift) when DecShift >= 0 ->
    NewValue = Value * ?NBASE,
    decode_numeric_bin_scale(NewValue, DecShift - 1).

decode_oid(Oid, Pool) ->
    case ets:lookup(Pool, Oid) of
        [{_, OIDName}] -> OIDName;
        [] -> Oid
    end.

decode_time(<<Time:64/signed-integer>>, true, DecodeOptions) ->
    Seconds = Time div 1000000,
    USecs = Time rem 1000000,
    decode_time0(Seconds, USecs, DecodeOptions);
decode_time(<<Time:64/float>>, false, DecodeOptions) ->
    Seconds = trunc(Time),
    USecs = round((Time - Seconds) * 1000000),   % Maximum documented PostgreSQL precision is usec.
    decode_time0(Seconds, USecs, DecodeOptions).

decode_time0(Seconds, USecs, DecodeOptions) ->
    {Hour, Min, Secs0} = calendar:seconds_to_time(Seconds),
    Secs1 = cast_datetime_usecs(Secs0, USecs, DecodeOptions),
    {Hour, Min, Secs1}.

decode_time_tz(<<TimeBin:8/binary, TZ:32/signed-integer>>, IntegerDateTimes, DecodeOptions) ->
    Decoded = decode_time(TimeBin, IntegerDateTimes, DecodeOptions),
    adjust_time(Decoded, - (TZ div 60)).

adjust_time(Time, 0) -> Time;
adjust_time({Hour, Min, Secs}, TZDelta) when TZDelta > 0 ->
    {(24 + Hour - (TZDelta div 60)) rem 24, (60 + Min - (TZDelta rem 60)) rem 60, Secs};
adjust_time({Hour, Min, Secs}, TZDelta) ->
    {(Hour - (TZDelta div 60)) rem 24, (Min - (TZDelta rem 60)) rem 60, Secs}.

decode_timestamp(<<16#7FFFFFFFFFFFFFFF:64/signed-integer>>, true, _DecodeOptions) -> infinity;
decode_timestamp(<<-16#8000000000000000:64/signed-integer>>, true, _DecodeOptions) -> '-infinity';
decode_timestamp(<<127,240,0,0,0,0,0,0>>, false, _DecodeOptions) -> infinity;
decode_timestamp(<<255,240,0,0,0,0,0,0>>, false, _DecodeOptions) -> '-infinity';
decode_timestamp(<<Timestamp:64/signed-integer>>, true, DecodeOptions) ->
    TimestampSecs = Timestamp div 1000000,
    USecs = Timestamp rem 1000000,
    decode_timestamp0(TimestampSecs, USecs, DecodeOptions);
decode_timestamp(<<Timestamp:64/float>>, false, DecodeOptions) ->
    TimestampSecs = trunc(Timestamp),
    USecs = round((Timestamp - TimestampSecs) * 1000000), % Maximum documented PostgreSQL precision is usec.
    decode_timestamp0(TimestampSecs, USecs, DecodeOptions).

decode_timestamp0(Secs, USecs, DecodeOptions) ->
    {Date, {Hour, Min, Secs0}} = calendar:gregorian_seconds_to_datetime(Secs + ?POSTGRESQL_GS_EPOCH),
    Secs1 = cast_datetime_usecs(Secs0, USecs, DecodeOptions),
    Time = {Hour, Min, Secs1},
    {Date, Time}.
