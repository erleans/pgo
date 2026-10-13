%% Feel free to use, reuse and abuse the code in this file.
%%
%% The purpose of the DB Connector is to have a standard interface for the rest of the app to communicate with PGO.
%% This will make it easy to reflect any changes in the return type of PGO (e.g. when it moved from returning tuples to maps)
%% Or to replace another tool with PGO, or vice versa. 
%% So the app calls functions defined in a "helper" file. 
%% And the helper file communicates with PGO via the connector. So in case of any changes with PGO, only this connector file needs to be updated, and not every pgo:query call.

%% @doc PGO connector

-module(db_connector).
-export([simple_query/1, extended_query/2]).

simple_query(Query) -> 
    #{command := Command, num_rows := NumRows, rows := Result} = 
        pgo:query(Query, [], #{decode_opts => 
        [return_rows_as_maps, column_name_as_atom]} ),
    #{command => Command, num_rows => NumRows, result => Result}.

extended_query(Query, ParamList) ->
    #{command := Command, num_rows := NumRows, rows := Result} = 
        pgo:query(Query, ParamList, #{decode_opts => [return_rows_as_maps, column_name_as_atom]}),
    #{command => Command, num_rows => NumRows, result => Result}.
