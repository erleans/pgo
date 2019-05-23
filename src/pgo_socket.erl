-module(pgo_socket).

-export([send/2,
         recv/2,
         close/1]).

send(Socket, Packet) ->
    case is_port(Socket) of
        true ->
            gen_tcp:send(Socket, Packet);
        false ->
            ssl:send(Socket, Packet)
    end.

recv(Socket, Length) ->
    case is_port(Socket) of
        true ->
            gen_tcp:recv(Socket, Length);
        _ ->
            ssl:recv(Socket, Length)
    end.

close(Socket) ->
    Socket2 = case Socket of
        {sslsocket,{gen_tcp, S, _, _}, _} -> S;
        _ -> Socket
    end,
    unlink(Socket2),
    exit(Socket2, shutdown).
