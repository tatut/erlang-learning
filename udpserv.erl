-module(udpserv).
-export([listen/2, hello/1, reverser/1]).

% Listen to UDP messages, run a function to get response

listen(Port, Handler) ->
    {ok, Socket} = gen_udp:open(Port,[binary]),
    loop(Socket,Handler).

loop(Socket,Handler)->
    receive
        {udp, Socket, Host,Port,Bin} ->
            AsList = binary_to_list(Bin),
            io:format("[~p:~p] ~p~n", [Host,Port,AsList]),
            gen_udp:send(Socket, Host, Port, list_to_binary(Handler(AsList))),
            loop(Socket,Handler);
        Else -> io:format("ei yymmarra: ~p~n", [Else])
    end.

hello(Port) ->
    listen(Port, fun(X) -> "Hello, " ++ string:trim(X) ++ "!" end).


reverser(Port) ->
    listen(Port, fun(X) -> lists:reverse(string:trim(X)) end).
