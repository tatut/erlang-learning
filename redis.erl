-module(redis).
-export([read/1, write/1, start/1]).

% A very minimal Redis wire protocol server that saves
% values to ETS.

%%%%%%%
% RESP Wire format
% https://redis.io/docs/reference/protocol-spec/

resp_split(Bin) ->
    binary:split(Bin,<<"\r\n">>).

resp_value(Bin, Parser) ->
    [Data,Rest] = resp_split(Bin),
    {ok, Parser(Data), Rest}.


read(<<$+, Str/binary>>) -> resp_value(Str, fun binary_to_list/1);
read(<<$-, Err/binary>>) -> resp_value(Err, fun binary_to_list/1);
read(<<$:, Int/binary>>) -> resp_value(Int, fun binary_to_integer/1);
read(<<$*, Arr/binary>>) ->
    {ok, Count, Rest} = resp_value(Arr, fun binary_to_integer/1),
    if
        Count == -1 -> {ok, null, Rest}; % array of -1 len is considered the null
        true -> read_array(Count, Rest, [])
    end;
read(<<$$, Bulk/binary>>) ->
    %% prefixed bulk string, return as binary
    {ok, Size, Rest} = resp_value(Bulk, fun binary_to_integer/1),
    {Bin, Rest1} = split_binary(Rest, Size),
    {_, Rest2} = split_binary(Rest1, 2), % remove CRLF
    {ok, if
             Bin == <<"Null">> -> null;
             true -> Bin
         end,
     Rest2}.

read_array(0, Rest, Arr) ->
    {ok, lists:reverse(Arr), Rest};
read_array(Items, Bin, Arr) ->
    {ok, Item, Rest} = read(Bin),
    read_array(Items-1, Rest, [Item|Arr]).

write(Int) when is_integer(Int) ->
    [":", integer_to_list(Int), <<"\r\n">>];
write({error, Message}) ->
    ["-", Message, <<"\r\n">>];
write(Bin) when is_binary(Bin) ->
    ["$", integer_to_list(size(Bin)), <<"\r\n">>, Bin, <<"\r\n">>];
write(StrOrList) when is_list(StrOrList) ->
    case io_lib:printable_unicode_list(StrOrList) of
        true -> ["+", StrOrList, <<"\r\n">>];
        false -> ["*", integer_to_list(length(StrOrList)), <<"\r\n">>,
                  [write(X) || X <- StrOrList],
                 <<"\r\n">>]
    end.



%% redis:read(<<"*3\r\n:420\r\n$5\r\nhello\r\n+world\r\n">>).
%% {ok, [420, <<"hello">>, "world"], <<>>}
start(Port) ->
    Table = ets:new(redisdata, [set,public]),
    {ok, Listen} = gen_tcp:listen(Port, [binary,{reuseaddr,true}]),
    {ok, Socket} = gen_tcp:accept(Listen),
    inet:setopts(Socket,[{packet,0},binary,{active, true}]),
    serve(Table,Socket),
    gen_tcp:close(Listen).

serve(Table,Socket) ->
    receive
        {tcp, Socket, Data} ->
            {ok, [Cmd | Args] = FullCmd, _} = read(Data),
            io:format("GOT: ~p~nRAW: ~p~n", [FullCmd, Data]),
            Res = try
                      process(Table, Cmd, Args)
                  catch
                      throw:Msg -> {error, Msg};
                      error:E ->
                          io:format("ERROR ~p~n", [E]),
                          {error, "Internal error, see log"}
                  end,
            io:format("  -> ~p~n", [Res]),
            gen_tcp:send(Socket, write(Res)),
            serve(Table,Socket);
        {tcp_closed, Socket} ->
            ok
    end.

process(_, <<"COMMAND">>, [<<"DOCS">>]) -> "OK";
process(T, <<"SET">>, [Key, Val]) -> ets:insert(T, {Key,Val}), Val;
process(T, <<"GET">>, [Key]) ->
    [{_,Val}] = ets:lookup(T, Key),
    Val;
process(T, <<"INCR">>, [Key]) ->
    Old = case  ets:lookup(T, Key) of
              [{_,Val}] -> Val;
              _ -> <<"0">>
          end,
    New = try binary_to_integer(Old) + 1
          catch error:badarg -> throw("Not integer")
          end,
    ets:insert(T, {Key, integer_to_binary(New)}),
    New.
