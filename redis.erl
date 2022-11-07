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
%% FIXME: large message will not be delivered all at once...
%% if binary is too small, we need to return a continuation function
%% that will receive more data
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

write(null) ->
    <<"*-1\r\n">>;
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
                  [write(X) || X <- StrOrList]]
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
            ok = process_cmds(Table, Data, Socket),
            serve(Table,Socket);
        {tcp_closed, Socket} ->
            ok
    end.

process_cmds(Table, Data, Socket) ->
    {ok, [Cmd | Args] = FullCmd, Rest} = read(Data),
    io:format("GOT: ~p~nRAW: ~p~n", [FullCmd, Data]),
    Res = try
              process(Table, Cmd, Args)
          catch
              throw:Msg -> {error, Msg};
              error:E ->
                  io:format("ERROR ~p~n", [E]),
                  {error, "Internal error, see log"}
          end,
    io:format("  -> ~p~nRAW> ~p~n", [Res, iolist_to_binary(write(Res))]),
    gen_tcp:send(Socket, write(Res)),
    if size(Rest) == 0 -> ok;
       true -> process_cmds(Table, Rest, Socket)
    end.

coerce_int(Bin) ->
    try binary_to_integer(Bin)
    catch error:badarg -> throw("Not integer")
    end.

update(T, Key, Default, UpdateFn) ->
    Old = case ets:lookup(T, Key) of
              [{_, Val}] -> Val;
              _ -> Default
          end,
    {New,Ret} = UpdateFn(Old),
    ets:insert(T, {Key, New}),
    Ret.

incr(T, Key, By) ->
    update(T, Key, <<"0">>,
           fun(Bin) ->
                   New = coerce_int(Bin) + By,
                   {integer_to_binary(New), New}
           end).

hupdate(T, Key, Field, Default, UpdateFn) ->
    update(T, Key, #{},
           fun(Map) when is_map(Map) ->
                   NewVal = UpdateFn(maps:get(Field, Map, Default)),
                   NewMap = maps:put(Field, NewVal, Map),
                   {NewMap, NewVal};
              (_) -> throw("Not a map")
           end).

process(_, <<"COMMAND">>, [<<"DOCS">>]) -> "OK";
process(_, <<"PING">>, []) -> "PONG";
process(_, <<"PING">>, [Msg]) -> Msg;
%process(_, <<"CONFIG">>, [Cmd, Name]) ->
%    [Name,
%     case [Cmd, Name] of
%         [<<"GET">>, <<"save">>] -> "3600 1 300 100 60 10000";
%         [<<"GET">>, <<"appendonly">>] -> "no"
%     end];
process(T, <<"SET">>, [Key, Val]) -> ets:insert(T, {Key,Val}), Val;
process(T, <<"GET">>, [Key]) ->
    case ets:lookup(T, Key) of
        [{_,Val}] -> Val;
        [] -> null
    end;
process(T, <<"INCR">>, [Key]) ->
    incr(T, Key, 1);
process(T, <<"INCRBY">>, [Key, By]) ->
    incr(T, Key, coerce_int(By));
process(T, <<"DECR">>, [Key]) ->
    incr(T, Key, -1);
process(T, <<"DECRBY">>, [Key, By]) ->
    incr(T, Key, -1 * coerce_int(By));
process(T, <<"HSET">>, [Key, Field, Value]) ->
    hupdate(T, Key, Field, ignore,
            fun(_) -> Value end);
process(T, <<"HGET">>, [Key, Field]) ->
    case ets:lookup(T, Key) of
        [{_,Val}] -> maps:get(Field, Val, null);
        [] -> null
    end;
process(T, <<"HINCRBY">>, [Key, Field, By]) ->
    hupdate(T, Key, Field, <<"0">>, fun(N) -> integer_to_binary(coerce_int(N) + coerce_int(By)) end);
process(T, <<"HKEYS">>, [Key]) ->
    case ets:lookup(T, Key) of
        [{_,Val}] -> maps:keys(Val);
        [] -> []
    end;
process(T, <<"DEL">>, Keys) ->
    lists:foldl(fun(K,A) ->
                        A + case ets:lookup(T, K) of
                                [] -> 0;
                                _ -> ets:delete(T, K), 1
                            end
                end, 0, Keys).
