-module(redis).
-export([read/1, write/1, start/1, demo/0]).

% A very minimal Redis wire protocol server that saves
% values to ETS.

%%%%%%%
% RESP Wire format
% https://redis.io/docs/reference/protocol-spec/

resp_split(Bin) ->
    binary:split(Bin,<<"\r\n">>).

-type incomplete() :: {incomplete, function()}.
-type complete() :: {ok, any(), binary()}.
-type parsed() :: complete() | incomplete().

%% Read Count amount of bytes, or if there are not enough,
%% return an incomplete with a continuation
-spec resp_read(binary(), integer(), function()) -> parsed().
resp_read(Bin, Count, Parser) ->
    Received = iolist_size(Bin),
    if
        Count =< Received ->
            %% We have enough
            {Read, Rest} = split_binary(iolist_to_binary(Bin), Count),
            {ok, Parser(Read), Rest};
        true ->
            %% We need more
            {incomplete,
             fun(MoreBin) ->
                     resp_read([Bin, MoreBin], Count, Parser)
             end}
    end.

-spec with_value(binary(), function()) -> parsed().
with_value(Bin, Cont) ->
    case resp_split(Bin) of
        [Bin] ->
            {incomplete,
             fun(MoreBin) ->
                     with_value(iolist_to_binary([Bin,MoreBin]), Cont)
             end};
        [Val,Rest] ->
            Cont(Val, Rest)
    end.

-spec read(binary()) -> parsed().
read(<<$+, Str/binary>>) ->
    with_value(Str,
               fun(S,Rest) ->
                       {ok, binary_to_list(S), Rest}
               end);
read(<<$-, Err/binary>>) ->
    with_value(Err,
               fun(E,Rest) ->
                       {ok, binary_to_list(E), Rest}
               end);
read(<<$:, Int/binary>>) ->
    with_value(Int,
               fun(I,Rest) ->
                       {ok, binary_to_integer(I), Rest}
               end);
read(<<$*, Arr/binary>>) ->
    with_value(
      Arr,
      fun (CountB, Rest) ->
              Count = binary_to_integer(CountB),
              if
                  %% array of -1 len is considered the null
                  Count == -1 -> {ok, null, Rest};
                  true -> read_array(Count, Rest, [])
              end
      end);
read(<<$$, Bulk/binary>>) ->
    %% prefixed bulk string, return as binary
    with_value(
      Bulk,
      fun(SizeB, Rest) ->
              Size = binary_to_integer(SizeB),
              resp_read(Rest, Size + 2, % include CRLF
                        fun(<<"Null\r\n">>) -> null;
                           (Bin) -> {BinStr,_} = split_binary(Bin, Size), BinStr
                        end)
      end).

-spec read_array(integer(), binary(), [any()]) -> [any()].
read_array(0, Rest, Arr) ->
    {ok, lists:reverse(Arr), Rest};
read_array(Items, Bin, Arr) ->
    case read(Bin) of
        {ok, Item, Rest} ->
            read_array(Items-1, Rest, [Item|Arr]);
        {incomplete, Cont} ->
            read_array_cont(Items,Arr,Cont)
    end.

-spec read_array_cont(integer(), [any()], function()) -> incomplete().
read_array_cont(Items,Arr,Cont) ->
    {incomplete,
     fun(MoreBin) ->
             case Cont(MoreBin) of
                 {incomplete, Cont1} ->
                     %io:format("still incomplete ~p~n", [Arr]),
                     read_array_cont(Items,Arr,Cont1);
                 {ok, Item, Rest} ->
                     %io:format("item complete ~p~n", [Item]),
                     read_array(Items-1, Rest, [Item|Arr])
             end
     end}.

-spec write(any()) -> iolist().
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

-spec start(integer()) -> function().
start(Port) ->
    Table = ets:new(redisdata, [set,public]),
    {ok, Listen} = gen_tcp:listen(Port, [binary,{reuseaddr,true}]),
    spawn(fun() -> accept(Table, Listen) end),
    %% return function to stop server
    fun() -> gen_tcp:close(Listen) end.

accept(Table,Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    inet:setopts(Socket,[{packet,0},binary,{active, true}]),
    spawn(fun() -> accept(Table,Listen) end),
    serve(Table,Socket).

demo()->
    observer:start(),
    start(6666).

serve(Table,Socket) -> serve(Table,Socket, fun read/1).
serve(Table,Socket,ReadFn) ->
    receive
        {tcp, Socket, Data} ->
            NextReadFn = process_cmds(Table, Data, Socket, ReadFn),
            serve(Table,Socket,NextReadFn);
        {tcp_closed, Socket} ->
            ok;
        Else -> io:format("got something weird: ~p~n", [Else])
    end.

process_cmds(Table, Data, Socket, ReadFn) ->
    case ReadFn(Data) of
        {incomplete, Cont} -> Cont;
        {ok, [Cmd | Args] = FullCmd, Rest} ->
            %%io:format("GOT: ~p~nRAW: ~p~n", [FullCmd, Data]),
            Res = try
                      process(Table, Cmd, Args)
                  catch
                      throw:Msg -> {error, Msg};
                      error:E ->
                          io:format("ERROR ~p~n", [E]),
                          {error, "Internal error, see log"}
                  end,
            %%io:format("  -> ~p~nRAW> ~p~n", [Res, iolist_to_binary(write(Res))]),
            gen_tcp:send(Socket, write(Res)),
            if size(Rest) == 0 ->
                    %% Read everything, just return
                    fun read/1;
               true ->
                    %% io:format("MORE TO READ ~p\n", [size(Rest)]),
                    %% Still more data to read, try reading next
                    process_cmds(Table, Rest, Socket, fun read/1)
            end
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
process(_, <<"CONFIG">>, [Cmd, Name]) ->
    [Name,
     case [Cmd, Name] of
         [<<"GET">>, <<"save">>] -> <<"3600 1 300 100 60 10000">>;
         [<<"GET">>, <<"appendonly">>] -> <<"no">>
     end];
process(T, <<"SET">>, [Key, Val]) -> ets:insert(T, {Key,Val}), "OK";
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
