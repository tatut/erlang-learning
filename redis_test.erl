-module(redis_test).
-include_lib("eunit/include/eunit.hrl").


%%%%%
%% Tests for RESP read/write

read(Bin) ->
    {ok, Val, <<>>} = redis:read(Bin),
    Val.

read_simple_string_test() ->
    "Hello World" = read(<<"+Hello World\r\n">>).

read_list_test() ->
    [420, <<"hello">>, "world"] =
        read(<<"*3\r\n:420\r\n$5\r\nhello\r\n+world\r\n">>).

read_cont(D1, D2) -> read_cont(D1,D2,<<>>).
read_cont(D1, D2, Rest) ->
    {incomplete,Cont} = redis:read(D1),
    %% Check that parsing with continuation retuns same
    %% data as parsing in one go
    {ok, Val, Rest} = Cont(D2),
    {ok, Val, Rest} = redis:read(iolist_to_binary([D1,D2])),
    Val.

read_continuation_test() ->
    %% Reading a binary string without enough data yields
    %% a continuation to be called when more data is received.
    <<"Hello Erlang">> = read_cont(<<"$12\r\nHello ">>, <<"Erlang\r\n">>).

read_continuation_arr_test() ->
    [420, 666, "end"] = read_cont(
                          <<"*3\r\n:420\r\n:">>,
                          <<"666\r\n+end\r\nREST">>,
                          <<"REST">>).
