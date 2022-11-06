-module(httputil).
-compile(export_all).
-record(url, {scheme = http :: atom(),
              port = 80 :: integer(),
              host :: string(),
              path = "/" :: string()}).
-record(response, {status :: integer(), message :: string(),
                   headers = #{} :: #{atom() => string()},
                   body :: binary()}).

% This is a toy http client library, don't actually use!
% Just a learning exercise.


parse_url(Url) ->
    Part = fun({Start,Len}) -> lists:sublist(Url, Start+1, Len) end,
    case re:run(Url, "http://([^/]*)(/.*)?$") of
        {match, [_, Host, Path]} ->
            #url{host=Part(Host), path=Part(Path)};
        {match, [_, Host]} ->
            #url{host=Part(Host)};
        _ -> {error, {badurl, Url}}
    end.

parse_headers_and_body(Bin) -> parse_headers_and_body(Bin, #{}).
parse_headers_and_body(Bin, Headers) ->
    [HeaderLine, Rest] = binary:split(Bin, <<"\r\n">>),
    case HeaderLine of
        % empty line, we are done with headers, return them
        <<>> -> {ok, Headers, Rest};

        % parse one header
        _ ->
            [Name,Value] = binary:split(HeaderLine, <<": ">>),
            parse_headers_and_body(Rest, maps:put(binary_to_atom(Name), binary_to_list(Value), Headers))
    end.

parse_response(Data) ->
    [StatusLine, HeadersAndBody] = binary:split(Data, <<"\r\n">>),
    case StatusLine of
        <<"HTTP/1.1 ", Code:3/binary, " ", Message/binary>> ->
            {ok, Headers,Body} = parse_headers_and_body(HeadersAndBody),
            #response{status = list_to_integer(binary_to_list(Code)),
                      message = binary_to_list(Message),
                      headers = Headers,
                      body = Body};

        % anything else, we don't understand
        _ -> {error, unrecognized_response, Data}
    end.

read_response(Socket, Read) ->
    receive
        {tcp, Socket, Data} ->
            read_response(Socket, [Data,Read]);
        {tcp_closed,Socket} ->
            parse_response(list_to_binary(lists:reverse(Read)))
    after
        30000 -> {error, timeout_reading_response}
    end.

http_get(Socket, Host, Path) ->
    ok = gen_tcp:send(Socket,["GET ", Path, " HTTP/1.1\r\n",
                              "Connection: close\r\n",
                              "Host: ", Host, "\r\n\r\n"]),
    %ok = gen_tcp:close(Socket),
    read_response(Socket, []).

get(Url) ->
    case parse_url(Url) of
        #url{scheme=http, host=Host, path=Path, port=Port} ->
            {ok,Socket} = gen_tcp:connect(Host,Port,[binary,{packet,0}]),
            http_get(Socket, Host, Path);

        {error, Why} ->
            io:format("ei yhteytt",[]),
            Why
    end.
