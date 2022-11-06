-module(ring).
-compile(export_all).


%% Send a message around a ring of processes
%% logs the time it took

ring_handler() ->
    receive {First, NextProc} ->
            ring_handler(First, NextProc)
    end.
ring_handler(First, NextProc) ->
    receive
        {Me, Time, Msg} when is_pid(Me) andalso Me =:= self() ->
            io:format("[~p] Got msg back around the ring ~p (~pms) ~n",
                      [self(), Msg, (erlang:system_time()-Time)/1000000]),
            ring_handler(First,NextProc);
        Msg when First ->
            io:format("[~p] Start send ~p to ~p~n", [self(), Msg, NextProc]),
            NextProc ! {self(), erlang:system_time(), Msg},
            ring_handler(First,NextProc);
        Msg ->
            %io:format("[~p] Passing thru ~p to ~p~n", [self(), Msg, NextProc]),
            NextProc ! Msg,
            ring_handler(First,NextProc)
    end.

ring(N) ->
    Initial = spawn(fun ring_handler/0),
    Initial ! {true, ring(Initial, N-1)},
    Initial.

ring(Initial,N) ->
    Next = spawn(fun ring_handler/0),
    case N of
        0 -> Next ! {false, Initial};
        _ -> Next ! {false, ring(Initial, N-1)}
    end,
    Next.
