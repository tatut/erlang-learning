-module(proletariat).
-compile(export_all).
-record(workstate, {final_result_pid,
                    work_collected = false,
                    work = [],
                    results = #{},
                    workers,
                    in_progress = 0}).

% a process worker experiment with silly names


% bourgeoisie process is the one controlling means of production (gives out work)
%
% NumProles controls concurrency, how many proletariat workers are spawned.
% Proletariat send results to bourgeoisie with {'RESULT', worker_pid, work_id, work_result}.
%
% WorkerFun is the function the proletariat will execute. It receives
% the {bourgeoisie_pid, work_data} and sends back the work result.
%
% OnComplete will be called with the end result of doing all the work
%
% returns function to submit more work, which must be called with
% {work_id, work_data} or 'DONE' to signal all work is done.

bourgeoisie(NumProles, WorkerFun, OnComplete) ->
    Bourgeoisie = spawn(piiskuri(NumProles, WorkerFun)),
    Final = spawn(fun() -> receive Result -> OnComplete(Result) end end),
    Bourgeoisie ! {'FINAL', Final},
    fun ({WorkId,WorkData}) -> Bourgeoisie ! {'WORK', WorkId, WorkData};
        ('DONE') -> Bourgeoisie ! 'DONE'
    end.

piiskuri(NumProles, WorkerFun) ->
    % spawn worker processes and spawn the
    Proles = lists:map(
               % map proletariat pid to its state (initially IDLE)
               fun(_) -> duunari(WorkerFun) end,
               lists:seq(1, NumProles)),
    fun () ->
            io:format("Started piiskuri with ~p~n", [NumProles]),
            piiskuri(#workstate{workers=Proles})
    end.

duunari(Duuni) ->
    spawn(fun() -> duunari_loop(Duuni) end).

duunari_loop(Duuni) ->
    receive
        {PiiskuriPid, {WorkId, WorkData}} ->
            io:format("tehdaan hommia ~p datalla ~p~n", [WorkId, WorkData]),
            PiiskuriPid ! {'RESULT', self(), WorkId, Duuni(WorkData)},
            duunari_loop(Duuni)
    end.

piiskuri(State) ->
    receive
        {'FINAL', Pid} ->
            %% mark the Pid to send final results
            piiskuri(State#workstate{final_result_pid = Pid});
        {'WORK', WorkId, WorkData} ->
            %% mark new work as found
            piiskuri(State#workstate{
                       work =
                           [{WorkId,WorkData} | State#workstate.work]});
        {'RESULT', WorkerPid, WorkId, WorkResult} ->
            %% mark result of a single work
            piiskuri(State#workstate{
                       %% Add result to result map
                       results = maps:put(WorkId, WorkResult, State#workstate.results),
                       %% Set worker as idle
                       workers = [WorkerPid | State#workstate.workers],

                       %% Reduce in progress count
                       in_progress = State#workstate.in_progress - 1});
        'DONE' ->
            %% mark all work as collected
            io:format("hommat done ~p~n", [State#workstate{work_collected = true}]),
            piiskuri(State#workstate{work_collected = true})
    after
        10 ->
            case State of
                %% No more work and no in progress, send final result
                #workstate{work=[], work_collected=true, in_progress=0,
                           final_result_pid = Pid,
                           results = Results} ->
                    Pid ! Results;

                %% Have some work to do
                #workstate{work=[Work|OtherWork],
                           workers=[Worker|OtherWorkers],
                           in_progress=InProgress} ->
                    Worker ! {self(), Work},
                    piiskuri(State#workstate{
                               work = OtherWork,
                               workers = OtherWorkers,
                               in_progress = InProgress + 1});
                %% waiting for work
                _ -> piiskuri(State)
            end
    end.

%%
%% Work = proletariat:bourgeoisie(20, fun(X) -> X * 2 end, fun(Res) -> io:format("kaikki valmista: ~p~n", [Res]) end).
%% Work({kymp, 10}).
%% Work({kolmekaks, 32}).
%% Work('DONE').
%% -> prints results
