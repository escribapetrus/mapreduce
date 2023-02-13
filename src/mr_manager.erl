-module(mr_manager).
-behaviour(gen_server).

-export([start_link/2, run/1, put_keys/2, put_completed_worker/1]).
-export([init/1, handle_cast/2, handle_call/3]).

-define(CRYPTO_ALG, sha256). 
-record(mr_manager, {mapped = 0,
                     mappers,
                     reduced = 0,
                     reducers
                    }).

%% API
start_link(NumMap, NumReduce) -> gen_server:start_link({local, ?MODULE}, ?MODULE, {NumMap, NumReduce}, []).

run(Type) -> gen_server:cast(?MODULE, {run, Type}).

put_keys(Keys, Type) -> gen_server:call(?MODULE, {put_keys, Keys, Type}).

put_completed_worker(Type) -> gen_server:cast(?MODULE, {put_completed_worker, Type}).

%% Callbacks
init({NumMap, NumReduce}) ->
    {ok, #mr_manager{mappers = NumMap, reducers = NumReduce}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({run, map}, #mr_manager{mapped = 0} = Data) ->
    lists:foreach(fun(Pid) -> mr_worker:run(Pid) end, map_sup:list_pids()),    
    {noreply, Data};
handle_cast({run, map}, Data) ->
    io:format("Map is already running.~n", []),
    {noreply, Data};
handle_cast({run, reduce}, #mr_manager{reduced = 0} = Data) ->
    lists:foreach(fun(Pid) -> mr_worker:run(Pid) end, reduce_sup:list_pids()),   
    {noreply, Data};
handle_cast({run, reduce}, Data) ->
    io:format("Reduce is already running.~n", []),
    {noreply, Data};

handle_cast({put_keys, [H | _] = Keys, map}, Data) when is_list(H) ->
    lists:foreach(fun(K) -> put_keys(K, map) end, Keys),
    {noreply, Data};
handle_cast({put_keys, Key, map}, Data) ->   
    MapCount = map_sup:count_children(),
    Target = lists:nth(target(Key, MapCount) + 1, map_sup:list_pids()),
    mr_worker:put_key(Target, Key),     
    {noreply, Data};
handle_cast({put_keys, [H |_ ] = Keys, reduce}, Data) when is_list(H) ->
    lists:foreach(fun(K) -> put_keys(K, reduce) end, Keys),
    {noreply, Data};
handle_cast({put_keys, Key, reduce}, Data) ->
    ReduceCount = reduce_sup:count_children(),
    Target = lists:nth(target(Key, ReduceCount) + 1, reduce_sup:list_pids()),
    mr_worker:put_key(Target, Key),
    {noreply, Data};

handle_cast({put_completed_worker, map}, #mr_manager{mapped = N, mappers = T } = Data) ->
    Mapped = N + 1,

    if Mapped == T ->
            run(reduce),
            {noreply, Data#mr_manager{mapped = Mapped}};
       true -> 
            {noreply, Data#mr_manager{mapped = Mapped}}
    end;
handle_cast({put_completed_worker, reduce}, #mr_manager{reduced = N, reducers = T } = Data) ->
    Reduced = N + 1,

    if Reduced == T ->
            io:format("finished processing", []),
            {noreply, Data#mr_manager{mapped = 0, reduced = 0}};
       true -> 
            {noreply, Data#mr_manager{reduced = Reduced}}
    end.

-spec target(Key :: string() | binary(), N :: integer()) -> integer().
target(Key, N) ->
    Int = crypto:bytes_to_integer(crypto:hash(?CRYPTO_ALG, Key)),
    Int rem N.
