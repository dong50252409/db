%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(db_ets_child_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/4, stop/1]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Tab, DBPool, ModName, Options) ->
    supervisor:start_child(?MODULE, [{Tab, DBPool, ModName, Options}]).

stop(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 0,
        period => 1
    },
    ChildSpec = #{
        id => db_ets_child,
        start => {db_ets_child, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [db_ets_child]
    },
    {ok, {SupFlags, [ChildSpec]}}.