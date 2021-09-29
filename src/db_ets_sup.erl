%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(db_ets_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 10,
        period => 5
    },
    DBETSSpec = get_db_ets_spec(),
    DBETSChildSupSpec = get_db_ets_child_sup_spec(),
    {ok, {SupFlags, [DBETSSpec, DBETSChildSupSpec]}}.

get_db_ets_spec() ->
    #{
        id => db_ets,
        start => {db_ets, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [db_ets]
    }.

get_db_ets_child_sup_spec() ->
    #{
        id => db_ets_child_sup,
        start => {db_ets_child_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [db_ets_child_sup]
    }.