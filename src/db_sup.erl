%%%-------------------------------------------------------------------
%% @doc db top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(db_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
    PoolBoyChildSpecs = get_poolboy_child_specs(),
    DBETSSupSpec = get_db_ets_sup_spec(),
    {ok, {SupFlags, PoolBoyChildSpecs ++ [DBETSSupSpec]}}.

%% internal functions
get_poolboy_child_specs() ->
    [
        begin
            PoolArgs1 = [{name, {local, PoolName}}, {worker_module, mysql} | PoolArgs],
            poolboy:child_spec(PoolName, PoolArgs1, MySQLArgs)
        end
        || {PoolName, {PoolArgs, MySQLArgs}} <- application:get_env(db, mysql_pool, [])
    ].

get_db_ets_sup_spec() ->
    #{
        id => db_ets_sup,
        start => {db_ets_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [db_ets_sup]
    }.
