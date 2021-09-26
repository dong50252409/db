%%%-------------------------------------------------------------------
%% @doc db top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(db_sup).

-behaviour(supervisor).

-export([start_link/0, start_child/3]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Tab, ModName, Options) ->
    ChildSpec = #{
        id => Tab,
        start => {db_agent_ets, start_link, [Tab, ModName, Options]}
    },
    supervisor:start_child(?MODULE, ChildSpec).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    ets:new(db_agent_ets, [set, public, named_table, {read_concurrency, true}]),
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
    ChildSpecs = get_poolboy_child_specs(),
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

get_poolboy_child_specs() ->
    [
        begin
            PoolArgs1 = [{name, {local, PoolName}}, {worker_module, mysql} | PoolArgs],
            poolboy:child_spec(PoolName, PoolArgs1, MySQLArgs)
        end
        || {PoolName, {PoolArgs, MySQLArgs}} <- application:get_env(db, mysql_pool, [])
    ].