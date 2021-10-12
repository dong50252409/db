%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% ETS表持久化代理
%%%
%%% @end
%%% Created : 27. 9月 2021 16:47
%%%-------------------------------------------------------------------
-module(db_ets).
-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/0, new/2, reg/4, reg_select/5, init_insert/2, flush/1, flush/2, pull/1, pull/2
]).

%% ETS_API
-export([
    delete/1, delete/2, delete_all_objects/1, delete_object/2, select_delete/2, match_delete/2, take/2,
    insert/2, insert_new/2,
    select_replace/2, update_counter/3, update_counter/4, update_element/3
]).


%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).


-define(ETS_DB_ETS_INFO, ets_db_ets_info).
-define(CHECK_TAB_LIST, [owner, type, keypos]).
-define(CHECK_OPTIONS_LIST, [mode, flush_interval]).

-export_type([option/0]).
-type option() :: {mode, auto|{callback, module()}}|{flush_interval, timeout()}.

%%-------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% 注册ETS表，自动记录更改，并更新数据，仅支持set，ordered_set类型，不支持bag，duplicate_bag类型
%% @end
%%------------------------------------------------------------------------------
-spec reg(Tab :: ets:tab(), DBPool :: db_mysql:db_pool(), ModName :: module(), Options :: [option()]) -> ok.
reg(Tab, DBPool, ModName, Options) ->
    check_reg_conditions(Tab, Options),
    {ok, Pid} = gen_server:call(?MODULE, {reg, Tab, DBPool, ModName, Options}),
    ets:setopts(Tab, {heir, Pid, nothing}),
    ok.

-spec reg_select(Tab :: ets:tab(), DBPool :: db_mysql:db_pool(), ModName :: module(),
    Conditions :: [db_mysql:condition()], Options :: [option()]) -> ok.
reg_select(Tab, DBPool, ModName, Conditions, Options) ->
    check_reg_conditions(Tab, Options),
    {ok, Pid} = gen_server:call(?MODULE, {reg, Tab, DBPool, ModName, Options}),
    TableName = ModName:get_table_name(),
    {ok, _Columns, Rows} = db_mysql:select(DBPool, TableName, Conditions),
    RecordList = [ModName:as_record(Record) || Record <- Rows],
    init_insert(Tab, RecordList),
    ets:setopts(Tab, {heir, Pid, nothing}),
    ok.

check_reg_conditions(Tab, Options) ->
    case check_tab(Tab, ?CHECK_TAB_LIST) of
        true ->
            check_options(Options, ?CHECK_OPTIONS_LIST);
        Error ->
            Error
    end.

check_tab(Tab, [owner | T]) ->
    case self() =/= ets:info(Tab, owner) of
        true ->
            throw({error, ets_owner});
        false ->
            check_tab(Tab, T)
    end;
check_tab(Tab, [type | T]) ->
    case ets:info(Tab, type) of
        Type when Type =:= bag;Type =:= duplicate_bag ->
            throw({error, ets_type});
        _ ->
            check_tab(Tab, T)
    end;
check_tab(Tab, [keypos | T]) ->
    case ets:info(Tab, keypos) of
        1 ->
            throw({error, ets_keypos});
        _ ->
            check_tab(Tab, T)
    end;
check_tab(_Tab, []) ->
    true.

check_options(Options, [mode | T]) ->
    case proplists:get_value(mode, Options) of
        auto ->
            check_options(Options, T);
        {callback, Mod} when is_atom(Mod) ->
            case is_process_alive(erlang:whereis(Mod)) of
                true ->
                    check_options(Options, T);
                false ->
                    throw({error, mode_process_died})
            end;
        {callback, Pid} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    check_options(Options, T);
                false ->
                    throw({error, mode_process_died})
            end;
        _ ->
            throw({error, mode_undefined})
    end;
check_options(Options, [flush_interval | T]) ->
    case proplists:get_value(flush_interval, Options) of
        undefined ->
            check_options(Options, T);
        Timeout when is_integer(Timeout), Timeout > 0 ->
            check_options(Options, T);
        _ ->
            throw({error, flush_interval_undefined})
    end;
check_options(_Options, []) ->
    true.
%%------------------------------------------------------------------------------
%% @doc
%% 初始化插入数据，数据将被标记为nothing状态
%% @end
%%------------------------------------------------------------------------------
-spec init_insert(ets:tab(), tuple()|list()) -> true.
init_insert(Tab, Objects) when is_list(Objects) ->
    init_insert_1(Tab, Objects);
init_insert(Tab, Object) when is_tuple(Object) ->
    init_insert_1(Tab, [Object]).

init_insert_1(Tab, Objects) ->
    ets:insert(Tab, Objects),
    KeyMaps = maps:from_list([{get_keys(Tab, Object), 0} || Object <- Objects]),
    gen_server:cast(get_pid(Tab), {init_insert, KeyMaps}),
    true.

%%------------------------------------------------------------------------------
%% @doc
%% 主动同步脏数据到数据库，如果指定了flush_interval选项，则会重新启动定时器
%% @end
%%------------------------------------------------------------------------------
-spec flush(Tab :: ets:tab()) -> ok|error.
flush(Tab) ->
    gen_server:call(get_pid(Tab), flush).

-spec flush(Tab :: ets:tab(), Timeout :: timeout()|infinity) -> ok|error.
flush(Tab, Timeout) ->
    gen_server:call(get_pid(Tab), flush, Timeout).

%%------------------------------------------------------------------------------
%% @doc
%% 拉取脏数据的键值，如果指定了flush_interval选项，则会重新启动定时器
%% @end
%%------------------------------------------------------------------------------
-spec pull(Tab :: ets:tab()) -> {InsertKeyList :: list(), UpdateKeyList :: list(), DeleteKeyList :: list()}|error.
pull(Tab) ->
    gen_server:call(get_pid(Tab), pull).

-spec pull(Tab :: ets:tab(), Timeout :: timeout()|infinity) -> {InsertKeyList :: list(), UpdateKeyList :: list(), DeleteKeyList :: list()}|error.
pull(Tab, Timeout) ->
    gen_server:call(get_pid(Tab), pull, Timeout).
%%--------------------------------------------------------------------
%% ETS API
%%--------------------------------------------------------------------

-spec new(Tab :: ets:tab(), Options :: proplists:proplist()) -> ets:tab().
new(Tab, Options) ->
    {ETSOptions, DBETSOptions} = get_options(Options, [db_pool, mod_name, conditions, options]),
    Tab = ets:new(Tab, ETSOptions),
    case DBETSOptions of
        [DBPool, ModName, OPList] ->
            ok = reg(Tab, DBPool, ModName, OPList);
        [DBPool, ModName, Conditions, OPList] ->
            ok = reg_select(Tab, DBPool, ModName, Conditions, OPList)
    end,
    Tab.

get_options(Options, [Key | T]) ->
    {ETSOptions, DBETSOptions} = get_options(proplists:delete(Key, Options), T),
    {ETSOptions, [proplists:get_value(Key, Options, [])] ++ DBETSOptions};
get_options(Options, []) ->
    {Options, []}.

-spec delete(ets:tab()) -> true.
delete(Tab) ->
    Pid = get_pid(Tab),
    gen_server:cast(Pid, delete),
    true = ets:delete(Tab),
    db_ets_child_sup:stop(Pid),
    true.

-spec delete(ets:tab(), term()) -> true.
delete(Tab, Key) ->
    true = ets:delete(Tab, Key),
    gen_server:cast(get_pid(Tab), {delete, [Key]}),
    true.

-spec delete_all_objects(ets:tab()) -> true.
delete_all_objects(Tab) ->
    true = ets:delete_all_objects(Tab),
    gen_server:cast(get_pid(Tab), delete_all),
    true.

-spec delete_object(ets:tab(), tuple()) -> true.
delete_object(Tab, Object) ->
    true = ets:delete_object(Tab, Object),
    gen_server:cast(get_pid(Tab), {delete, [get_keys(Tab, Object)]}),
    true.

-spec select_delete(ets:tab(), ets:match_spec()) -> non_neg_integer().
select_delete(Tab, [{'_', [], [true]}] = MatchSpec) ->
    NumDeleted = ets:select_delete(Tab, MatchSpec),
    gen_server:cast(get_pid(Tab), delete_all),
    NumDeleted;
select_delete(Tab, [{Pattern, Condition, _}] = MatchSpec) ->
    KeyPos = ets:info(Tab, keypos),
    MS = [{Pattern, Condition, [{element, KeyPos, '$_'}]}],
    Keys = ets:select(Tab, MS),
    NumDeleted = ets:select_delete(Tab, MatchSpec),
    gen_server:cast(get_pid(Tab), {delete, Keys}),
    NumDeleted.

-spec match_delete(ets:tab(), ets:match_pattern()) -> non_neg_integer().
match_delete(Tab, Pattern) ->
    select_delete(Tab, [{Pattern, [], [true]}]).

-spec take(ets:tab(), term()) -> [tuple()].
take(Tab, Key) ->
    Objects = ets:take(Tab, Key),
    gen_server:cast(get_pid(Tab), {delete, [get_keys(Tab, Object) || Object <- Objects]}),
    Objects.

-spec insert(ets:tab(), tuple() | [tuple()]) -> true.
insert(_Tab, []) ->
    true;
insert(Tab, Objects) when is_list(Objects) ->
    insert_1(Tab, Objects);
insert(Tab, Object) when is_tuple(Object) ->
    insert_1(Tab, [Object]).

insert_1(Tab, Objects) ->
    ets:insert(Tab, Objects),
    gen_server:cast(get_pid(Tab), {insert, [get_keys(Tab, Object) || Object <- Objects]}),
    true.

-spec insert_new(ets:tab(), tuple() | [tuple()]) -> true.
insert_new(_Tab, []) ->
    true;
insert_new(Tab, Objects) when is_list(Objects) ->
    insert_new_1(Tab, Objects);
insert_new(Tab, Object) when is_tuple(Object) ->
    insert_new_1(Tab, [Object]).

insert_new_1(Tab, Objects) ->
    case ets:insert_new(Tab, Objects) of
        true ->
            gen_server:cast(get_pid(Tab), {insert, [get_keys(Tab, Object) || Object <- Objects]}),
            true;
        false ->
            false
    end.

-spec select_replace(ets:tab(), ets:match_spec()) -> non_neg_integer().
select_replace(Tab, [{Pattern, Condition, _}] = MatchSpec) ->
    KeyPos = ets:info(Tab, keypos),
    MS = [{Pattern, Condition, [{element, KeyPos, '$_'}]}],
    Keys = ets:select(Tab, MS),
    NumReplaced = ets:select_replace(Tab, MatchSpec),
    gen_server:cast(get_pid(Tab), {insert, Keys}),
    NumReplaced.

-spec update_counter(ets:tab(), term(), UpdateOp | [UpdateOp]) -> Result | [Result] when
    UpdateOp :: {Pos, Incr} | {Pos, Incr, Threshold, SetValue},
    Pos :: integer(),
    Incr :: integer(),
    Threshold :: integer(),
    SetValue :: integer(),
    Result :: integer().
update_counter(Tab, Key, UpdateOp) ->
    Result = ets:update_counter(Tab, Key, UpdateOp),
    gen_server:cast(get_pid(Tab), {update, [Key]}),
    Result.

-spec update_counter(ets:tab(), term(), UpdateOp | [UpdateOp], tuple()) -> Result | [Result] when
    UpdateOp :: {Pos, Incr} | {Pos, Incr, Threshold, SetValue},
    Pos :: integer(),
    Incr :: integer(),
    Threshold :: integer(),
    SetValue :: integer(),
    Result :: integer().
update_counter(Tab, Key, UpdateOp, Default) ->
    Result = ets:update_counter(Tab, Key, UpdateOp, Default),
    gen_server:cast(get_pid(Tab), {update, [Key]}),
    Result.

-spec update_element(atom(), term(), {Pos, Value} | [{Pos, Value}]) -> boolean() when
    Pos :: pos_integer(),
    Value :: term().
update_element(Tab, Key, ElementSpec) ->
    case ets:update_element(Tab, Key, ElementSpec) of
        true ->
            gen_server:cast(get_pid(Tab), {update, [Key]}),
            true;
        false ->
            false
    end.

get_pid(Tab) ->
    ets:lookup_element(?ETS_DB_ETS_INFO, Tab, 2).

get_keys(Tab, Object) ->
    Keypos = ets:info(Tab, keypos),
    erlang:element(Keypos, Object).
%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    ets:new(?ETS_DB_ETS_INFO, [set, named_table, {read_concurrency, true}]),
    {ok, #{}}.

handle_call(Request, From, State) ->
    try
        do_call(Request, From, State)
    catch
        Err:Reason:Stacktrace ->
            ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
            {reply, error, State}
    end.

handle_cast(Request, State) ->
    try
        do_cast(Request, State)
    catch
        Err:Reason:Stacktrace ->
            ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
            {noreply, State}
    end.

handle_info(Request, State) ->
    try
        do_info(Request, State)
    catch
        Err:Reason:Stacktrace ->
            ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
            {noreply, State}
    end.

do_call({reg, Tab, DBPool, ModName, Options}, _From, State) ->
    Reply = do_reg(Tab, DBPool, ModName, Options),
    {reply, Reply, State};

do_call(_Request, _From, State) ->
    {reply, ok, State}.

do_cast(_Request, State) ->
    {noreply, State}.

do_info({'DOWN', MonitorRef, _Type, _Object, _Info}, State) ->
    [{_, Tab}] = ets:take(?ETS_DB_ETS_INFO, MonitorRef),
    ets:delete(?ETS_DB_ETS_INFO, Tab),
    {noreply, State};

do_info(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
do_reg(Tab, DBPool, ModName, Options) ->
    case db_ets_child_sup:start_child(Tab, DBPool, ModName, Options) of
        {ok, Pid} ->
            Ref = erlang:monitor(process, Pid),
            ets:insert(?ETS_DB_ETS_INFO, {Tab, Pid}),
            ets:insert(?ETS_DB_ETS_INFO, {Ref, Tab}),
            {ok, Pid};
        Err ->
            Err
    end.
