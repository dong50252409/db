%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% ETS表持久化代理
%%%
%%% 通过parse_transform的方式将调用ets模块的字句替换为db_agent_ets模块，以下函数调用将会被替换
%%%
%%% delete/1, delete/2, delete_all_objects/1, delete_object/2, select_delete/2, match_delete/2, take/2,
%%% insert/2, insert_new/2,
%%% select_replace/2, update_counter/3, update_counter/4, update_element/3
%%%
%%% @end
%%% Created : 10. 9月 2021 17:30
%%%-------------------------------------------------------------------
-module(db_agent_ets).

-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/0, reg/3, init_insert/2, flush/1, pull/1
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
    handle_info/2]).


-define(CHECK_OPTIONS_LIST, [flush_interval, callback, db]).
-define(CHECK_TAB_LIST, [type, protection, keypos]).

-define(DIRTY_NOTHING, 0). % 无状态
-define(DIRTY_INSERT, 1).   % 插入状态
-define(DIRTY_UPDATE, 2).   % 更新状态
-define(DIRTY_DELETE, 3).   % 删除状态

-type option() :: {flush_interval, timeout()}|{db, db_mysql:db_name()}|{callback, module()|pid()}|heir.

%%-------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% 注册ETS表，自动记录更改，并更新数据，仅支持set，ordered_set类型，不支持bag，duplicate_bag类型
%% @end
%%------------------------------------------------------------------------------
-spec reg(Tab :: ets:tab(), ModName :: module(), Options :: [option()]) -> ok|{error, term()}.
reg(Tab, ModName, Options) ->
    case check_options(Options, ?CHECK_OPTIONS_LIST) of
        true ->
            case check_tab(Tab, ?CHECK_TAB_LIST) of
                true ->
                    proplists:is_defined(heir, Options) andalso ets:setopts(Tab, {heir, erlang:whereis(?MODULE), nothing}),
                    ok = gen_server:call(?MODULE, {reg, Tab, ModName, self(), Options}, infinity);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

check_options(Options, [flush_interval | T]) ->
    case proplists:get_value(timeout, Options) of
        undefined ->
            check_options(Options, T);
        Timeout when is_integer(Timeout), Timeout > 0 ->
            check_options(Options, T);
        _ ->
            {error, flush_interval_error}
    end;
check_options(Options, [db | T]) ->
    case proplists:get_value(db, Options) of
        undefined ->
            check_options(Options, T);
        DB when is_atom(DB);is_pid(DB);is_tuple(DB) ->
            check_options(Options, T);
        _ ->
            {error, db_error}
    end;
check_options(Options, [callback | T]) ->
    case proplists:get_value(callback, Options) of
        undefined ->
            check_options(Options, T);
        Callback when is_atom(Callback);is_pid(Callback) ->
            case Callback of
                Callback when is_atom(Callback) ->
                    Pid = erlang:whereis(Callback);
                Callback when is_pid(Callback) ->
                    Pid = Callback
            end,
            case erlang:is_process_alive(Pid) of
                true ->
                    check_options(Options, T);
                false ->
                    {error, callback_process_nonexistent}
            end;
        _ ->
            {error, callback_error}
    end;
check_options(_Options, []) ->
    true.

check_tab(Tab, [type | T]) ->
    case ets:info(Tab, type) of
        Type when Type =:= bag;Type =:= duplicate_bag ->
            {error, ets_type};
        _ ->
            check_tab(Tab, T)
    end;
check_tab(Tab, [protection | T]) ->
    case ets:info(Tab, protection) of
        private ->
            {error, ets_private};
        _ ->
            check_tab(Tab, T)
    end;
check_tab(Tab, [keypos | T]) ->
    case ets:info(Tab, keypos) of
        1 ->
            {error, ets_keypos};
        _ ->
            check_tab(Tab, T)
    end;
check_tab(_Tab, []) ->
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
    KeyMap = maps:from_list([{get_keys(Tab, Object), ?DIRTY_NOTHING} || Object <- Objects]),
    gen_server:cast(?MODULE, {init_insert, Tab, KeyMap}),
    true.

%%------------------------------------------------------------------------------
%% @doc
%% 主动同步脏数据到数据库，如果指定了<b>flush_interval</b>选项，则会重新启动定时器
%% @end
%%------------------------------------------------------------------------------
-spec flush(Tab :: ets:tab()) -> ok.
flush(Tab) ->
    gen_server:call(?MODULE, {flush, Tab}).

%%------------------------------------------------------------------------------
%% @doc
%% 拉取脏数据的键值，如果指定了<b>flush_interval</b>选项，则会重新启动定时器
%% @end
%%------------------------------------------------------------------------------
-spec pull(Tab :: ets:tab()) -> {InsertKeyList :: list(), UpdateKeyList :: list(), DeleteKeyList :: list()}.
pull(Tab) ->
    gen_server:call(?MODULE, {pull, Tab}).

%%--------------------------------------------------------------------
%% ETS API
%%--------------------------------------------------------------------
-spec delete(ets:tab()) -> true.
delete(Tab) ->
    true = ets:delete(Tab),
    gen_server:cast(?MODULE, {delete, Tab}),
    true.

-spec delete(ets:tab(), term()) -> true.
delete(Tab, Key) ->
    true = ets:delete(Tab, Key),
    gen_server:cast(?MODULE, {delete, Tab, [Key]}),
    true.

-spec delete_all_objects(ets:tab()) -> true.
delete_all_objects(Tab) ->
    true = ets:delete_all_objects(Tab),
    gen_server:cast(?MODULE, {delete_all, Tab}),
    true.

-spec delete_object(ets:tab(), tuple()) -> true.
delete_object(Tab, Object) ->
    true = ets:delete_object(Tab, Object),
    gen_server:cast(?MODULE, {delete, Tab, [get_keys(Tab, Object)]}),
    true.

-spec select_delete(ets:tab(), ets:match_spec()) -> non_neg_integer().
select_delete(Tab, [{Pattern, Condition, _}] = MatchSpec) ->
    KeyPos = ets:info(Tab, keypos),
    MS = [{Pattern, Condition, [{element, KeyPos, '$_'}]}],
    Keys = ets:select(Tab, MS),
    NumDeleted = ets:select_delete(Tab, MatchSpec),
    gen_server:cast(?MODULE, {delete, Tab, Keys}),
    NumDeleted.

-spec match_delete(ets:tab(), ets:match_pattern()) -> non_neg_integer().
match_delete(Tab, Pattern) ->
    select_delete(Tab, [{Pattern, [], [true]}]).

-spec take(ets:tab(), term()) -> [tuple()].
take(Tab, Key) ->
    Objects = ets:take(Tab, Key),
    gen_server:cast(?MODULE, {delete, Tab, [get_keys(Tab, Object) || Object <- Objects]}),
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
    gen_server:cast(?MODULE, {insert, Tab, [get_keys(Tab, Object) || Object <- Objects]}),
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
            gen_server:cast(?MODULE, {insert, Tab, [get_keys(Tab, Object) || Object <- Objects]}),
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
    gen_server:cast(?MODULE, {insert, Tab, Keys}),
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
    gen_server:cast(?MODULE, {update, Tab, [Key]}),
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
    gen_server:cast(?MODULE, {update, Tab, [Key]}),
    Result.

-spec update_element(atom(), term(), {Pos, Value} | [{Pos, Value}]) -> boolean() when
    Pos :: pos_integer(),
    Value :: term().
update_element(Tab, Key, ElementSpec) ->
    case ets:update_element(Tab, Key, ElementSpec) of
        true ->
            gen_server:cast(?MODULE, {update, Tab, [Key]}),
            true;
        false ->
            false
    end.

get_keys(Tab, Object) ->
    Keypos = ets:info(Tab, keypos),
    erlang:element(Keypos, Object).
%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
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

do_call({reg, Tab, ModName, Pid, Options}, _From, State) ->
    State1 = do_reg(Tab, ModName, Pid, Options, State),
    {reply, ok, State1};

do_call({flush, Tab}, _From, State) ->
    State1 = do_flush(Tab, State),
    TableInfo = do_start_timer(Tab, maps:get(Tab, State1)),
    {reply, ok, State1#{Tab := TableInfo}};

do_call({pull, Tab}, _From, State) ->
    {Reply, State1} = do_pull(Tab, State),
    TableInfo = do_start_timer(Tab, maps:get(Tab, State1)),
    {reply, Reply, State1#{Tab := TableInfo}};

do_call(_Request, _From, State) ->
    {reply, ok, State}.

do_cast({init_insert, Tab, KeyMaps}, State) ->
    State1 = do_init_insert(Tab, KeyMaps, State),
    {noreply, State1};

do_cast({delete, Tab}, State) ->
    State1 = do_delete(Tab, State),
    {noreply, State1};

do_cast({delete, Tab, Keys}, State) ->
    State1 = do_delete(Tab, Keys, State),
    {noreply, State1};

do_cast({delete_all, Tab}, State) ->
    State1 = do_delete_all(Tab, State),
    {noreply, State1};

do_cast({insert, Tab, Keys}, State) ->
    State1 = do_insert(Tab, Keys, State),
    {noreply, State1};

do_cast({update, Tab, Keys}, State) ->
    State1 = do_update(Tab, Keys, State),
    {noreply, State1};

do_cast(_Request, State) ->
    {noreply, State}.

do_info({flush, Tab}, State) ->
    State1 = do_flush(Tab, State),
    TableInfo = do_start_timer(Tab, maps:get(Tab, State1)),
    {noreply, State1#{Tab := TableInfo}};

do_info({callback, Tab}, State) ->
    State1 = do_callback(Tab, State),
    TableInfo = do_start_timer(Tab, maps:get(Tab, State1)),
    {noreply, State1#{Tab := TableInfo}};

do_info({'ETS-TRANSFER', TId, _FromPid, _HeirData}, State) ->
    State1 = do_ets_transfer(TId, State),
    {noreply, State1};

do_info(_Request, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_start_timer(Tab, #{db := DB, interval := Interval, timer_ref := TimerRef} = TabInfo) when DB =/= undefined, is_integer(Interval) ->
    cancel_timer(Tab, TimerRef),
    TabInfo#{timer_ref := erlang:send_after(Interval, self(), {flush, Tab})};
do_start_timer(Tab, #{callback := Callback, interval := Interval, timer_ref := TimerRef} = TabInfo) when Callback =/= undefined, is_integer(Interval) ->
    cancel_timer(Tab, TimerRef),
    TabInfo#{timer_ref := erlang:send_after(Interval, self(), {callback, Tab})};
do_start_timer(_Tab, TabInfo) ->
    TabInfo.

cancel_timer(_Tab, undefined) ->
    ok;
cancel_timer(Tab, TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            receive
                {flush, Tab} -> ok
            after 0 ->
                ok
            end;
        _ ->
            ok
    end.

delete_compare([Key | T], KeyMaps) ->
    case KeyMaps of
        #{Key := ?DIRTY_NOTHING} ->
            delete_compare(T, KeyMaps#{Key := ?DIRTY_DELETE});
        #{Key := ?DIRTY_INSERT} ->
            delete_compare(T, maps:remove(Key, KeyMaps));
        #{Key := ?DIRTY_UPDATE} ->
            delete_compare(T, KeyMaps#{Key := ?DIRTY_DELETE});
        _ ->
            delete_compare(T, KeyMaps)
    end;
delete_compare([], KeyMaps) ->
    KeyMaps.

insert_compare([Key | T], KeyMaps) ->
    case KeyMaps of
        #{Key := ?DIRTY_NOTHING} ->
            insert_compare(T, KeyMaps#{Key := ?DIRTY_UPDATE});
        #{Key := ?DIRTY_DELETE} ->
            insert_compare(T, KeyMaps#{Key := ?DIRTY_UPDATE});
        #{Key := _} ->
            insert_compare(T, KeyMaps);
        _ ->
            insert_compare(T, KeyMaps#{Key => ?DIRTY_INSERT})
    end;
insert_compare([], KeyMaps) ->
    KeyMaps.

update_compare([Key | T], KeyMaps) ->
    case KeyMaps of
        #{Key := ?DIRTY_NOTHING} ->
            update_compare(T, KeyMaps#{Key := ?DIRTY_UPDATE});
        _ ->
            update_compare(T, KeyMaps)
    end;
update_compare([], KeyMaps) ->
    KeyMaps.

do_reg(Tab, ModName, Pid, Options, State) ->
    {DB, Callback} = get_db_and_callback(Pid, Options),
    TabInfo = #{
        mod_name => ModName, db => DB, callback => Callback,
        interval => proplists:get_value(timeout, Options),
        timer_ref => undefined, key_maps => #{}
    },
    TabInfo1 = do_start_timer(Tab, TabInfo),
    State#{Tab => TabInfo1}.

get_db_and_callback(Pid, Options) ->
    case proplists:get_value(db, Options) of
        undefined ->
            {undefined, proplists:get_value(callback, Options, Pid)};
        DB ->
            {DB, undefined}
    end.

do_init_insert(Tab, KeyMaps, State) ->
    TableInfo = maps:get(Tab, State),
    State#{Tab := TableInfo#{key_maps := KeyMaps}}.

do_delete(Tab, State) ->
    {#{db := DB, mod_name := ModName, timer_ref := TRef}, State1} = maps:take(Tab, State),
    cancel_timer(Tab, TRef),
    TableName = ModName:get_table_name(),
    ok = db_mysql:truncate_table(DB, TableName),
    State1.

do_delete(Tab, Keys, State) ->
    #{key_maps := KeyMaps} = TableInfo = maps:get(Tab, State),
    KeyMaps1 = delete_compare(Keys, KeyMaps),
    State#{Tab := TableInfo#{key_maps := KeyMaps1}}.

do_delete_all(Tab, State) ->
    #{db := DB, mod_name := ModName} = TableInfo = maps:get(Tab, State),
    TableName = ModName:get_table_name(),
    ok = db_mysql:truncate_table(DB, TableName),
    State#{Tab := TableInfo#{key_maps := #{}}}.

do_insert(Tab, Keys, State) ->
    #{key_maps := KeyMaps} = TableInfo = maps:get(Tab, State),
    KeyMaps1 = insert_compare(Keys, KeyMaps),
    State#{Tab := TableInfo#{key_maps := KeyMaps1}}.

do_update(Tab, Keys, State) ->
    #{key_maps := KeyMaps} = TableInfo = maps:get(Tab, State),
    KeyMaps1 = update_compare(Keys, KeyMaps),
    State#{Tab := TableInfo#{key_maps := KeyMaps1}}.

do_flush(Tab, State) ->
    #{db := DB, mod_name := ModName, key_maps := KeyMaps} = TableInfo = maps:get(Tab, State),
    Iter = maps:iterator(KeyMaps),
    {KeyMaps1, InsertValuesList, UpdateValuesList, DeleteValuesList} = do_collect_flush_list(Tab, maps:next(Iter), KeyMaps, [], [], []),
    insert_record_list(DB, ModName, InsertValuesList),
    update_record_list(DB, ModName, UpdateValuesList),
    delete_record_list(DB, ModName, DeleteValuesList),
    TableInfo1 = TableInfo#{key_maps := KeyMaps1},
    State#{Tab := TableInfo1}.

do_collect_flush_list(Tab, {_Key, ?DIRTY_NOTHING, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_flush_list(Tab, maps:next(NextIter), KeyMaps, InsertList, UpdateList, DeleteList);

do_collect_flush_list(Tab, {Key, ?DIRTY_INSERT, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    InsertList1 = ets:lookup(Tab, Key) ++ InsertList,
    do_collect_flush_list(Tab, maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList1, UpdateList, DeleteList);

do_collect_flush_list(Tab, {Key, ?DIRTY_UPDATE, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    UpdateList1 = ets:lookup(Tab, Key) ++ UpdateList,
    do_collect_flush_list(Tab, maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList, UpdateList1, DeleteList);

do_collect_flush_list(Tab, {Key, ?DIRTY_DELETE, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_flush_list(Tab, maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList, UpdateList, [[Key] | DeleteList]);

do_collect_flush_list(_Tab, none, KeyMaps, InsertList, UpdateList, DeleteList) ->
    {KeyMaps, InsertList, UpdateList, DeleteList}.

insert_record_list(DB, ModName, RecordList) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_filed_list(),
    ValuesList = [ModName:get_table_values(Record) || Record <- RecordList],
    {ok, _} = db_mysql:insert_rows(DB, TableName, FieldList, ValuesList).

update_record_list(DB, ModName, RecordList) ->
    TableName = ModName:get_table_name(),
    UpdateFields = ModName:get_table_filed_list(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    {KeyValuesList, UpdateValuesList} = get_key_values_and_values_list(ModName, RecordList),
    {ok, _} = db_mysql:update_rows(DB, TableName, UpdateFields, UpdateValuesList, KeyFieldList, KeyValuesList).

get_key_values_and_values_list(ModName, [Struct | T]) ->
    {KeyValuesList, ValuesList} = get_key_values_and_values_list(ModName, T),
    KeyValues = ModName:get_table_key_values(Struct),
    Values = ModName:get_table_values(Struct),
    {[KeyValues | KeyValuesList], [Values | ValuesList]};
get_key_values_and_values_list(_ModName, []) ->
    {[], []}.

delete_record_list(DB, ModName, KeyValuesList) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    {ok, _} = db_mysql:delete_rows(DB, TableName, KeyFieldList, KeyValuesList).

do_pull(Tab, State) ->
    #{Tab := #{key_maps := KeyMaps} = TableInfo} = maps:get(Tab, State),
    Iter = maps:iterator(KeyMaps),
    {KeyMaps1, InsertList, UpdateList, DeleteList} = do_collect_dirty_list(Tab, maps:next(Iter), KeyMaps, [], [], []),
    {{InsertList, UpdateList, DeleteList}, State#{Tab := TableInfo#{key_maps := KeyMaps1}}}.

do_callback(Tab, State) ->
    #{Tab := #{callback := Callback, key_maps := KeyMaps} = TableInfo} = maps:get(Tab, State),
    Iter = maps:iterator(KeyMaps),
    {KeyMaps1, InsertList, UpdateList, DeleteList} = do_collect_dirty_list(Tab, maps:next(Iter), KeyMaps, [], [], []),
    Callback ! {ets_dirty_info, InsertList, UpdateList, DeleteList},
    State#{Tab := TableInfo#{key_maps := KeyMaps1}}.

do_collect_dirty_list(Tab, {_Key, ?DIRTY_NOTHING, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(Tab, maps:next(NextIter), KeyMaps, InsertList, UpdateList, DeleteList);

do_collect_dirty_list(Tab, {Key, ?DIRTY_INSERT, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(Tab, maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, [Key | InsertList], UpdateList, DeleteList);

do_collect_dirty_list(Tab, {Key, ?DIRTY_UPDATE, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(Tab, maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList, [Key | UpdateList], DeleteList);

do_collect_dirty_list(Tab, {Key, ?DIRTY_DELETE, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(Tab, maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList, UpdateList, [Key | DeleteList]);

do_collect_dirty_list(_Tab, none, KeyMaps, InsertList, UpdateList, DeleteList) ->
    {KeyMaps, InsertList, UpdateList, DeleteList}.

do_ets_transfer(TId, State) ->
    case ets:info(TId, named_table) of
        true ->
            Tab = ets:info(TId, name);
        false ->
            Tab = TId
    end,
    State1 = do_flush(Tab, State),
    ets:delete(Tab),
    {TableInfo, State2} = maps:take(Tab, State1),
    cancel_timer(Tab, maps:get(timer_ref, TableInfo)),
    State2.
