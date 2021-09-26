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

-behavior(gen_server).

-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/3, reg/3, init_insert/2, flush/1, pull/1
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


-define(CHECK_OPTIONS_LIST, [flush_interval, schema]).
-define(CHECK_TAB_LIST, [type, protection, keypos]).

-define(DIRTY_NOTHING, 0).      % 无状态
-define(DIRTY_INSERT, 1).       % 插入状态
-define(DIRTY_UPDATE, 2).       % 更新状态
-define(DIRTY_DELETE, 3).       % 删除状态

-type option() :: {flush_interval, timeout()}|{schema, {auto, db_mysql:db_name()}|{callback, module()}}.

%%-------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link(Tab, ModName, Options) ->
    gen_server:start_link(?MODULE, [Tab, ModName, Options], []).

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
                    {ok, Pid} = db_sup:start_child(Tab, ModName, Options),
                    try_heir_ets(Tab, Pid, Options),
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

check_options(Options, [flush_interval | T]) ->
    case proplists:get_value(timeout, Options) of
        Timeout when is_integer(Timeout), Timeout > 0 ->
            check_options(Options, T);
        _ ->
            {error, flush_interval_undefined}
    end;
check_options(Options, [schema | T]) ->
    case proplists:get_value(schema, Options) of
        {auto, DB} when is_atom(DB);is_pid(DB);is_tuple(DB) ->
            check_options(Options, T);
        {callback, Mod} when is_atom(Mod) ->
            case is_process_alive(erlang:whereis(Mod)) of
                true ->
                    check_options(Options, T);
                false ->
                    {error, schema_process_died}
            end;
        {callback, Pid} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    check_options(Options, T);
                false ->
                    {error, schema_process_died}
            end;
        _ ->
            {error, schema_undefined}
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

try_heir_ets(Tab, Pid, Options) ->
    case proplists:get_value(schema, Options) of
        {auto, _} ->
            ets:setopts(Tab, {heir, Pid, nothing});
        _ ->
            ok
    end.
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
    gen_server:cast(db_sup:get_pid(Tab), {init_insert, Tab, KeyMap}),
    true.

%%------------------------------------------------------------------------------
%% @doc
%% 主动同步脏数据到数据库，如果指定了flush_interval选项，则会重新启动定时器
%% @end
%%------------------------------------------------------------------------------
-spec flush(Tab :: ets:tab()) -> ok|error.
flush(Tab) ->
    gen_server:call(db_sup:get_pid(Tab), {flush, Tab}).

%%------------------------------------------------------------------------------
%% @doc
%% 拉取脏数据的键值，如果指定了flush_interval选项，则会重新启动定时器
%% @end
%%------------------------------------------------------------------------------
-spec pull(Tab :: ets:tab()) -> {InsertKeyList :: list(), UpdateKeyList :: list(), DeleteKeyList :: list()}|error.
pull(Tab) ->
    gen_server:call(db_sup:get_pid(Tab), {pull, Tab}).

%%--------------------------------------------------------------------
%% ETS API
%%--------------------------------------------------------------------
-spec delete(ets:tab()) -> true.
delete(Tab) ->
    true = ets:delete(Tab),
    gen_server:cast(db_sup:get_pid(Tab), {delete, Tab}),
    true.

-spec delete(ets:tab(), term()) -> true.
delete(Tab, Key) ->
    true = ets:delete(Tab, Key),
    gen_server:cast(db_sup:get_pid(Tab), {delete, Tab, [Key]}),
    true.

-spec delete_all_objects(ets:tab()) -> true.
delete_all_objects(Tab) ->
    true = ets:delete_all_objects(Tab),
    gen_server:cast(db_sup:get_pid(Tab), {delete_all, Tab}),
    true.

-spec delete_object(ets:tab(), tuple()) -> true.
delete_object(Tab, Object) ->
    true = ets:delete_object(Tab, Object),
    gen_server:cast(db_sup:get_pid(Tab), {delete, Tab, [get_keys(Tab, Object)]}),
    true.

-spec select_delete(ets:tab(), ets:match_spec()) -> non_neg_integer().
select_delete(Tab, [{'_', [], [true]}] = MatchSpec) ->
    NumDeleted = ets:select_delete(Tab, MatchSpec),
    gen_server:cast(db_sup:get_pid(Tab), {delete_all, Tab}),
    NumDeleted;
select_delete(Tab, [{Pattern, Condition, _}] = MatchSpec) ->
    KeyPos = ets:info(Tab, keypos),
    MS = [{Pattern, Condition, [{element, KeyPos, '$_'}]}],
    Keys = ets:select(Tab, MS),
    NumDeleted = ets:select_delete(Tab, MatchSpec),
    gen_server:cast(db_sup:get_pid(Tab), {delete, Tab, Keys}),
    NumDeleted.

-spec match_delete(ets:tab(), ets:match_pattern()) -> non_neg_integer().
match_delete(Tab, Pattern) ->
    select_delete(Tab, [{Pattern, [], [true]}]).

-spec take(ets:tab(), term()) -> [tuple()].
take(Tab, Key) ->
    Objects = ets:take(Tab, Key),
    gen_server:cast(db_sup:get_pid(Tab), {delete, Tab, [get_keys(Tab, Object) || Object <- Objects]}),
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
    gen_server:cast(db_sup:get_pid(Tab), {insert, Tab, [get_keys(Tab, Object) || Object <- Objects]}),
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
            gen_server:cast(db_sup:get_pid(Tab), {insert, Tab, [get_keys(Tab, Object) || Object <- Objects]}),
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
    gen_server:cast(db_sup:get_pid(Tab), {insert, Tab, Keys}),
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
    gen_server:cast(db_sup:get_pid(Tab), {update, Tab, [Key]}),
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
    gen_server:cast(db_sup:get_pid(Tab), {update, Tab, [Key]}),
    Result.

-spec update_element(atom(), term(), {Pos, Value} | [{Pos, Value}]) -> boolean() when
    Pos :: pos_integer(),
    Value :: term().
update_element(Tab, Key, ElementSpec) ->
    case ets:update_element(Tab, Key, ElementSpec) of
        true ->
            gen_server:cast(db_sup:get_pid(Tab), {update, Tab, [Key]}),
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
init([Tab, ModName, Options]) ->
    process_flag(trap_exit, true),
    State = do_reg(Tab, ModName, Options),
    {ok, State}.

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

do_call(flush, _From, State) ->
    case do_flush(State) of
        {error, State1} ->
            {stop, normal, error, State1};
        {Reply, State1} ->
            State2 = do_start_timer(State1),
            {reply, Reply, State2}
    end;

do_call(pull, _From, State) ->
    {Reply, State1} = do_pull(State),
    {reply, Reply, State1};

do_call(_Request, _From, State) ->
    {reply, ok, State}.

do_cast({init_insert, KeyMaps}, State) ->
    State1 = do_init_insert(KeyMaps, State),
    {noreply, State1};

do_cast(delete, State) ->
    State1 = do_delete(State),
    {stop, noreply, State1};

do_cast({delete, Keys}, State) ->
    State1 = do_delete(Keys, State),
    {noreply, State1};

do_cast(delete_all, State) ->
    State1 = do_delete_all(State),
    {noreply, State1};

do_cast({insert, Keys}, State) ->
    State1 = do_insert(Keys, State),
    {noreply, State1};

do_cast({update, Keys}, State) ->
    State1 = do_update(Keys, State),
    {noreply, State1};

do_cast(_Request, State) ->
    {noreply, State}.

do_info(flush, State) ->
    case do_flush(State) of
        {error, State1} ->
            {stop, normal, State1};
        {_, State1} ->
            State2 = do_start_timer(State1),
            {noreply, State2}
    end;

do_info(callback, State) ->
    State1 = do_callback(State),
    {noreply, State1};

do_info({'ETS-TRANSFER', _TId, _FromPid, _HeirData}, State) ->
    State1 = do_ets_transfer(State),
    {noreply, State1};

do_info(_Request, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_start_timer(#{callback := undefined, interval := Interval, timer_ref := TimerRef} = State) ->
    do_cancel_timer(TimerRef),
    State#{timer_ref := erlang:send_after(Interval, self(), flush)};
do_start_timer(#{db := undefined, interval := Interval, timer_ref := TimerRef} = State) ->
    do_cancel_timer(TimerRef),
    State#{timer_ref := erlang:send_after(Interval, self(), callback)}.

do_cancel_timer(TRef) when is_reference(TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            receive
                flush -> ok;
                callback -> ok
            after 0 ->
                ok
            end;
        _ ->
            ok
    end;
do_cancel_timer(undefined) ->
    ok.

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

do_reg(Tab, ModName, Options) ->
    Schema = proplists:get_value(schema, Options),
    State = #{
        tab => Tab, mod_name => ModName,
        db => proplists:get_value(auto, [Schema]),
        callback => proplists:get_value(callback, [Schema]),
        interval => proplists:get_value(flush_interval, Options),
        timer_ref => undefined, key_maps => #{}
    },
    State1 = do_start_timer(State),
    State1.

do_init_insert(KeyMaps, State) ->
    State#{key_maps := KeyMaps}.

do_delete(#{db := DB, mod_name := ModName, timer_ref := TRef} = State) ->
    TableName = ModName:get_table_name(),
    ok = db_mysql:truncate_table(DB, TableName),
    do_cancel_timer(TRef),
    State#{timer_ref := undefined, key_maps := #{}}.

do_delete(Keys, #{key_maps := KeyMaps} = State) ->
    KeyMaps1 = delete_compare(Keys, KeyMaps),
    State#{key_maps := KeyMaps1}.

do_delete_all(#{db := DB, mod_name := ModName} = State) ->
    TableName = ModName:get_table_name(),
    ok = db_mysql:truncate_table(DB, TableName),
    State#{key_maps := #{}}.

do_insert(Keys, #{key_maps := KeyMaps} = State) ->
    KeyMaps1 = insert_compare(Keys, KeyMaps),
    State#{key_maps := KeyMaps1}.

do_update(Keys, #{key_maps := KeyMaps} = State) ->
    KeyMaps1 = update_compare(Keys, KeyMaps),
    State#{key_maps := KeyMaps1}.

do_flush(State) ->
    case insert_record_list(State) of
        {error, State1} ->
            {error, State1};
        {_, State1} ->
            case update_record_list(State1) of
                {error, State2} ->
                    {error, State2};
                {ok, State2} ->
                    delete_record_list(State2)
            end
    end.

insert_record_list(#{tab := Tab, db := DB, mod_name := ModName, key_maps := KeyMaps} = State) ->
    case do_collect_insert_list(Tab, ModName, KeyMaps) of
        {KeyMaps1, ValuesList} ->
            TableName = ModName:get_table_name(),
            FieldList = ModName:get_table_field_list(),
            try
                {ok, _} = db_mysql:insert_rows(DB, TableName, FieldList, ValuesList),
                {ok, State#{key_maps := KeyMaps1}}
            catch
                Err:Reason:Stacktrace ->
                    ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
                    {retry, State}
            end;
        error ->
            {error, State}
    end.

do_collect_insert_list(Tab, ModName, KeyMaps) ->
    try
        Iter = maps:iterator(KeyMaps),
        do_collect_insert_list(Tab, maps:next(Iter), ModName, KeyMaps, [])
    catch
        Err:Reason:Stacktrace ->
            ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
            error
    end.

do_collect_insert_list(Tab, {Key, ?DIRTY_INSERT, NextIter}, ModName, KeyMaps, ValuesList) ->
    KeyMaps1 = KeyMaps#{Key := ?DIRTY_NOTHING},
    ValuesList1 = [ModName:get_table_values(Record) || Record <- ets:lookup(Tab, Key)] ++ ValuesList,
    do_collect_insert_list(Tab, maps:next(NextIter), ModName, KeyMaps1, ValuesList1);
do_collect_insert_list(Tab, {_Key, _Value, NextIter}, ModName, KeyMaps, ValuesList) ->
    do_collect_insert_list(Tab, maps:next(NextIter), ModName, KeyMaps, ValuesList);
do_collect_insert_list(_Tab, none, _ModName, KeyMaps, ValuesList) ->
    {KeyMaps, ValuesList}.

update_record_list(#{tab := Tab, db := DB, mod_name := ModName, key_maps := KeyMaps} = State) ->
    case do_collect_update_list(Tab, ModName, KeyMaps) of
        {KeyMaps1, KeyValuesList, ValuesList} ->
            TableName = ModName:get_table_name(),
            UpdateFields = ModName:get_table_field_list(),
            KeyFieldList = ModName:get_table_key_field_list(),
            try
                {ok, _} = db_mysql:update_rows(DB, TableName, UpdateFields, ValuesList, KeyFieldList, KeyValuesList),
                {ok, State#{key_maps := KeyMaps1}}
            catch
                Err:Reason:Stacktrace ->
                    ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
                    {retry, State}
            end;
        error ->
            {error, State}
    end.

do_collect_update_list(Tab, ModName, KeyMaps) ->
    try
        Iter = maps:iterator(KeyMaps),
        do_collect_update_list(Tab, maps:next(Iter), ModName, KeyMaps, [], [])
    catch
        Err:Reason:Stacktrace ->
            ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
            error
    end.

do_collect_update_list(Tab, {Key, ?DIRTY_UPDATE, NextIter}, ModName, KeyMaps, KeyValuesList, ValuesList) ->
    KeyMaps1 = KeyMaps#{Key := ?DIRTY_NOTHING},
    {KeyValuesList1, UpdateValuesList1} = get_key_values_and_values_list(ModName, ets:lookup(Tab, Key), KeyValuesList, ValuesList),
    do_collect_update_list(Tab, maps:next(NextIter), ModName, KeyMaps1, KeyValuesList1, UpdateValuesList1);
do_collect_update_list(Tab, {_Key, _Value, NextIter}, ModName, KeyMaps, KeyValuesList, ValuesList) ->
    do_collect_update_list(Tab, maps:next(NextIter), ModName, KeyMaps, KeyValuesList, ValuesList);
do_collect_update_list(_Tab, none, _ModName, KeyMaps, KeyValuesList, ValuesList) ->
    {KeyMaps, KeyValuesList, ValuesList}.

get_key_values_and_values_list(ModName, [Record | T], KeyValuesList, ValuesList) ->
    KeyValues = ModName:get_table_key_values(Record),
    Values = ModName:get_table_values(Record),
    get_key_values_and_values_list(ModName, T, [KeyValues | KeyValuesList], [Values | ValuesList]);
get_key_values_and_values_list(_ModName, [], KeyValuesList, ValuesList) ->
    {KeyValuesList, ValuesList}.

delete_record_list(#{db := DB, mod_name := ModName, key_maps := KeyMaps} = State) ->
    case do_collect_delete_list(ModName, KeyMaps) of
        {KeyMaps1, KeyValuesList} ->
            TableName = ModName:get_table_name(),
            KeyFieldList = ModName:get_table_key_field_list(),
            try
                {ok, _} = db_mysql:delete_rows(DB, TableName, KeyFieldList, KeyValuesList),
                {ok, State#{key_maps := KeyMaps1}}
            catch
                Err:Reason:Stacktrace ->
                    ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
                    {retry, State}
            end;
        error ->
            {error, State}
    end.

do_collect_delete_list(ModName, KeyMaps) ->
    try
        Iter = maps:iterator(KeyMaps),
        do_collect_delete_list(maps:next(Iter), ModName, KeyMaps, [])
    catch
        Err:Reason:Stacktrace ->
            ?LOG_ERROR("Err:~w, Reason:~w~nStacktrace:~tp", [Err, Reason, Stacktrace]),
            error
    end.

do_collect_delete_list({Key, ?DIRTY_DELETE, NextIter}, ModName, KeyMaps, ValuesList) ->
    KeyMaps1 = KeyMaps#{Key := ?DIRTY_NOTHING},
    ValuesList1 = [[Key] | ValuesList],
    do_collect_delete_list(maps:next(NextIter), ModName, KeyMaps1, ValuesList1);
do_collect_delete_list({_Key, _Value, NextIter}, ModName, KeyMaps, ValuesList) ->
    do_collect_delete_list(maps:next(NextIter), ModName, KeyMaps, ValuesList);
do_collect_delete_list(none, _ModName, KeyMaps, ValuesList) ->
    {KeyMaps, ValuesList}.

do_pull(#{key_maps := KeyMaps} = State) ->
    Iter = maps:iterator(KeyMaps),
    {KeyMaps1, InsertList, UpdateList, DeleteList} = do_collect_dirty_list(maps:next(Iter), KeyMaps, [], [], []),
    State1 = do_start_timer(State),
    State2 = State1#{key_maps := KeyMaps1},
    {{InsertList, UpdateList, DeleteList}, State2}.

do_callback(#{callback := Callback, key_maps := KeyMaps} = State) ->
    Iter = maps:iterator(KeyMaps),
    {KeyMaps1, InsertList, UpdateList, DeleteList} = do_collect_dirty_list(maps:next(Iter), KeyMaps, [], [], []),
    Callback ! {ets_dirty_list, InsertList, UpdateList, DeleteList},
    State1 = do_start_timer(State),
    State1#{key_maps := KeyMaps1}.

do_collect_dirty_list({_Key, ?DIRTY_NOTHING, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(maps:next(NextIter), KeyMaps, InsertList, UpdateList, DeleteList);

do_collect_dirty_list({Key, ?DIRTY_INSERT, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, [Key | InsertList], UpdateList, DeleteList);

do_collect_dirty_list({Key, ?DIRTY_UPDATE, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList, [Key | UpdateList], DeleteList);

do_collect_dirty_list({Key, ?DIRTY_DELETE, NextIter}, KeyMaps, InsertList, UpdateList, DeleteList) ->
    do_collect_dirty_list(maps:next(NextIter), KeyMaps#{Key := ?DIRTY_NOTHING}, InsertList, UpdateList, [Key | DeleteList]);

do_collect_dirty_list(none, KeyMaps, InsertList, UpdateList, DeleteList) ->
    {KeyMaps, InsertList, UpdateList, DeleteList}.

do_ets_transfer(State) ->
    case do_flush(State) of
        {retry, State1} ->
            %% 数据库连接出现问题了，可能是暂时的，启动一个5秒的定时器，期待下次能够成功保存数据并删掉ETS表
            State2 = do_start_timer(State1#{interval := 5000}),
            {noreply, State2};
        {_Ret, #{tab := Tab} = State1} ->
            %% 如果`_Ret`为`ok`可以正常删除ETS表并结束进程
            %% 如果`_Ret`为`error`说明某些原因导致ETS表已经不存在了，这里直接删掉ETS表并取消定时器即可
            catch ets:delete(Tab),
            {stop, normal, State1}
    end.
