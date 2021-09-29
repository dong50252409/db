%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(db_ets_child).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(DIRTY_NOTHING, 0).      % 无状态
-define(DIRTY_INSERT, 1).       % 插入状态
-define(DIRTY_UPDATE, 2).       % 更新状态
-define(DIRTY_DELETE, 3).       % 删除状态

%%-------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link(Tab, ModName, Options) ->
    gen_server:start_link(?MODULE, [Tab, ModName, Options], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([Tab, ModName, Options]) ->
    process_flag(trap_exit, true),
    State = do_init(Tab, ModName, Options),
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

terminate(normal, State) ->
    do_terminate(State),
    ok;

terminate(_Reason, _State) ->
    ok.

do_call(flush, _From, State) ->
    case do_flush(State) of
        {error, State1} ->
            {stop, shutdown, error, State1};
        {Reply, State1} ->
            State2 = do_start_timer(State1),
            {reply, Reply, State2}
    end;

do_call(pull, _From, State) ->
    {Reply, State1} = do_pull(State),
    {reply, Reply, State1};

do_call(_Request, _From, State) ->
    {reply, ok, State}.

do_cast({init_insert, Keys}, State) ->
    State1 = do_init_insert(Keys, State),
    {noreply, State1};

do_cast(delete, State) ->
    State1 = do_delete(State),
    {stop, shutdown, State1};

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
            {stop, shutdown, State1};
        {_, State1} ->
            State2 = do_start_timer(State1),
            {noreply, State2}
    end;

do_info(callback, State) ->
    State1 = do_callback(State),
    {noreply, State1};

do_info({'ETS-TRANSFER', _TId, _FromPid, _HeirData}, State) ->
    %% 这里直接关闭进程，在terminate中处理善后工作
    {stop, normal, State};

do_info(_Request, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_start_timer(#{mode := db_mode, interval := Interval, timer_ref := TimerRef} = State) ->
    do_cancel_timer(TimerRef),
    State#{timer_ref := erlang:send_after(Interval, self(), flush)};
do_start_timer(#{mode := callback_mode, interval := Interval, timer_ref := TimerRef} = State) ->
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

do_init(Tab, ModName, Options) ->
    {Mode, DBPool, Callback} = get_mode(Options),
    State = #{
        tab => Tab, mod_name => ModName,
        mode => Mode, db_pool => DBPool, callback => Callback,
        interval => proplists:get_value(flush_interval, Options),
        timer_ref => undefined, key_maps => #{}
    },
    State1 = do_start_timer(State),
    State1.

get_mode(Options) ->
    case proplists:get_value(mode, Options) of
        {auto, DBPool} ->
            {db_mode, DBPool, undefined};
        {callback, Pid} when is_pid(Pid) ->
            {callback_mode, undefined, Pid};
        {callback, Mod} when is_atom(Mod) ->
            {callback_mode, undefined, erlang:whereis(Mod)}
    end.

do_init_insert(Keys, State) ->
    State#{key_maps := [{Key, ?DIRTY_NOTHING} || Key <- Keys]}.

do_delete(#{db_pool := DBPool, mod_name := ModName, timer_ref := TRef} = State) ->
    TableName = ModName:get_table_name(),
    ok = db_mysql:truncate_table(DBPool, TableName),
    do_cancel_timer(TRef),
    State#{timer_ref := undefined, key_maps := #{}}.

do_delete(Keys, #{key_maps := KeyMaps} = State) ->
    KeyMaps1 = delete_compare(Keys, KeyMaps),
    State#{key_maps := KeyMaps1}.

do_delete_all(#{db_pool := DBPool, mod_name := ModName} = State) ->
    TableName = ModName:get_table_name(),
    ok = db_mysql:truncate_table(DBPool, TableName),
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

insert_record_list(#{tab := Tab, db_pool := DBPool, mod_name := ModName, key_maps := KeyMaps} = State) ->
    case do_collect_insert_list(Tab, ModName, KeyMaps) of
        {_KeyMaps1, []} ->
            {ok, State};
        {KeyMaps1, ValuesList} ->
            TableName = ModName:get_table_name(),
            FieldList = ModName:get_table_field_list(),
            try
                {ok, _} = db_mysql:insert_rows(DBPool, TableName, FieldList, ValuesList),
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

update_record_list(#{tab := Tab, db_pool := DBPool, mod_name := ModName, key_maps := KeyMaps} = State) ->
    case do_collect_update_list(Tab, ModName, KeyMaps) of
        {_KeyMaps1, [], []} ->
            {ok, State};
        {KeyMaps1, KeyValuesList, ValuesList} ->
            TableName = ModName:get_table_name(),
            UpdateFields = ModName:get_table_field_list(),
            KeyFieldList = ModName:get_table_key_field_list(),
            try
                {ok, _} = db_mysql:update_rows(DBPool, TableName, UpdateFields, ValuesList, KeyFieldList, KeyValuesList),
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

delete_record_list(#{db_pool := DBPool, mod_name := ModName, key_maps := KeyMaps} = State) ->
    case do_collect_delete_list(ModName, KeyMaps) of
        {_KeyMaps1, []} ->
            {ok, State};
        {KeyMaps1, KeyValuesList} ->
            TableName = ModName:get_table_name(),
            KeyFieldList = ModName:get_table_key_field_list(),
            try
                {ok, _} = db_mysql:delete_rows(DBPool, TableName, KeyFieldList, KeyValuesList),
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
    erlang:send(Callback, {ets_dirty_list, InsertList, UpdateList, DeleteList}),
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

do_terminate(State) ->
    case State of
        #{tab := Tab, mode := db_mode} ->
            {_Ret, State1} = do_flush(State);
        #{tab := Tab, mode := callback_mode} ->
            State1 = do_callback(State)
    end,
    catch ets:delete(Tab),
    State1.