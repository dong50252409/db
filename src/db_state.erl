%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% 进程State持久化代理
%%% 调用reg/3或通过select/4将通过进程字典创建一个键为{'$db_table_info', ModName}的数据，值为需要同步的数据结构
%%% @end
%%% Created : 08. 9月 2021 14:29
%%%-------------------------------------------------------------------
-module(db_state).

-define(CHECK_OPTIONS_LIST, [struct_type]).

%% API
-export([reg/3, reg_select/4, flush/2]).

-export_type([option/0, struct_type/0, struct/0]).

-type option() :: {struct_type, struct_type()}.
-type struct_type() :: map | maps | record | record_list.
-type struct() :: map() | tuple() | [tuple()] | undefined.

%%------------------------------------------------------------------------------
%% @doc
%% 注册要管理的表模块
%% @end
%%------------------------------------------------------------------------------
-spec reg(DBPool :: db_mysql:db_pool(), ModName :: module(), Options :: [option()]) -> ok|{error, term()}.
reg(DBPool, ModName, Options) ->
    case check_options(Options, ?CHECK_OPTIONS_LIST) of
        true ->
            {StructType, Cache} = get_struct_type_and_cache(Options),
            TableInfo = #{db_pool => DBPool, struct_type => StructType, cache => Cache},
            erlang:put({'$db_table_info', ModName}, TableInfo),
            ok;
        Err ->
            Err
    end.

%%------------------------------------------------------------------------------
%% @doc
%% 通过表模块以及条件查询数据，并返回指定结构，并注册管理此表模块
%% @end
%%------------------------------------------------------------------------------
-spec reg_select(DBPool :: db_mysql:db_pool(), ModName :: module(), Conditions :: [db_mysql:condition()], Options :: [option()]) ->
    Result :: {ok, struct()}|{error, term()}|db_mysql:query_error().
reg_select(DBPool, ModName, Conditions, Options) ->
    case check_options(Options, ?CHECK_OPTIONS_LIST) of
        true ->
            TableName = ModName:get_table_name(),
            case db_mysql:select(DBPool, TableName, [], Conditions) of
                {ok, _Columns, Rows} ->
                    AsStruct = proplists:get_value(struct_type, Options),
                    Struct = as_struct(ModName, Rows, AsStruct),
                    TableInfo = #{db_pool => DBPool, struct_type => AsStruct, cache => Struct},
                    erlang:put({'$db_table_info', ModName}, TableInfo),
                    {ok, Struct};
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

%%------------------------------------------------------------------------------
%% @doc
%% 主动对比表模块数据，并更新保存到数据库
%% @end
%%------------------------------------------------------------------------------
-spec flush(ModName :: module(), NewStruct :: struct()) -> ok|{error,mod_unregister}.
flush(ModName, NewStruct) ->
    case get({'$db_table_info', ModName}) of
        #{db_pool := DBPool, struct_type := StructType, cache := Struct} = TableInfo ->
            compare(StructType, DBPool, ModName, NewStruct, Struct),
            put({'$db_table_info', ModName}, TableInfo#{cache := NewStruct}),
            ok;
        undefined ->
            {error, mod_unregister}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_options(Options, [struct_type | T]) ->
    StructType = proplists:get_value(struct_type, Options),
    case lists:member(StructType, [map, maps, record, record_list]) of
        true ->
            check_options(Options, T);
        false ->
            {error, undefined_struct_type}
    end;
check_options(_Options, []) ->
    true.

get_struct_type_and_cache(Options) ->
    case proplists:get_value(struct_type, Options) of
        StructType when StructType =:= maps ->
            {StructType, #{}};
        StructType when StructType =:= record_list ->
            {StructType, []};
        StructType ->
            {StructType, undefined}
    end.

as_struct(_ModName, [], map) ->
    undefined;
as_struct(ModName, [Row], map) ->
    ModName:as_map(Row);
as_struct(ModName, Rows, maps) ->
    as_maps(ModName, Rows, #{});
as_struct(_ModName, [], record) ->
    undefined;
as_struct(ModName, [Row], record) ->
    ModName:as_record(Row);
as_struct(ModName, Rows, record_list) ->
    [ModName:as_record(Row) || Row <- Rows].

as_maps(ModName, [Row | T], AccMaps) ->
    Map = ModName:as_map(Row),
    Key = list_to_tuple(ModName:get_table_key_values(Map)),
    as_maps(ModName, T, AccMaps#{Key => Map});
as_maps(_ModName, [], AccMaps) ->
    AccMaps.

compare(map, DBPool, ModName, New, Old) ->
    compare_map(DBPool, ModName, New, Old);
compare(maps, DBPool, ModName, New, Old) ->
    compare_maps(DBPool, ModName, New, Old);
compare(record, DBPool, ModName, New, Old) ->
    compare_record(DBPool, ModName, New, Old);
compare(record_list, DBPool, ModName, New, Old) ->
    compare_record_list(DBPool, ModName, New, Old).

compare_map(DBPool, ModName, NewMap, undefined) when is_map(NewMap) ->
    insert_map(DBPool, ModName, NewMap);
compare_map(DBPool, ModName, undefined, OldMap) when is_map(OldMap) ->
    delete_map(DBPool, ModName, OldMap);
compare_map(DBPool, ModName, NewMap, OldMap) when is_map(NewMap), is_map(OldMap), NewMap =/= OldMap ->
    update_map(DBPool, ModName, NewMap);
compare_map(_DB, _ModName, _, _) ->
    nothing.

compare_maps(DBPool, ModName, NewMaps, undefined) when is_map(NewMaps) ->
    insert_maps(DBPool, ModName, maps:values(NewMaps));
compare_maps(DBPool, ModName, undefined, OldMaps) when is_map(OldMaps) ->
    delete_maps(DBPool, ModName, maps:values(OldMaps));
compare_maps(DBPool, ModName, NewMaps, OldMaps) when is_map(NewMaps), is_map(OldMaps) ->
    Iter = maps:iterator(NewMaps),
    {InsertList, UpdateList, DeleteList} = compare_maps_1(maps:next(Iter), OldMaps, [], []),
    insert_maps(DBPool, ModName, InsertList),
    update_maps(DBPool, ModName, UpdateList),
    delete_maps(DBPool, ModName, DeleteList).

compare_maps_1({Key, Map, NextIter}, OldMaps, InsertList, UpdateList) ->
    case maps:take(Key, OldMaps) of
        error ->
            compare_maps_1(maps:next(NextIter), OldMaps, [Map | InsertList], UpdateList);
        {Map, OldMaps1} ->
            compare_maps_1(maps:next(NextIter), OldMaps1, InsertList, UpdateList);
        {_OldMap, OldMaps1} ->
            compare_maps_1(maps:next(NextIter), OldMaps1, InsertList, [Map | UpdateList])
    end;
compare_maps_1(none, OldMaps, InsertList, UpdateList) ->
    {InsertList, UpdateList, maps:values(OldMaps)}.

compare_record(DBPool, ModName, NewRecord, undefined) when is_tuple(NewRecord) ->
    insert_record(DBPool, ModName, NewRecord);
compare_record(DBPool, ModName, undefined, OldRecord) when is_tuple(OldRecord) ->
    delete_record(DBPool, ModName, OldRecord);
compare_record(DBPool, ModName, NewRecord, OldRecord) when is_tuple(NewRecord), is_tuple(OldRecord), NewRecord =/= OldRecord ->
    update_record(DBPool, ModName, NewRecord);
compare_record(_DB, _ModName, _, _) ->
    nothing.

compare_record_list(DBPool, ModName, NewRecordList, OldRecordList) when is_list(NewRecordList), is_list(OldRecordList) ->
    {InsertList, UpdateList, DeleteRecordList} = compare_record_list_1(ModName, NewRecordList, OldRecordList, [], []),
    insert_record_list(DBPool, ModName, InsertList),
    update_record_list(DBPool, ModName, UpdateList),
    delete_record_list(DBPool, ModName, DeleteRecordList).

compare_record_list_1(ModName, [Record | T], OldRecordList, InsertList, UpdateList) ->
    case compare_record_list_2(ModName, Record, OldRecordList) of
        error ->
            compare_record_list_1(ModName, T, OldRecordList, [Record | InsertList], UpdateList);
        {Record, OtherOldRecordList} ->
            compare_record_list_1(ModName, T, OtherOldRecordList, InsertList, UpdateList);
        {_OldRecord, OtherOldRecordList} ->
            compare_record_list_1(ModName, T, OtherOldRecordList, InsertList, [Record | UpdateList])
    end;
compare_record_list_1(_KeyIndexIndex, [], OldRecordList, InsertList, UpdateList) ->
    {InsertList, UpdateList, OldRecordList}.

compare_record_list_2(ModName, Record, [H | T]) ->
    case ModName:get_table_key_values(Record) =:= ModName:get_table_key_values(H) of
        true ->
            {H, T};
        false ->
            case compare_record_list_2(ModName, Record, T) of
                {R, OtherList} ->
                    {R, [H | OtherList]};
                Other ->
                    Other
            end
    end;
compare_record_list_2(_ModName, _V, []) ->
    error.

insert_map(DBPool, ModName, Map) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_field_list(),
    Values = ModName:get_table_values(Map),
    {ok, _} = db_mysql:insert_row(DBPool, TableName, FieldList, Values).

update_map(DBPool, ModName, Map) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_field_list(),
    Values = ModName:get_table_values(Map),
    KeyFieldList = ModName:get_table_key_field_list(),
    KeyValues = ModName:get_table_key_values(Map),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:update_rows(DBPool, TableName, FieldList, Values, Conditions).

delete_map(DBPool, ModName, Map) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_field_list(),
    KeyValues = ModName:get_table_key_values(Map),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:delete_rows(DBPool, TableName, Conditions).

insert_maps(_DB, _ModName, []) ->
    ok;
insert_maps(DBPool, ModName, MapList) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_field_list(),
    ValuesList = [ModName:get_table_values(Map) || Map <- MapList],
    {ok, _} = db_mysql:insert_rows(DBPool, TableName, FieldList, ValuesList).

update_maps(_DB, _ModName, []) ->
    ok;
update_maps(DBPool, ModName, MapList) ->
    TableName = ModName:get_table_name(),
    UpdateFields = ModName:get_table_field_list(),
    KeyFieldList = ModName:get_table_key_field_list(),
    {KeyValuesList, UpdateValuesList} = get_key_values_and_values_list(ModName, MapList),
    {ok, _} = db_mysql:update_rows(DBPool, TableName, UpdateFields, UpdateValuesList, KeyFieldList, KeyValuesList).

delete_maps(_DB, _ModName, []) ->
    ok;
delete_maps(DBPool, ModName, MapList) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_field_list(),
    KeyValuesList = [ModName:get_table_key_values(Map) || Map <- MapList],
    {ok, _} = db_mysql:delete_rows(DBPool, TableName, KeyFieldList, KeyValuesList).

insert_record(DBPool, ModName, Record) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_field_list(),
    Values = ModName:get_table_values(Record),
    {ok, _} = db_mysql:insert_row(DBPool, TableName, FieldList, Values).

update_record(DBPool, ModName, Record) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_field_list(),
    Values = ModName:get_table_values(Record),
    KeyFieldList = ModName:get_table_key_field_list(),
    KeyValues = ModName:get_table_key_values(Record),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:update_rows(DBPool, TableName, FieldList, Values, Conditions).

delete_record(DBPool, ModName, Record) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_field_list(),
    KeyValues = ModName:get_table_key_values(Record),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:delete_rows(DBPool, TableName, Conditions).

insert_record_list(_DB, _ModName, []) ->
    ok;
insert_record_list(DBPool, ModName, RecordList) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_field_list(),
    ValuesList = [ModName:get_table_values(Record) || Record <- RecordList],
    {ok, _} = db_mysql:insert_rows(DBPool, TableName, FieldList, ValuesList).

update_record_list(_DB, _ModName, []) ->
    ok;
update_record_list(DBPool, ModName, RecordList) ->
    TableName = ModName:get_table_name(),
    UpdateFields = ModName:get_table_field_list(),
    KeyFieldList = ModName:get_table_key_field_list(),
    {KeyValuesList, UpdateValuesList} = get_key_values_and_values_list(ModName, RecordList),
    {ok, _} = db_mysql:update_rows(DBPool, TableName, UpdateFields, UpdateValuesList, KeyFieldList, KeyValuesList).

delete_record_list(_DB, _ModName, []) ->
    ok;
delete_record_list(DBPool, ModName, RecordList) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_field_list(),
    KeyValuesList = [ModName:get_table_key_values(Record) || Record <- RecordList],
    {ok, _} = db_mysql:delete_rows(DBPool, TableName, KeyFieldList, KeyValuesList).

get_key_values_and_values_list(ModName, [Struct | T]) ->
    {KeyValuesList, ValuesList} = get_key_values_and_values_list(ModName, T),
    KeyValues = ModName:get_table_key_values(Struct),
    Values = ModName:get_table_values(Struct),
    {[KeyValues | KeyValuesList], [Values | ValuesList]};
get_key_values_and_values_list(_ModName, []) ->
    {[], []}.

key_conditions([KeyField], [Value]) ->
    [{KeyField, '=', Value}];
key_conditions([KeyField | T1], [Value | T2]) ->
    [{KeyField, '=', Value}, 'AND' | key_conditions(T1, T2)].
