%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% 数据库代理
%%% @end
%%% Created : 08. 9月 2021 14:29
%%%-------------------------------------------------------------------
-module(db_agent).

-include("db_mysql.hrl").

-define(CHECK_OPTIONS_LIST, [struct_type]).

%% API
-export([reg/3, select/4, flush/2]).

-spec reg(DB :: db_name(), ModName :: module(), Options :: [option()]) -> ok|{error, term()}.
reg(DB, ModName, Options) ->
    case check_options(Options, ?CHECK_OPTIONS_LIST) of
        true ->
            {StructType, Cache} = get_struct_type_and_cache(Options),
            TableInfo = #{db => DB, struct_type => StructType, cache => Cache},
            erlang:put({'$db_table_info', ModName}, TableInfo),
            ok;
        Err ->
            Err
    end.

-spec select(DB :: db_name(), ModName :: module(), Conditions :: [condition()], Options :: [option()]) ->
    Result :: {ok, struct()}|{error, term()}|query_error().
select(DB, ModName, Conditions, Options) ->
    case check_options(Options, ?CHECK_OPTIONS_LIST) of
        true ->
            TableName = ModName:get_table_name(),
            case db_mysql:select(DB, TableName, [], Conditions) of
                {ok, _Columns, Rows} ->
                    AsStruct = proplists:get_value(struct_type, Options),
                    Struct = as_struct(ModName, Rows, AsStruct),
                    TableInfo = #{db => DB, struct_type => AsStruct, cache => Struct},
                    erlang:put({'$db_table_info', ModName}, TableInfo),
                    {ok, Struct};
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

-spec flush(ModName :: module(), NewStruct :: struct()) -> ok.
flush(ModName, NewStruct) ->
    case get({'$db_table_info', ModName}) of
        #{db := DB, struct_type := StructType, cache := Struct} = TableInfo ->
            compare(StructType, DB, ModName, NewStruct, Struct),
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

compare(map, DB, ModName, NewStruct, Struct) ->
    compare_map(DB, ModName, NewStruct, Struct);
compare(maps, DB, ModName, NewStruct, Struct) ->
    compare_maps(DB, ModName, NewStruct, Struct);
compare(record, DB, ModName, NewStruct, Struct) ->
    compare_record(DB, ModName, NewStruct, Struct);
compare(record_list, DB, ModName, NewStruct, Struct) ->
    compare_record_list(DB, ModName, NewStruct, Struct).

compare_map(_DB, _ModName, Map, Map) ->
    nothing;
compare_map(DB, ModName, undefined, OldMap) ->
    delete_map(DB, ModName, OldMap);
compare_map(DB, ModName, NewMap, undefined) ->
    insert_map(DB, ModName, NewMap);
compare_map(DB, ModName, NewMap, _OldMap) ->
    update_map(DB, ModName, NewMap).

compare_maps(DB, ModName, NewMaps, undefined) ->
    insert_maps(DB, ModName, maps:values(NewMaps));
compare_maps(DB, ModName, NewMaps, OldMaps) ->
    Iter = maps:iterator(NewMaps),
    {InsertList, UpdateList, DeleteList} = compare_maps_1(maps:next(Iter), OldMaps, [], []),
    insert_maps(DB, ModName, InsertList),
    update_maps(DB, ModName, UpdateList),
    delete_maps(DB, ModName, DeleteList).

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

compare_record(_DB, _ModName, Record, Record) ->
    nothing;
compare_record(DB, ModName, undefined, OldRecord) ->
    delete_record(DB, ModName, OldRecord);
compare_record(DB, ModName, NewRecord, undefined) ->
    insert_record(DB, ModName, NewRecord);
compare_record(DB, ModName, NewRecord, _OldRecord) ->
    update_record(DB, ModName, NewRecord).

compare_record_list(DB, ModName, NewRecordList, OldRecordList) ->
    {InsertList, UpdateList, DeleteRecordList} = compare_record_list_1(ModName, NewRecordList, OldRecordList, [], []),
    insert_record_list(DB, ModName, InsertList),
    update_record_list(DB, ModName, UpdateList),
    delete_record_list(DB, ModName, DeleteRecordList).

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

insert_map(DB, ModName, Map) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_filed_list(),
    Values = ModName:get_table_values(Map),
    {ok, _} = db_mysql:insert_row(DB, TableName, FieldList, Values).

update_map(DB, ModName, Map) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_filed_list(),
    Values = ModName:get_table_values(Map),
    KeyFieldList = ModName:get_table_key_filed_list(),
    KeyValues = ModName:get_table_key_values(Map),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:update_rows(DB, TableName, FieldList, Values, Conditions).

delete_map(DB, ModName, Map) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    KeyValues = ModName:get_table_key_values(Map),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:delete_rows(DB, TableName, Conditions).

insert_maps(DB, ModName, MapList) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_filed_list(),
    ValuesList = [ModName:get_table_values(Map) || Map <- MapList],
    {ok, _} = db_mysql:insert_rows(DB, TableName, FieldList, ValuesList).

update_maps(DB, ModName, MapList) ->
    TableName = ModName:get_table_name(),
    UpdateFields = ModName:get_table_filed_list(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    {KeyValuesList, UpdateValuesList} = get_key_values_and_values_list(ModName, MapList),
    {ok, _} = db_mysql:update_rows(DB, TableName, UpdateFields, UpdateValuesList, KeyFieldList, KeyValuesList).

delete_maps(DB, ModName, MapList) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    KeyValuesList = [ModName:get_table_key_values(Map) || Map <- MapList],
    {ok, _} = db_mysql:delete_rows(DB, TableName, KeyFieldList, KeyValuesList).

insert_record(DB, ModName, Record) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_filed_list(),
    Values = ModName:get_table_values(Record),
    {ok, _} = db_mysql:insert_row(DB, TableName, FieldList, Values).

update_record(DB, ModName, Record) ->
    TableName = ModName:get_table_name(),
    FieldList = ModName:get_table_filed_list(),
    Values = ModName:get_table_values(Record),
    KeyFieldList = ModName:get_table_key_filed_list(),
    KeyValues = ModName:get_table_key_values(Record),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:update_rows(DB, TableName, FieldList, Values, Conditions).

delete_record(DB, ModName, Record) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    KeyValues = ModName:get_table_key_values(Record),
    Conditions = key_conditions(KeyFieldList, KeyValues),
    {ok, _} = db_mysql:delete_rows(DB, TableName, Conditions).

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

delete_record_list(DB, ModName, RecordList) ->
    TableName = ModName:get_table_name(),
    KeyFieldList = ModName:get_table_key_filed_list(),
    KeyValuesList = [ModName:get_table_values(Record) || Record <- RecordList],
    {ok, _} = db_mysql:delete_rows(DB, TableName, KeyFieldList, KeyValuesList).

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
