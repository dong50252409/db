%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% Behavior模板
%%% @end
%%% Created : 13. 10月 2021 17:49
%%%-------------------------------------------------------------------
-module(db_model).

-export_type([table_name/0, table_field/0, table_value/0]).

-type table_name() :: atom().
-type table_field() :: atom().
-type table_value() :: term().

-callback get_table_name() -> table_name().
-callback new_map() -> map().
-callback as_map([table_value()]) -> map().
-callback new_record() -> tuple().
-callback as_record([table_value()]) -> tuple().
-callback get_table_field_list() -> [table_field()].
-callback get_table_key_field_list() -> [table_field()].
-callback get_table_key_values(term()) -> [table_value(), ...].
-callback get_table_values(term()) -> [table_value(), ...].

-optional_callbacks([new_map/0, as_map/1, new_record/0, as_record/1]).

