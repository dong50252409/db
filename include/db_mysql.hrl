%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 9月 2021 17:12
%%%-------------------------------------------------------------------
-ifndef(DB_MYSQL_H).
-define(DB_MYSQL_H, true).

-ifdef(show_sql).
-define(SHOW_SQL(Params), io:format("SQL:~ts~nParams:~tp~n", Params)).
-else.
-define(SHOW_SQL(_Params), ok).
-endif.

-export_type([
    db_name/0, table_name/0,
    field/0, fields/0,
    value/0, values/0,
    sql/0,
    operator/0, condition/0,
    affected_rows/0, query_error/0,
    struct_type/0, struct/0]).

-type db_name() :: poolboy:pool().
-type mysql_conn() :: mysql:connection().
-type table_name() :: atom().
-type field() :: atom().
-type fields() :: [field()].
-type value() :: term().
-type values() :: [value()].
-type sql() :: iodata().
-type operator() :: '='|'!='|'>'|'<'|'>='|'<='|'LIKE'|'BETWEEN'|'AND'|'OR'|'IN'|'NOT IN'.
-type condition() :: {field(), operator(), value()}|{field(), operator(), value(), operator(), value()}|operator().
-type affected_rows() :: non_neg_integer().
-type query_error() :: {error, mysql:server_reason()}.
-type options() :: [option()].
-type option() :: {struct_type, struct_type()} | {flush_interval, {timeout(), Args :: term()}}.
-type struct_type() :: map | maps | record | record_list.
-type struct() :: map() | tuple() | [tuple()] | undefined.

-endif.