%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% MySQL封装API
%%% @end
%%% Created : 25. 8月 2021 17:27
%%%-------------------------------------------------------------------
-module(db_mysql).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-export([
    insert_row/4, insert_rows/4, replace_rows/4,
    truncate_table/2, delete_rows/3, delete_rows/4,
    update_all/4, update_rows/5, update_rows/6,
    select/3, select/4]).

-export([
    checkin/2, checkout/1,
    query/2, query/3,
    prepare/2,
    transaction/2, transaction/3, transaction/4
]).

-export_type([db_pool/0, mysql_conn/0, table_name/0, field/0, value/0, sql/0, operator/0, condition/0, affected_rows/0, query_error/0]).

-type db_pool() :: poolboy:pool().
-type mysql_conn() :: mysql:connection().
-type table_name() :: atom().
-type field() :: atom().
-type value() :: term().
-type sql() :: iodata().
-type operator() :: '='|'!='|'>'|'<'|'>='|'<='|'LIKE'|'BETWEEN'|'AND'|'OR'|'IN'|'NOT IN'.
-type condition() :: {field(), operator(), value()}|{field(), operator(), value(), operator(), value()}|operator().
-type affected_rows() :: non_neg_integer().
-type query_error() :: {error, mysql:server_reason()}.


-ifdef(SHOW_SQL).
-define(SQL(Params), io:format("SQL:~ts~nParams:~tp~n", Params)).
-else.
-define(SQL(_Params), ok).
-endif.

%%%-------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
%% @doc 插入一行数据
-spec insert_row(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()], Values :: [value()]) ->
    Result :: {ok, affected_rows()}|mysql:query_result().
insert_row(DBPool, TableName, Fields, Values) when is_atom(TableName), is_list(Fields), is_list(Values) ->
    SQL = io_lib:format("INSERT INTO `~ts` (~ts) VALUES (~ts);", [TableName, join_fields(Fields), join_values(Values)]),
    query(DBPool, SQL, Values).

%% @doc 插入多行数据
-spec insert_rows(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()], ValuesList :: [[value()]]) ->
    Result :: {ok, affected_rows()}|query_error().
insert_rows(DBPool, TableName, Fields, ValuesList) when is_atom(TableName), is_list(Fields), is_list(ValuesList) ->
    SQL = io_lib:format("INSERT INTO `~ts` (~ts) VALUES (~ts);", [TableName, join_fields(Fields), join_values(hd(ValuesList))]),
    ?SQL([SQL, ValuesList]),
    case transaction(DBPool, fun insert_transaction/3, [SQL, ValuesList]) of
        {atomic, Result} ->
            Result;
        {aborted, Err} ->
            Err
    end.

%% @doc 替换多行数据
-spec replace_rows(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()], ValuesList :: [[value()]]) ->
    Result :: {ok, affected_rows()}|query_error().
replace_rows(DBPool, TableName, Fields, ValuesList) when is_atom(TableName), is_list(Fields), is_list(ValuesList) ->
    SQL = io_lib:format("REPLACE INTO `~ts` (~ts) VALUES (~ts);", [TableName, join_fields(Fields), join_values(hd(ValuesList))]),
    ?SQL([SQL, ValuesList]),
    case transaction(DBPool, fun insert_transaction/3, [SQL, ValuesList]) of
        {atomic, Result} ->
            Result;
        {aborted, Err} ->
            Err
    end.

%% @doc 截断表
-spec truncate_table(DBPool :: db_pool(), TableName :: table_name()) -> ok.
truncate_table(DBPool, TableName) when is_atom(TableName) ->
    SQL = io_lib:format("TRUNCATE `~ts`;", [TableName]),
    query(DBPool, SQL),
    ok.

%% @doc 根据条件，删除表数据
-spec delete_rows(DBPool :: db_pool(), TableName :: table_name(), Conditions :: [condition()]) ->
    Result :: {ok, affected_rows()}|mysql:query_result().
delete_rows(DBPool, TableName, Conditions) when is_atom(TableName), is_list(Conditions) ->
    {ConvertCondition, ConvertValues} = condition_convert(Conditions),
    SQL = io_lib:format("DELETE FROM `~ts` WHERE ~ts;", [TableName, ConvertCondition]),
    query(DBPool, SQL, ConvertValues).

%% @doc 根据主键列表和值列表，删除表数据
-spec delete_rows(DBPool :: db_pool(), TableName :: table_name(), KeyFields :: [field()], KeyValuesList :: [[value()]]) ->
    Result :: {ok, affected_rows()}|mysql:query_result().
delete_rows(DBPool, TableName, KeyFields, KeyValuesList) when is_atom(TableName), is_list(KeyFields), is_list(KeyValuesList) ->
    Conditions = lists:join(" AND ", [[atom_to_list(KeyField), "=?"] || KeyField <- KeyFields]),
    SQL = io_lib:format("DELETE FROM `~ts` WHERE ~ts;", [TableName, Conditions]),
    ?SQL([SQL, KeyValuesList]),
    case transaction(DBPool, fun delete_rows_transaction/3, [SQL, KeyValuesList]) of
        {atomic, Result} ->
            Result;
        {aborted, Err} ->
            Err
    end.

%% @doc 更新表所有数据
-spec update_all(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()], Values :: [value()]) ->
    Result :: ok|{ok, affected_rows()}|mysql:query_result().
update_all(DBPool, TableName, Fields, Values) when is_atom(TableName), is_list(Fields), is_list(Values) ->
    SQL = io_lib:format("UPDATE `~ts` SET ~ts;", [TableName, join_update_fields(Fields)]),
    query(DBPool, SQL, Values).

%% @doc 根据条件，更新表数据
-spec update_rows(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()], Values :: [value()],
    Conditions :: [condition()]) -> Result :: ok|{ok, affected_rows()}|mysql:query_result().
update_rows(DBPool, TableName, Fields, Values, Conditions)
    when is_atom(TableName), is_list(Fields), is_list(Values), is_list(Conditions) ->
    {ConvertCondition, ConvertValues} = condition_convert(Conditions),
    SQL = io_lib:format("UPDATE `~ts` SET ~ts WHERE ~ts;", [TableName, join_update_fields(Fields), ConvertCondition]),
    query(DBPool, SQL, Values ++ ConvertValues).

%% @doc 更新多行表数据，每个更新的字段数必须一致
-spec update_rows(DBPool :: db_pool(), TableName :: table_name(), UpdateFields :: [field()], UpdateValuesList :: [[value()]],
    KeyFields :: [field()], KeyValuesList :: [[value()]]) -> Result :: {ok, affected_rows()}|mysql:query_result().
update_rows(DBPool, TableName, UpdateFields, UpdateValuesList, KeyFields, KeyValuesList)
    when is_atom(TableName), is_list(UpdateFields), is_list(UpdateValuesList), is_list(KeyFields), is_list(KeyValuesList) ->
    Conditions = [[atom_to_list(KeyField), "=?"] || KeyField <- KeyFields],
    SQL = io_lib:format("UPDATE `~ts` SET ~ts WHERE ~ts;", [TableName, join_update_fields(UpdateFields), lists:join(" AND ", Conditions)]),
    case transaction(DBPool, fun update_rows_transaction/4, [SQL, UpdateValuesList, KeyValuesList]) of
        {atomic, Result} ->
            Result;
        {aborted, Err} ->
            Err
    end.

%% @doc 查询表数据，Fields为空则查询所有字段
-spec select(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()]) -> Result :: mysql:query_result().
select(DBPool, TableName, Fields) when is_atom(TableName), is_list(Fields) ->
    case Fields of
        [] ->
            SQL = io_lib:format("SELECT * FROM `~ts`;", [TableName]);
        _ ->
            SQL = io_lib:format("SELECT ~ts FROM `~ts`;", [join_fields(Fields), TableName])
    end,
    query(DBPool, SQL).

%% @doc 根据指定条件，查询表数据，Fields为空则查询所有字段
-spec select(DBPool :: db_pool(), TableName :: table_name(), Fields :: [field()], Conditions :: [condition()]) ->
    Result :: mysql:query_result().
select(DBPool, TableName, Fields, Conditions) when is_atom(TableName), is_list(Fields), is_list(Conditions) ->
    {ConvertCondition, ConvertValues} = condition_convert(Conditions),
    case Fields of
        [] ->
            SQL = io_lib:format("SELECT * FROM `~ts` WHERE ~ts;", [TableName, ConvertCondition]);
        _ ->
            SQL = io_lib:format("SELECT ~ts FROM `~ts` WHERE ~ts;", [join_fields(Fields), TableName, ConvertCondition])
    end,
    query(DBPool, SQL, ConvertValues).


%%================================================
%% 基础功能API
%%================================================
%% @doc 获取一个MySQL连接
-spec checkin(DBPool :: db_pool(), Connection :: pid()) -> ok.
checkin(DBPool, Connection) when is_pid(Connection) ->
    poolboy:checkin(DBPool, Connection).

%% @doc 返回一个MySQL连接
-spec checkout(DBPool :: db_pool()) -> pid().
checkout(DBPool) ->
    poolboy:checkout(DBPool).

%% @doc 执行一条SQL语句
-spec query(DBPool :: db_pool(), SQL :: sql()) -> Result :: ok|{ok, affected_rows()}|mysql:query_result().
query(DBPool, SQL) ->
    query(DBPool, SQL, [], default_timeout).

%% @doc 执行一条SQL语句
-spec query(DBPool :: db_pool(), Query :: sql(), Values :: [value()]) -> Result :: ok|{ok, affected_rows()}|mysql:query_result().
query(DBPool, SQL, Values) ->
    query(DBPool, SQL, Values, default_timeout).

%% @doc 执行一条SQL语句，指定超时时长
-spec query(DBPool :: db_pool(), SQL :: sql(), Values :: [value()], Timeout :: infinity | default_timeout | timeout()) ->
    Result :: ok|{ok, affected_rows()}|mysql:query_result().
query(DBPool, SQL, Values, Timeout) ->
    ?SQL([SQL, Values]),
    Fun =
        fun(MySQLConn) ->
            case mysql:query(MySQLConn, SQL, Values, Timeout) of
                ok ->
                    case mysql:affected_rows(MySQLConn) of
                        0 ->
                            ok;
                        AffectedRows ->
                            {ok, AffectedRows}
                    end;
                OtherResult ->
                    OtherResult
            end
        end,
    poolboy:transaction(DBPool, Fun, infinity).


%% @doc 预编译，用于一条SQL语句多次执行
-spec prepare(MySQLConn :: mysql_conn(), SQL :: sql()) -> Result :: {ok, affected_rows()} | {error, mysql:server_reason()}.
prepare(MySQLConn, SQL) ->
    mysql:prepare(MySQLConn, SQL).

%% @doc 执行一个事务函数，给定的函数需要有一个参数用来传递MYSQL连接
-spec transaction(DBPool :: db_pool(), TransactionFun :: fun((mysql_conn()) -> term())) ->
    Result :: {atomic, term()} |{aborted, term()}.
transaction(DBPool, TransactionFun) when is_function(TransactionFun, 1) ->
    Fun = fun(MySQLConn) -> mysql:transaction(MySQLConn, TransactionFun, [MySQLConn], infinity) end,
    poolboy:transaction(DBPool, Fun, infinity).

%% @doc 执行一个事务函数，给定的函数需要有一个参数用来传递MYSQL连接，带有额外参数
-spec transaction(DBPool :: db_pool(), TransactionFun :: function(), Args :: list()) ->
    Result :: {atomic, term()} |{aborted, term()}.
transaction(DBPool, TransactionFun, Args) when is_function(TransactionFun, length(Args) + 1) ->
    Fun = fun(MySQLConn) -> mysql:transaction(MySQLConn, TransactionFun, [MySQLConn | Args], infinity) end,
    poolboy:transaction(DBPool, Fun, infinity).

%% @doc 执行一个事务函数，给定的函数需要有一个参数用来传递MYSQL连接，带有额外参数，可以指定重试次数
-spec transaction(DBPool :: db_pool(), TransactionFun :: function(), Args :: list(), Retries :: non_neg_integer() | infinity) ->
    Result :: {aborted, term()}.
transaction(DBPool, TransactionFun, Args, Retries) when is_function(TransactionFun, length(Args) + 1) ->
    Fun = fun(MySQLConn) -> mysql:transaction(MySQLConn, TransactionFun, [MySQLConn | Args], Retries) end,
    poolboy:transaction(DBPool, Fun, infinity).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
%% @doc 插入多行数据事务函数
-spec insert_transaction(mysql_conn(), sql(), [[value()]]) -> {atomic, {ok, affected_rows()}} |{aborted, term()}.
insert_transaction(MySQLConn, SQL, ValuesList) ->
    case mysql:prepare(MySQLConn, SQL) of
        {ok, StatementRef} ->
            InternalFun =
                fun(Values, AffectedRows) ->
                    ok = mysql:execute(MySQLConn, StatementRef, Values),
                    AffectedRows + mysql:affected_rows(MySQLConn)
                end,
            Ret = lists:foldl(InternalFun, 0, ValuesList),
            mysql:unprepare(MySQLConn, StatementRef),
            {ok, Ret};
        Err ->
            Err
    end.

%% @doc 根据主键删除多个表数据事物函数
-spec delete_rows_transaction(mysql_conn(), sql(), [[value()]]) -> {atomic, {ok, affected_rows()}} |{aborted, term()}.
delete_rows_transaction(MySQLConn, SQL, Params) ->
    case mysql:prepare(MySQLConn, SQL) of
        {ok, StatementRef} ->
            InternalFun =
                fun(Param, AffectedRows) ->
                    ok = mysql:execute(MySQLConn, StatementRef, Param),
                    AffectedRows + mysql:affected_rows(MySQLConn)
                end,
            Ret = lists:foldl(InternalFun, 0, Params),
            mysql:unprepare(MySQLConn, StatementRef),
            {ok, Ret};
        Err ->
            Err
    end.

%% @doc  更新多个相同的Record结构事物函数
-spec update_rows_transaction(mysql_conn(), sql(), [[value()]], [[value()]]) -> {atomic, {ok, affected_rows()}} |{aborted, term()}.
update_rows_transaction(MySQLConn, SQL, ValuesList, KeyValuesList) ->
    ?SQL([SQL, lists:zip(ValuesList, KeyValuesList)]),
    case mysql:prepare(MySQLConn, SQL) of
        {ok, StatementRef} ->
            InternalFun =
                fun({Values, KeyValues}, AffectedRows) ->
                    ok = mysql:execute(MySQLConn, StatementRef, Values ++ KeyValues),
                    AffectedRows + mysql:affected_rows(MySQLConn)
                end,
            Ret = lists:foldl(InternalFun, 0, lists:zip(ValuesList, KeyValuesList)),
            mysql:unprepare(MySQLConn, StatementRef),
            {ok, Ret};
        Err ->
            Err
    end.

%% @doc 拼接更新字符串
-spec join_update_fields([atom(), ...]) -> list().
join_update_fields([Field]) ->
    ["`", atom_to_list(Field), "`=?"];
join_update_fields([Field | T]) ->
    ["`", atom_to_list(Field), "`=?," | join_update_fields(T)];
join_update_fields([]) ->
    [].

%% @doc 拼接字符串
-spec join_fields([atom(), ...]) -> list().
join_fields([Field]) ->
    ["`", atom_to_list(Field), "`"];
join_fields([Field | T]) ->
    ["`", atom_to_list(Field), "`," | join_fields(T)];
join_fields([]) ->
    [].

%% @doc 平整化占位符
-spec join_values([value()]) -> list().
join_values([_Value]) ->
    ["?"];
join_values([_Value | T]) ->
    ["?," | join_values(T)];
join_values([]) ->
    [].

%% @doc 条件转换
-spec condition_convert([condition(), ...]) -> {list(), [value()]}.
condition_convert([{Field, Operator, Value} | T])
    when Operator =:= '='; Operator =:= '!='; Operator =:= '>'; Operator =:= '<'; Operator =:= '>='; Operator =:= '<='; Operator =:= 'LIKE' ->
    {Conditions, ConditionValue} = condition_convert(T),
    {["`", atom_to_list(Field), "`", atom_to_list(Operator), "?" | Conditions], [Value | ConditionValue]};
condition_convert([{Field, 'BETWEEN', Value1, 'AND', Value2} | T]) ->
    {Conditions, ConditionValue} = condition_convert(T),
    {["`", atom_to_list(Field), "` BETWEEN ? AND ?" | Conditions], [Value1, Value2 | ConditionValue]};
condition_convert([Operator | T]) when Operator =:= 'AND'; Operator =:= 'OR' ->
    {Conditions, ConditionValue} = condition_convert(T),
    {[" ", atom_to_list(Operator), " " | Conditions], ConditionValue};
condition_convert([Operator | T]) when Operator =:= '('; Operator =:= ')' ->
    {Conditions, ConditionValue} = condition_convert(T),
    {[" ", atom_to_list(Operator), " " | Conditions], ConditionValue};
condition_convert([{Field, Operator, Values} | T]) when Operator =:= 'IN'; Operator =:= 'NOT IN' ->
    {Conditions, ConditionValue} = condition_convert(T),
    {["`", atom_to_list(Field), "` ", atom_to_list(Operator), " ("] ++ join_values(Values) ++ [")" | Conditions], Values ++ ConditionValue};
condition_convert([{Field, Operator} | T]) when Operator =:= 'ASC'; Operator =:= 'DESC' ->
    {Conditions, ConditionValue} = condition_convert(T),
    {[" ORDER BY `", atom_to_list(Field), "` ", atom_to_list(Operator), " " | Conditions], ConditionValue};
condition_convert([{'LIMIT', PageSize, 'OFFSET', Offset} | T]) ->
    {Conditions, ConditionValue} = condition_convert(T),
    {[" LIMIT ", integer_to_list(PageSize), " OFFSET ", integer_to_list(Offset) | Conditions], ConditionValue};
condition_convert([]) ->
    {[], []}.
