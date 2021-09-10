db
=====

Erlang的半自动数据库持久化应用，当前仅支持MySQL，与 [db_tools](https://github.com/dong50252409/db_tools) 配套使用

本应用使用到了一下第三方Erlang开源库

* [mysql-otp](https://github.com/mysql-otp/mysql-otp) MySQL驱动，默认安装
* [poolboy](https://github.com/devinus/poolboy) 进程池，默认安装
* [jsx](https://github.com/talentdeficit/jsx) JSON序列化反序列化，当需要将JSON转为Erlang项式时安装，默认不安装

使用方法
-----

1. 通过 [db_tools](https://github.com/dong50252409/db_tools) 生成数据库表model文件
2. 添加依赖到rebar.config

  ```
   %% 添加依赖到rebar.config
   {deps, [
        {db, {git, "https://github.com/dong50252409/db", {branch, "master"}
   ]}.
  ```

3. 配置数据库参数

  ```
  {mysql_pool, [
      {test_db, {[                  
          {size, 10},               % 这部分是poolboy的参数
          {max_overflow, 20},
          {strategy, fifo}
      ], [
          {user, "root"},           % 这部分是mysql-otp的参数
          {password, "root"},
          {host, "localhost"},
          {database, "test_db"},
          {port, 3306}
      ]}}
  ]}.
  ```

4. 使用 **db_agent:ref/3** 注册要管理的数据库表模块，或者通过 **db_agent:select/4** 查找要管理的数据库表模块数据
5. 使用 **db_agent:flush/2** 更新保存数据到数据库表中
6. 使用db_agent:ref/3 例子

  ```
  State = #{},
  DB = test_db,
  ModName = table_1,
  Options = [{struct_type, map}],
  ok = db_agent:ref(DB, ModName, Options),
  Table1 = #{field_1 => 1, field_2 => 100},
  State = #{ModName => Table1},
  _Ref = erlang:send_after(1000 * 60, self(), {db_flush, ModName}), % 一分钟后触发
  receive
    {db_flush, ModName} ->
      db_agent:flush(ModName, maps:get(ModName, State))
  end.
  ```

6. 使用db_agent:select/4 例子

  ```  
  State = #{},
  DB = test_db,
  ModName = table_1,
  Options = [{struct_type, map}],
  {ok, Table1} = db_agent:select(DB, ModName, [{field_1, '=', 100}], Options),
  NewTable1 = Table1#{field_2 := 100},
  _Ref = erlang:send_after(1000 * 60, self(), {db_flush, ModName}), % 一分钟后触发
  receive
    {db_flush, ModName} ->
      db_agent:flush(ModName, maps:get(ModName, State))
  end.
  ```

类型
----

* -type db_name() :: poolboy:pool().

  数据库连接池


* -type mysql_conn() :: mysql:connection().

  MySQL连接进程


* -type table_name() :: atom().

  数据库表名，可通过 ModName:get_table_name/0 获取


* -type field() :: atom().

  数据库表字段


* -type value() :: term().

  数据库表数据


* -type sql() :: iodata().

  SQL语句


* -type operator() :: '='|'!='|'>'|'<'|'>='|'<='|'LIKE'|'BETWEEN'|'AND'|'OR'|'IN'|'NOT IN'.

  可用的WHERE条件


* -type condition() :: {field(), operator(), value()}|{field(), operator(), value(), operator(), value()}|operator().

  单个WHERE条件格式，例如
    * [{field_1, '>=', 100}, 'AND', {field_1, '<=', 500}]
    * [field_1, 'BETWEEN', 100, 'AND', 500]
    * [field_1, 'IN', [100, 200, 300, 400, 500]]


* -type affected_rows() :: non_neg_integer().

  执行SQL后的受影响行数，仅update、delete操作有返回


* -type query_error() :: {error, mysql:server_reason()}.

  执行SQL报错后返回的信息


* -type option() :: {struct_type, struct_type()}.

  指定被管理表的选项
    * struct_type 指定数据保存的形式，此选项为必选项


* -type struct_type() :: map | maps | record | record_list.

  可选的数据保存形式
    * map 适用于单行数据保存，将数据库表数据以map形式保存，key为表字段，value为表字段值
    * maps 适用于多行数据保存，将数据库表数据以map形式保存，key为表主键列字段的值，如果存在多个主键列，则将其组织成tuple形式，value为表一行数据所组成的map
    * record 适用于单行数据保存，将数据库表数据以record形式保存
    * record_list 适用于多行数据保存，将数据库表数据以列表形式保存，每个元素未record


* -type struct() :: map() | tuple() | [tuple()] | undefined.

  指定 {struct_type, struct_type()}
  选项后可返回的数据保存形式，对于map、tuple两种保存形式，如果数据为空返回undefined。实际开发中，如果想要删除通过map、tuple两种保存形式的数据，仅需将数据赋值为undefined，当调用 db_agent:
  flush/2 函数时将自动删除对应数据

主要模块
----

* [db_agent](https://github.com/dong50252409/db/blob/master/src/db_agent.erl) 负责管理进程数据持久化
* [db_mysql](https://github.com/dong50252409/db/blob/master/src/db_mysql.erl) MySQL操作封装
