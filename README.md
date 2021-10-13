db
=====

Erlang的半自动数据库持久化应用，当前仅支持MySQL，与 [db_tools](https://github.com/dong50252409/db_tools) 配套使用

本应用使用到了以下第三方Erlang开源库

* [mysql-otp](https://github.com/mysql-otp/mysql-otp) MySQL驱动，默认安装
* [poolboy](https://github.com/devinus/poolboy) 进程池，默认安装
* [jsx](https://github.com/talentdeficit/jsx) JSON序列化反序列化，当需要将JSON转为Erlang项式时安装，默认不安装

主要模块
-----

<table>
  <tr>
    <td><b>db_state</b></td>
    <td>进程State持久化管理</td>
  </tr>
  <tr>
    <td><b>db_ets</b></td>
    <td>ETS表持久化管理</td>
  </tr>
<tr>
    <td><b>db_ets_transform</b></td>
    <td>处理注册ETS表的函数模块替换</td>
  </tr>
<tr>
    <td><b>db_mysql</b></td>
    <td>MySQL增删改查API封装</td>
  </tr>
<tr>
    <td><b>db_util</b></td>
    <td>一些工具函数</td>
  </tr>
</table>

配置及使用
-----

1. 通过 [db_tools](https://github.com/dong50252409/db_tools) 生成数据库表model文件

2. 添加依赖到`rebar.config`

  ```
   {deps, [
        {db, {git, "https://github.com/dong50252409/db", {branch, "master"}
   ]}.
  ```

3. 配置数据库参数

  ```
  {mysql_pool, [
      %% {数据库连接池名，{[poolboy的参数], [mysql-otp的参数]}}
      {db_pool, {[                  
          {size, 10},               
          {max_overflow, 20},
          {strategy, fifo}
      ], [
          {user, "root"},           
          {password, "root"},
          {host, "localhost"},
          {database, "test_db"},
          {port, 3306}
      ]}}
  ]}.
  ```

### db_state使用方法

1. 使用`db_state:reg/3`注册要管理的数据库表模块，或者通过`db_state:reg_select/4`注册要管理的数据库表模块，并查数据库表
2. 使用`db_state:flush/2`更新同步数据到数据库表中
3. 使用`db_state:reg/3`例子

  ```
  State = #{},
  DBPool = db_pool,
  ModName = table_1,
  Options = [{struct_type, map}],
  ok = db_state:reg(DBPool, ModName, Options),
  Table1 = #{field_1 => 1, field_2 => 100},
  State = #{ModName => Table1},
  _Ref = erlang:send_after(1000 * 60, self(), {db_flush, ModName}), % 一分钟后触发
  receive
    {db_flush, ModName} ->
      db_state:flush(ModName, maps:get(ModName, State))
  end.
  ```

4. 使用`db_state:reg_select/4`例子

  ```  
  State = #{},
  DBPool = db_pool,
  ModName = table_1,
  Conditions = [{field_1, '=', 100}],
  Options = [{struct_type, map}],
  {ok, Table1} = db_state:reg_select(DBPool, ModName, Conditions, Options),
  NewTable1 = Table1#{field_2 := 100},
  _Ref = erlang:send_after(1000 * 60, self(), {db_flush, ModName}), % 一分钟后触发
  receive
    {db_flush, ModName} ->
      db_state:flush(ModName, maps:get(ModName, State))
  end.
  ```

### db_ets使用方法

1. 使用`db_ets:reg/4`注册要管理的ETS表，使用`db_ets:init_insert/2`插入数据库中已存在的数据，或通过`db_ets:reg_select/5`注册要管理的ETS表，并查找数据并自动插入ETS表
2. 使用`db_ets:flush/1`更新同步数据到数据库表中
3. 使用`db_ets:pull/1`获取脏数据列表
4. 实例1，通过`db_ets:reg/4`管理ETS表

  ```
  ModName = table_1,
  TableName = ModName:get_table_name(),
  DBPool = db_pool,
  
  {ok, _Columns, Rows}  = db_mysql:select(DBPool, TableName, []),
  Records = [ModName:as_record(Row) || Row <- Rows],
  
  Options = [{mode, auto}],
  Tab = ets:new(ets_table_1, [named_table, set, {keypos, #table_1.field_1}]),
  ok = db_ets:reg(Tab, DBPool, ModName, Options),
  db_ets:init_insert(Tab, Records),
  
  ets:update_element(Tab, 1, {#table_1.field_2, 200}),
  ok.
  ```

6. 实例2，通过`db_ets:reg_select/5`管理ETS表

  ```
  ModName = table_1,
  DBPool = db_pool,
  Conditions = [],
  Options = [{mode, auto}],
  Tab = ets:new(ets_table_1, [named_table, set, {keypos, #table_1.field_1}]),
  ok = db_ets:reg_select(Tab, DBPool, ModName, Conditions, Options),
  ets:update_element(Tab, 1, {#table_1.field_2, 200}),
  
  ```

7. 实例3，通过设置回调模块定时接收脏数据列表，并做更新处理

  ```
  ModName = table_1,
  DBPool = db_pool,
  Conditions = [],
  Options = [{mode, {callback, ?MODULE}}],
  Tab = ets:new(ets_table_1, [named_table, set, {keypos, #table_1.field_1}]),
  ok = db_agent_ets:reg_select(Tab, DBPool, ModName, Conditions, Options),
  
  Records = [#table_1{field_1 => 1, field_2 => 100}, #table_1{field_1 => 2, field_2 => 500}],
  ets:insert(Tab, Records),
  
  receive
    {ets_dirty_list, InsertKeyList, UpdateKeyList, DeleteKeyList} ->
      ok  % 执行自己的同步逻辑
  end.
  ```

进阶使用
-----

1. 实现`db_ets_transform.erl`中规定的回调函数

  ```
  -module(xx_callback).
  
  -behavior(db_ets_transform).
  
  -export([reg_list/0]).

  reg_list() ->
      ETSTab1 = ets_table_1,
      ETSTab2 = ets_table_2,
      DBPool = db_pool,
      ModeName1 = table_1,
      ModeName2 = table_2,
      Conditions = [],
      Options = [{mode, auto}, {flush_interval, 5000}],
      [
          {ETSTab1, DBPool, ModeName1, Conditions, Options},    % 注册ETS表，并根据Conditions自动从数据库中读取数据，并插入到给定ETS表中
          {ETSTab2, DBPool, ModeName2, Options}                 % 仅注册ETS表
      ].
  ```

2. 追加以下内容到`rebar.config`

  ```
  {erl_opts, [
      {parse_transform, db_ets_transform},                        % 增加parse_transform编译选项
      {db_ets_callback, xx_callback},                             % 配置回调模块名
      db_ets_verbose                                              % 如果需要显示parse_transform替换函数详情，则定义此值
  ]}.
  
  {erl_first_files, ["src/xx_callback.erl"]}.                     % 确保回调模块在被db_ets_transform模块调用前被编译完成
  ```

3. 使用过程中无需显式调用`db_ets:xx`相关函数，编译过程中由`db_ets_transform.erl`模块自动处理。
   **注意!!! 调用`ets:xx`相关函数的第一个参数必须显式传入atom()类型的ETS表名,否则无法识别替换（实例1为错误示范）**

4. 实例1，错误示范

  ```
  ETSTab = ets_table_1,
  ModName = table_1,
  
  % ETSTab变量在parse_transform过程中无法识别，ets:new/2无法进行替换
  Tab = ets:new(ETSTab, [named_table, set, {keypos, #table_1.field_1}]),
  
  %% ETSTab变量在parse_transform过程中无法识别，ets:update_element/3无法进行替换，
  本次操将无法正确同步到数据库中
  ets:update_element(ETSTab, 1, {#table_1.field_2, 200}),                   
  ok.
  ```

5. 正确示范

  ```
  ModName = table_1,
  Tab = ets:new(ets_table_1, [named_table, set, {keypos, #table_1.field_1}]),
  ets:update_element(ets_table_1, 1, {#table_1.field_2, 200}),
  ok.
  ```

  ```
  -define(ETS_TABLE_2, ets_table_2).
  
  ModName = table_2,
  {ok, _Columns, Rows}  = db_mysql:select(DBPool, TableName, []),
  Records = [ModName:as_record(Row) || Row <- Rows],
 
  Tab = ets:new(?ETS_TABLE_2, [named_table, set, {keypos, #table_2.field_1}]),
  db_ets:init_insert(?ETS_TABLE_2, Records),
  
  ets:update_element(?ETS_TABLE_2, 1, {#table_2.field_2, 200}),
  ok.
  ```

db_mysql类型
----

* `-type db_pool() :: poolboy:pool().`

  数据库连接池


* `-type mysql_conn() :: mysql:connection().`

  MySQL连接进程


* `-type table_name() :: atom().`

  数据库表名，可通过`ModName:get_table_name/0`获取


* `-type field() :: atom().`

  数据库表字段


* `-type value() :: term().`

  数据库表数据


* `-type sql() :: iodata().`

  SQL语句


* `-type operator() :: '='|'!='|'>'|'<'|'>='|'<='|'LIKE'|'BETWEEN'|'AND'|'OR'|'IN'|'NOT IN'.`

  可用的WHERE条件


* `-type condition() :: {field(), operator(), value()}|{field(), operator(), value(), operator(), value()}|operator().`

  单个WHERE条件格式，例如
    * [{field_1, '>=', 100}, 'AND', {field_1, '<=', 500}]
    * [field_1, 'BETWEEN', 100, 'AND', 500]
    * [field_1, 'IN', [100, 200, 300, 400, 500]]


* `-type affected_rows() :: non_neg_integer().`

  执行SQL后的受影响行数，仅`update_all/4, update_rows/5, update_rows/6, delete_rows/3, delete_rows/4`操作有返回


* `-type query_error() :: {error, mysql:server_reason()}.`

  执行SQL报错后返回的信息

db_state类型
----

* `-type option() :: {struct_type, struct_type()}.`

  指定被管理表的选项，此选项为必选项


* `-type struct_type() :: map | maps | record | record_list.`

  可选的数据保存形式
    * `map`适用于单行数据保存，将数据库表数据以map形式保存，key为表字段，value为表字段值
    * `maps`适用于多行数据保存，将数据库表数据以map形式保存，key为表主键列字段的值，如果存在多个主键列，则将其组织成tuple形式，value为表一行数据所组成的map
    * `record`适用于单行数据保存，将数据库表数据以record形式保存
    * `record_list`适用于多行数据保存，将数据库表数据以列表形式保存，每个元素为record


* `-type struct() :: map() | tuple() | [tuple()] | undefined.`

  指定`{struct_type, struct_type()}`
  选项后可返回的数据保存形式，对于map、tuple两种保存形式，如果数据为空返回undefined。实际开发中，如果想要删除通过map、tuple两种保存形式的数据，仅需将数据赋值为undefined，当调用`
  db_state:flush/2`函数时将自动删除对应数据

db_ets类型
----

* `-type option() :: {mode, auto|{callback, module()|pid()}}|{flush_interval, timeout()}.`

  db_ets可用选项

    * `mode` 指定运作模式，所有受管理的ETS表都将设置`heir`选项，以便在ETS销毁时做善后工作
        * `auto` 指定每个刷新间隔自动同步数据库
        * `{callback, module()|pid()}` 指定回调进程注册名或进程PID，每个刷新间隔发送消息`{ets_dirty_list, InsertList, UpdateList, DeleteList}`
          到回调进程，主要用于当ETS表的`protection`为`private`时的一种妥协方案
    * `{flush_interval, timeout()}` 指定自动同步数据库或触发回调间隔，默认：5秒

db_ets_transform类型
----

* `-type reg_info() :: {ets:tab(), db_mysql:db_pool(), module(), [db_mysql:condition()], [db_ets:option()]}|{ets:tab(), db_mysql:db_pool(), module(), [db_ets:option()]}.`

  db_ets_transform回调返回参数
    