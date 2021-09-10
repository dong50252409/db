db
=====

Erlang的半自动数据库持久化应用，当前仅支持MySQL，与 [db_tools](https://github.com/dong50252409/db_tools) 配套使用

通过 [poolboy](https://github.com/devinus/poolboy) 实现MySQL进程池

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
3. 使用 **db_agent:ref/3** 注册要管理的数据库表模块，或者通过 **db_agent:select/4** 查找要管理的数据库表模块数据
4. 使用 **db_agent:flush/2** 更新保存数据到数据库表中

主要模块
----
* [db_agent](https://github.com/dong50252409/db/blob/master/src/db_agent.erl) 负责管理进程数据持久化
* [db_mysql](https://github.com/dong50252409/db/blob/master/src/db_mysql.erl) MySQL操作封装

