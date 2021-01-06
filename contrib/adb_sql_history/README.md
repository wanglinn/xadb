
# SQL语句记录插件

通过将SQL语句写入共享内存的方式 ，记录最近若干条SQL语句。

## 相关脚本

```
adb_sql_history--1.0.sql
```

## 注意事项

1. 需要数据库超级用户登录
2. postgres.conf中增加配置预加载库:shared_preload_libraries='adb_sql_history'
3. 增加3个GUC变量： adb_sql_history.enable, adb_sql_history.track, adb_sql_history.sql_num
   其中:adb_sql_history.enable表示开关，取值为true和false;
        adb_sql_history.track有两个取值top和all，top表示记录用户输入的SQL命令，all表示记录执行用户SQL命令的所有子SQL语句；
        adb_sql_history.sql_num表示单个backend记录的SQL语句条数，共享内存按MaxBackends*单backend最大adb_sql_history.sql_num条*(单条sql最大1024字节加SQLHistoryItem结构体对齐后32字节)计算，由于共享内存大小和work_mem有限，所以条数不能设置过大，建议60条以下。
4. 插件默认打开，可以在数据库超级用户下通过set命令将adb_sql_history.enable设置为false关闭。如果需要再次打开，可以重新设置为true。adb_sql_history.sql_num不支持在线修改，修改需要重启生效。

## 使用方式

1. 使用超级用户登录 postgres 库（或其他库），运行创建脚本：

```shell
psql -U antdb -h localhost -d postgres
postgres=# create extension adb_sql_history;
```

2. 查看记录的SQL语句

```sql
postgres=# select * from adb_sql_history;
  pid   |  dbname  |                          sql                           |           lasttime            | ecount
--------+----------+--------------------------------------------------------+-------------------------------+--------
 165102 | postgres | select * from adb_sql_history                          | 2020-12-24 15:45:45.039282+08 |      1
 165102 | postgres | insert into t_test_1 select generate_series(1,1000),10 | 2020-12-24 15:45:31.507213+08 |      1
(2 rows)
```
