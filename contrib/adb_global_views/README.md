
# 全局视图相关说明

通过 postgres_fdw 扩展 + pgxc_node 表，实现全局视图访问。


## 相关脚本

```
adb_global_views--1.0.sql
```

目前脚本中实现了 pg_locks/pg_stat_activity/gv_stat_all_tables 三个全局视图。

若有其他全局视图需求，可参照这三个视图的创建方式进行额外创建。


## 注意事项

1. 需要使用超级用户登录
2. 关于节点间访问的密码（默认使用当前登录的用户，以及同名密码）
    * 配置节点间的免密登录（即 pg_hba.conf 中配置为 trust 认证）
    * 节点间当前登录的超级用户，密码与用户名相同
    * 修改脚本中用户名和密码
3. 目前全局视图查询了 pgxc_node 中记录的所有 Coordinator 和 Datanode 节点
4. 全局视图增加和节点信息相关的 3 个字段： node_oid, node_name, node_type


## 使用方式

1. 使用超级用户登录 postgres 库（或其他库），运行创建脚本：

```shell
psql -U antdb -h localhost -d postgres
create extension if not exists adb_global_views cascade;
```

2. 查看全局视图

```sql
postgres=# \dv gv*
              List of relations
 Schema |        Name        | Type | Owner  
--------+--------------------+------+--------
 public | gv_locks           | view | hongye
 public | gv_stat_activity   | view | hongye
 public | gv_stat_all_tables | view | hongye
```

3. 使用全局视图

```sql
postgres=# select * from gv_Locks;
 node_oid | node_name | node_type |  locktype  | database | relation | page | tuple | virtualxid | transactionid | classid | objid | objsubid | virtualtransaction |  pid  |      mode       | granted | fastpath 
----------+-----------+-----------+------------+----------+----------+------+-------+------------+---------------+---------+-------+----------+--------------------+-------+-----------------+---------+----------
    11861 | cm_1      | C         | relation   |    13522 |    11717 |      |       |            |               |         |       |          | 3/22895            | 31092 | AccessShareLock | t       | t
    11861 | cm_1      | C         | virtualxid |          |          |      |       | 3/22895    |               |         |       |          | 3/22895            | 31092 | ExclusiveLock   | t       | t
    11861 | cm_1      | C         | relation   |    13522 |    49503 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | relation   |    13522 |     1259 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | relation   |    13522 |     2615 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | relation   |    13522 |      549 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | relation   |    13522 |      113 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | relation   |    13522 |     1417 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | relation   |    13522 |    49285 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | t
    11861 | cm_1      | C         | virtualxid |          |          |      |       | 2/336626   |               |         |       |          | 2/336626           | 30525 | ExclusiveLock   | t       | t
    11861 | cm_1      | C         | advisory   |    13522 |          |      |       |            |               |   65535 | 65535 |        2 | 3/22895            | 31092 | ShareLock       | t       | f
    11861 | cm_1      | C         | relation   |        0 |     9015 |      |       |            |               |         |       |          | 2/336626           | 30525 | AccessShareLock | t       | f
    16384 | cm_2      | C         | relation   |    13522 |    11717 |      |       |            |               |         |       |          | 3/1568             | 31097 | AccessShareLock | t       | t
    16384 | cm_2      | C         | virtualxid |          |          |      |       | 3/1568     |               |         |       |          | 3/1568             | 31097 | ExclusiveLock   | t       | t
    16384 | cm_2      | C         | advisory   |    13522 |          |      |       |            |               |   65535 | 65535 |        2 | 3/1568             | 31097 | ShareLock       | t       | f
    16385 | dm_1      | D         | relation   |    13522 |    11717 |      |       |            |               |         |       |          | 3/472441           | 31128 | AccessShareLock | t       | t
    16385 | dm_1      | D         | virtualxid |          |          |      |       | 3/472441   |               |         |       |          | 3/472441           | 31128 | ExclusiveLock   | t       | t
    16386 | dm_2      | D         | relation   |    13522 |    11717 |      |       |            |               |         |       |          | 3/1570             | 31136 | AccessShareLock | t       | t
    16386 | dm_2      | D         | virtualxid |          |          |      |       | 3/1570     |               |         |       |          | 3/1570             | 31136 | ExclusiveLock   | t       | t
(19 rows)
```
