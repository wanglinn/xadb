# Save and count plans in PostgreSQL or/and AntDB

This plugin requires plugin pg_stat_statement, and plugin **pg_stat_statement** must load before adb_stat_statements.
If you need to work in an AntDB cluster, first make sure that the plugin **adb_global_views** is installed.

# How to use
1. Configure postgresql.conf, shared_preload_libraries = 'pg_stat_statement,adb_stat_statements'.  
2. Restart PostgreSQL.  
3. Login PostgreSQL as super user. Execute some commands like:  
```shell
psql -U antdb -h localhost -d postgres
create extension if not exists adb_stat_statements cascade;
```
This extension will be created in the antdb schema  

4. Show all statement statistics.  
```shell
select * from antdb.adb_stat_statements;
```

5. Reset all statement statistics.  
```shell
select antdb.adb_stat_statements_reset();
```

# How to uninstall  
Login PostgreSQL as super user, execute  
```shell
psql -U antdb -h localhost -d postgres
drop extension adb_stat_statements cascade;
```

# GUC Options:  
| name | default value | context | description |
| :- | :- | :- | :- | 
| adb_stat_statements.enable | true | sighup | enable adb_stat_statements. |
| adb_stat_statements.store | shm | postmaster | Selects where(shm or table) adb_stat_statements stores these plans. Note that, it is NOT recommended to set to 'table' in AntDB environment. |
| adb_stat_statements.max_plans | 5000 | postmaster | Sets the maximum number of plans tracked by adb_stat_statements. |
| adb_stat_statements.max_length | 1048579 (1M) | superuser | Sets the maximum length of a single plan tracked by adb_stat_statements. |
| adb_stat_statements.track  | top | superuser | Selects which statements are tracked by adb_stat_statements. |
| adb_stat_statements.explain_analyze | true | superuser | Sets EXPLAIN ANALYZE of plans tracked by adb_stat_statements. |
| adb_stat_statements.explain_verbose | false | superuser | Sets EXPLAIN VERBOSE of plans tracked by adb_stat_statements. |
| adb_stat_statements.explain_buffers | false | superuser | Sets EXPLAIN buffers usage of plans tracked by adb_stat_statements. |
| adb_stat_statements.explain_triggers | false | superuser | Sets Include trigger statistics of plans tracked by adb_stat_statements. |
| adb_stat_statements.explain_timing | true | superuser | Sets EXPLAIN TIMING of plans tracked by adb_stat_statements. |
| adb_stat_statements.explain_format | text | superuser | Sets EXPLAIN format of plan tracked by adb_stat_statements. |