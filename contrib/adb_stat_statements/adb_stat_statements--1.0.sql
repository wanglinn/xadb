/* contrib/adb_stat_statements/adb_stat_statements--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION adb_stat_statements" to load this file. \quit

-- Start all doctor process
CREATE OR REPLACE FUNCTION explain_rtable_of_query(IN query pg_catalog.text,
												   OUT schemaname pg_catalog.name,
												   OUT relname pg_catalog.name)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION explain_rtable_plan_of_query(IN query pg_catalog.text,
														OUT schemaname pg_catalog.name,
														OUT relname pg_catalog.name,
														OUT attname pg_catalog.name,
														OUT planname pg_catalog.text)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Create function create_node_server
create or replace function create_node_server ( pi_node_name name
                                              , pi_node_port int
                                              , pi_node_host name
                                              , pi_view_name varchar default null
                                              )
returns boolean
as
$$
declare
  l_item_found  int;
begin

  select count(*)
    into l_item_found
    from pg_foreign_server
   where srvname = 'gvsrv_'||pi_node_name
     and srvoptions[0] != '{host='||pi_node_host||',port='||pi_node_port||',dbname='||current_database()||'}';

  if l_item_found > 0
  then
    -- raise notice 'Found server with wrong options for node [%]', pi_node_name;
    return false;
  end if;

  -- Check server
  select count(*)
    into l_item_found
    from pg_foreign_server
   where srvname = 'gvsrv_'||pi_node_name;

  if l_item_found > 0
  then
    -- raise notice 'Server found, skip creation for node [%]', pi_node_name;
  else
    -- Create Foreign Server
    execute 'create server if not exists gvsrv_'||pi_node_name||' foreign data wrapper postgres_fdw OPTIONS (host '''||pi_node_host||''', port '''||pi_node_port||''', dbname '''||current_database()||''')';
    -- Create User Mapping
    execute 'create user mapping if not exists FOR CURRENT_USER SERVER gvsrv_'||pi_node_name||' options (user '''||session_user||''', password '''||session_user||''')';
  end if;

  -- Check schema
  select count(*)
    into l_item_found
    from pg_namespace
   where nspname = 'gvfdw_'||pi_node_name;

  if l_item_found = 0
  then
    -- Create Schema
    execute 'Create schema if not exists gvfdw_'||pi_node_name;
  end if;

  -- Check foreign table
  select count(*)
    into l_item_found
    from pg_class
   where relnamespace::regnamespace::text = 'gvfdw_'||pi_node_name
     and relname::varchar = pi_view_name;

  if l_item_found = 0
  then
    -- Import from pg_catalog
    execute 'import FOREIGN SCHEMA pg_catalog
              limit to ( '||pi_view_name||')
               from server gvsrv_'||pi_node_name||' into gvfdw_'||pi_node_name;

    -- Check foreign table again
    select count(*)
      into l_item_found
      from pg_class
     where relnamespace::regnamespace::text = 'gvfdw_'||pi_node_name
       and relname::varchar = pi_view_name;

    if l_item_found = 0
    then
      -- Import from public
      execute 'import FOREIGN SCHEMA public
                limit to ( '||pi_view_name||')
                 from server gvsrv_'||pi_node_name||' into gvfdw_'||pi_node_name;
    end if;
  end if;

  return true;
end;
$$ language plpgsql;


-- Create function query_gv_views
create or replace function query_gv_views (pi_view_name  varchar)
returns setof record
as
$$
declare
  l_node_record record;
  l_create_node boolean;
begin
  for l_node_record in select oid as node_oid, node_name, node_type, node_port, node_host from pgxc_node where node_type in ('C', 'D')
  loop
    select create_node_server(l_node_record.node_name, l_node_record.node_port, l_node_record.node_host, pi_view_name) into l_create_node;
    return query execute 'select $1 as node_oid, $2 as node_name, $3 as node_type, * from gvfdw_'||l_node_record.node_name||'.'||pi_view_name
                   using l_node_record.node_oid, l_node_record.node_name, l_node_record.node_type;
  end loop;
end;
$$ language plpgsql;


-- Global view for [gv_stat_activity]
create or replace view gv_stat_activity
as
select * from query_gv_views('pg_stat_activity')
as
t(node_oid oid,node_name name,node_type "char"
 ,datid oid,datname name,pid integer,usesysid oid,usename name,application_name text,client_addr inet,client_hostname text,client_port integer,backend_start timestamp with time zone,xact_start timestamp with time zone,query_start timestamp with time zone,state_change timestamp with time zone,wait_event_type text,wait_event text,state text,backend_xid xid,backend_xmin xid,query text,backend_type text
 );

-- Global view for [gv_locks]
create or replace view gv_locks
as
select * from query_gv_views('pg_locks')
as
t(node_oid oid,node_name name,node_type "char"
 ,locktype text,database oid,relation oid,page integer,tuple smallint,virtualxid text,transactionid xid,classid oid,objid oid,objsubid smallint,virtualtransaction text,pid integer,mode text,granted boolean,fastpath boolean
 );

-- Global view for [gv_stat_all_tables]
create or replace view gv_stat_all_tables
as
select schemaname
     , relname
     , sum(seq_scan) as seq_scan
     , sum(seq_tup_read) as seq_tup_read
     , sum(idx_scan) as idx_scan
     , sum(idx_tup_fetch) as idx_tup_fetch
     , sum(n_tup_ins) as n_tup_ins
     , sum(n_tup_upd) as n_tup_upd
     , sum(n_tup_del) as n_tup_del
     , sum(n_tup_hot_upd) as n_tup_hot_upd
     , max(n_live_tup) as n_live_tup
     , max(n_dead_tup) as n_dead_tup
     , sum(n_mod_since_analyze) as n_mod_since_analyze
     , max(last_vacuum) as last_vacuum
     , max(last_autovacuum) as last_autovacuum
     , max(last_analyze) as last_analyze
     , max(last_autoanalyze) as last_autoanalyze
     , sum(vacuum_count) as vacuum_count
     , sum(autovacuum_count) as autovacuum_count
     , sum(analyze_count) as analyze_count
     , sum(autoanalyze_count) as autoanalyze_count
  from query_gv_views('pg_stat_all_tables')
    as t(node_oid oid,node_name name,node_type "char"
        ,relid oid,schemaname name,relname name,seq_scan bigint,seq_tup_read bigint,idx_scan bigint,idx_tup_fetch bigint,n_tup_ins bigint,n_tup_upd bigint,n_tup_del bigint,n_tup_hot_upd bigint,n_live_tup bigint,n_dead_tup bigint,n_mod_since_analyze bigint,last_vacuum timestamp with time zone,last_autovacuum timestamp with time zone,last_analyze timestamp with time zone,last_autoanalyze timestamp with time zone,vacuum_count bigint,autovacuum_count bigint,analyze_count bigint,autoanalyze_count bigint
        )
 where node_type = 'C'
 group by schemaname, relname;

-- Global view for [pg_stat_statements]
create or replace view gv_stat_statements
as
select * from query_gv_views('pg_stat_statements')
as
t(node_oid oid,node_name name,node_type "char"
 ,userid oid, dbid oid, queryid bigint, query text, calls bigint, total_time double precision, min_time double precision, max_time double precision, mean_time double precision, stddev_time double precision, rows bigint, shared_blks_hit bigint, shared_blks_read bigint, shared_blks_dirtied bigint, shared_blks_written bigint, local_blks_hit bigint, local_blks_read bigint, local_blks_dirtied bigint, local_blks_written bigint, temp_blks_read bigint, temp_blks_written bigint, blk_read_time double precision, blk_write_time double precision
 );
