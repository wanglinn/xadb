/* contrib/adb_stat_statements/adb_stat_statements--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "create extension if not exists adb_stat_statements cascade" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS antdb;

CREATE FUNCTION explain_plan(plan text,
                             format cstring default null)
RETURNS text
AS 'MODULE_PATHNAME', 'explain_plan'
LANGUAGE C VOLATILE;

CREATE FUNCTION adb_stat_statements_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'adb_stat_statements_reset'
LANGUAGE C PARALLEL SAFE;

CREATE OR REPLACE FUNCTION explain_rtable_of_plan(IN userid oid,
												  IN dbid oid,
												  IN queryid bigint,
												  IN planid bigint,
                                                  OUT schemaname pg_catalog.name,
                                                  OUT relname pg_catalog.name)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION explain_plan_nodes_of_plan(IN userid oid,
													  IN dbid oid,
													  IN queryid bigint,
													  IN planid bigint,
                                                      OUT schemaname pg_catalog.name,
                                                      OUT relname pg_catalog.name,
                                                      OUT attname pg_catalog.name,
                                                      OUT planname pg_catalog.text)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

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

-- If you want save to table, "create table" instead of "create function".
-- The order, type, etc. of the fields correspond to those defined in the code. 
-- If you change the structure of this field, the code should also be changed, 
-- such as AdbssAttributes, checkAdbssAttrs, etc. 
-- Note that the fields defined here should be consistent with TABLE adb_stat_statements
CREATE FUNCTION adb_stat_statements(
    OUT userid oid,
    OUT dbid oid,
    OUT queryid bigint,
    OUT planid bigint,
    OUT calls bigint,
	OUT rows bigint,
    OUT total_time double precision,
    OUT min_time double precision,
    OUT max_time double precision,
    OUT mean_time double precision,
    OUT last_execution timestamp with time zone,
    OUT query text,
    OUT plan text,
    OUT explain_format int,
    OUT explain_plan text,
    OUT bound_params text[]
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'adb_stat_statements'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

-- If you want save to table, "create table" instead of "create view".
-- Register a view on the function for ease of use.
CREATE VIEW adb_stat_statements AS
  SELECT * FROM adb_stat_statements();

-- If you want save to shared memory, "create view" instead of "create table".
-- The order, type, etc. of the fields correspond to those defined in the code. 
-- If you change the structure of this field, the code should also be changed, 
-- such as AdbssAttributes, checkAdbssAttrs, etc. 
-- Note that the fields defined here should be consistent with FUNCTION adb_stat_statements()
/**
CREATE TABLE adb_stat_statements (
    userid oid,
    dbid oid,
    queryid bigint,
    planid bigint,
    calls bigint,
	rows bigint,
    total_time double precision,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    last_execution timestamp with time zone,
    query text,
    plan text,
    explain_format int,
    explain_plan text,
	bound_params text[]
);
CREATE INDEX adb_stat_statements_queryid ON adb_stat_statements (queryid);
*/

-- If is antdb cluster mode, create global view.
do $$
declare
  l_gv_found int  :=0;
begin
  select count(*) into l_gv_found from pg_extension where extname = 'adb_global_views';
  if l_gv_found > 0
  then
    create or replace view gv_adb_stat_statements
    as
    select * from query_gv_views('adb_stat_statements','antdb')
    as
    t(node_oid oid,node_name name,node_type "char"
      ,userid oid, dbid oid, queryid bigint, planid bigint, calls bigint, rows bigint, total_time double precision, min_time double precision, max_time double precision, mean_time double precision, last_execution timestamp with time zone, query text, plan text, explain_format int, explain_plan text, bound_params text[]
    );
  end if;
end;
$$ language plpgsql;
