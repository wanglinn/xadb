/* contrib/adb_sql_history/adb_sql_history--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION adb_sql_history" to load this file. \quit

CREATE FUNCTION adb_sql_history(
	OUT pid integer,
	OUT dbid oid,
    OUT query text,
    OUT lasttime timestamptz,
	OUT ecount int8
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'adb_sql_history'
LANGUAGE C STRICT;


-- Register a view on the function for ease of use.
CREATE OR REPLACE
VIEW adb_sql_history AS
SELECT
	t1.pid as pid,
	t2.datname as dbname,
	t1.query as sql,
	t1.lasttime as lasttime,
	t1.ecount
FROM
	adb_sql_history() t1 left join pg_database t2 on t1.dbid = t2.oid
ORDER BY pid, lasttime DESC;
