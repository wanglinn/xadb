/* contrib/adb_snap_state/adb_snap_state--1.0.sql */
--complain if script is sourced in psql rather than via ALTER EXTENSION
\echo Use "CRAETE EXTENSION adb_snap_state" to load this file. \quit

CREATE FUNCTION adb_snap_state()
RETURNS text
AS 'MODULE_PATHNAME', 'adb_snap_state'
LANGUAGE C STRICT;