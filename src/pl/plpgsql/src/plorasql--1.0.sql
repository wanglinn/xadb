/* src/pl/plpgsql/src/plpgsql--1.0.sql */

/*
 * Currently, all the interesting stuff is done by CREATE LANGUAGE.
 * Later we will probably "dumb down" that command and put more of the
 * knowledge into this script.
 */

CREATE FUNCTION oracle.plorasql_expr_callback(internal, int4, int4)
 RETURNS internal
 LANGUAGE c
AS '$libdir/plpgsql', $$plorasql_expr_callback$$
PARALLEL UNSAFE;

CREATE PROCEDURAL LANGUAGE plorasql;

COMMENT ON PROCEDURAL LANGUAGE plorasql IS 'PL/oraSQL procedural language';
