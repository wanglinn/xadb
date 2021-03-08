/* src/pl/plpgsql/src/plorasql--1.0.sql */

CREATE FUNCTION plorasql_call_handler() RETURNS language_handler
  LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION plorasql_inline_handler(internal) RETURNS void
  STRICT LANGUAGE c AS 'MODULE_PATHNAME';

CREATE FUNCTION plorasql_validator(oid) RETURNS void
  STRICT LANGUAGE c AS 'MODULE_PATHNAME';

CREATE TRUSTED LANGUAGE plorasql
  HANDLER plorasql_call_handler
  INLINE plorasql_inline_handler
  VALIDATOR plorasql_validator;

CREATE FUNCTION oracle.plorasql_expr_callback(internal, int4, int4)
 RETURNS internal
 LANGUAGE c
AS '$libdir/plpgsql', $$plorasql_expr_callback$$
PARALLEL UNSAFE;

-- The language object, but not the functions, can be owned by a non-superuser.
ALTER LANGUAGE plorasql OWNER TO @extowner@;

COMMENT ON PROCEDURAL LANGUAGE plorasql IS 'PL/oraSQL procedural language';
