/* src/pl/plpgsql/src/plpgsql--unpackaged--1.0.sql */

ALTER EXTENSION plorasql ADD PROCEDURAL LANGUAGE plorasql;
-- ALTER ADD LANGUAGE doesn't pick up the support functions, so we have to.
ALTER EXTENSION plorasql ADD FUNCTION plorasql_call_handler();
ALTER EXTENSION plorasql ADD FUNCTION plorasql_inline_handler(internal);
ALTER EXTENSION plorasql ADD FUNCTION plorasql_validator(oid);
