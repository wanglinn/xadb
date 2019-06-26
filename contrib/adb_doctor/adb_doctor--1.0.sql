/* contrib/adb_doctor/adb_doctor--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION adb_doctor" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS adb_extension;

CREATE OR REPLACE FUNCTION adb_doctor_start()
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OR REPLACE FUNCTION adb_doctor_stop()
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OR REPLACE FUNCTION adb_doctor_param(datalevel pg_catalog.int4 default 0,
					   probeinterval pg_catalog.int4 default 30)
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE TABLE IF NOT EXISTS adb_doctor_conf (
    k       varchar(64) PRIMARY KEY,
    v       varchar(256) NOT NULL
);

INSERT INTO adb_doctor_conf VALUES (
	'datalevel',
	'0'
);

INSERT INTO adb_doctor_conf VALUES (
	'probeinterval',
	'30'
);
