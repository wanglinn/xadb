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

CREATE OR REPLACE FUNCTION adb_doctor_param(k pg_catalog.text default '',
					   v pg_catalog.text default '')
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE TABLE IF NOT EXISTS adb_doctor_conf (
    k       varchar(64) PRIMARY KEY, -- k must be lowercase string
    v       varchar(256) NOT NULL,
	desp	varchar
);

INSERT INTO adb_doctor_conf VALUES (
	'datalevel',
	'0',
	'useless'
);

INSERT INTO adb_doctor_conf VALUES (
	'probeinterval',
	'30',
	'useless'
);

INSERT INTO adb_doctor_conf VALUES (
	'agentdeadline',
	'30',
	'AGENT死线，单位秒，如果一直不能明确判定AGENT的运行状态，则一旦达到这个时间，认为AGENT已经崩溃，这是一个悲观值，通常情况下程序能够自动识别出AGENT的运行状态。'
);