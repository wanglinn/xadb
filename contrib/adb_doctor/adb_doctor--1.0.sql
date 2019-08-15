/* contrib/adb_doctor/adb_doctor--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION adb_doctor" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS adb_doctor;

-- Start all doctor process
CREATE OR REPLACE FUNCTION adb_doctor_start()
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- Stop all doctor processes
CREATE OR REPLACE FUNCTION adb_doctor_stop()
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- Set configuration variables stored in table adb_doctor_conf
CREATE OR REPLACE FUNCTION adb_doctor_param(k pg_catalog.text default '',
											v pg_catalog.text default '')
    RETURNS pg_catalog.void STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- List editable configuration variables stored in table adb_doctor_conf
CREATE OR REPLACE FUNCTION adb_doctor_list(OUT k pg_catalog.text,
										   OUT v pg_catalog.text,
										   OUT comment pg_catalog.text)
    RETURNS SETOF record STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- Store the configuration variables needed
CREATE TABLE IF NOT EXISTS adb_doctor_conf (
    k       	varchar(64) PRIMARY KEY, -- k is not case sensitive
    v       	varchar(256) NOT NULL,
	editable	boolean NOT NULL,
	comment		varchar
);

-- user editable configuration variables
INSERT INTO adb_doctor_conf VALUES (
	'forceswitch',
	'0',
	't',
	'0:false,1:true.是否强制主/备切换，请注意强制切换可能导致数据丢失。Whether to force the master/slave switch, note that force switch may cause data loss.'
);
INSERT INTO adb_doctor_conf VALUES (
	'switchinterval',
	'30',
	't',
	'The time interval(seconds) for retrying the switch when an error occurs in the switch.'
);
INSERT INTO adb_doctor_conf VALUES (
	'nodedeadline',
	'30',
	't',
	'NODE死线，单位秒，判定NODE运行状态的最大忍耐时间。'
);
INSERT INTO adb_doctor_conf VALUES (
	'agentdeadline',
	'5',
	't',
	'AGENT死线，单位秒，判定AGENT运行状态的最大忍耐时间。'
);


-- The following data does not allow user editing

-- node monitor
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_crashed_master',
	'1',
	'f',
	'当master节点崩溃的时候，是否重启MASTER节点。1：重启master，0：不重启master而立刻进行主备切换。'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_master_timeout_ms',
	'60000',
	'f',
	'当master节点崩溃的时候，重启MASTER节点超时毫秒数。'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_shutdown_timeout_ms',
	'60000',
	'f',
	'node处于shutdown状态的最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_connection_error_num_max',
	'3',
	'f',
	'node连接错误最大个数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_connect_timeout_ms_min',
	'2000',
	'f',
	'node连接超时最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_connect_timeout_ms_max',
	'60000',
	'f',
	'node连接超时最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_reconnect_delay_ms_min',
	'500',
	'f',
	'node重连延迟最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_reconnect_delay_ms_max',
	'10000',
	'f',
	'node重连延迟最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_timeout_ms_min',
	'2000',
	'f',
	'node查询超时最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_timeout_ms_max',
	'60000',
	'f',
	'node查询超时最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_interval_ms_min',
	'2000',
	'f',
	'node查询间隔最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_interval_ms_max',
	'60000',
	'f',
	'node查询间隔最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_delay_ms_min',
	'5000',
	'f',
	'node重启延迟最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_delay_ms_max',
	'300000',
	'f',
	'node重启延迟最大毫秒数'
);

-- host monitor
INSERT INTO adb_doctor_conf VALUES (
	'agent_connection_error_num_max',
	'3',
	'f',
	'agent连接错误最大个数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_connect_timeout_ms_min',
	'2000',
	'f',
	'agent连接超时最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_connect_timeout_ms_max',
	'60000',
	'f',
	'agent连接超时最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_reconnect_delay_ms_min',
	'500',
	'f',
	'agent重连延迟最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_reconnect_delay_ms_max',
	'10000',
	'f',
	'agent重连延迟最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_timeout_ms_min',
	'2000',
	'f',
	'agent心跳消息超时最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_timeout_ms_max',
	'60000',
	'f',
	'agent心跳消息超时最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_interval_ms_min',
	'2000',
	'f',
	'agent心跳消息间隔最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_interval_ms_max',
	'60000',
	'f',
	'agent心跳消息间隔最大毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_restart_delay_ms_min',
	'1000',
	'f',
	'agent重启延迟最小毫秒数'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_restart_delay_ms_max',
	'30000',
	'f',
	'agent重启延迟最大毫秒数'
);

-- switcher
