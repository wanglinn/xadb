/* contrib/adb_doctor/adb_doctor--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION adb_doctor" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS adb_doctor;

-- Start all doctor process
CREATE OR REPLACE FUNCTION adb_doctor_start()
    RETURNS pg_catalog.bool STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- Stop all doctor processes
CREATE OR REPLACE FUNCTION adb_doctor_stop()
    RETURNS pg_catalog.bool STRICT
	AS 'MODULE_PATHNAME' LANGUAGE C;

-- Set configuration variables stored in table adb_doctor_conf
CREATE OR REPLACE FUNCTION adb_doctor_param(k pg_catalog.text default '',
											v pg_catalog.text default '')
    RETURNS pg_catalog.bool STRICT
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
	sortnumber  int,
	comment		varchar
);

-- user editable configuration variables
INSERT INTO adb_doctor_conf VALUES (
	'enable',
	'0',
	't',
	1,
	'0:false, 1:true. If true, doctor processes will be launched, or else, doctor processes exit.'
);
INSERT INTO adb_doctor_conf VALUES (
	'forceswitch',
	'0',
	't',
	2,
	'0:false, 1:true. Whether force to switch the master/slave, note that force switch may cause data loss.'
);
INSERT INTO adb_doctor_conf VALUES (
	'switchinterval',
	'30',
	't',
	3,
	'In seconds, The time interval for doctor retry the switching if an error occurred in the previous switching.'
);
INSERT INTO adb_doctor_conf VALUES (
	'nodedeadline',
	'30',
	't',
	4,
	'In seconds. The maximum time for doctor tolerate a NODE running abnormally.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agentdeadline',
	'5',
	't',
	5,
	'In seconds. The maximum time for doctor tolerate a AGENT running abnormally.'
);


-- The following data does not allow user editing

-- node monitor
INSERT INTO adb_doctor_conf VALUES (
	'node_shutdown_timeout_ms',
	'60000',
	'f',
	6,
	'In milliseconds. If the time of a node is in shutting down exceeds this value, doctor will shut down that node by Immediate Shutdown mode and then start it up.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_connection_error_num_max',
	'3',
	'f',
	7,
	'If the number of connection errors on a node exceeds this value, the doctor thinks that node has crashed.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_connect_timeout_ms_min',
	'2000',
	'f',
	8,
	'In milliseconds. The value of node_connect_timeout is calculated based on nodedeadline. In order to make this value reasonable, it needs to be limited in the range of node_connect_timeout_ms_min and node_connect_timeout_ms_max. Other similar parameters(suffixes such as _min and _max) are also this strategy.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_connect_timeout_ms_max',
	'60000',
	'f',
	9,
	'In milliseconds. In pairs with node_connect_timeout_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_reconnect_delay_ms_min',
	'500',
	'f',
	10,
	'In milliseconds. The minimum time interval to reconnect node.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_reconnect_delay_ms_max',
	'10000',
	'f',
	11,
	'In milliseconds. In pairs with node_reconnect_delay_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_timeout_ms_min',
	'2000',
	'f',
	12,
	'In milliseconds. The minimum time of getting query result from node.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_timeout_ms_max',
	'60000',
	'f',
	13,
	'In milliseconds. In pairs with node_query_timeout_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_interval_ms_min',
	'2000',
	'f',
	14,
	'In milliseconds. The minimum time interval of querying a node by sql.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_query_interval_ms_max',
	'60000',
	'f',
	15,
	'In milliseconds. In pairs with node_query_interval_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_delay_ms_min',
	'5000',
	'f',
	16,
	'In milliseconds. The minimum time interval to restart crashed node.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_delay_ms_max',
	'300000',
	'f',
	17,
	'In milliseconds. In pairs with node_restart_delay_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_retry_follow_master_interval_ms',
	'60000',
	'f',
	18,
	'In milliseconds. The time interval to retry after slave node failed to follow the master node.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_retry_rewind_interval_ms',
	'60000',
	'f',
	19,
	'In milliseconds. The time interval to retry after slave node failed to rewind.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_master_count',
	'1',
	'f',
	20,
	'The number of times the doctor tries to restart a crashed master (GTM or datanode) node. If it finally restart failed, doctor will do switching immediately.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_master_interval_ms',
	'10000',
	'f',
	21,
	'In milliseconds. The time interval for the master node (GTM or datanode) to retry after a failed restart.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_slave_count',
	'1',
	'f',
	22,
	'The number of times the doctor tries to restart a crashed slave (GTM or datanode) node. If it finally restart failed, doctor will kick it out of cluster.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_slave_interval_ms',
	'10000',
	'f',
	23,
	'In milliseconds. The time interval for the slave (GTM or datanode) node to retry after a failed restart.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_coordinator_count',
	'3',
	'f',
	24,
	'The number of times the doctor tries to restart a crashed coordinator node. If it finally restart failed, doctor will kick it out of cluster.'
);
INSERT INTO adb_doctor_conf VALUES (
	'node_restart_coordinator_interval_ms',
	'10000',
	'f',
	25,
	'In milliseconds. The time interval for the coordinator to retry after a failed restart.'
);

-- host monitor
INSERT INTO adb_doctor_conf VALUES (
	'agent_connection_error_num_max',
	'3',
	'f',
	26,
	'If the number of connection errors on a agent exceeds this value, the doctor thinks that node has crashed.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_connect_timeout_ms_min',
	'2000',
	'f',
	27,
	'In milliseconds. The minimum time of connecting agent.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_connect_timeout_ms_max',
	'60000',
	'f',
	28,
	'In milliseconds. In pairs with agent_connect_timeout_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_reconnect_delay_ms_min',
	'500',
	'f',
	28,
	'In milliseconds. The minimum time interval to reconnect agent.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_reconnect_delay_ms_max',
	'10000',
	'f',
	30,
	'In milliseconds. In pairs with agent_reconnect_delay_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_timeout_ms_min',
	'2000',
	'f',
	31,
	'In milliseconds. The minimum time of receiving heartbeat message from agent.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_timeout_ms_max',
	'60000',
	'f',
	32,
	'In milliseconds. In pairs with agent_heartbeat_timeout_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_interval_ms_min',
	'2000',
	'f',
	33,
	'In milliseconds. The minimum time interval of sending heartbeat message to agent.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_heartbeat_interval_ms_max',
	'60000',
	'f',
	34,
	'In milliseconds. In pairs with agent_heartbeat_interval_ms_min.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_restart_delay_ms_min',
	'1000',
	'f',
	35,
	'In milliseconds. The minimum time interval to restart crashed agent.'
);
INSERT INTO adb_doctor_conf VALUES (
	'agent_restart_delay_ms_max',
	'30000',
	'f',
	36,
	'In milliseconds. In pairs with agent_restart_delay_ms_min.'
);

INSERT INTO adb_doctor_conf VALUES (
	'retry_repair_interval_ms',
	'30000',
	'f',
	37,
	'In milliseconds. The time interval for doctor retry the repairing node if an error occurred in the previous tries.'
);
