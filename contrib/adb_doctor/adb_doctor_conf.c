/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "utils/resowner.h"
#include "utils/formatting.h"
#include "storage/lwlock.h"
#include "adb_doctor_conf.h"
#include "access/htup_details.h"
#include "executor/spi.h"

void checkAdbDoctorConf(AdbDoctorConf *src)
{
	src->enable = LIMIT_VALUE_RANGE(0, 1, src->enable);
	src->forceswitch = LIMIT_VALUE_RANGE(0, 1, src->forceswitch);
	src->switchinterval = LIMIT_VALUE_RANGE(ADB_DOCTOR_CONF_SWITCHINTERVAL_MIN,
											ADB_DOCTOR_CONF_SWITCHINTERVAL_MAX,
											src->switchinterval);
	src->nodedeadline = LIMIT_VALUE_RANGE(ADB_DOCTOR_CONF_NODEDEADLINE_MIN,
										  ADB_DOCTOR_CONF_NODEDEADLINE_MAX,
										  src->nodedeadline);
	src->agentdeadline = LIMIT_VALUE_RANGE(ADB_DOCTOR_CONF_AGENTDEADLINE_MIN,
										   ADB_DOCTOR_CONF_AGENTDEADLINE_MAX,
										   src->agentdeadline);
	src->node_shutdown_timeout_ms =
		LIMIT_VALUE_RANGE(5000, 999999999, src->node_shutdown_timeout_ms);
	src->node_connection_error_num_max =
		LIMIT_VALUE_RANGE(1, 999999999, src->node_connection_error_num_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 node_connect_timeout_ms_min,
										 node_connect_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 node_reconnect_delay_ms_min,
										 node_reconnect_delay_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 node_query_timeout_ms_min,
										 node_query_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 node_query_interval_ms_min,
										 node_query_interval_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 node_restart_delay_ms_min,
										 node_restart_delay_ms_max);
	src->node_retry_follow_master_interval_ms =
		LIMIT_VALUE_RANGE(1, 999999999, src->node_retry_follow_master_interval_ms);
	src->node_retry_rewind_interval_ms =
		LIMIT_VALUE_RANGE(1, 999999999, src->node_retry_rewind_interval_ms);
	src->node_restart_master_count = LIMIT_VALUE_RANGE(0, 999999999,
													   src->node_restart_master_count);
	src->node_restart_master_interval_ms =
		LIMIT_VALUE_RANGE(1, 999999999, src->node_restart_master_interval_ms);
	src->node_restart_slave_count = LIMIT_VALUE_RANGE(0, 999999999,
													  src->node_restart_slave_count);
	src->node_restart_slave_interval_ms =
		LIMIT_VALUE_RANGE(1, 999999999, src->node_restart_slave_interval_ms);
	src->node_restart_coordinator_count =
		LIMIT_VALUE_RANGE(0, 999999999, src->node_restart_coordinator_count);
	src->node_restart_coordinator_interval_ms =
		LIMIT_VALUE_RANGE(1, 999999999, src->node_restart_coordinator_interval_ms);
	src->agent_connection_error_num_max =
		LIMIT_VALUE_RANGE(1, 999999999, src->agent_connection_error_num_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 agent_connect_timeout_ms_min,
										 agent_connect_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 agent_reconnect_delay_ms_min,
										 agent_reconnect_delay_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 agent_heartbeat_timeout_ms_min,
										 agent_heartbeat_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 agent_heartbeat_interval_ms_min,
										 agent_heartbeat_interval_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX_MEMBER(src,
										 agent_restart_delay_ms_min,
										 agent_restart_delay_ms_max);
	src->retry_repair_interval_ms =
		LIMIT_VALUE_RANGE(1, 999999999, src->retry_repair_interval_ms);
}

bool equalsAdbDoctorConfIgnoreLock(AdbDoctorConf *conf1,
								   AdbDoctorConf *conf2)
{
	Size addrOffset;
	Size objectSize = sizeof(AdbDoctorConf);
	Size lockSize = sizeof(LWLock);
	Size lockOffset = offsetof(AdbDoctorConf, lock);
	/* if the lock is the first element? */
	if (lockOffset > 0)
	{
		if (memcmp(conf1, conf2, lockOffset) != 0)
			return false;
	}
	addrOffset = lockOffset + lockSize;
	/* if the lock is the last element? */
	if (addrOffset < objectSize)
	{
		if (memcmp(((char *)conf1) + addrOffset,
				   ((char *)conf2) + addrOffset,
				   objectSize - addrOffset) != 0)
			return false;
	}
	return true;
}

AdbDoctorConfShm *setupAdbDoctorConfShm(AdbDoctorConf *conf)
{
	shm_toc_estimator e;
	dsm_segment *seg;
	Size segsize;
	shm_toc *toc;
	int tocKey = 0;
	AdbDoctorConf *confInShm;
	AdbDoctorConfShm *confShm;

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(AdbDoctorConf));
	shm_toc_estimate_keys(&e, 1);
	segsize = shm_toc_estimate(&e);

	seg = dsm_create(segsize, 0);

	toc = shm_toc_create(ADB_DOCTOR_CONF_SHM_MAGIC, dsm_segment_address(seg),
						 segsize);

	confInShm = shm_toc_allocate(toc, sizeof(AdbDoctorConf));
	memcpy(confInShm, conf, sizeof(AdbDoctorConf));

	LWLockInitialize(&confInShm->lock, LWLockNewTrancheId());
	LWLockRegisterTranche(confInShm->lock.tranche, "adb_doctor_conf");

	shm_toc_insert(toc, tocKey++, confInShm);

	confShm = palloc0(sizeof(AdbDoctorConfShm));
	confShm->seg = seg;
	confShm->toc = toc;
	confShm->confInShm = confInShm;
	return confShm;
}

AdbDoctorConfShm *attachAdbDoctorConfShm(dsm_handle handle, char *name)
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorConf *confInShm;
	int tocKey = 0;
	AdbDoctorConfShm *confShm;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, name);
	seg = dsm_attach(handle);
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("unable to map common dynamic shared memory segment")));

	toc = shm_toc_attach(ADB_DOCTOR_CONF_SHM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	confInShm = shm_toc_lookup(toc, tocKey++, false);

	confShm = palloc0(sizeof(AdbDoctorConfShm));
	confShm->seg = seg;
	confShm->toc = toc;
	confShm->confInShm = confInShm;
	return confShm;
}

AdbDoctorConf *copyAdbDoctorConfFromShm(AdbDoctorConfShm *shm)
{
	AdbDoctorConf *confInShm;
	AdbDoctorConf *copy;

	Assert(shm);
	confInShm = shm->confInShm;
	Assert(confInShm);

	copy = palloc(sizeof(AdbDoctorConf));
	LWLockAcquire(&confInShm->lock, LW_SHARED);
	memcpy(copy, confInShm, sizeof(AdbDoctorConf));
	LWLockRelease(&confInShm->lock);
	return copy;
}

/**
 * Memory copy bypass the element lock. 
 */
void copyAdbDoctorConfAvoidLock(AdbDoctorConf *dest,
								AdbDoctorConf *src)
{
	Size addrOffset;
	Size objectSize = sizeof(AdbDoctorConf);
	Size lockSize = sizeof(LWLock);
	Size lockOffset = offsetof(AdbDoctorConf, lock);
	/* if the lock is the first element? */
	if (lockOffset > 0)
	{
		/* Copy the data before lock */
		memcpy(dest, src, lockOffset);
	}
	addrOffset = lockOffset + lockSize;
	/* if the lock is the last element? */
	if (addrOffset < objectSize)
	{
		/* Copy the data after lock */
		memcpy(((char *)dest) + addrOffset,
			   ((char *)src) + addrOffset,
			   objectSize - addrOffset);
	}
}

void refreshAdbDoctorConfInShm(AdbDoctorConf *confInLocal,
							   AdbDoctorConfShm *shm)
{
	AdbDoctorConf *confInShm;

	Assert(shm);
	confInShm = shm->confInShm;
	Assert(confInShm);

	LWLockAcquire(&confInShm->lock, LW_EXCLUSIVE);
	copyAdbDoctorConfAvoidLock(confInShm, confInLocal);
	LWLockRelease(&confInShm->lock);
}

void validateAdbDoctorConfEditableEntry(char *k, char *v)
{
	int v_int;
	if (k == NULL || strlen(k) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter k:%s must not empty", k)));
	if (v == NULL || strlen(v) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter v:%s must not empty", v)));
	v_int = pg_atoi(v, sizeof(int), 0);
	if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_ENABLE) == 0)
	{
		CHECK_ADB_DOCTOR_CONF_VALUE_RANGE(ADB_DOCTOR_CONF_KEY_ENABLE,
										  v_int,
										  0,
										  1);
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_FORCESWITCH) == 0)
	{
		CHECK_ADB_DOCTOR_CONF_VALUE_RANGE(ADB_DOCTOR_CONF_KEY_FORCESWITCH,
										  v_int,
										  0,
										  1);
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_SWITCHINTERVAL) == 0)
	{
		CHECK_ADB_DOCTOR_CONF_VALUE_RANGE(ADB_DOCTOR_CONF_KEY_SWITCHINTERVAL,
										  v_int,
										  ADB_DOCTOR_CONF_SWITCHINTERVAL_MIN,
										  ADB_DOCTOR_CONF_SWITCHINTERVAL_MAX);
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_NODEDEADLINE) == 0)
	{
		CHECK_ADB_DOCTOR_CONF_VALUE_RANGE(ADB_DOCTOR_CONF_KEY_NODEDEADLINE,
										  v_int,
										  ADB_DOCTOR_CONF_NODEDEADLINE_MIN,
										  ADB_DOCTOR_CONF_NODEDEADLINE_MAX);
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_AGENTDEADLINE) == 0)
	{
		CHECK_ADB_DOCTOR_CONF_VALUE_RANGE(ADB_DOCTOR_CONF_KEY_AGENTDEADLINE,
										  v_int,
										  ADB_DOCTOR_CONF_AGENTDEADLINE_MIN,
										  ADB_DOCTOR_CONF_AGENTDEADLINE_MAX);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognize antdb doctor conf key:%s",
						k)));
	}
}

void updateAdbDoctorConf(char *key, char *value)
{
	StringInfoData buf;
	int ret;
	char *key_lower;
	uint64 rows;
	/* k is not case sensitive */
	key_lower = asc_tolower(key, strlen(key));
	initStringInfo(&buf);
	appendStringInfo(&buf, "update %s.%s set %s = '%s' where %s = '%s'",
					 ADB_DOCTOR_SCHEMA,
					 ADB_DOCTOR_CONF_RELNAME,
					 ADB_DOCTOR_CONF_ATTR_VALUE,
					 value,
					 ADB_DOCTOR_CONF_ATTR_KEY,
					 key_lower);
	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);
	pfree(key_lower);

	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d",
						ret)));

	rows = SPI_processed;
	if (rows != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: expected the number of rows:%d, but actual:%lu",
						1,
						rows)));
}

/*
 * The result is returned in memory allocated using palloc.
 * You can use pfree to release the memory when you don't need it anymore.
 */
char *selectAdbDoctConfByKey(char *key)
{
	char *v;
	StringInfoData buf;
	int ret;
	uint64 rows;
	SPITupleTable *tupTable;

	initStringInfo(&buf);
	appendStringInfo(&buf, "select %s from %s.%s where %s = '%s'",
					 ADB_DOCTOR_CONF_ATTR_VALUE,
					 ADB_DOCTOR_SCHEMA,
					 ADB_DOCTOR_CONF_RELNAME,
					 ADB_DOCTOR_CONF_ATTR_KEY,
					 key);
	ret = SPI_execute(buf.data, false, 0);
	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d",
						ret)));
	rows = SPI_processed;
	tupTable = SPI_tuptable;
	if (rows == 1 && tupTable != NULL)
	{
		v = SPI_getvalue(tupTable->vals[0], tupTable->tupdesc, 1);
		return v;
	}
	else
	{
		return NULL;
	}
}

int selectAdbDoctorConfInt(char *key)
{
	int value;
	char *valueStr = selectAdbDoctConfByKey(key);
	if (valueStr && strlen(valueStr) > 0)
	{
		value = pg_atoi(valueStr, sizeof(int), 0);
		pfree(valueStr);
		return value;
	}
	else
	{
		ereport(ERROR,
				(errmsg("%s, invalid value : NULL",
						key)));
	}
}

AdbDoctorConf *selectAllAdbDoctorConf(MemoryContext spiContext)
{
	AdbDoctorConf *conf;
	StringInfoData buf;
	int ret, j, valueInt;
	uint64 rows;
	HeapTuple tuple;
	TupleDesc tupdesc;
	MemoryContext oldCtx;
	char *keyStr, *valueStr;
	SPITupleTable *tupTable;

	initStringInfo(&buf);
	appendStringInfo(&buf, "select %s,%s from %s.%s",
					 ADB_DOCTOR_CONF_ATTR_KEY,
					 ADB_DOCTOR_CONF_ATTR_VALUE,
					 ADB_DOCTOR_SCHEMA,
					 ADB_DOCTOR_CONF_RELNAME);

	oldCtx = MemoryContextSwitchTo(spiContext);
	ret = SPI_execute(buf.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d",
						ret)));

	rows = SPI_processed;
	tupTable = SPI_tuptable;
	if (rows > 0 && tupTable != NULL)
	{
		conf = palloc0(sizeof(AdbDoctorConf));
		tupdesc = tupTable->tupdesc;
		for (j = 0; j < rows; j++)
		{
			tuple = tupTable->vals[j];
			keyStr = SPI_getvalue(tuple, tupdesc, 1);
			valueStr = SPI_getvalue(tuple, tupdesc, 2);
			if (valueStr)
			{
				valueInt = pg_atoi(valueStr, sizeof(int), 0);
			}
			else
			{
				ereport(ERROR,
						(errmsg("%s, invalid value : NULL",
								keyStr)));
			}
			if (pg_strcasecmp(keyStr, ADB_DOCTOR_CONF_KEY_ENABLE) == 0)
			{
				conf->enable = valueInt;
			}
			else if (pg_strcasecmp(keyStr, ADB_DOCTOR_CONF_KEY_FORCESWITCH) == 0)
			{
				conf->forceswitch = valueInt;
			}
			else if (pg_strcasecmp(keyStr, ADB_DOCTOR_CONF_KEY_SWITCHINTERVAL) == 0)
			{
				conf->switchinterval = valueInt;
			}
			else if (pg_strcasecmp(keyStr, ADB_DOCTOR_CONF_KEY_NODEDEADLINE) == 0)
			{
				conf->nodedeadline = valueInt;
			}
			else if (pg_strcasecmp(keyStr, ADB_DOCTOR_CONF_KEY_AGENTDEADLINE) == 0)
			{
				conf->agentdeadline = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_shutdown_timeout_ms") == 0)
			{
				conf->node_shutdown_timeout_ms = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_connection_error_num_max") == 0)
			{
				conf->node_connection_error_num_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_connect_timeout_ms_min") == 0)
			{
				conf->node_connect_timeout_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_connect_timeout_ms_max") == 0)
			{
				conf->node_connect_timeout_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_reconnect_delay_ms_min") == 0)
			{
				conf->node_reconnect_delay_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_reconnect_delay_ms_max") == 0)
			{
				conf->node_reconnect_delay_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_query_timeout_ms_min") == 0)
			{
				conf->node_query_timeout_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_query_timeout_ms_max") == 0)
			{
				conf->node_query_timeout_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_query_interval_ms_min") == 0)
			{
				conf->node_query_interval_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_query_interval_ms_max") == 0)
			{
				conf->node_query_interval_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_delay_ms_min") == 0)
			{
				conf->node_restart_delay_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_delay_ms_max") == 0)
			{
				conf->node_restart_delay_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_retry_follow_master_interval_ms") == 0)
			{
				conf->node_retry_follow_master_interval_ms = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_retry_rewind_interval_ms") == 0)
			{
				conf->node_retry_rewind_interval_ms = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_master_count") == 0)
			{
				conf->node_restart_master_count = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_master_interval_ms") == 0)
			{
				conf->node_restart_master_interval_ms = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_slave_count") == 0)
			{
				conf->node_restart_slave_count = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_slave_interval_ms") == 0)
			{
				conf->node_restart_slave_interval_ms = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_coordinator_count") == 0)
			{
				conf->node_restart_coordinator_count = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "node_restart_coordinator_interval_ms") == 0)
			{
				conf->node_restart_coordinator_interval_ms = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_connection_error_num_max") == 0)
			{
				conf->agent_connection_error_num_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_connect_timeout_ms_min") == 0)
			{
				conf->agent_connect_timeout_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_connect_timeout_ms_max") == 0)
			{
				conf->agent_connect_timeout_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_reconnect_delay_ms_min") == 0)
			{
				conf->agent_reconnect_delay_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_reconnect_delay_ms_max") == 0)
			{
				conf->agent_reconnect_delay_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_heartbeat_timeout_ms_min") == 0)
			{
				conf->agent_heartbeat_timeout_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_heartbeat_timeout_ms_max") == 0)
			{
				conf->agent_heartbeat_timeout_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_heartbeat_interval_ms_min") == 0)
			{
				conf->agent_heartbeat_interval_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_heartbeat_interval_ms_max") == 0)
			{
				conf->agent_heartbeat_interval_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_restart_delay_ms_min") == 0)
			{
				conf->agent_restart_delay_ms_min = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "agent_restart_delay_ms_max") == 0)
			{
				conf->agent_restart_delay_ms_max = valueInt;
			}
			else if (pg_strcasecmp(keyStr, "retry_repair_interval_ms") == 0)
			{
				conf->retry_repair_interval_ms = valueInt;
			}
			pfree(keyStr);
			pfree(valueStr);
		}
		checkAdbDoctorConf(conf);
	}
	else
	{
		conf = NULL;
	}
	return conf;
}

int selectEditableAdbDoctorConf(MemoryContext spiContext,
								AdbDoctorConfRow **rowDataP)
{
	AdbDoctorConfRow *rowData;
	StringInfoData buf;
	int ret, j;
	uint64 rows;
	HeapTuple tuple;
	TupleDesc tupdesc;
	MemoryContext oldCtx;
	SPITupleTable *tupTable;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "select %s,%s,%s from %s.%s "
					 "where editable = %d::boolean "
					 "order by sortnumber asc",
					 ADB_DOCTOR_CONF_ATTR_KEY,
					 ADB_DOCTOR_CONF_ATTR_VALUE,
					 ADB_DOCTOR_CONF_ATTR_COMMENT,
					 ADB_DOCTOR_SCHEMA,
					 ADB_DOCTOR_CONF_RELNAME,
					 true);

	oldCtx = MemoryContextSwitchTo(spiContext);
	ret = SPI_execute(buf.data, false, 0);
	MemoryContextSwitchTo(oldCtx);

	pfree(buf.data);

	if (ret != SPI_OK_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("SPI_execute failed: error code %d",
						ret)));

	rows = SPI_processed;
	tupTable = SPI_tuptable;
	if (rows > 0 && tupTable != NULL)
	{
		/* do outside the spi memory context because the spi will be freed. */
		rowData = palloc(sizeof(AdbDoctorConfRow) * rows);
		tupdesc = tupTable->tupdesc;
		for (j = 0; j < rows; j++)
		{
			tuple = tupTable->vals[j];
			rowData[j].k = SPI_getvalue(tuple, tupdesc, 1);
			rowData[j].v = SPI_getvalue(tuple, tupdesc, 2);
			rowData[j].comment = SPI_getvalue(tuple, tupdesc, 3);
		}
	}
	else
	{
		rows = 0;
		rowData = NULL;
	}

	*rowDataP = rowData;
	return rows;
}
