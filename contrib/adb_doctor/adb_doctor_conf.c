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
#include "storage/lwlock.h"
#include "adb_doctor_conf.h"

void checkAdbDoctorConf(AdbDoctorConf *src)
{
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
	src->node_restart_crashed_master = LIMIT_VALUE_RANGE(0, 1,
														 src->node_restart_crashed_master);
	if (src->node_restart_master_timeout_ms < 1)
		ereport(ERROR,
				(errmsg("node_restart_master_timeout_ms must > 0")));
	if (src->node_shutdown_timeout_ms < 1)
		ereport(ERROR,
				(errmsg("node_shutdown_timeout_ms must > 0")));
	if (src->node_connection_error_num_max < 1)
		ereport(ERROR,
				(errmsg("node_connection_error_num_max must > 0")));
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  node_connect_timeout_ms_min,
								  node_connect_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  node_reconnect_delay_ms_min,
								  node_reconnect_delay_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  node_query_timeout_ms_min,
								  node_query_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  node_query_interval_ms_min,
								  node_query_interval_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  node_restart_delay_ms_min,
								  node_restart_delay_ms_max);
	if (src->agent_connection_error_num_max < 1)
		ereport(ERROR,
				(errmsg("agent_connection_error_num_max must > 0")));
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  agent_connect_timeout_ms_min,
								  agent_connect_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  agent_reconnect_delay_ms_min,
								  agent_reconnect_delay_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  agent_heartbeat_timeout_ms_min,
								  agent_heartbeat_timeout_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  agent_heartbeat_interval_ms_min,
								  agent_heartbeat_interval_ms_max);
	CHECK_ADB_DOCTOR_CONF_MIN_MAX(src,
								  agent_restart_delay_ms_min,
								  agent_restart_delay_ms_max);
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
	if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_FORCESWITCH) == 0)
	{
		if (v_int < 0 || v_int > 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("forceswitch must between %d and %d",
							0,
							1)));
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_SWITCHINTERVAL) == 0)
	{
		if (v_int < ADB_DOCTOR_CONF_SWITCHINTERVAL_MIN ||
			v_int > ADB_DOCTOR_CONF_SWITCHINTERVAL_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("switchinterval must between %d and %d",
							ADB_DOCTOR_CONF_SWITCHINTERVAL_MIN,
							ADB_DOCTOR_CONF_SWITCHINTERVAL_MAX)));
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_NODEDEADLINE) == 0)
	{
		if (v_int < ADB_DOCTOR_CONF_NODEDEADLINE_MIN ||
			v_int > ADB_DOCTOR_CONF_NODEDEADLINE_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("nodedeadline must between %d and %d",
							ADB_DOCTOR_CONF_NODEDEADLINE_MIN,
							ADB_DOCTOR_CONF_NODEDEADLINE_MAX)));
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_AGENTDEADLINE) == 0)
	{
		if (v_int < ADB_DOCTOR_CONF_AGENTDEADLINE_MIN ||
			v_int > ADB_DOCTOR_CONF_AGENTDEADLINE_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("agentdeadline must between %d and %d",
							ADB_DOCTOR_CONF_AGENTDEADLINE_MIN,
							ADB_DOCTOR_CONF_AGENTDEADLINE_MAX)));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognize adb doctor conf key:%s",
						k)));
	}
}