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

/* 
 * compare the conf values. if conf changed, update the value of staleConf, return true.
 * either conf changed or not, the freshConf will be pfreed.
 */
bool compareAndUpdateAdbDoctorConf(AdbDoctorConf *staleConf,
								   AdbDoctorConf *freshConf)
{
	if (!equalsAdbDoctorConf(staleConf, freshConf))
	{
		memcpy(staleConf, freshConf, sizeof(AdbDoctorConf));
		pfree(freshConf);
		return true;
	}
	else
	{
		pfree(freshConf);
		return false;
	}
}
/* 
 * compare conf in local whith conf in shm. if conf changed,
 * update the value of confInLocal, then return true.
 */
bool compareShmAndUpdateAdbDoctorConf(AdbDoctorConf *confInLocal,
									  AdbDoctorConfShm *shm)
{
	AdbDoctorConf *copyOfShm;

	copyOfShm = copyAdbDoctorConfFromShm(shm);

	return compareAndUpdateAdbDoctorConf(confInLocal, copyOfShm);
}

bool equalsAdbDoctorConf(AdbDoctorConf *conf1, AdbDoctorConf *conf2)
{
	return conf1->datalevel == conf2->datalevel &&
		   conf1->probeinterval == conf2->probeinterval &&
		   conf1->agentdeadline == conf2->agentdeadline;
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
	*confInShm = *conf;

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

	Assert(shm != NULL);
	confInShm = shm->confInShm;
	Assert(confInShm != NULL);

	copy = palloc0(sizeof(AdbDoctorConf));
	LWLockAcquire(&confInShm->lock, LW_SHARED);
	*copy = *confInShm;
	LWLockRelease(&confInShm->lock);
	safeAdbDoctorConf(copy);
	return copy;
}

void refreshAdbDoctorConfInShm(AdbDoctorConf *confInLocal, AdbDoctorConfShm *shm)
{
	AdbDoctorConf *confInShm;
	Assert(shm != NULL);
	confInShm = shm->confInShm;
	Assert(confInShm != NULL);

	LWLockAcquire(&confInShm->lock, LW_EXCLUSIVE);
	copyAdbDoctorConfAvoidLock(confInShm, confInLocal);
	LWLockRelease(&confInShm->lock);
}

extern void validateAdbDoctorConfElement(char *k, char *v)
{
	if (k == NULL || strlen(k) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter k:%s must not empty", k)));
	if (v == NULL || strlen(v) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("parameter v:%s must not empty", v)));
	if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_DATALEVEL) == 0)
	{
		if (!isValidAdbDoctorConf_datalevel(pg_atoi(v, sizeof(int), 0)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("datalevel must between %d and %d",
							NO_DATA_LOST_BUT_MAY_SLOW,
							MAY_LOST_DATA_BUT_QUICK)));
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_PROBEINTERVAL) == 0)
	{
		if (!isValidAdbDoctorConf_probeinterval(pg_atoi(v, sizeof(int), 0)))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("probeinterval must between %d and %d",
							ADB_DOCTOR_CONF_PROBEINTERVAL_MIN,
							ADB_DOCTOR_CONF_PROBEINTERVAL_MAX)));
	}
	else if (pg_strcasecmp(k, ADB_DOCTOR_CONF_KEY_AGENTDEADLINE) == 0)
	{
		if (!isValidAdbDoctorConf_agentdeadline(pg_atoi(v, sizeof(int), 0)))
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