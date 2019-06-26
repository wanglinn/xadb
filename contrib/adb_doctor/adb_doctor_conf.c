/*--------------------------------------------------------------------------
 *
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
		   conf1->probeinterval == conf2->probeinterval;
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
	AdbDoctorConf *confInShm;
	int tocKey = 0;
	AdbDoctorConfShm *confShm;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, name);
	seg = dsm_attach(handle);
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("unable to map common dynamic shared memory segment")));

	shm_toc *toc = shm_toc_attach(ADB_DOCTOR_CONF_SHM_MAGIC, dsm_segment_address(seg));
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
	Assert(shm != NULL);
	confInShm = shm->confInShm;
	Assert(confInShm != NULL);

	AdbDoctorConf *copy = palloc0(sizeof(AdbDoctorConf));
	LWLockAcquire(&confInShm->lock, LW_SHARED);
	*copy = *confInShm;
	LWLockRelease(&confInShm->lock);
	copy->datalevel = safeGetAdbDoctorConf_datalevel(copy->datalevel);
	copy->probeinterval = safeGetAdbDoctorConf_probeinterval(copy->probeinterval);
	return copy;
}

void refreshAdbDoctorConfInShm(AdbDoctorConf *confInLocal, AdbDoctorConfShm *shm)
{
	AdbDoctorConf *confInShm;
	Assert(shm != NULL);
	confInShm = shm->confInShm;
	Assert(confInShm != NULL);

	LWLockAcquire(&confInShm->lock, LW_EXCLUSIVE);
	confInShm->datalevel = confInLocal->datalevel;
	confInShm->probeinterval = confInLocal->probeinterval;
	LWLockRelease(&confInShm->lock);
}