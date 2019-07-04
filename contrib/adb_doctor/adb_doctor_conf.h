/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_CONF_H
#define ADB_DOCTOR_CONF_H

#include "storage/lwlock.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"

/* Limit the value to the range between the minimum and maximum. */
#define LIMIT_VALUE_RANGE(min, max, val) Min(max, Max(min, val))

#define ADB_DOCTOR_CONF_PROBEINTERVAL_MIN 1
#define ADB_DOCTOR_CONF_PROBEINTERVAL_MAX 1800
#define ADB_DOCTOR_CONF_AGENTDEADLINE_MIN 5
#define ADB_DOCTOR_CONF_AGENTDEADLINE_MAX 1800

#define ADB_DOCTOR_CONF_SHM_MAGIC 0x79fb2449

#define ADB_DOCTOR_CONF_RELNAME "adb_doctor_conf"
#define ADB_DOCTOR_CONF_ATTR_KEY "k"
#define ADB_DOCTOR_CONF_ATTR_VALUE "v"
#define ADB_DOCTOR_CONF_ATTR_DESP "desp"
#define ADB_DOCTOR_CONF_KEY_DATALEVEL "datalevel"
#define ADB_DOCTOR_CONF_KEY_PROBEINTERVAL "probeinterval"
#define ADB_DOCTOR_CONF_KEY_AGENTDEADLINE "agentdeadline"

typedef enum Adb_Doctor_Conf_Datalevel
{
	NO_DATA_LOST_BUT_MAY_SLOW,
	MAY_LOST_DATA_BUT_QUICK,

	ADB_DOCTOR_CONF_DATALEVEL_BOUND /* must be the last */
} Adb_Doctor_Conf_Datalevel;

/* AdbDoctorConf elements all in one */
typedef struct AdbDoctorConf
{
	
	int datalevel;
	LWLock lock;
	int probeinterval;
	int agentdeadline;
} AdbDoctorConf;

typedef struct AdbDoctorConfShm
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorConf *confInShm;
} AdbDoctorConfShm;

static inline bool isValidAdbDoctorConf_datalevel(int src)
{
	return src >= NO_DATA_LOST_BUT_MAY_SLOW &&
		   src < ADB_DOCTOR_CONF_DATALEVEL_BOUND;
}

static inline bool isValidAdbDoctorConf_probeinterval(int src)
{
	return src >= ADB_DOCTOR_CONF_PROBEINTERVAL_MIN &&
		   src <= ADB_DOCTOR_CONF_PROBEINTERVAL_MAX;
}

static inline bool isValidAdbDoctorConf_agentdeadline(int src)
{
	return src >= ADB_DOCTOR_CONF_AGENTDEADLINE_MIN &&
		   src <= ADB_DOCTOR_CONF_AGENTDEADLINE_MAX;
}

static inline int safeAdbDoctorConf_datalevel(int src)
{
	if (isValidAdbDoctorConf_datalevel(src))
		return src;
	else
		return NO_DATA_LOST_BUT_MAY_SLOW;
}

static inline int safeAdbDoctorConf_probeinterval(int src)
{
	return LIMIT_VALUE_RANGE(ADB_DOCTOR_CONF_PROBEINTERVAL_MIN, ADB_DOCTOR_CONF_PROBEINTERVAL_MAX, src);
}

static inline int safeAdbDoctorConf_agentdeadline(int src)
{
	return LIMIT_VALUE_RANGE(ADB_DOCTOR_CONF_AGENTDEADLINE_MIN, ADB_DOCTOR_CONF_AGENTDEADLINE_MAX, src);
}

static inline void safeAdbDoctorConf(AdbDoctorConf *src)
{
	src->datalevel = safeAdbDoctorConf_datalevel(src->datalevel);
	src->probeinterval = safeAdbDoctorConf_probeinterval(src->probeinterval);
	src->agentdeadline = safeAdbDoctorConf_agentdeadline(src->agentdeadline);
}

/* Memory copy bypass the element lock.  */
static inline void copyAdbDoctorConfAvoidLock(AdbDoctorConf *dest, AdbDoctorConf *src)
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
		memcpy(((char *)dest) + addrOffset, ((char *)src) + addrOffset, objectSize - addrOffset);
	}
}
static inline void pfreeAdbDoctorConfShm(AdbDoctorConfShm *confShm)
{
	if (confShm == NULL)
		return;
	dsm_detach(confShm->seg);
	pfree(confShm);
}

extern bool compareAndUpdateAdbDoctorConf(AdbDoctorConf *staleConf, AdbDoctorConf *freshConf);
extern bool compareShmAndUpdateAdbDoctorConf(AdbDoctorConf *confInLocal, AdbDoctorConfShm *shm);
extern bool equalsAdbDoctorConf(AdbDoctorConf *conf1, AdbDoctorConf *conf2);
extern AdbDoctorConfShm *setupAdbDoctorConfShm(AdbDoctorConf *conf);
/* after attach, parameter confP host the pointer of conf in shm.  */
extern AdbDoctorConfShm *attachAdbDoctorConfShm(dsm_handle handle, char *name);
extern AdbDoctorConf *copyAdbDoctorConfFromShm(AdbDoctorConfShm *shm);
extern void refreshAdbDoctorConfInShm(AdbDoctorConf *confInLocal, AdbDoctorConfShm *shm);
extern void validateAdbDoctorConfElement(char *k, char *v);

#endif