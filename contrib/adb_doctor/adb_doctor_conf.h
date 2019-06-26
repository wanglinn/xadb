
/*--------------------------------------------------------------------------
 *
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


#define ADB_DOCTOR_CONF_PROBEINTERVAL_MIN 1
#define ADB_DOCTOR_CONF_PROBEINTERVAL_MAX 1800
#define ADB_DOCTOR_CONF_PROBEINTERVAL_DEFAULT 5

#define ADB_DOCTOR_CONF_SHM_MAGIC 0x79fb2449

#define ADB_DOCTOR_CONF_RELNAME "adb_doctor_conf"
#define ADB_DOCTOR_CONF_ATTR_KEY "k"
#define ADB_DOCTOR_CONF_ATTR_VALUE "v"
#define ADB_DOCTOR_CONF_KEY_DATALEVEL "datalevel"
#define ADB_DOCTOR_CONF_KEY_PROBEINTERVAL "probeinterval"

typedef enum Adb_Doctor_Conf_Datalevel
{
	NO_DATA_LOST_BUT_MAY_SLOW,
	MAY_LOST_DATA_BUT_QUICK,

	ADB_DOCTOR_CONF_DATALEVEL_BOUND /* must be the last */
} Adb_Doctor_Conf_Datalevel;

typedef struct AdbDoctorConf
{
	LWLock lock;
	int datalevel;
	int probeinterval;
} AdbDoctorConf;

typedef struct AdbDoctorConfShm
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorConf *confInShm;
} AdbDoctorConfShm;

static inline bool isValidAdbDoctorConf_probeinterval(int probeinterval)
{
	return probeinterval >= ADB_DOCTOR_CONF_PROBEINTERVAL_MIN &&
		   probeinterval <= ADB_DOCTOR_CONF_PROBEINTERVAL_MAX;
}

static inline bool isValidAdbDoctorConf_datalevel(int datalevel)
{
	return datalevel >= NO_DATA_LOST_BUT_MAY_SLOW &&
		   datalevel < ADB_DOCTOR_CONF_DATALEVEL_BOUND;
}

static inline int safeGetAdbDoctorConf_probeinterval(int probeinterval)
{
	if (isValidAdbDoctorConf_probeinterval(probeinterval))
		return probeinterval;
	else
		return ADB_DOCTOR_CONF_PROBEINTERVAL_DEFAULT;
}

static inline int safeGetAdbDoctorConf_datalevel(int datalevel)
{
	if (isValidAdbDoctorConf_datalevel(datalevel))
		return datalevel;
	else
		return NO_DATA_LOST_BUT_MAY_SLOW;
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

#endif