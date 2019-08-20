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
#include "adb_doctor_utils.h"

#define ADB_DOCTOR_CONF_SWITCHINTERVAL_MIN 1
#define ADB_DOCTOR_CONF_SWITCHINTERVAL_MAX 7200
#define ADB_DOCTOR_CONF_NODEDEADLINE_MIN 1
#define ADB_DOCTOR_CONF_NODEDEADLINE_MAX 7200
#define ADB_DOCTOR_CONF_AGENTDEADLINE_MIN 1
#define ADB_DOCTOR_CONF_AGENTDEADLINE_MAX 7200

#define ADB_DOCTOR_CONF_SHM_MAGIC 0x79fb2449

#define ADB_DOCTOR_SCHEMA "adb_doctor"
#define ADB_DOCTOR_CONF_RELNAME "adb_doctor_conf"
#define ADB_DOCTOR_CONF_ATTR_KEY "k"
#define ADB_DOCTOR_CONF_ATTR_VALUE "v"
#define ADB_DOCTOR_CONF_ATTR_COMMENT "comment"
#define ADB_DOCTOR_CONF_KEY_FORCESWITCH "forceswitch"
#define ADB_DOCTOR_CONF_KEY_SWITCHINTERVAL "switchinterval"
#define ADB_DOCTOR_CONF_KEY_NODEDEADLINE "nodedeadline"
#define ADB_DOCTOR_CONF_KEY_AGENTDEADLINE "agentdeadline"

#define CHECK_ADB_DOCTOR_CONF_MIN_MAX(ptr, minMember, maxMember) \
	do                                                           \
	{                                                            \
		if (ptr->minMember < 1)                                  \
			ereport(ERROR,                                       \
					(errmsg(#minMember " must > 0")));           \
		if (ptr->maxMember < 1)                                  \
			ereport(ERROR,                                       \
					(errmsg(#maxMember " must > 0")));           \
		if (ptr->minMember > ptr->maxMember)                     \
			ereport(ERROR,                                       \
					(errmsg(#maxMember " must > " #minMember))); \
	} while (0)

/* AdbDoctorConf elements all in one */
typedef struct AdbDoctorConf
{
	LWLock lock;
	/* Below three elements are editable */
	int forceswitch;
	int switchinterval;
	int nodedeadline;
	int agentdeadline;
	/* The elements below are not editable, keep these member names 
	 * the same as the k field values in the table adb_doctor_conf */
	int node_restart_crashed_master;
	int node_restart_master_timeout_ms;
	int node_shutdown_timeout_ms;
	int node_connection_error_num_max;
	int node_connect_timeout_ms_min;
	int node_connect_timeout_ms_max;
	int node_reconnect_delay_ms_min;
	int node_reconnect_delay_ms_max;
	int node_query_timeout_ms_min;
	int node_query_timeout_ms_max;
	int node_query_interval_ms_min;
	int node_query_interval_ms_max;
	int node_restart_delay_ms_min;
	int node_restart_delay_ms_max;
	int agent_connection_error_num_max;
	int agent_connect_timeout_ms_min;
	int agent_connect_timeout_ms_max;
	int agent_reconnect_delay_ms_min;
	int agent_reconnect_delay_ms_max;
	int agent_heartbeat_timeout_ms_min;
	int agent_heartbeat_timeout_ms_max;
	int agent_heartbeat_interval_ms_min;
	int agent_heartbeat_interval_ms_max;
	int agent_restart_delay_ms_min;
	int agent_restart_delay_ms_max;
} AdbDoctorConf;

typedef struct AdbDoctorConfShm
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorConf *confInShm;
} AdbDoctorConfShm;

typedef struct AdbDoctorConfRow
{
	char *k;
	char *v;
	bool editable;
	char *comment;
} AdbDoctorConfRow;

static inline void pfreeAdbDoctorConfShm(AdbDoctorConfShm *confShm)
{
	if (confShm)
	{
		dsm_detach(confShm->seg);
		confShm->seg = NULL;
		pfree(confShm);
		confShm = NULL;
	}
}

static inline void pfreeAdbDoctorConfRow(AdbDoctorConfRow *src)
{
	if (src)
	{
		pfree(src->k);
		pfree(src->v);
		pfree(src->comment);
		pfree(src);
		src = NULL;
	}
}

extern void checkAdbDoctorConf(AdbDoctorConf *src);
extern bool equalsAdbDoctorConfIgnoreLock(AdbDoctorConf *conf1,
										  AdbDoctorConf *conf2);
extern AdbDoctorConfShm *setupAdbDoctorConfShm(AdbDoctorConf *conf);
/* after attach, parameter confP host the pointer of conf in shm.  */
extern AdbDoctorConfShm *attachAdbDoctorConfShm(dsm_handle handle,
												char *name);
extern AdbDoctorConf *copyAdbDoctorConfFromShm(AdbDoctorConfShm *shm);
extern void copyAdbDoctorConfAvoidLock(AdbDoctorConf *dest,
									   AdbDoctorConf *src);
extern void refreshAdbDoctorConfInShm(AdbDoctorConf *confInLocal,
									  AdbDoctorConfShm *shm);
extern void validateAdbDoctorConfEditableEntry(char *k, char *v);

extern int selectAdbDoctorConfInt(char *key);
extern char *selectAdbDoctConfByKey(char *key);
extern AdbDoctorConf *selectAllAdbDoctorConf(MemoryContext spiContext);
extern int selectEditableAdbDoctorConf(MemoryContext spiContext,
									   AdbDoctorConfRow **rowDataP);
extern void updateAdbDoctorConf(char *key, char *value);

#endif