
/*--------------------------------------------------------------------------
 *
 * 
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#ifndef ADB_DOCTOR_DATA_H
#define ADB_DOCTOR_DATA_H

#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/s_lock.h"
#include "postmaster/bgworker.h"
#include "catalog/mgr_node.h"
#include "catalog/mgr_host.h"
#include "adb_doctor_list.h"

#define ADB_DOCTOR_SHM_DATA_MAGIC 0x79fb2450

typedef enum Adb_Doctor_Bgworker_Type
{
	ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR = 1, /* avoid the default value 0 */
	ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR,
	ADB_DOCTOR_BGWORKER_TYPE_SWITCHER
} Adb_Doctor_Bgworker_Type;

/*
 * wrapper for FormData_mgr_host
 */
typedef struct AdbMgrHostWrapper
{
	FormData_mgr_host fdmh;
	Oid oid;
	char *hostaddr;
	char *hostadbhome;
} AdbMgrHostWrapper;

/*
 * wrapper for FormData_mgr_node
 */
typedef struct AdbMgrNodeWrapper
{
	FormData_mgr_node fdmn;
	Oid oid;
	char *nodepath; /* It is best not to change the order of attrs */
	NameData hostuser;
	char *hostaddr;
} AdbMgrNodeWrapper;

typedef struct AdbDoctorBgworkerData
{
	slock_t mutex;
	/* used to judge doctor type, and can also determine struct type. */
	Adb_Doctor_Bgworker_Type type;
	/* the handle of a shm that all doctor attached */
	dsm_handle commonShmHandle;
	/* used to judge the doctor is working properly, before use it, get it's value from shm. */
	bool ready;
} AdbDoctorBgworkerData;

typedef struct AdbDoctorBgworkerDataShm
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorBgworkerData *dataInShm;
} AdbDoctorBgworkerDataShm;

/* doctor type node monitor need these data */
typedef struct AdbDoctorNodeData
{
	AdbDoctorBgworkerData header; /* must be the first field */
	AdbMgrNodeWrapper *wrapper;
} AdbDoctorNodeData;

/* doctor type host monitor need these data */
typedef struct AdbDoctorHostData
{
	AdbDoctorBgworkerData header; /* must be the first field */
	AdbDoctorList *list;		  /* linked to a list of hosts */
} AdbDoctorHostData;

/* doctor type switcher need these data */
typedef struct AdbDoctorSwitcherData
{
	AdbDoctorBgworkerData header; /* must be the first field */
	AdbMgrNodeWrapper *wrapper;
} AdbDoctorSwitcherData;

typedef struct AdbDoctorBgworkerStatus
{
	dlist_node wi_links;
	char *name;
	BackgroundWorkerHandle *handle;
	pid_t pid;
	BgwHandleStatus status;
	AdbDoctorBgworkerData *data;
} AdbDoctorBgworkerStatus;

extern Size sizeofAdbDoctorBgworkerData(AdbDoctorBgworkerData *data);
extern bool isSameAdbDoctorBgworkerData(AdbDoctorBgworkerData *data1, AdbDoctorBgworkerData *data2);

/* "EQUALS" functions */
extern bool equalsAdbMgrNodeWrapper(AdbMgrNodeWrapper *data1, AdbMgrNodeWrapper *data2);
extern bool equalsAdbMgrHostWrapper(AdbMgrHostWrapper *data1, AdbMgrHostWrapper *data2);
extern bool equalsAdbDoctorHostData(AdbDoctorHostData *data1, AdbDoctorHostData *data2);
extern bool equalsAdbDoctorBgworkerData(AdbDoctorBgworkerData *data1, AdbDoctorBgworkerData *data2);
extern bool equalsAdbDoctorNodeData(AdbDoctorNodeData *data1, AdbDoctorNodeData *data2);
extern bool equalsAdbDoctorSwitcherData(AdbDoctorSwitcherData *data1, AdbDoctorSwitcherData *data2);

/* "PFREE" functions */
extern void pfreeAdbMgrHostWrapper(AdbMgrHostWrapper *src);
extern void pfreeAdbMgrNodeWrapper(AdbMgrNodeWrapper *src);
extern void pfreeAdbDoctorHostData(AdbDoctorHostData *src);
extern void pfreeAdbDoctorBgworkerData(AdbDoctorBgworkerData *src);
extern void pfreeAdbDoctorBgworkerStatus(AdbDoctorBgworkerStatus *src, bool freeData);
extern void pfreeAdbDoctorNodeData(AdbDoctorNodeData *src);
extern void pfreeAdbDoctorSwitcherData(AdbDoctorSwitcherData *src);

extern void appendAdbDoctorBgworkerData(AdbDoctorList *dest, AdbDoctorBgworkerData *data);

/* LOG functions */
extern void logAdbDoctorNodeData(AdbDoctorNodeData *src, char *title, int elevel);
extern void logAdbDoctorHostData(AdbDoctorHostData *src, char *title, int elevel);
extern void logAdbDoctorSwitcherData(AdbDoctorSwitcherData *src, char *title, int elevel);
extern void logAdbDoctorBgworkerData(AdbDoctorBgworkerData *src, char *title, int elevel);
extern void logAdbDoctorBgworkerDataList(AdbDoctorList *src, char *title, int elevel);
#endif