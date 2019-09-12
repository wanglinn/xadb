/*--------------------------------------------------------------------------
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
#include "mgr/mgr_helper.h"

#define ADB_DOCTOR_SHM_DATA_MAGIC 0x79fb2450

typedef enum Adb_Doctor_Type
{
	ADB_DOCTOR_TYPE_NODE_MONITOR = 1, /* avoid the default value 0 */
	ADB_DOCTOR_TYPE_HOST_MONITOR,
	ADB_DOCTOR_TYPE_SWITCHER,
	ADB_DOCTOR_TYPE_REPAIRER
} Adb_Doctor_Type;

typedef struct AdbDoctorBgworkerData
{
	slock_t mutex;
	/* Use the type and oid as unique identification */
	Adb_Doctor_Type type;
	Oid oid;
	char *displayName;
	/* the handle of a shm that all doctor attached */
	dsm_handle commonShmHandle;
	/* used to judge the doctor is working properly, before use it, 
	 * get it's value from shm. */
	bool ready;
} AdbDoctorBgworkerData;

typedef struct AdbDoctorBgworkerDataShm
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorBgworkerData *dataInShm;
} AdbDoctorBgworkerDataShm;

typedef struct AdbDoctorBgworkerStatus
{
	dlist_node wi_links;
	BackgroundWorkerHandle *handle;
	pid_t pid;
	BgwHandleStatus status;
	AdbDoctorBgworkerData *bgworkerData;
} AdbDoctorBgworkerStatus;

extern AdbDoctorBgworkerDataShm *
setupAdbDoctorBgworkerDataShm(AdbDoctorBgworkerData *data);
extern AdbDoctorBgworkerData *
attachAdbDoctorBgworkerDataShm(Datum main_arg, char *name);

extern bool isIdenticalDoctorMgrNode(MgrNodeWrapper *data1,
									 MgrNodeWrapper *data2);
extern bool isIdenticalDoctorMgrHost(MgrHostWrapper *data1,
									 MgrHostWrapper *data2);
extern bool isIdenticalDoctorMgrNodes(dlist_head *list1,
									  dlist_head *list2);
extern bool isIdenticalDoctorMgrHosts(dlist_head *list1,
									  dlist_head *list2);

extern void pfreeAdbDoctorBgworkerData(AdbDoctorBgworkerData *src);
extern void pfreeAdbDoctorBgworkerStatus(AdbDoctorBgworkerStatus *src,
										 bool freeData);

extern void logAdbDoctorBgworkerData(AdbDoctorBgworkerData *src,
									 char *title, int elevel);

#endif