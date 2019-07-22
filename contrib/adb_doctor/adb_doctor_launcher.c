/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "access/xlog.h"
#include "adb_doctor.h"

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

/* setup shm which create by backend */
static void attachShm(Datum main_arg,
					  bool *needNotify,
					  dsm_segment **segP,
					  shm_mq_handle **mqhP);
/* tell backend */
static void sendBackendMessage(shm_mq_handle *outqh,
							   char *message);
/* do work when signaled or timedout */
static void launcherLoop(AdbDoctorConf *conf);

static void queryBgworkerDataAndConf(AdbDoctorConf **confP,
									 AdbDoctorList **dataListP);

/* give each doctor process a unique name */
static char *getDoctorName(AdbDoctorBgworkerData *data);

/* create a shm with size segsize, insert data into this shm,
 * then return these things. */
static AdbDoctorBgworkerDataShm *
initializeBgworkerDataShm(AdbDoctorBgworkerData *data,
						  Size segsize,
						  Size datasize,
						  uint64 *tocKey);
/* setup a single shm for each doctor process, 
 * used to transfer data to it */
static AdbDoctorBgworkerDataShm *
setupNodeDataShm(AdbDoctorNodeData *nodeData);
/* setup a single shm for host monitor doctor process,
 * used to transfer data to it */
static AdbDoctorBgworkerDataShm *
setupHostDataShm(AdbDoctorHostData *hostData);
/* setup a single shm for switcher doctor process, 
 * used to transfer data to it */
static AdbDoctorBgworkerDataShm *
setupSwitcherDataShm(AdbDoctorSwitcherData *switcherData);
/* setup a single shm for each doctor process, 
 *used to transfer data to it */
static AdbDoctorBgworkerDataShm *
setupDataShm(AdbDoctorBgworkerStatus *bgworkerStatus);

/* register doctor process as Background Worker Processe */
static void
registerDoctorAsBgworker(AdbDoctorBgworkerStatus *bgworkerStatus,
						 AdbDoctorBgworkerDataShm *dataShm);
static void
waitForDoctorBecomeReady(AdbDoctorBgworkerStatus *bgworkerStatus,
						 volatile AdbDoctorBgworkerData *headerInShm);
/* launch doctor process as Background Worker Processe sequencially */
static void launchDoctorList(AdbDoctorList *dataList);
static AdbDoctorBgworkerStatus *launchDoctor(AdbDoctorBgworkerData *data);

/* terminate functions */
static void terminateAllDoctor(void);
static void terminateDoctorList(AdbDoctorList *dataList);
static void terminateDoctorByName(char *name);
static void terminateDoctor(AdbDoctorBgworkerStatus *bgworkerStatus,
							bool waitFor);

/* check the doctor process running status */
static void checkDoctorRunningStatus(void);

/* signal doctor process */
static void signalDoctorByName(char *name);
static void signalDoctorList(AdbDoctorList *dataList);

/* bgworkerStatusList saved the stale doctor data, read it out as list */
static AdbDoctorList *getStaleBgworkerDataList(void);

static void compareAndRefreshDoctor(bool confChanged,
									AdbDoctorList *staleDataList,
									AdbDoctorList *freshDataList);
static void compareBgworkerDataList(AdbDoctorList *staleDataList,
									AdbDoctorList *freshDataList,
									AdbDoctorList **addedDataListP,
									AdbDoctorList **deletedDataListP,
									AdbDoctorList **changedDataListP,
									AdbDoctorList **identicalDataListP);

/* this list link the running bgworker of doctor processed, so we can view or control doctor process as we want. */
static dlist_head bgworkerStatusList = DLIST_STATIC_INIT(bgworkerStatusList);
static AdbDoctorConfShm *confShm;

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

/*
 * make sure there is an extension named adb_doctor in your mgr.
 * there are two way can setup this launcher:
 * 1: type command select adb_doctor.adb_doctor_start.
 * 2: register it as bgworker in function PostmasterMain.
 * this function is the entrypoint of backgroundworker.
 */
void adbDoctorLauncherMain(Datum main_arg)
{
	dsm_segment *segBackend = NULL;
	shm_mq_handle *mqh = NULL;
	bool needNotify = false;
	AdbDoctorList *dataList = NULL;
	AdbDoctorConf *conf = NULL;

	pqsignal(SIGTERM, handleSigterm);
	pqsignal(SIGUSR1, handleSigusr1);
	BackgroundWorkerUnblockSignals();

	PG_TRY();
	{
		adbDoctorStopBgworkers(false);

		BackgroundWorkerInitializeConnection(ADBMGR_DBNAME, NULL, 0);

		/* if can not attach to shm, it may restarted by postmaster */
		attachShm(main_arg, &needNotify, &segBackend, &mqh);
		if (needNotify)
			notifyAdbDoctorRegistrant();

		ereport(LOG,
				(errmsg("%s started", MyBgworkerEntry->bgw_name)));

		queryBgworkerDataAndConf(&conf, &dataList);
		if (!conf)
			ereport(ERROR,
					(errmsg("%s configuration failed",
							MyBgworkerEntry->bgw_name)));

		/* Create common shared memory to store configuration for all doctors */
		confShm = setupAdbDoctorConfShm(conf);

		/* launch doctor process sequencially */
		launchDoctorList(dataList);

		/* tell backend we do it well, and cut the contact with backend */
		if (needNotify)
			sendBackendMessage(mqh, ADB_DOCTORS_LAUNCH_OK);

		if (segBackend != NULL)
			dsm_detach(segBackend);
	}
	PG_CATCH();
	{
		/* Report the error to the server log */
		EmitErrorReport();

		terminateAllDoctor();
		pfreeAdbDoctorConfShm(confShm);

		/* cut the contact with backend */
		if (mqh != NULL)
			sendBackendMessage(mqh, ADB_DOCTORS_LAUNCH_FAILURE);
		if (segBackend != NULL)
			dsm_detach(segBackend);

		/* exits with an exit code of 0, it will be automatically unregistered by the postmaster on exit. */
		proc_exit(0);
	}
	PG_END_TRY();

	/* long loop until error or terminated */
	PG_TRY();
	{
		/* main loop,  */
		launcherLoop(conf);
	}
	PG_CATCH();
	{
		/* clean all resources */
		terminateAllDoctor();
		pfreeAdbDoctorConfShm(confShm);
		proc_exit(1);
	}
	PG_END_TRY();

	terminateAllDoctor();
	pfreeAdbDoctorConfShm(confShm);
	proc_exit(1);
}

/* 
 * when signaled, check the status of doctor processes, check the conf and
 * (mgr_node,mgr_host) data change.
 * if conf changed, we signal all the doctor processes.
 * if add some new (mgr_node,mgr_host) data, we launche new doctor process.
 * if delete some data, we terminate corresponding doctor process.
 * if change some data's field, terminate the old process and then startup a new process.
 */
static void launcherLoop(AdbDoctorConf *conf)
{
	int rc;
	bool confChanged = false;
	AdbDoctorConf *staleConf = conf;
	AdbDoctorConf *freshConf;
	AdbDoctorList *staleDataList;
	AdbDoctorList *freshDataList;

	while (!gotSigterm)
	{
		CHECK_FOR_INTERRUPTS();

		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   PGINVALID_SOCKET,
							   10000L,
							   PG_WAIT_EXTENSION);
		/* Reset the latch, bail out if postmaster died, otherwise loop. */
		ResetLatch(&MyProc->procLatch);
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (gotSigusr1)
		{
			gotSigusr1 = false;
			checkDoctorRunningStatus();
		}

		queryBgworkerDataAndConf(&freshConf, &freshDataList);

		if (equalsAdbDoctorConfIgnoreLock(staleConf, freshConf))
		{
			confChanged = false;
			pfree(freshConf);
			freshConf = NULL;
		}
		else
		{
			confChanged = true;
			/* if conf changed, update the value of staleConf */
			pfree(staleConf);
			staleConf = freshConf;
			freshConf = NULL;
			/* refresh shm before manipulate doctor processes */
			refreshAdbDoctorConfInShm(staleConf, confShm);
		}

		staleDataList = getStaleBgworkerDataList();

		compareAndRefreshDoctor(confChanged, staleDataList, freshDataList);

		staleDataList = NULL;
		freshDataList = NULL;
	}
}

/**
 * give each doctor process a unique name 
 */
static char *getDoctorName(AdbDoctorBgworkerData *data)
{
	char *name;
	if (data->type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
	{
		name = psprintf("adb doctor node monitor %u",
						((AdbDoctorNodeData *)data)->wrapper->oid);
	}
	else if (data->type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
	{
		name = psprintf("adb doctor host monitor");
	}
	else if (data->type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
	{
		name = psprintf("adb doctor switcher");
	}
	else
	{
		name = NULL;
		ereport(ERROR,
				(errmsg("could not recognize type:%d",
						data->type)));
	}
	return name;
}

/* 
 * create a shm with size segsize (estimated by shm_toc_estimator), 
 * create a shm_toc for convenience to insert data into shm. memcpy data with
 * size datasize (according to actual size) of data into this shm with the tocKey,
 * then return a pointer to a struct which contain these things.
 */
static AdbDoctorBgworkerDataShm *
initializeBgworkerDataShm(AdbDoctorBgworkerData *data,
						  Size segsize,
						  Size datasize,
						  uint64 *tocKey)
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorBgworkerDataShm *dataShm;
	AdbDoctorBgworkerData *dataInShm;

	seg = dsm_create(segsize, 0);

	toc = shm_toc_create(ADB_DOCTOR_SHM_DATA_MAGIC,
						 dsm_segment_address(seg),
						 segsize);

	/* insert data into shm */
	dataInShm = shm_toc_allocate(toc, datasize);
	shm_toc_insert(toc, (*tocKey)++, dataInShm);
	memcpy(dataInShm, data, datasize);

	/* initialize header data */
	SpinLockInit(&dataInShm->mutex);
	Assert(confShm != NULL);
	dataInShm->commonShmHandle = dsm_segment_handle(confShm->seg);
	dataInShm->ready = false;

	/* this struct contain these things about shm */
	dataShm = palloc0(sizeof(AdbDoctorBgworkerDataShm));
	dataShm->seg = seg;
	dataShm->toc = toc;
	dataShm->dataInShm = dataInShm;
	return dataShm;
}

/**
 * setup a single shm for each node monitor doctor process, used to transfer data to it 
 */
static AdbDoctorBgworkerDataShm *
setupNodeDataShm(AdbDoctorNodeData *nodeData)
{
	AdbDoctorNodeData *nodeDataInShm;
	AdbDoctorBgworkerDataShm *dataShm;

	Size segsize;
	Size datasize;
	Size nkeys = 0;
	Size nbytes;
	shm_toc_estimator e;
	shm_toc *toc;
	uint64 tocKey = 0;

	shm_toc_initialize_estimator(&e);

	datasize = sizeof(AdbDoctorNodeData);
	shm_toc_estimate_chunk(&e, datasize);
	nkeys++;

	shm_toc_estimate_chunk(&e, sizeof(AdbMgrNodeWrapper));
	nkeys++;

	/* need the tail \0 */
	nbytes = strlen(nodeData->wrapper->nodepath) + 1;
	shm_toc_estimate_chunk(&e, nbytes);
	nkeys++;

	nbytes = strlen(nodeData->wrapper->hostaddr) + 1;
	shm_toc_estimate_chunk(&e, nbytes);
	nkeys++;

	shm_toc_estimate_keys(&e, nkeys);
	segsize = shm_toc_estimate(&e);

	dataShm = initializeBgworkerDataShm((AdbDoctorBgworkerData *)nodeData,
										segsize,
										datasize,
										&tocKey);

	nodeDataInShm = (AdbDoctorNodeData *)dataShm->dataInShm;
	toc = dataShm->toc;

	nodeDataInShm->wrapper = shm_toc_allocate(toc, sizeof(AdbMgrNodeWrapper));
	memcpy(nodeDataInShm->wrapper, nodeData->wrapper, sizeof(AdbMgrNodeWrapper));
	shm_toc_insert(toc, tocKey++, nodeDataInShm->wrapper);

	/* need the tail \0 */
	nbytes = strlen(nodeData->wrapper->nodepath) + 1;
	nodeDataInShm->wrapper->nodepath = shm_toc_allocate(toc, nbytes);
	strcpy(nodeDataInShm->wrapper->nodepath, nodeData->wrapper->nodepath);
	shm_toc_insert(toc, tocKey++, nodeDataInShm->wrapper->nodepath);

	nbytes = strlen(nodeData->wrapper->hostaddr) + 1;
	nodeDataInShm->wrapper->hostaddr = shm_toc_allocate(toc, nbytes);
	strcpy(nodeDataInShm->wrapper->hostaddr, nodeData->wrapper->hostaddr);
	shm_toc_insert(toc, tocKey++, nodeDataInShm->wrapper->hostaddr);

	return dataShm;
}

/**
 * setup a single shm for host monitor doctor process, 
 * used to transfer data to it.
 */
static AdbDoctorBgworkerDataShm *
setupHostDataShm(AdbDoctorHostData *hostData)
{
	AdbDoctorHostData *hostDataInShm;
	AdbDoctorBgworkerDataShm *dataShm;
	AdbDoctorLink *hostLink;
	AdbDoctorLink *hostLinkInShm;
	AdbMgrHostWrapper *hostWrapper;
	AdbMgrHostWrapper *hostWrapperInShm;
	dlist_iter iter;

	Size segsize;
	Size datasize;
	Size nkeys = 0;
	Size nbytes;
	shm_toc_estimator e;
	shm_toc *toc;
	uint64 tocKey = 0;

	shm_toc_initialize_estimator(&e);

	datasize = sizeof(AdbDoctorHostData);
	shm_toc_estimate_chunk(&e, datasize);
	nkeys++;

	shm_toc_estimate_chunk(&e, sizeof(AdbDoctorList));
	nkeys++;

	dlist_foreach(iter, &hostData->list->head)
	{
		hostLink = dlist_container(AdbDoctorLink, wi_links, iter.cur);
		hostWrapper = hostLink->data;

		shm_toc_estimate_chunk(&e, sizeof(AdbDoctorLink));
		nkeys++;

		shm_toc_estimate_chunk(&e, sizeof(AdbMgrHostWrapper));
		nkeys++;

		/* need the tail \0 */
		nbytes = strlen(hostWrapper->hostaddr) + 1;
		shm_toc_estimate_chunk(&e, nbytes);
		nkeys++;

		nbytes = strlen(hostWrapper->hostadbhome) + 1;
		shm_toc_estimate_chunk(&e, nbytes);
		nkeys++;
	}

	shm_toc_estimate_keys(&e, nkeys);
	segsize = shm_toc_estimate(&e);

	dataShm = initializeBgworkerDataShm((AdbDoctorBgworkerData *)hostData,
										segsize,
										datasize,
										&tocKey);

	hostDataInShm = (AdbDoctorHostData *)dataShm->dataInShm;
	toc = dataShm->toc;

	hostDataInShm->list = shm_toc_allocate(toc, sizeof(AdbDoctorList));
	shm_toc_insert(toc, tocKey++, hostDataInShm->list);
	memcpy(hostDataInShm->list, hostData->list, sizeof(AdbDoctorList));
	dlist_init(&hostDataInShm->list->head);

	dlist_foreach(iter, &hostData->list->head)
	{
		hostLink = dlist_container(AdbDoctorLink, wi_links, iter.cur);
		hostWrapper = hostLink->data;

		hostLinkInShm = shm_toc_allocate(toc, sizeof(AdbDoctorLink));
		shm_toc_insert(toc, tocKey++, hostLinkInShm);
		memcpy(hostLinkInShm, hostLink, sizeof(AdbDoctorLink));
		dlist_push_tail(&hostDataInShm->list->head, &hostLinkInShm->wi_links);

		hostWrapperInShm = shm_toc_allocate(toc, sizeof(AdbMgrHostWrapper));
		shm_toc_insert(toc, tocKey++, hostWrapperInShm);
		memcpy(hostWrapperInShm, hostWrapper, sizeof(AdbMgrHostWrapper));
		hostLinkInShm->data = hostWrapperInShm;

		/* need the tail \0 */
		nbytes = strlen(hostWrapper->hostaddr) + 1;
		hostWrapperInShm->hostaddr = shm_toc_allocate(toc, nbytes);
		shm_toc_insert(toc, tocKey++, hostWrapperInShm->hostaddr);
		strcpy(hostWrapperInShm->hostaddr, hostWrapper->hostaddr);

		nbytes = strlen(hostWrapper->hostadbhome) + 1;
		hostWrapperInShm->hostadbhome = shm_toc_allocate(toc, nbytes);
		shm_toc_insert(toc, tocKey++, hostWrapperInShm->hostadbhome);
		strcpy(hostWrapperInShm->hostadbhome, hostWrapper->hostadbhome);
	}

	return dataShm;
}

/**
 * setup a single shm for switcher doctor process, 
 * used to transfer data to it.
 */
static AdbDoctorBgworkerDataShm *
setupSwitcherDataShm(AdbDoctorSwitcherData *switcherData)
{
	AdbDoctorSwitcherData *switcherDataInShm = NULL;
	AdbDoctorBgworkerDataShm *dataShm;

	Size segsize;
	Size datasize;
	Size nkeys = 0;
	Size nbytes;
	shm_toc_estimator e;
	shm_toc *toc;
	uint64 tocKey = 0;

	shm_toc_initialize_estimator(&e);

	datasize = sizeof(AdbDoctorSwitcherData);
	shm_toc_estimate_chunk(&e, datasize);
	nkeys++;

	shm_toc_estimate_chunk(&e, sizeof(AdbMgrNodeWrapper));
	nkeys++;

	/* need the tail \0 */
	nbytes = strlen(switcherData->wrapper->nodepath) + 1;
	shm_toc_estimate_chunk(&e, nbytes);
	nkeys++;

	nbytes = strlen(switcherData->wrapper->hostaddr) + 1;
	shm_toc_estimate_chunk(&e, nbytes);
	nkeys++;

	shm_toc_estimate_keys(&e, nkeys);
	segsize = shm_toc_estimate(&e);

	dataShm = initializeBgworkerDataShm((AdbDoctorBgworkerData *)switcherData,
										segsize,
										datasize,
										&tocKey);

	switcherDataInShm = (AdbDoctorSwitcherData *)dataShm->dataInShm;
	toc = dataShm->toc;

	switcherDataInShm->wrapper = shm_toc_allocate(toc, sizeof(AdbMgrNodeWrapper));
	memcpy(switcherDataInShm->wrapper, switcherData->wrapper, sizeof(AdbMgrNodeWrapper));
	shm_toc_insert(toc, tocKey++, switcherDataInShm->wrapper);

	/* need the tail \0 */
	nbytes = strlen(switcherData->wrapper->nodepath) + 1;
	switcherDataInShm->wrapper->nodepath = shm_toc_allocate(toc, nbytes);
	strcpy(switcherDataInShm->wrapper->nodepath, switcherData->wrapper->nodepath);
	shm_toc_insert(toc, tocKey++, switcherDataInShm->wrapper->nodepath);

	nbytes = strlen(switcherData->wrapper->hostaddr) + 1;
	switcherDataInShm->wrapper->hostaddr = shm_toc_allocate(toc, nbytes);
	strcpy(switcherDataInShm->wrapper->hostaddr, switcherData->wrapper->hostaddr);
	shm_toc_insert(toc, tocKey++, switcherDataInShm->wrapper->hostaddr);

	return dataShm;
}

/**
 * setup a single shm for each doctor process, used to transfer data to it 
 */
static AdbDoctorBgworkerDataShm *
setupDataShm(AdbDoctorBgworkerStatus *bgworkerStatus)
{
	Adb_Doctor_Bgworker_Type type;
	AdbDoctorBgworkerData *data;
	AdbDoctorBgworkerDataShm *dataShm;

	data = bgworkerStatus->data;
	type = data->type;

	if (type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
	{
		dataShm = setupNodeDataShm((AdbDoctorNodeData *)data);
	}
	else if (type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
	{
		dataShm = setupHostDataShm((AdbDoctorHostData *)data);
	}
	else if (type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
	{
		dataShm = setupSwitcherDataShm((AdbDoctorSwitcherData *)data);
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unrecognized Adb_Doctor_Bgworker_Type:%d", type)));
	}
	return dataShm;
}

/**
 * register doctor process as Background Worker Processe
 * use the name as Unique identification
 * set bgw_start_time=BgWorkerStart_RecoveryFinished 
 * (start as soon as the system has entered normal read-write state)
 * set bgw_restart_time=BGW_NEVER_RESTART, indicating not to 
 * restart the process in case of a crash,
 * If it collapsed, it doesn't matter, the launcher will restart it if necessary.
 */
static void registerDoctorAsBgworker(AdbDoctorBgworkerStatus *bgworkerStatus,
									 AdbDoctorBgworkerDataShm *dataShm)
{
	Adb_Doctor_Bgworker_Type type;
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, ADB_DOCTOR_BGW_LIBRARY_NAME);
	type = bgworkerStatus->data->type;
	if (type == ADB_DOCTOR_BGWORKER_TYPE_NODE_MONITOR)
	{
		sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_NODE_MONITOR);
	}
	else if (type == ADB_DOCTOR_BGWORKER_TYPE_HOST_MONITOR)
	{
		sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_HOST_MONITOR);
	}
	else if (type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER)
	{
		sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_SWITCHER);
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unrecognized Adb_Doctor_Bgworker_Type:%d", type)));
	}
	snprintf(worker.bgw_type, BGW_MAXLEN, ADB_DOCTOR_BGW_TYPE_WORKER);
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(dataShm->seg));
	worker.bgw_notify_pid = MyProcPid;
	Assert(strlen(bgworkerStatus->name) > 0);
	strncpy(worker.bgw_name, bgworkerStatus->name, BGW_MAXLEN);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("Could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));

	bgworkerStatus->handle = handle;
}

/**
 * after doctor process attaches the data shm, it will set the variable "ready" to true.
 * This indicates that it's working properly.
 */
static void waitForDoctorBecomeReady(AdbDoctorBgworkerStatus *bgworkerStatus,
									 volatile AdbDoctorBgworkerData *dataInShm)
{
	bool ready;
	BgwHandleStatus status;

	/* wait for postmaster startup the doctor process as a bgworker */
	status = WaitForBackgroundWorkerStartup(bgworkerStatus->handle,
											&bgworkerStatus->pid);
	bgworkerStatus->status = status;
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	/* the BGWH_STARTED can not indicate that it is working properly, 
	 * should wait for the variable "ready" in shm be set to true */
	for (;;)
	{
		/* An interrupt may have occurred while we were waiting. */
		CHECK_FOR_INTERRUPTS();

		/* If the doctor process set header->ready=true, we have succeeded. */
		SpinLockAcquire(&dataInShm->mutex);
		ready = dataInShm->ready;
		SpinLockRelease(&dataInShm->mutex);

		if (ready)
		{
			break;
		}

		// doctor process may crashed
		status = GetBackgroundWorkerPid(bgworkerStatus->handle,
										&bgworkerStatus->pid);
		if (status != BGWH_STARTED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("could not start background process"),
					 errhint("More details may be available in the server log.")));

		/* Wait to be signalled. */
		WaitLatch(MyLatch, WL_LATCH_SET, 0, PG_WAIT_EXTENSION);

		/* Reset the latch so we don't spin. */
		ResetLatch(MyLatch);
	}
}
/* 
 * We launch doctor processes sequencially. There will be two steps:
 * we push corresponding AdbDoctorBgworkerStatus of the doctor process to 
 * the private global variable bgworkerStatusList when a doctor process 
 * launched. with the help of bgworkerStatusList, we can view or control 
 * doctor process status as we want.
 */
static void launchDoctorList(AdbDoctorList *dataList)
{
	AdbDoctorLink *link;
	AdbDoctorBgworkerStatus *bgworkerStatus;
	dlist_iter iter;

	if (dataList == NULL || dlist_is_empty(&dataList->head))
		return;

	dlist_foreach(iter, &dataList->head)
	{
		link = dlist_container(AdbDoctorLink, wi_links, iter.cur);

		bgworkerStatus = launchDoctor(link->data);

		dlist_push_tail(&bgworkerStatusList, &bgworkerStatus->wi_links);
	}
}

/**
 * the steps of launch a doctor process include:
 * setup individual bgworker data shm.
 * startup it as Background Worker Processe, wait for it become ready.
 * store doctor process running data into bgworkerStatus then return.
 */
static AdbDoctorBgworkerStatus *launchDoctor(AdbDoctorBgworkerData *data)
{
	/* used to store the bgwworker's infomation */
	AdbDoctorBgworkerStatus *bgworkerStatus;
	AdbDoctorBgworkerDataShm *dataShm;
	char *name;

	bgworkerStatus = palloc0(sizeof(AdbDoctorBgworkerStatus));
	name = getDoctorName(data);
	bgworkerStatus->name = name;
	bgworkerStatus->data = data;

	/* Create single data shared memory for each doctor, give data to them */
	dataShm = setupDataShm(bgworkerStatus);

	/* startup as Background Worker Processe */
	registerDoctorAsBgworker(bgworkerStatus, dataShm);
	/* if launcher exits abnormally, the shm will be detached,
	*  the worker launched should exit too. */
	on_dsm_detach(dataShm->seg, cleanupAdbDoctorBgworker,
				  PointerGetDatum(bgworkerStatus->handle));

	waitForDoctorBecomeReady(bgworkerStatus,
							 (AdbDoctorBgworkerData *)dataShm->dataInShm);
	/* the doctor process become ready, the shm should be detached,
	 * this time do not let the doctor process exit. */
	cancel_on_dsm_detach(dataShm->seg,
						 cleanupAdbDoctorBgworker,
						 PointerGetDatum(bgworkerStatus->handle));

	/* we do not need this shm anymore */
	dsm_detach(dataShm->seg);
	pfree(dataShm);

	return bgworkerStatus;
}

/* 
 * clean up all doctor, teminate all the processes and free the memory
 */
static void terminateAllDoctor(void)
{
	AdbDoctorBgworkerStatus *bgworkerStatus;
	dlist_mutable_iter miter;

	if (dlist_is_empty(&bgworkerStatusList))
		return;

	dlist_foreach_modify(miter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, miter.cur);

		terminateDoctor(bgworkerStatus, false);

		dlist_delete(miter.cur);
		pfreeAdbDoctorBgworkerStatus(bgworkerStatus, true);
	}
}

static void terminateDoctorList(AdbDoctorList *dataList)
{
	AdbDoctorLink *link;
	char *name;
	dlist_iter iter;

	if (dataList == NULL || dlist_is_empty(&dataList->head))
		return;

	dlist_foreach(iter, &dataList->head)
	{
		link = dlist_container(AdbDoctorLink, wi_links, iter.cur);
		name = getDoctorName(link->data);

		terminateDoctorByName(name);

		pfree(name);
	}
}

static void terminateDoctorByName(char *name)
{
	AdbDoctorBgworkerStatus *bgworkerStatus;
	dlist_mutable_iter miter;

	if (dlist_is_empty(&bgworkerStatusList))
		return;

	dlist_foreach_modify(miter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, miter.cur);
		if (strcmp(name, bgworkerStatus->name) == 0)
		{
			terminateDoctor(bgworkerStatus, true);

			dlist_delete(miter.cur);
			pfreeAdbDoctorBgworkerStatus(bgworkerStatus, true);
			/* we do not break in case of duplicate name */
		}
	}
}

static void terminateDoctor(AdbDoctorBgworkerStatus *bgworkerStatus,
							bool waitFor)
{
	BgwHandleStatus status;

	/* signal postmaster to terminate it */
	TerminateBackgroundWorker(bgworkerStatus->handle);
	if (waitFor)
	{
		/* wait for shutdown */
		status = WaitForBackgroundWorkerShutdown(bgworkerStatus->handle);
		if (status != BGWH_STOPPED)
			ereport(ERROR,
					(errmsg("could not terminate background process, "
							"the postmaster may died.")));
	}
}

/**
 * check the status of doctor processes.
 * if status = BGWH_STOPPED, terminate it to ensure it would not restart.
 * status == BGWH_POSTMASTER_DIED, indicate there some error occured.
 */
static void checkDoctorRunningStatus(void)
{
	AdbDoctorBgworkerStatus *bgworkerStatus;
	BgwHandleStatus status;
	dlist_mutable_iter miter;

	if (dlist_is_empty(&bgworkerStatusList))
		return;

	dlist_foreach_modify(miter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, miter.cur);

		status = GetBackgroundWorkerPid(bgworkerStatus->handle,
										&bgworkerStatus->pid);
		if (status == BGWH_STOPPED)
		{
			/* ensure to terminate it */
			terminateDoctor(bgworkerStatus, false);

			dlist_delete(miter.cur);
			pfreeAdbDoctorBgworkerStatus(bgworkerStatus, true);
		}
		else if (status == BGWH_POSTMASTER_DIED)
		{
			ereport(ERROR,
					(errmsg("the postmaster died")));
		}
		else
		{
			bgworkerStatus->status = status;
			continue;
		}
	}
}

static void signalDoctorByName(char *name)
{
	AdbDoctorBgworkerStatus *bgworkerStatus;
	BgwHandleStatus status;

	dlist_iter iter;
	dlist_foreach(iter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, iter.cur);
		if (strcmp(bgworkerStatus->name, name) == 0)
		{
			status = GetBackgroundWorkerPid(bgworkerStatus->handle,
											&bgworkerStatus->pid);
			if (status == BGWH_STARTED && bgworkerStatus->pid > 0)
				kill(bgworkerStatus->pid, SIGUSR1);
		}
	}
}

/**
 * signal a list of the doctor process
 */
static void signalDoctorList(AdbDoctorList *dataList)
{
	AdbDoctorLink *link;
	char *name;
	dlist_iter iter;

	if (dlist_is_empty(&dataList->head))
		return;

	dlist_foreach(iter, &dataList->head)
	{
		link = dlist_container(AdbDoctorLink, wi_links, iter.cur);

		name = getDoctorName(link->data);
		/* send signal */
		signalDoctorByName(name);

		pfree(name);
	}
}

/*
 * query out all configuration values from table adb_doctor_conf,
 * query out all data from table mgr_node and mgr_host that needs to be monitored.
 * the data in mgr_node and mgr_host is very critical for doctor processes,
 * these data are so called AdbDoctorBgworkerData.
 */
static void queryBgworkerDataAndConf(AdbDoctorConf **confP,
									 AdbDoctorList **dataListP)
{
	MemoryContext oldContext;
	AdbDoctorList *dataList;
	AdbDoctorList *nodeDataList;
	AdbDoctorList *switcherDataList;
	AdbDoctorHostData *hostData;
	int ret;

	oldContext = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						(errmsg("SPI_connect failed, connect return:%d",
								ret))));
	}
	PushActiveSnapshot(GetTransactionSnapshot());

	/* query out all configuration values from table adb_doctor_conf */
	*confP = SPI_selectAdbDoctorConfAll(oldContext);

	/* query out all data from table mgr_node that need to be monitored */
	nodeDataList = SPI_selectMgrNodeForMonitor(oldContext);

	/* query out all data from table mgr_host that need to be monitored */
	hostData = SPI_selectMgrHostForMonitor(oldContext);

	/* query out all data from table mgr_node that need to be switched */
	switcherDataList = SPI_selectMgrNodeForSwitcher(oldContext);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	MemoryContextSwitchTo(oldContext);

	/* must palloc here, do not palloc in spi context. */
	dataList = newAdbDoctorList();
	/* because SPI will free his memory context, so append data here. */
	appendAdbDoctorList(dataList, nodeDataList, true);
	appendAdbDoctorBgworkerData(dataList, (AdbDoctorBgworkerData *)hostData);
	appendAdbDoctorList(dataList, switcherDataList, true);

	*dataListP = dataList;
}

/*
 * When we receive a SIGTERM, we set InterruptPending and ProcDiePending just
 * like a normal backend.  The next CHECK_FOR_INTERRUPTS() will do the right
 * thing.
 */
static void handleSigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	gotSigterm = true;

	SetLatch(MyLatch);

	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	errno = save_errno;
}

static void handleSigusr1(SIGNAL_ARGS)
{
	int save_errno = errno;

	gotSigusr1 = true;

	procsignal_sigusr1_handler(postgres_signal_arg);

	errno = save_errno;
}

/**
 * backend create a dsm_segment, and transfer a variable dsm_handle 
 * to here by BackgroundWorker mechanism,  use variable dsm_handle 
 * to attach the dsm_segment, and then get the shm_mq that backend 
 * have set.  when all doctor processes launched ok, we can send 
 * message "OK" to backend by this shm_mq.
 */
void attachShm(Datum main_arg, bool *needNotify,
			   dsm_segment **segP, shm_mq_handle **mqhP)
{
	dsm_segment *seg;
	shm_toc *toc;
	shm_mq *mq;
	shm_mq_handle *mqh;

	if (main_arg == 0)
	{
		/* Indicate called directly from postmaster */
		*needNotify = false;
		return;
	}
	CurrentResourceOwner = ResourceOwnerCreate(NULL,
											   MyBgworkerEntry->bgw_name);
	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
	{
		*needNotify = false;
		ereport(LOG,
				(errmsg("unable to map dynamic shared memory segment, ignore")));
	}
	else
	{
		*needNotify = true;
		ereport(DEBUG1,
				(errmsg("map dynamic shared memory segment success")));
		*segP = seg;
		toc = shm_toc_attach(ADB_DOCTOR_LAUNCHER_MAGIC, dsm_segment_address(seg));
		if (toc == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("bad magic number in dynamic shared memory segment")));
		mq = shm_toc_lookup(toc, 0, false);
		shm_mq_set_sender(mq, MyProc);
		mqh = shm_mq_attach(mq, seg, NULL);
		*mqhP = mqh;
	}
}

static void sendBackendMessage(shm_mq_handle *outqh, char *message)
{
	int len = strlen(message);
	int res = shm_mq_send(outqh, len, message, false);
	if (res != SHM_MQ_SUCCESS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not send message")));
	}
}

/**
 * bgworkerStatusList saved the stale doctor data, read it out as list
 */
static AdbDoctorList *getStaleBgworkerDataList(void)
{
	AdbDoctorList *staleList;
	AdbDoctorBgworkerStatus *bgworkerStatus;
	dlist_iter iter;

	staleList = newAdbDoctorList();

	if (dlist_is_empty(&bgworkerStatusList))
		return staleList;

	dlist_foreach(iter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, iter.cur);

		appendAdbDoctorBgworkerData(staleList, bgworkerStatus->data);
	}
	return staleList;
}

/*
 * compare the stale data in bgworkerStatusList to the fresh data from database,
 * refresh doctor processes (include terminate,launch,signal etc) if necessary, 
 * or do nothing. the so called BgworkerData is the data in tables mgr_node 
 * and mgr_host, when data changed(added,deleted,updated) in these tables,
 * the corresponding doctor processes must be freshed.
 * if any doctor processes have been terminate unexpectedly, reluanch it.
 */
static void compareAndRefreshDoctor(bool confChanged,
									AdbDoctorList *staleDataList,
									AdbDoctorList *freshDataList)
{
	AdbDoctorList *addedDataList;
	AdbDoctorList *deletedDataList;
	AdbDoctorList *changedDataList;
	AdbDoctorList *identicalDataList;

	/* 
     * get the information of (mgr_node,mgr_host) BgworkerData changes, 
     * manipulate doctor processes according to different scenarios
     */
	compareBgworkerDataList(staleDataList,
							freshDataList,
							&addedDataList,
							&deletedDataList,
							&changedDataList,
							&identicalDataList);

	/* if deleted, terminate it. */
	terminateDoctorList(deletedDataList);
	pfreeAdbDoctorList(deletedDataList, false);

	/* if new added, we launch them as Background Worker Processes */
	launchDoctorList(addedDataList);
	/* the data is now linking to bgworkerStatusList,so we should not pfree them */
	pfreeAdbDoctorList(addedDataList, false);

	/* if changed, terminate the doctor process, and then launch a new one. */
	terminateDoctorList(changedDataList);
	launchDoctorList(changedDataList);
	/* the data is now linking to bgworkerStatusList,so we should not pfree them */
	pfreeAdbDoctorList(changedDataList, false);

	/* if stay same, check if the configuration changed and signal doctor processes */
	if (confChanged)
	{
		ereport(LOG,
				(errmsg("adb doctor configuration changed, signal doctor processes.")));
		signalDoctorList(identicalDataList);
	}
	/* bgworkerStatusList would not link any data in identicalDataList, 
	 * so pfree it and data in it. */
	pfreeAdbDoctorList(identicalDataList, true);

	addedDataList = NULL;
	deletedDataList = NULL;
	changedDataList = NULL;
	identicalDataList = NULL;
}
/*
 * get information list of added,deleted,changed,identical data list
 *  through comparing the fresh and stale (mgr_node,mgr_host) data.
 * NB: the data in freshDataList may changed here.
 * These list in memory allocated using palloc.
 * release the memory when you don't need it anymore.
 * the recomended way is release the memory except the AdbDoctorMgrNodeData 
 * linked in the AdbDoctorList, see below:
 * pfreeAdbDoctorList(addedDataList, false);
 * pfreeAdbDoctorList(changedDataList, false);
 * pfreeAdbDoctorList(identicalDataList, true);
 * pfreeAdbDoctorList(deletedDataList, false);
 * why? because AdbDoctorBgworkerData from fresh and stale list only has one copy,
 * if you free them, may take wrong.
 */
static void compareBgworkerDataList(AdbDoctorList *staleDataList,
									AdbDoctorList *freshDataList,
									AdbDoctorList **addedDataListP,
									AdbDoctorList **deletedDataListP,
									AdbDoctorList **changedDataListP,
									AdbDoctorList **identicalDataListP)
{
	AdbDoctorList *addedDataList;
	AdbDoctorList *deletedDataList;
	AdbDoctorList *changedDataList;
	AdbDoctorList *identicalDataList;
	dlist_mutable_iter freshIter;
	AdbDoctorLink *freshLink;
	dlist_mutable_iter staleIter;
	AdbDoctorLink *staleLink;

	changedDataList = newAdbDoctorList();
	identicalDataList = newAdbDoctorList();

	/*
     * get information of added,deleted,changed through comparing the fresh 
	 * and stale data(mgr_node,mgr_host). when all of this comparison done, 
     * the above lists can be used to do some thing about control doctor process.
     */
	dlist_foreach_modify(freshIter, &freshDataList->head)
	{
		freshLink = dlist_container(AdbDoctorLink, wi_links, freshIter.cur);

		dlist_foreach_modify(staleIter, &staleDataList->head)
		{
			staleLink = dlist_container(AdbDoctorLink, wi_links, staleIter.cur);
			if (isSameAdbDoctorBgworkerData(staleLink->data, freshLink->data))
			{
				/*
                 * in order to reduce the number of loop iterations, we delete it from list,
                 * anther benifit is when all of the comparision done, 
                 * the remaining data in staleList exactly is deletedList.  
                 */
				deleteFromAdbDoctorList(staleDataList, staleIter);

				/* 
                 * we delete it from list, the benifit is when all iteration completed, 
                 * the remaing data in freshList exactly is addedList.
                 */
				deleteFromAdbDoctorList(freshDataList, freshIter);

				/* 
                 * the "equals" function means nothing is changed on the data, 
                 * if anything changed, push the data to changedList.
                 */
				if (equalsAdbDoctorBgworkerData(freshLink->data, staleLink->data))
				{
					/* copy from fresh, because the stale data will be pfreed as soon as possible */
					dlist_push_tail(&identicalDataList->head, &freshLink->wi_links);
					identicalDataList->num++;
				}
				else
				{
					/* copy from fresh, because the stale data will be pfreed as soon as possible */
					dlist_push_tail(&changedDataList->head, &freshLink->wi_links);
					changedDataList->num++;
				}
				break;
			}
			else
			{
				continue;
			}
		}
	}

	addedDataList = freshDataList;
	deletedDataList = staleDataList;

	*addedDataListP = addedDataList;
	*deletedDataListP = deletedDataList;
	*changedDataListP = changedDataList;
	*identicalDataListP = identicalDataList;
}
