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
#include "mgr/mgr_helper.h"

/* setup shm which create by backend */
static void attachShm(Datum main_arg, bool *needNotify,
					  dsm_segment **segP, shm_mq_handle **mqhP);
/* tell backend */
static void sendBackendMessage(shm_mq_handle *outqh, char *message);
/* do work when signaled or timedout */
static void launcherLoop(AdbDoctorConf *conf);
static void queryBgworkerDataAndConf(AdbDoctorConf **confP,
									 dlist_head *monitorNodes,
									 dlist_head *switchNodes,
									 dlist_head *monitorHosts);
static char *getDoctorDisplayName(Adb_Doctor_Type type,
								  MgrNodeWrapper *mgrNode);
static void registerDoctorAsBgworker(AdbDoctorBgworkerStatus *bgworkerStatus,
									 AdbDoctorBgworkerDataShm *dataShm);
static void waitForDoctorBecomeReady(AdbDoctorBgworkerStatus *bgworkerStatus,
									 AdbDoctorBgworkerData *dataInShm);
static void launchNodeMonitorDoctors(dlist_head *monitorNodes);
static void launchNodeMonitorDoctor(MgrNodeWrapper *mgrNode);
static void launchSwitcherDoctor(dlist_head *nodes);
static void launchHostMonitorDoctor(dlist_head *hosts);
static AdbDoctorBgworkerStatus *launchDoctor(AdbDoctorBgworkerData *bgworkerData);
static void terminateAllDoctor(void);
static bool equalsDoctorUniqueId(Adb_Doctor_Type type, Oid oid,
								 AdbDoctorBgworkerData *bgworkerData);
static void terminateDoctorByUniqueId(Adb_Doctor_Type type,
									  Oid oid);
static void terminateDoctor(AdbDoctorBgworkerStatus *bgworkerStatus,
							bool waitFor);
static void checkDoctorRunningStatus(void);
static void signalDoctorByUniqueId(Adb_Doctor_Type type, Oid oid);
static void tryRefreshNodeMonitorDoctors(bool confChanged,
										 dlist_head *staleNodes,
										 dlist_head *freshNodes);
static void tryRefreshSwitcherDoctor(bool confChanged,
									 dlist_head *staleNodes,
									 dlist_head *freshNodes);
static void tryRefreshHostMonitorDoctor(bool confChanged,
										dlist_head *staleHosts,
										dlist_head *freshHosts);

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

/* this list link the running bgworker of doctor processed, 
 * so we can view or control doctor process as we want. */
static dlist_head bgworkerStatusList = DLIST_STATIC_INIT(bgworkerStatusList);
static AdbDoctorConfShm *confShm;
static dlist_head *cachedMonitorNodes;
static dlist_head *cachedSwitchNodes;
static dlist_head *cachedMonitorHosts;

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
	ErrorData *edata = NULL;
	MemoryContext oldContext;
	dsm_segment *segBackend = NULL;
	shm_mq_handle *mqh = NULL;
	bool needNotify = false;
	AdbDoctorConf *conf = NULL;

	oldContext = CurrentMemoryContext;
	pqsignal(SIGTERM, handleSigterm);
	pqsignal(SIGUSR1, handleSigusr1);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(DEFAULT_DB, NULL, 0);

	PG_TRY();
	{
		adbDoctorStopBgworkers(false);

		/* if can not attach to shm, it may restarted by postmaster */
		attachShm(main_arg, &needNotify, &segBackend, &mqh);
		if (needNotify)
			notifyAdbDoctorRegistrant();

		ereport(LOG,
				(errmsg("%s started", MyBgworkerEntry->bgw_name)));

		cachedMonitorNodes = palloc0(sizeof(dlist_head));
		dlist_init(cachedMonitorNodes);
		cachedSwitchNodes = palloc0(sizeof(dlist_head));
		dlist_init(cachedSwitchNodes);
		cachedMonitorHosts = palloc0(sizeof(dlist_head));
		dlist_init(cachedMonitorHosts);
		queryBgworkerDataAndConf(&conf, cachedMonitorNodes,
								 cachedSwitchNodes, cachedMonitorHosts);
		if (!conf)
			ereport(ERROR,
					(errmsg("%s configuration failed",
							MyBgworkerEntry->bgw_name)));

		/* Create common shared memory to store configuration for all doctors */
		confShm = setupAdbDoctorConfShm(conf);
		/* 
		* We launch doctor processes sequencially. There will be two steps:
		* we push corresponding AdbDoctorBgworkerStatus of the doctor process to 
		* the private global variable bgworkerStatusList when a doctor process 
		* launched. with the help of bgworkerStatusList, we can view or control 
		* doctor process status as we want.
		*/
		launchNodeMonitorDoctors(cachedMonitorNodes);
		launchSwitcherDoctor(cachedSwitchNodes);
		launchHostMonitorDoctor(cachedMonitorHosts);

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
		FlushErrorState();

		terminateAllDoctor();
		pfreeAdbDoctorConfShm(confShm);
		/* cut the contact with backend */
		if (mqh != NULL)
			sendBackendMessage(mqh, ADB_DOCTORS_LAUNCH_FAILURE);
		if (segBackend != NULL)
			dsm_detach(segBackend);
		/* exits with an exit code of 0, it will be automatically 
		 * unregistered by the postmaster on exit. */
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
		/* Save error info in our stmt_mcontext */
		MemoryContextSwitchTo(oldContext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	/* clean all resources */
	terminateAllDoctor();
	pfreeAdbDoctorConfShm(confShm);
	if (edata)
		ReThrowError(edata);
	else
		proc_exit(1);
}

/* 
 * when signaled, check the status of doctor processes, check the conf and
 * (mgr_node,mgr_host) data change.
 * if conf changed, we signal all the doctor processes.
 * if add some new (mgr_node,mgr_host) data, we launche new doctor process.
 * if delete some data, we terminate corresponding doctor process.
 * if change some data's field, terminate the old process and then startup 
 * a new process.
 */
static void launcherLoop(AdbDoctorConf *conf)
{
	int rc;
	bool confChanged = false;
	AdbDoctorConf *freshConf;
	dlist_head *freshMonitorNodes;
	dlist_head *freshSwitchNodes;
	dlist_head *freshMonitorHosts;
	while (!gotSigterm)
	{
		CHECK_FOR_INTERRUPTS();

		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   PGINVALID_SOCKET,
							   10000L,
							   PG_WAIT_EXTENSION);
		/* Reset the latch, bail out if postmaster died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
		/* Interrupted? */
		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (gotSigusr1)
		{
			gotSigusr1 = false;
			checkDoctorRunningStatus();
		}

		freshMonitorNodes = palloc0(sizeof(dlist_head));
		dlist_init(freshMonitorNodes);
		freshSwitchNodes = palloc0(sizeof(dlist_head));
		dlist_init(freshSwitchNodes);
		freshMonitorHosts = palloc0(sizeof(dlist_head));
		dlist_init(freshMonitorHosts);
		queryBgworkerDataAndConf(&freshConf, freshMonitorNodes,
								 freshSwitchNodes, freshMonitorHosts);

		if (equalsAdbDoctorConfIgnoreLock(conf, freshConf))
		{
			confChanged = false;
			pfree(freshConf);
			freshConf = NULL;
		}
		else
		{
			confChanged = true;
			/* if conf changed, update the value of staleConf */
			pfree(conf);
			conf = freshConf;
			freshConf = NULL;
			/* refresh shm before manipulate doctor processes */
			refreshAdbDoctorConfInShm(conf, confShm);
		}

		/*
		* Compare the cached doctor data in to the data queried from the 
		* tables mgr_node and mgr_host, refresh doctor processes 
		* (include terminate,launch,signal etc) if necessary,  or do nothing. 
		* when data changed(added,deleted,updated) in these tables,
		* the corresponding doctor processes must be refreshed.
		* If any doctor processes have been terminate unexpectedly, reluanch it.
		*/
		tryRefreshNodeMonitorDoctors(confChanged,
									 cachedMonitorNodes,
									 freshMonitorNodes);
		pfreeMgrNodeWrapperList(cachedMonitorNodes, NULL);
		pfree(cachedMonitorNodes);
		cachedMonitorNodes = freshMonitorNodes;

		tryRefreshSwitcherDoctor(confChanged,
								 cachedSwitchNodes,
								 freshSwitchNodes);
		pfreeMgrNodeWrapperList(cachedSwitchNodes, NULL);
		pfree(cachedSwitchNodes);
		cachedSwitchNodes = freshSwitchNodes;

		tryRefreshHostMonitorDoctor(confChanged,
									cachedMonitorHosts,
									freshMonitorHosts);
		pfreeMgrHostWrapperList(cachedMonitorHosts, NULL);
		pfree(cachedMonitorHosts);
		cachedMonitorHosts = freshMonitorHosts;
	}
}

/**
 * give each doctor process a user friendly display name.
 */
static char *getDoctorDisplayName(Adb_Doctor_Type type,
								  MgrNodeWrapper *mgrNode)
{
	char *name;
	if (type == ADB_DOCTOR_TYPE_NODE_MONITOR)
	{
		name = psprintf("antdb doctor node monitor %s",
						NameStr(mgrNode->form.nodename));
	}
	else if (type == ADB_DOCTOR_TYPE_HOST_MONITOR)
	{
		name = psprintf("antdb doctor host monitor");
	}
	else if (type == ADB_DOCTOR_TYPE_SWITCHER)
	{
		name = psprintf("antdb doctor switcher");
	}
	else
	{
		name = NULL;
		ereport(ERROR,
				(errmsg("could not recognize type:%d",
						type)));
	}
	return name;
}

/**
 * register doctor process as Background Worker Processe,
 * Use the type and oid as unique identification, and
 * use the displayName as a user friendly display name.
 * set bgw_start_time=BgWorkerStart_RecoveryFinished,
 * (start as soon as the system has entered normal read-write state)
 * set bgw_restart_time=BGW_NEVER_RESTART, indicating not to 
 * restart the process in case of a crash,
 * If it collapsed, it doesn't matter, the launcher will restart it if necessary.
 */
static void registerDoctorAsBgworker(AdbDoctorBgworkerStatus *bgworkerStatus,
									 AdbDoctorBgworkerDataShm *dataShm)
{
	Adb_Doctor_Type type;
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, ADB_DOCTOR_BGW_LIBRARY_NAME);
	type = bgworkerStatus->bgworkerData->type;
	if (type == ADB_DOCTOR_TYPE_NODE_MONITOR)
	{
		sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_NODE_MONITOR);
	}
	else if (type == ADB_DOCTOR_TYPE_HOST_MONITOR)
	{
		sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_HOST_MONITOR);
	}
	else if (type == ADB_DOCTOR_TYPE_SWITCHER)
	{
		sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_SWITCHER);
	}
	else
	{
		ereport(ERROR,
				(errmsg("Unrecognized Adb_Doctor_Type:%d",
						type)));
	}
	snprintf(worker.bgw_type, BGW_MAXLEN, ADB_DOCTOR_BGW_TYPE_WORKER);
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(dataShm->seg));
	worker.bgw_notify_pid = MyProcPid;
	Assert(strlen(bgworkerStatus->bgworkerData->displayName) > 0);
	/* bgw_name will be displayed as process name, 
	 * see "init_ps_display(worker->bgw_name, "", "", "");" in bgwworker.c */
	strncpy(worker.bgw_name, bgworkerStatus->bgworkerData->displayName, BGW_MAXLEN);

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
									 AdbDoctorBgworkerData *dataInShm)
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

static void launchNodeMonitorDoctors(dlist_head *monitorNodes)
{
	dlist_iter iter;
	MgrNodeWrapper *mgrNode;

	dlist_foreach(iter, monitorNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, iter.cur);
		launchNodeMonitorDoctor(mgrNode);
	}
}

static void launchNodeMonitorDoctor(MgrNodeWrapper *mgrNode)
{
	AdbDoctorBgworkerData *bgworkerData;
	AdbDoctorBgworkerStatus *bgworkerStatus;

	bgworkerData = palloc0(sizeof(AdbDoctorBgworkerData));
	bgworkerData->type = ADB_DOCTOR_TYPE_NODE_MONITOR;
	bgworkerData->oid = mgrNode->oid;
	bgworkerData->displayName = getDoctorDisplayName(bgworkerData->type, mgrNode);
	Assert(confShm != NULL);
	bgworkerData->commonShmHandle = dsm_segment_handle(confShm->seg);
	bgworkerData->ready = false;
	bgworkerStatus = launchDoctor(bgworkerData);
	dlist_push_tail(&bgworkerStatusList, &bgworkerStatus->wi_links);
}

static void launchSwitcherDoctor(dlist_head *nodes)
{
	AdbDoctorBgworkerData *bgworkerData;
	AdbDoctorBgworkerStatus *bgworkerStatus;

	if (!dlist_is_empty(nodes))
	{
		bgworkerData = palloc0(sizeof(AdbDoctorBgworkerData));
		bgworkerData->type = ADB_DOCTOR_TYPE_SWITCHER;
		/* because there is only one switcher doctor process */
		bgworkerData->oid = 0;
		bgworkerData->displayName = getDoctorDisplayName(bgworkerData->type, NULL);
		Assert(confShm != NULL);
		bgworkerData->commonShmHandle = dsm_segment_handle(confShm->seg);
		bgworkerData->ready = false;
		bgworkerStatus = launchDoctor(bgworkerData);
		dlist_push_tail(&bgworkerStatusList, &bgworkerStatus->wi_links);
	}
}

static void launchHostMonitorDoctor(dlist_head *hosts)
{
	AdbDoctorBgworkerData *bgworkerData;
	AdbDoctorBgworkerStatus *bgworkerStatus;

	if (!dlist_is_empty(hosts))
	{
		bgworkerData = palloc0(sizeof(AdbDoctorBgworkerData));
		bgworkerData->type = ADB_DOCTOR_TYPE_HOST_MONITOR;
		/* because there is only one host monitor doctor process */
		bgworkerData->oid = 0;
		bgworkerData->displayName = getDoctorDisplayName(bgworkerData->type, NULL);
		Assert(confShm != NULL);
		bgworkerData->commonShmHandle = dsm_segment_handle(confShm->seg);
		bgworkerData->ready = false;
		bgworkerStatus = launchDoctor(bgworkerData);
		dlist_push_tail(&bgworkerStatusList, &bgworkerStatus->wi_links);
	}
}

/**
 * the steps of launch a doctor process include:
 * setup individual bgworker data shm.
 * startup it as Background Worker Processe, wait for it become ready.
 * store doctor process running data into bgworkerStatus then return.
 */
static AdbDoctorBgworkerStatus *launchDoctor(AdbDoctorBgworkerData *bgworkerData)
{
	/* used to store the bgwworker's infomation */
	AdbDoctorBgworkerStatus *bgworkerStatus;
	AdbDoctorBgworkerDataShm *dataShm;

	bgworkerStatus = palloc0(sizeof(AdbDoctorBgworkerStatus));
	bgworkerStatus->bgworkerData = bgworkerData;

	/* Create single data shared memory for each doctor, give data to them */
	dataShm = setupAdbDoctorBgworkerDataShm(bgworkerData);

	/* startup as Background Worker Processe */
	registerDoctorAsBgworker(bgworkerStatus, dataShm);
	/* if launcher exits abnormally, the shm will be detached,
	*  the worker launched should exit too. */
	on_dsm_detach(dataShm->seg, cleanupAdbDoctorBgworker,
				  PointerGetDatum(bgworkerStatus->handle));
	waitForDoctorBecomeReady(bgworkerStatus, dataShm->dataInShm);
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
		dlist_delete(miter.cur);
		terminateDoctor(bgworkerStatus, false);
	}
}

static bool equalsDoctorUniqueId(Adb_Doctor_Type type, Oid oid,
								 AdbDoctorBgworkerData *bgworkerData)
{
	return type == bgworkerData->type && oid == bgworkerData->oid;
}

static void terminateDoctorByUniqueId(Adb_Doctor_Type type,
									  Oid oid)
{
	AdbDoctorBgworkerStatus *bgworkerStatus;
	dlist_mutable_iter miter;

	if (dlist_is_empty(&bgworkerStatusList))
		return;

	dlist_foreach_modify(miter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, miter.cur);
		if (equalsDoctorUniqueId(type, oid, bgworkerStatus->bgworkerData))
		{
			dlist_delete(miter.cur);
			terminateDoctor(bgworkerStatus, true);
		}
	}
}

static void terminateDoctor(AdbDoctorBgworkerStatus *bgworkerStatus,
							bool waitFor)
{
	BgwHandleStatus status;
	dlist_mutable_iter miter;
	MgrNodeWrapper *mgrNode;
	Adb_Doctor_Type type;
	Oid oid;

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
	type = bgworkerStatus->bgworkerData->type;
	oid = bgworkerStatus->bgworkerData->oid;
	pfreeAdbDoctorBgworkerStatus(bgworkerStatus, true);

	/* delete corresponding data */
	if (type == ADB_DOCTOR_TYPE_NODE_MONITOR)
	{
		dlist_foreach_modify(miter, cachedMonitorNodes)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, miter.cur);
			if (mgrNode->oid == oid)
			{
				dlist_delete(miter.cur);
				pfreeMgrNodeWrapper(mgrNode);
			}
		}
	}
	else if (type == ADB_DOCTOR_TYPE_SWITCHER)
	{
		pfreeMgrNodeWrapperList(cachedSwitchNodes, NULL);
		dlist_init(cachedSwitchNodes);
	}
	else if (type == ADB_DOCTOR_TYPE_HOST_MONITOR)
	{
		pfreeMgrHostWrapperList(cachedMonitorHosts, NULL);
		dlist_init(cachedMonitorHosts);
	}
	else
	{
		ereport(ERROR,
				(errmsg("could not recognize type:%d",
						bgworkerStatus->bgworkerData->type)));
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
			dlist_delete(miter.cur);
			/* ensure to terminate it */
			terminateDoctor(bgworkerStatus, false);
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

static void signalDoctorByUniqueId(Adb_Doctor_Type type,
								   Oid oid)
{
	AdbDoctorBgworkerStatus *bgworkerStatus;
	BgwHandleStatus status;

	dlist_iter iter;
	dlist_foreach(iter, &bgworkerStatusList)
	{
		bgworkerStatus = dlist_container(AdbDoctorBgworkerStatus,
										 wi_links, iter.cur);
		if (equalsDoctorUniqueId(type, oid, bgworkerStatus->bgworkerData))
		{
			status = GetBackgroundWorkerPid(bgworkerStatus->handle,
											&bgworkerStatus->pid);
			if (status == BGWH_STARTED && bgworkerStatus->pid > 0)
				kill(bgworkerStatus->pid, SIGUSR1);
		}
	}
}

/*
 * query out all configuration values from table adb_doctor_conf,
 * query out all data from table mgr_node and mgr_host that needs to be monitored.
 * the data in mgr_node and mgr_host is very critical for doctor processes,
 * these data are so called AdbDoctorBgworkerData.
 */
static void queryBgworkerDataAndConf(AdbDoctorConf **confP,
									 dlist_head *monitorNodes,
									 dlist_head *switchNodes,
									 dlist_head *monitorHosts)
{
	MemoryContext oldContext;
	MemoryContext spiContext;
	int ret;

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(ret, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);

	/* query out all configuration values from table adb_doctor_conf */
	*confP = selectAllAdbDoctorConf(spiContext);
	if ((*confP)->enable)
	{
		/* query out all data from table mgr_node that need to be monitored */
		selectMgrNodesForNodeDoctors(spiContext, monitorNodes);
		/* query out all data from table mgr_node that need to be switched */
		selectMgrNodesForSwitcherDoctor(spiContext, switchNodes);
		/* query out all data from table mgr_host that need to be monitored */
		selectMgrHostsForHostDoctor(spiContext, monitorHosts);
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("antdb doctor was disabled")));
	}
	SPI_FINISH_TRANSACTIONAL_COMMIT();
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

static void tryRefreshNodeMonitorDoctors(bool confChanged,
										 dlist_head *staleNodes,
										 dlist_head *freshNodes)
{
	dlist_mutable_iter staleIter;
	dlist_mutable_iter freshIter;
	dlist_mutable_iter miter;
	MgrNodeWrapper *staleNode;
	MgrNodeWrapper *freshNode;
	MgrNodeWrapper *mgrNode;

	dlist_head staleCmpList = DLIST_STATIC_INIT(staleCmpList);
	dlist_head freshCmpList = DLIST_STATIC_INIT(freshCmpList);
	dlist_head addedList = DLIST_STATIC_INIT(addedList);
	dlist_head deletedList = DLIST_STATIC_INIT(deletedList);
	dlist_head changedList = DLIST_STATIC_INIT(changedList);
	dlist_head identicalList = DLIST_STATIC_INIT(identicalList);

	/* Link these data to a new dlist by different dlist_node 
	 * to avoid messing up these data */
	dlist_foreach_modify(miter, staleNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, miter.cur);
		dlist_push_tail(&staleCmpList, &mgrNode->cmpLink);
	}
	dlist_foreach_modify(miter, freshNodes)
	{
		mgrNode = dlist_container(MgrNodeWrapper, link, miter.cur);
		dlist_push_tail(&freshCmpList, &mgrNode->cmpLink);
	}

	dlist_foreach_modify(staleIter, &staleCmpList)
	{
		staleNode = dlist_container(MgrNodeWrapper, cmpLink, staleIter.cur);
		dlist_foreach_modify(freshIter, &freshCmpList)
		{
			freshNode = dlist_container(MgrNodeWrapper, cmpLink, freshIter.cur);
			if (staleNode->oid == freshNode->oid)
			{
				dlist_delete(staleIter.cur);
				dlist_delete(freshIter.cur);
				if (isIdenticalDoctorMgrNode(staleNode, freshNode))
				{
					if (confChanged)
						signalDoctorByUniqueId(ADB_DOCTOR_TYPE_NODE_MONITOR,
											   freshNode->oid);
				}
				else
				{
					signalDoctorByUniqueId(ADB_DOCTOR_TYPE_NODE_MONITOR,
										   freshNode->oid);
				}
				break;
			}
			else
			{
				continue;
			}
		}
	}
	/* The remaining data in staleCmpList exactly is deletedList */
	dlist_foreach_modify(miter, &staleCmpList)
	{
		mgrNode = dlist_container(MgrNodeWrapper, cmpLink, miter.cur);
		/* When terminate a doctor process, the corresponding mgr_node data 
		 * in cachedMonitorNodes(also pointed by iter.cur) will be pfreed. */
		dlist_delete(miter.cur);
		terminateDoctorByUniqueId(ADB_DOCTOR_TYPE_NODE_MONITOR,
								  mgrNode->oid);
	}
	/* The remaining data in freshCmpList exactly is addedList. */
	dlist_foreach_modify(miter, &freshCmpList)
	{
		mgrNode = dlist_container(MgrNodeWrapper, cmpLink, miter.cur);
		launchNodeMonitorDoctor(mgrNode);
	}
}

static void tryRefreshSwitcherDoctor(bool confChanged,
									 dlist_head *staleNodes,
									 dlist_head *freshNodes)
{
	if (isIdenticalDoctorMgrNodes(staleNodes, freshNodes))
	{
		if (confChanged)
			signalDoctorByUniqueId(ADB_DOCTOR_TYPE_SWITCHER,
								   0);
	}
	else
	{
		if (dlist_is_empty(freshNodes))
		{
			terminateDoctorByUniqueId(ADB_DOCTOR_TYPE_SWITCHER,
									  0);
		}
		else
		{
			if (dlist_is_empty(staleNodes))
			{
				launchSwitcherDoctor(freshNodes);
			}
			else
			{
				signalDoctorByUniqueId(ADB_DOCTOR_TYPE_SWITCHER,
									   0);
			}
		}
	}
}

static void tryRefreshHostMonitorDoctor(bool confChanged,
										dlist_head *staleHosts,
										dlist_head *freshHosts)
{
	if (isIdenticalDoctorMgrHosts(staleHosts, freshHosts))
	{
		if (confChanged)
			signalDoctorByUniqueId(ADB_DOCTOR_TYPE_HOST_MONITOR,
								   0);
	}
	else
	{
		if (dlist_is_empty(freshHosts))
		{
			terminateDoctorByUniqueId(ADB_DOCTOR_TYPE_HOST_MONITOR,
									  0);
		}
		else
		{
			if (dlist_is_empty(staleHosts))
			{
				launchHostMonitorDoctor(freshHosts);
			}
			else
			{
				signalDoctorByUniqueId(ADB_DOCTOR_TYPE_HOST_MONITOR,
									   0);
			}
		}
	}
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