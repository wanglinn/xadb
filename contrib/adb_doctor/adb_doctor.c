/*--------------------------------------------------------------------------
 * 
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 * 
 * user interface of control adb_doctor, support function below:
 * select adb_doctor_start,
 * select adb_doctor_stop,
 * select adb_doctor_param.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "postmaster/bgworker.h"
#include "adb_doctor.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(adb_doctor_start);
PG_FUNCTION_INFO_V1(adb_doctor_stop);
PG_FUNCTION_INFO_V1(adb_doctor_param);
PG_FUNCTION_INFO_V1(adb_doctor_list);

void _PG_init(void);

static shm_mq *setupShmMQ(dsm_segment **segp);
static BackgroundWorkerHandle *startupLauncher(dsm_segment *seg);
static bool isLauncherOK(Size len, char *message);
static bool waitForLauncherOK(BackgroundWorkerHandle *launcherHandle, shm_mq_handle *inqh);

/*
 * Background workers can be initialized at the time that PostgreSQL is started 
 * by including the module name in shared_preload_libraries. A module wishing 
 * to run a background worker can register it by calling 
 * RegisterBackgroundWorker(BackgroundWorker *worker) from its _PG_init(). 
 * Background workers can also be started after the system is up and running by 
 * calling the function 
 * RegisterDynamicBackgroundWorker(BackgroundWorker *worker, BackgroundWorkerHandle **handle). 
 * Unlike RegisterBackgroundWorker, which can only be called from within the 
 * postmaster, RegisterDynamicBackgroundWorker must be called from a regular 
 * backend or another background worker.
 */
void _PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(LOG,
				(errmsg("start doctor failed, please check shared_preload_libraries")));
		return;
	}

	/* set up common data for all our workers */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 30;
	sprintf(worker.bgw_library_name, ADB_DOCTOR_BGW_LIBRARY_NAME);
	sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_LAUNCHER);
	snprintf(worker.bgw_name, BGW_MAXLEN, ADB_DOCTOR_BGW_TYPE_LAUNCHER);
	snprintf(worker.bgw_type, BGW_MAXLEN, ADB_DOCTOR_BGW_TYPE_LAUNCHER);
	worker.bgw_main_arg = UInt32GetDatum(0);
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}

/**
 * Start all doctor process.
 * Register an "adb doctor launcher" process with "postmaster", which runs as a 
 * background worker. The "adb doctor launcher" will start the doctor process 
 * according to the table MGR_NODE, MGR_HOST in the MGR database.
 */
Datum adb_doctor_start(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;
	shm_mq *mq;
	shm_mq_handle *inqh;
	BackgroundWorkerHandle *launcherHandle;
	bool masterMode;
	bool launcherOk;

	masterMode = !RecoveryInProgress();

	adbDoctorStopLauncher(false);

	mq = setupShmMQ(&seg);
	launcherHandle = startupLauncher(seg);

	on_dsm_detach(seg, cleanupAdbDoctorBgworker, PointerGetDatum(launcherHandle));

	if (masterMode)
	{
		inqh = shm_mq_attach(mq, seg, launcherHandle);
		launcherOk = waitForLauncherOK(launcherHandle, inqh);
	}
	else
	{
		launcherOk = true;
	}

	if (launcherOk)
	{
		cancel_on_dsm_detach(seg, cleanupAdbDoctorBgworker, PointerGetDatum(launcherHandle));
		dsm_detach(seg);
		PG_RETURN_BOOL(true);
	}
	else
	{
		dsm_detach(seg);
		ereport(ERROR,
				(errmsg("launch doctor worker failed"),
				 errdetail("please view server log for detail")));
		PG_RETURN_BOOL(false);
	}
}

/**
 * Stop all doctor processes.
 */
Datum adb_doctor_stop(PG_FUNCTION_ARGS)
{
	adbDoctorStopLauncher(false);
	adbDoctorStopBgworkers(false);
	PG_RETURN_BOOL(true);
}

/**
 * Set configuration variables stored in table adb_doctor_conf.
 * Use like this: select adb_doctor.adb_doctor_param('name', 'value');
 */
Datum adb_doctor_param(PG_FUNCTION_ARGS)
{
	text *k_txt = PG_GETARG_TEXT_PP(0);
	text *v_txt = PG_GETARG_TEXT_PP(1);
	char *k;
	char *v;
	int ret;

	k = text_to_cstring(k_txt);
	v = text_to_cstring(v_txt);

	validateAdbDoctorConfEditableEntry(k, v);

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		ereport(ERROR,
				(errmsg("SPI_connect failed, connect return:%d",
						ret)));
	}
	updateAdbDoctorConf(k, v);
	SPI_finish();

	adbDoctorSignalLauncher();

	pfree(k);
	pfree(v);
	PG_RETURN_BOOL(true);
}

/**
 * List editable configuration variables stored in table adb_doctor_conf.
 */
Datum adb_doctor_list(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	Datum datums[3];
	bool nulls[3];
	int rows, ret;
	AdbDoctorConfRow *rowData;
	FuncCallContext *funcctx;
	MemoryContext multi_call_memory_ctx;
	MemoryContext oldcontext;
	MemoryContext spiContext;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		multi_call_memory_ctx = funcctx->multi_call_memory_ctx;
		oldcontext = MemoryContextSwitchTo(multi_call_memory_ctx);

		ret = SPI_connect();
		if (ret != SPI_OK_CONNECT)
		{
			ereport(ERROR,
					(errmsg("SPI_connect failed, connect return:%d",
							ret)));
		}
		spiContext = CurrentMemoryContext;
		MemoryContextSwitchTo(multi_call_memory_ctx);
		rows = selectEditableAdbDoctorConf(spiContext, &rowData);
		SPI_finish();

		funcctx->max_calls = rows;
		funcctx->user_fctx = rowData;
		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber)1,
						   ADB_DOCTOR_CONF_ATTR_KEY,
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber)2,
						   ADB_DOCTOR_CONF_ATTR_VALUE,
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber)3,
						   ADB_DOCTOR_CONF_ATTR_COMMENT,
						   TEXTOID, -1, 0);
		tupdesc = BlessTupleDesc(tupdesc);
		funcctx->tuple_desc = tupdesc;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	rowData = funcctx->user_fctx;
	tupdesc = funcctx->tuple_desc;
	if (funcctx->call_cntr < funcctx->max_calls)
	{
		nulls[0] = rowData[funcctx->call_cntr].k == NULL;
		if (!nulls[0])
			datums[0] = CStringGetTextDatum(rowData[funcctx->call_cntr].k);
		nulls[1] = rowData[funcctx->call_cntr].v == NULL;
		if (!nulls[1])
			datums[1] = CStringGetTextDatum(rowData[funcctx->call_cntr].v);
		nulls[2] = rowData[funcctx->call_cntr].comment == NULL;
		if (!nulls[2])
			datums[2] = CStringGetTextDatum(rowData[funcctx->call_cntr].comment);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(heap_form_tuple(tupdesc, datums, nulls)));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

static shm_mq *
setupShmMQ(dsm_segment **segp)
{
	Size queue_size = 96;
	shm_toc_estimator e;
	dsm_segment *seg;
	Size segsize;
	shm_toc *toc;
	shm_mq *mq;

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, queue_size);
	shm_toc_estimate_keys(&e, 1);
	segsize = shm_toc_estimate(&e);

	seg = dsm_create(segsize, 0);
	*segp = seg;
	toc = shm_toc_create(ADB_DOCTOR_LAUNCHER_MAGIC, dsm_segment_address(seg),
						 segsize);

	mq = shm_mq_create(shm_toc_allocate(toc, (Size)queue_size), (Size)queue_size);
	shm_toc_insert(toc, 0, mq);
	shm_mq_set_receiver(mq, MyProc);
	return mq;
}

static BackgroundWorkerHandle *startupLauncher(dsm_segment *seg)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	/* Configure a worker. */
	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 30;
	sprintf(worker.bgw_library_name, ADB_DOCTOR_BGW_LIBRARY_NAME);
	sprintf(worker.bgw_function_name, ADB_DOCTOR_FUNCTION_NAME_LAUNCHER);
	snprintf(worker.bgw_name, BGW_MAXLEN, ADB_DOCTOR_BGW_TYPE_LAUNCHER);
	snprintf(worker.bgw_type, BGW_MAXLEN, ADB_DOCTOR_BGW_TYPE_LAUNCHER);
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));
	}
	ereport(LOG,
			(errmsg("register adb doctor launcher success")));

	return handle;
}

static bool isLauncherOK(Size len, char *message)
{
	Size expectedLen;

	expectedLen = strlen(ADB_DOCTORS_LAUNCH_OK);
	if (len != expectedLen)
		return false;
	if (strcmp(message, ADB_DOCTORS_LAUNCH_OK) != 0)
		return false;
	return true;
}

static bool waitForLauncherOK(BackgroundWorkerHandle *launcherHandle, shm_mq_handle *inqh)
{
	BgwHandleStatus status;
	pid_t pid;
	shm_mq_result res;
	Size len;
	void *message;

	/* wait for postmaster startup the worker, and then we setup next worker */
	status = WaitForBackgroundWorkerStartup(launcherHandle, &pid);
	if (status != BGWH_STARTED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	}

	while (true)
	{
		ProcessInterrupts();
		res = shm_mq_receive(inqh, &len, &message, false);
		if (res == SHM_MQ_SUCCESS)
		{
			return isLauncherOK(len, message);
		}
		else
		{
			pg_usleep(100000L);
		}
	}
}

void adbDoctorStopLauncher(bool waitForStopped)
{
	TerminateBackgroundWorkerByBgwType(ADB_DOCTOR_BGW_LIBRARY_NAME, ADB_DOCTOR_BGW_TYPE_LAUNCHER, waitForStopped);
}

void adbDoctorStopBgworkers(bool waitForStopped)
{
	TerminateBackgroundWorkerByBgwType(ADB_DOCTOR_BGW_LIBRARY_NAME, ADB_DOCTOR_BGW_TYPE_WORKER, waitForStopped);
}

void adbDoctorSignalLauncher(void)
{
	ReportToBackgroundWorkerByBgwType(ADB_DOCTOR_BGW_LIBRARY_NAME, ADB_DOCTOR_BGW_TYPE_LAUNCHER);
}

void cleanupAdbDoctorBgworker(dsm_segment *seg, Datum arg)
{
	BackgroundWorkerHandle *handle = (BackgroundWorkerHandle *)DatumGetPointer(arg);
	if (handle != NULL)
	{
		TerminateBackgroundWorker(handle);
	}
}

void notifyAdbDoctorRegistrant(void)
{
	PGPROC *registrant = BackendPidGetProc(MyBgworkerEntry->bgw_notify_pid);
	if (registrant == NULL)
	{
		ereport(ERROR,
				(errmsg("registrant backend has exited prematurely")));
		proc_exit(0);
	}
	SetLatch(&registrant->procLatch);
}

void usleepIgnoreSignal(long microsec)
{
	TimestampTz current;
	TimestampTz latest;
	current = GetCurrentTimestamp();
	while (microsec > 0)
	{
		latest = current;
		pg_usleep(microsec);
		current = GetCurrentTimestamp();
		microsec -= (current - latest);
	}
}