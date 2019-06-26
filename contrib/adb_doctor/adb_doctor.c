/*--------------------------------------------------------------------------
 * user interface of control adb_doctor, support function below:
 * select adb_doctor_start,
 * select adb_doctor_stop,
 * select adb_doctor_param.
 * adb_doctor.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
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
#include "adb_doctor_sql.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(adb_doctor_start);
PG_FUNCTION_INFO_V1(adb_doctor_stop);
PG_FUNCTION_INFO_V1(adb_doctor_param);

void _PG_init(void);

static shm_mq *setupShmMQ(dsm_segment **segp);
static BackgroundWorkerHandle *startupLauncher(dsm_segment *seg);
static bool isLauncherOK(Size len, char *message);
static void waitForLauncherOK(BackgroundWorkerHandle *launcherHandle, shm_mq_handle *inqh);

Datum
	adb_doctor_start(PG_FUNCTION_ARGS)
{
	dsm_segment *seg;
	shm_mq_handle *inqh;
	BackgroundWorkerHandle *launcherHandle;
	bool masterMode;

	masterMode = !RecoveryInProgress();

	adbDoctorStopLauncher(false);

	shm_mq *mq = setupShmMQ(&seg);
	launcherHandle = startupLauncher(seg);

	on_dsm_detach(seg, cleanupAdbDoctorBgworker, PointerGetDatum(launcherHandle));

	if (masterMode)
	{
		inqh = shm_mq_attach(mq, seg, launcherHandle);
		waitForLauncherOK(launcherHandle, inqh);
	}

	cancel_on_dsm_detach(seg, cleanupAdbDoctorBgworker, PointerGetDatum(launcherHandle));

	dsm_detach(seg);
	PG_RETURN_VOID();
}

Datum
	adb_doctor_stop(PG_FUNCTION_ARGS)
{
	adbDoctorStopLauncher(false);
	adbDoctorStopBgworkers(false);
	PG_RETURN_VOID();
}

Datum
	adb_doctor_param(PG_FUNCTION_ARGS)
{
	int32 datalevel = PG_GETARG_INT32(0);
	int32 probeinterval = PG_GETARG_INT32(1);
	int ret;
	char datalevelStr[12];
	char probeintervalStr[12];

	/* if value < 0, means ignore it. */
	if (datalevel >= 0)
	{
		if (!isValidAdbDoctorConf_datalevel(datalevel))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("datalevel must between %d and %d",
							NO_DATA_LOST_BUT_MAY_SLOW,
							MAY_LOST_DATA_BUT_QUICK)));
	}

	/* if value < 0, means ignore it. */
	if (probeinterval >= 0)
	{
		if (!isValidAdbDoctorConf_probeinterval(probeinterval))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("probeinterval must between %d and %d",
							ADB_DOCTOR_CONF_PROBEINTERVAL_MIN,
							ADB_DOCTOR_CONF_PROBEINTERVAL_MAX)));
	}

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						(errmsg("SPI_connect failed, connect return:%d", ret))));
	}
	pg_ltoa(datalevel, datalevelStr);
	if (datalevel >= 0)
		SPI_updateConfParam(ADB_DOCTOR_CONF_KEY_DATALEVEL, datalevelStr);

	pg_ltoa(probeinterval, probeintervalStr);
	if (probeinterval >= 0)
		SPI_updateConfParam(ADB_DOCTOR_CONF_KEY_PROBEINTERVAL, probeintervalStr);

	SPI_finish();

	adbDoctorSignalLauncher();

	PG_RETURN_VOID();
}

static shm_mq *
setupShmMQ(dsm_segment **segp)
{
	Size queue_size = 96;
	shm_toc_estimator e;
	dsm_segment *seg;
	Size segsize;
	shm_toc *toc;
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, queue_size);
	shm_toc_estimate_keys(&e, 1);
	segsize = shm_toc_estimate(&e);

	seg = dsm_create(segsize, 0);
	*segp = seg;
	toc = shm_toc_create(ADB_DOCTOR_LAUNCHER_MAGIC, dsm_segment_address(seg),
						 segsize);

	shm_mq *mq = shm_mq_create(shm_toc_allocate(toc, (Size)queue_size), (Size)queue_size);
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
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register background process"),
				 errhint("You may need to increase max_worker_processes.")));
	ereport(LOG,
			(errmsg("register adb doctor launcher success")));

	return handle;
}

static bool isLauncherOK(Size len, char *message)
{
	Size expectedLen = strlen(ADB_DOCTORS_LAUNCH_OK);
	if (len != expectedLen)
		return false;
	if (strcmp(message, ADB_DOCTORS_LAUNCH_OK) != 0)
		return false;
	return true;
}

static void waitForLauncherOK(BackgroundWorkerHandle *launcherHandle, shm_mq_handle *inqh)
{
	BgwHandleStatus status;
	pid_t pid;

	/* wait for postmaster startup the worker, and then we setup next worker */
	status = WaitForBackgroundWorkerStartup(launcherHandle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));

	shm_mq_result res;
	Size len;
	void *message;

	while (true)
	{
		ProcessInterrupts();
		res = shm_mq_receive(inqh, &len, &message, false);
		if (res == SHM_MQ_SUCCESS)
		{
			if (!isLauncherOK(len, message))
			{
				ereport(ERROR,
						(errmsg("launch doctor worker failed"),
						 errdetail("please view server log for detail")));
			}
			break;
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
	ReportByBgwType(ADB_DOCTOR_BGW_LIBRARY_NAME, ADB_DOCTOR_BGW_TYPE_LAUNCHER);
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