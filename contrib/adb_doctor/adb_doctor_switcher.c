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
#include "storage/spin.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "adb_doctor.h"

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static void switcherMainLoop(AdbDoctorSwitcherData *data);

static void attachDataShm(Datum main_arg, AdbDoctorSwitcherData **dataP);

static AdbDoctorConfShm *confShm;
static AdbDoctorConf *confInLocal;

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

void adbDoctorSwitcherMain(Datum main_arg)
{
	AdbDoctorSwitcherData *data;

	//  pg_usleep(20 * 1000000);

	pqsignal(SIGTERM, handleSigterm);
	pqsignal(SIGUSR1, handleSigusr1);

	BackgroundWorkerUnblockSignals();

	attachDataShm(main_arg, &data);

	notifyAdbDoctorRegistrant();

	confShm = attachAdbDoctorConfShm(data->header.commonShmHandle, MyBgworkerEntry->bgw_name);

	confInLocal = copyAdbDoctorConfFromShm(confShm);

	logAdbDoctorSwitcherData(data, psprintf("%s started", MyBgworkerEntry->bgw_name), LOG);

	switcherMainLoop(data);

	pfreeAdbDoctorConfShm(confShm);

	proc_exit(1);
}

static void switcherMainLoop(AdbDoctorSwitcherData *data)
{
	int rc;
	long timeout;
	while (!gotSigterm)
	{
		CHECK_FOR_INTERRUPTS();

		timeout = 1 * 1000L;

		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   PGINVALID_SOCKET,
							   timeout,
							   PG_WAIT_EXTENSION);
		/* Reset the latch, bail out if postmaster died, otherwise loop. */
		ResetLatch(&MyProc->procLatch);
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (gotSigusr1)
		{
			gotSigusr1 = false;
			ereport(LOG,
					(errmsg("%s, Refresh configuration completed",
							MyBgworkerEntry->bgw_name)));
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
/*
 * When we receive a SIGUSR1, we set gotSigusr1 = true
 */
static void handleSigusr1(SIGNAL_ARGS)
{
	int save_errno = errno;

	gotSigusr1 = true;

	procsignal_sigusr1_handler(postgres_signal_arg);

	errno = save_errno;
}

static void attachDataShm(Datum main_arg, AdbDoctorSwitcherData **dataP)
{
	dsm_segment *seg;
	shm_toc *toc;
	AdbDoctorSwitcherData *dataInShm;
	AdbDoctorSwitcherData *data;
	Adb_Doctor_Bgworker_Type type;
	uint64 tocKey = 0;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, MyBgworkerEntry->bgw_name);
	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errmsg("unable to map individual dynamic shared memory segment")));

	toc = shm_toc_attach(ADB_DOCTOR_SHM_DATA_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	dataInShm = shm_toc_lookup(toc, tocKey++, false);

	SpinLockAcquire(&dataInShm->header.mutex);

	type = dataInShm->header.type;
	Assert(type == ADB_DOCTOR_BGWORKER_TYPE_SWITCHER);

	data = palloc0(sizeof(AdbDoctorSwitcherData));
	/* this shm will be detached, copy out all the data */
	memcpy(data, dataInShm, sizeof(AdbDoctorSwitcherData));

	data->wrapper = palloc0(sizeof(AdbMgrNodeWrapper));
	memcpy(data->wrapper, shm_toc_lookup(toc, tocKey++, false), sizeof(AdbMgrNodeWrapper));

	data->wrapper->nodepath = pstrdup(shm_toc_lookup(toc, tocKey++, false));
	data->wrapper->hostaddr = pstrdup(shm_toc_lookup(toc, tocKey++, false));

	/* if true, launcher know this worker is ready, and then detach this shm */
	dataInShm->header.ready = true;
	SpinLockRelease(&dataInShm->header.mutex);

	*dataP = data;

	dsm_detach(seg);
}
