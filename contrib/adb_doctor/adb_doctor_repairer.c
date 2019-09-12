/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "executor/spi.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"
#include "../../src/interfaces/libpq/libpq-fe.h"
#include "../../src/interfaces/libpq/libpq-int.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_helper.h"
#include "mgr/mgr_switcher.h"
#include "adb_doctor.h"

typedef struct RepairerConfiguration
{
	long repairIntervalMs;
} RepairerConfiguration;

static void repairerMainLoop(dlist_head *mgrNodes);
static bool checkAndRepairNode(MgrNodeWrapper *mgrNode);
static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext);
static void getCheckMgrNodesForRepairer(dlist_head *mgrNodes);
static RepairerConfiguration *newRepairerConfiguration(AdbDoctorConf *conf);
static void examineAdbDoctorConf(dlist_head *mgrNodes);
static void resetRepairer(void);

static void handleSigterm(SIGNAL_ARGS);
static void handleSigusr1(SIGNAL_ARGS);

static AdbDoctorConfShm *confShm;
static RepairerConfiguration *repairerConfiguration;
static sigjmp_buf reset_repairer_sigjmp_buf;

static volatile sig_atomic_t gotSigterm = false;
static volatile sig_atomic_t gotSigusr1 = false;

void adbDoctorRepairerMain(Datum main_arg)
{
	AdbDoctorBgworkerData *bgworkerData;
	AdbDoctorConf *confInLocal;
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);

	pqsignal(SIGTERM, handleSigterm);
	pqsignal(SIGUSR1, handleSigusr1);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(DEFAULT_DB, NULL, 0);

	PG_TRY();
	{
		bgworkerData = attachAdbDoctorBgworkerDataShm(main_arg,
													  MyBgworkerEntry->bgw_name);
		notifyAdbDoctorRegistrant();
		ereport(LOG,
				(errmsg("%s started",
						MyBgworkerEntry->bgw_name)));

		confShm = attachAdbDoctorConfShm(bgworkerData->commonShmHandle,
										 MyBgworkerEntry->bgw_name);
		confInLocal = copyAdbDoctorConfFromShm(confShm);
		repairerConfiguration = newRepairerConfiguration(confInLocal);
		pfree(confInLocal);

		if (sigsetjmp(reset_repairer_sigjmp_buf, 1) != 0)
		{
			pfreeMgrNodeWrapperList(&mgrNodes, NULL);
		}
		dlist_init(&mgrNodes);

		getCheckMgrNodesForRepairer(&mgrNodes);
		repairerMainLoop(&mgrNodes);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();
	proc_exit(1);
}

static void repairerMainLoop(dlist_head *mgrNodes)
{
	int rc;
	dlist_mutable_iter miter;
	MgrNodeWrapper *mgrNode;

	while (!gotSigterm)
	{
		dlist_foreach_modify(miter, mgrNodes)
		{
			mgrNode = dlist_container(MgrNodeWrapper, link, miter.cur);
			/* do switch */
			if (checkAndRepairNode(mgrNode))
			{
				dlist_delete(miter.cur);
				pfreeMgrNodeWrapper(mgrNode);
			}
			CHECK_FOR_INTERRUPTS();
			examineAdbDoctorConf(mgrNodes);
		}
		if (dlist_is_empty(mgrNodes))
		{
			/* The switch task was completed, the process should exits */
			break;
		}
		set_ps_display("sleeping", false);
		rc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   PGINVALID_SOCKET,
							   repairerConfiguration->repairIntervalMs,
							   PG_WAIT_EXTENSION);
		/* Reset the latch, bail out if postmaster died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
		/* Interrupted? */
		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
		}
		CHECK_FOR_INTERRUPTS();
		examineAdbDoctorConf(mgrNodes);
	}
}

static bool checkAndRepairNode(MgrNodeWrapper *mgrNode)
{
	volatile bool done = false;
	int spiRes;
	MemoryContext oldContext;
	MemoryContext switchContext;
	MemoryContext spiContext;
	MgrNodeWrapper mgrNodeBackup;

	set_ps_display(NameStr(mgrNode->form.nodename), false);
	memcpy(&mgrNodeBackup, mgrNode, sizeof(MgrNodeWrapper));

	oldContext = CurrentMemoryContext;
	switchContext = AllocSetContextCreate(oldContext,
										  "checkAndRepairNode",
										  ALLOCSET_DEFAULT_SIZES);
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(switchContext);

	PG_TRY();
	{
		checkMgrNodeDataInDB(mgrNode, spiContext);
		pg_usleep(30L * 1000000L);
		done = true;
		ereport(LOG,
				(errmsg("%s repair completed",
						MyBgworkerEntry->bgw_name)));
	}
	PG_CATCH();
	{
		done = false;
		EmitErrorReport();
		FlushErrorState();
		/* do not throw this exception */
	}
	PG_END_TRY();

	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(switchContext);

	if (done)
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
	else
	{
		memcpy(mgrNode, &mgrNodeBackup, sizeof(MgrNodeWrapper));
		SPI_FINISH_TRANSACTIONAL_ABORT();
	}

	return done;
}

static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext)
{
	MgrNodeWrapper *nodeDataInDB;

	nodeDataInDB = selectMgrNodeByOid(nodeDataInMem->oid, spiContext);
	if (!nodeDataInDB)
	{
		ereport(ERROR,
				(errmsg("%s %s, data not exists in database",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	if (!nodeDataInDB->form.allowcure)
	{
		ereport(ERROR,
				(errmsg("%s %s, cure not allowed",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	if (nodeDataInDB->form.nodetype != CNDN_TYPE_COORDINATOR_MASTER)
	{
		ereport(ERROR,
				(errmsg("only coordinator repairing is supported")));
	}
	if (pg_strcasecmp(NameStr(nodeDataInDB->form.curestatus),
					  CURE_STATUS_ISOLATED) != 0)
	{
		ereport(ERROR,
				(errmsg("%s %s, curestatus:%s, it is not my duty",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename),
						NameStr(nodeDataInDB->form.curestatus))));
	}
	if (pg_strcasecmp(NameStr(nodeDataInMem->form.curestatus),
					  NameStr(nodeDataInDB->form.curestatus)) != 0)
	{
		ereport(ERROR,
				(errmsg("%s %s, curestatus not matched, in memory:%s, but in database:%s",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename),
						NameStr(nodeDataInMem->form.curestatus),
						NameStr(nodeDataInDB->form.curestatus))));
	}
	if (!isIdenticalDoctorMgrNode(nodeDataInMem, nodeDataInDB))
	{
		ereport(ERROR,
				(errmsg("%s %s, data has changed in database",
						MyBgworkerEntry->bgw_name,
						NameStr(nodeDataInDB->form.nodename))));
	}
	pfreeMgrNodeWrapper(nodeDataInDB);
	nodeDataInDB = NULL;
}

static void getCheckMgrNodesForRepairer(dlist_head *mgrNodes)
{
	MemoryContext oldContext;
	MemoryContext spiContext;
	int ret;

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(ret, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);
	selectMgrNodesForRepairerDoctor(spiContext, mgrNodes);
	SPI_FINISH_TRANSACTIONAL_COMMIT();
	if (dlist_is_empty(mgrNodes))
	{
		ereport(ERROR,
				(errmsg("%s There is no node to repair",
						MyBgworkerEntry->bgw_name)));
	}
}

static RepairerConfiguration *newRepairerConfiguration(AdbDoctorConf *conf)
{
	RepairerConfiguration *rc;

	checkAdbDoctorConf(conf);

	rc = palloc0(sizeof(RepairerConfiguration));

	rc->repairIntervalMs = conf->retry_repair_interval_ms;
	ereport(DEBUG1,
			(errmsg("%s configuration: "
					"repairIntervalMs:%ld",
					MyBgworkerEntry->bgw_name,
					rc->repairIntervalMs)));
	return rc;
}

static void examineAdbDoctorConf(dlist_head *mgrNodes)
{
	AdbDoctorConf *confInLocal;
	dlist_head freshMgrNodes = DLIST_STATIC_INIT(freshMgrNodes);
	dlist_head staleMgrNodes = DLIST_STATIC_INIT(staleMgrNodes);
	if (gotSigusr1)
	{
		gotSigusr1 = false;

		confInLocal = copyAdbDoctorConfFromShm(confShm);
		pfree(repairerConfiguration);
		repairerConfiguration = newRepairerConfiguration(confInLocal);
		pfree(confInLocal);

		ereport(LOG,
				(errmsg("%s, Refresh configuration completed",
						MyBgworkerEntry->bgw_name)));

		getCheckMgrNodesForRepairer(&freshMgrNodes);
		if (isIdenticalDoctorMgrNodes(&freshMgrNodes, &staleMgrNodes))
		{
			pfreeMgrNodeWrapperList(&freshMgrNodes, NULL);
		}
		else
		{
			pfreeMgrNodeWrapperList(&freshMgrNodes, NULL);
			resetRepairer();
		}
	}
}

static void resetRepairer()
{
	ereport(LOG,
			(errmsg("%s, reset repairer",
					MyBgworkerEntry->bgw_name)));
	siglongjmp(reset_repairer_sigjmp_buf, 1);
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