/*--------------------------------------------------------------------------
 *
 * Copyright (c) 2018-2019, Asiainfo Database Innovation Lab
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/spin.h"
#include "executor/spi.h"
#include "parser/parser.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
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
static void checkAndRepairNodes(dlist_head *faultNodes);
static bool cleanPgxcNodeOnCoordinators(dlist_head *activeCoordinators,
										MgrNodeWrapper *faultNode);
static bool cleanAndAppendNode(dlist_head *activeCoordinators,
							   MgrNodeWrapper *faultNode);
static void callAppendCoordinatorFor(MgrNodeWrapper *destCoordinator,
									 MgrNodeWrapper *srcCoordinator);
static void callAppendActivateCoordinator(MgrNodeWrapper *destCoordinator);
static MgrNodeWrapper *getSrcCoordForAppend(dlist_head *coordinators);
static void checkMgrNodeDataInDB(MgrNodeWrapper *nodeDataInMem,
								 MemoryContext spiContext);
static void getCheckMgrNodesForRepairer(dlist_head *mgrNodes);
static void refreshMgrNodeBeforeRepair(MgrNodeWrapper *mgrNode,
									   MemoryContext spiContext);
static void refreshMgrNodeAfterRepair(MgrNodeWrapper *mgrNode,
									  MemoryContext spiContext);
static void selectActiveCoordinators(dlist_head *resultList);
static void checkGetActiveCoordinators(dlist_head *coordinators);
static void dropFaultNodeFromActiveCoordinators(dlist_head *activeCoordinators,
												MgrNodeWrapper *faultNode,
												bool complain);
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

	while (!gotSigterm)
	{
		checkAndRepairNodes(mgrNodes);

		if (dlist_is_empty(mgrNodes))
		{
			/* The switch task was completed, the process should exits */
			break;
		}
		CHECK_FOR_INTERRUPTS();
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

static void checkAndRepairNodes(dlist_head *faultNodes)
{
	dlist_mutable_iter faultNodeIter;
	MgrNodeWrapper *faultNode;
	MemoryContext oldContext;
	MemoryContext workerContext;
	dlist_head activeCoordinators = DLIST_STATIC_INIT(activeCoordinators);

	oldContext = CurrentMemoryContext;
	workerContext = AllocSetContextCreate(oldContext,
										  "workerContext",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(workerContext);

	PG_TRY();
	{
		checkGetActiveCoordinators(&activeCoordinators);

		dlist_foreach_modify(faultNodeIter, faultNodes)
		{
			faultNode = dlist_container(MgrNodeWrapper, link, faultNodeIter.cur);
			if (!cleanPgxcNodeOnCoordinators(&activeCoordinators, faultNode))
			{
				ereport(ERROR,
						(errmsg("clean %s in table pgxc_node of all cooridnators failed",
								NameStr(faultNode->form.nodename))));
			}
		}

		dlist_foreach_modify(faultNodeIter, faultNodes)
		{
			faultNode = dlist_container(MgrNodeWrapper, link, faultNodeIter.cur);
			if (cleanAndAppendNode(&activeCoordinators, faultNode))
			{
				dlist_delete(faultNodeIter.cur);
				pfreeMgrNodeWrapper(faultNode);
				ereport(LOG, (errmsg("%s has been successfully repaired",
									  NameStr(faultNode->form.nodename))));
			}
		}
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	pfreeSwitcherNodeWrapperList(&activeCoordinators, NULL);
	(void)MemoryContextSwitchTo(oldContext);
	MemoryContextDelete(workerContext);
}

static bool cleanPgxcNodeOnCoordinators(dlist_head *activeCoordinators,
										MgrNodeWrapper *faultNode)
{
	int spiRes;
	int rows;
	volatile bool done;
	MemoryContext oldContext;
	MemoryContext spiContext;
	MgrNodeWrapper mgrNodeBackup;

	memcpy(&mgrNodeBackup, faultNode, sizeof(MgrNodeWrapper));

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);

	PG_TRY();
	{
		checkMgrNodeDataInDB(faultNode, spiContext);

		rows = updateMgrNodeCurestatus(faultNode, CURE_STATUS_CURING, spiContext);
		if (rows != 1)
		{
			ereport(ERROR,
					(errmsg("%s, can not transit to curestatus:%s",
							NameStr(faultNode->form.nodename),
							CURE_STATUS_CURING)));
		}
		else
		{
			namestrcpy(&faultNode->form.curestatus, CURE_STATUS_CURING);
		}

		dropFaultNodeFromActiveCoordinators(activeCoordinators,
											faultNode,
											true);

		rows = updateMgrNodeCurestatus(faultNode,
									   NameStr(mgrNodeBackup.form.curestatus),
									   spiContext);
		if (rows != 1)
		{
			ereport(ERROR,
					(errmsg("%s, can not transit to curestatus:%s",
							NameStr(faultNode->form.nodename),
							NameStr(mgrNodeBackup.form.curestatus))));
		}
		else
		{
			namestrcpy(&faultNode->form.curestatus, NameStr(mgrNodeBackup.form.curestatus));
		}
		done = true;
	}
	PG_CATCH();
	{
		done = false;
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	if (done)
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
	else
	{
		memcpy(faultNode, &mgrNodeBackup, sizeof(MgrNodeWrapper));
		SPI_FINISH_TRANSACTIONAL_ABORT();
	}
	return done;
}

static bool cleanAndAppendNode(dlist_head *activeCoordinators,
							   MgrNodeWrapper *faultNode)
{
	int spiRes;
	volatile bool done = false;
	MemoryContext oldContext;
	MemoryContext spiContext;
	GetAgentCmdRst getAgentCmdRst;
	MgrNodeWrapper mgrNodeBackup;
	MgrNodeWrapper *srcCoordinator;

	memcpy(&mgrNodeBackup, faultNode, sizeof(MgrNodeWrapper));

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);

	PG_TRY();
	{
		set_ps_display(NameStr(faultNode->form.nodename), false);

		refreshMgrNodeBeforeRepair(faultNode, spiContext);

		/* shutdown fault node */
		shutdownNodeWithinSeconds(faultNode,
								  SHUTDOWN_NODE_FAST_SECONDS,
								  SHUTDOWN_NODE_IMMEDIATE_SECONDS,
								  true);
		/* clean fault node */
		mgr_clean_node_folder(AGT_CMD_CLEAN_NODE,
							  faultNode->form.nodehost,
							  faultNode->nodepath,
							  &getAgentCmdRst);
		if (true == getAgentCmdRst.ret)
		{
			ereport(LOG,
					(errmsg("try clean node %s successed",
							NameStr(faultNode->form.nodename))));
		}
		else
		{
			ereport(ERROR,
					(errmsg("try clean node %s failed, %s",
							NameStr(faultNode->form.nodename),
							getAgentCmdRst.description.data)));
		}
		pfree(getAgentCmdRst.description.data);

		if (faultNode->form.nodetype != CNDN_TYPE_COORDINATOR_MASTER)
		{
			ereport(ERROR,
					(errmsg("only coordinator repairing is supported")));
		}

		srcCoordinator = getSrcCoordForAppend(activeCoordinators);
		callAppendCoordinatorFor(srcCoordinator, faultNode);
		callAppendActivateCoordinator(faultNode);

		refreshMgrNodeAfterRepair(faultNode, spiContext);
		done = true;
	}
	PG_CATCH();
	{
		done = false;
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	if (done)
	{
		SPI_FINISH_TRANSACTIONAL_COMMIT();
	}
	else
	{
		memcpy(faultNode, &mgrNodeBackup, sizeof(MgrNodeWrapper));
		SPI_FINISH_TRANSACTIONAL_ABORT();
	}
	return done;
}

static void callAppendCoordinatorFor(MgrNodeWrapper *destCoordinator,
									 MgrNodeWrapper *srcCoordinator)
{
	HeapTupleHeader tupleHeader;
	HeapTupleData tuple;
	TupleDesc tupdesc = NULL;
	Datum datum;
	bool isnull;

	tupleHeader =
		DatumGetHeapTupleHeader(
			DirectFunctionCall2(mgr_append_coord_to_coord,
								CStringGetDatum(NameStr(destCoordinator->form.nodename)),
								CStringGetDatum(NameStr(srcCoordinator->form.nodename))));
	tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(tupleHeader),
										  HeapTupleHeaderGetTypMod(tupleHeader));
	tuple.t_len = HeapTupleHeaderGetDatumLength(tupleHeader);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tupleHeader;
	datum = fastgetattr(&tuple, 2, tupdesc, &isnull);
	if (isnull)
	{
		ereport(ERROR,
				(errmsg("try append %s for %s failed, null return",
						NameStr(destCoordinator->form.nodename),
						NameStr(srcCoordinator->form.nodename))));
	}
	else
	{
		if (DatumGetBool(datum))
		{
			ereport(LOG,
					(errmsg("try append %s for %s successed",
							NameStr(destCoordinator->form.nodename),
							NameStr(srcCoordinator->form.nodename))));
		}
		else
		{
			datum = fastgetattr(&tuple, 3, tupdesc, &isnull);
			ereport(ERROR,
					(errmsg("try append %s for %s failed, %s",
							NameStr(destCoordinator->form.nodename),
							NameStr(srcCoordinator->form.nodename),
							isnull ? "unknow reason" : DatumGetCString(datum))));
		}
	}
}

static void callAppendActivateCoordinator(MgrNodeWrapper *destCoordinator)
{
	HeapTupleHeader tupleHeader;
	HeapTupleData tuple;
	TupleDesc tupdesc = NULL;
	Datum datum;
	bool isnull;

	tupleHeader =
		DatumGetHeapTupleHeader(
			DirectFunctionCall1(mgr_append_activate_coord,
								CStringGetDatum(NameStr(destCoordinator->form.nodename))));
	tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(tupleHeader),
										  HeapTupleHeaderGetTypMod(tupleHeader));
	tuple.t_len = HeapTupleHeaderGetDatumLength(tupleHeader);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tupleHeader;
	datum = fastgetattr(&tuple, 2, tupdesc, &isnull);
	if (isnull)
	{
		ereport(ERROR,
				(errmsg("try append activate %s failed, null return",
						NameStr(destCoordinator->form.nodename))));
	}
	else
	{
		if (DatumGetBool(datum))
		{
			ereport(LOG,
					(errmsg("try append activate %s successed",
							NameStr(destCoordinator->form.nodename))));
		}
		else
		{
			datum = fastgetattr(&tuple, 3, tupdesc, &isnull);
			ereport(ERROR,
					(errmsg("try append activate %s failed, %s",
							NameStr(destCoordinator->form.nodename),
							isnull ? "unknow reason" : DatumGetCString(datum))));
		}
	}
}

static MgrNodeWrapper *getSrcCoordForAppend(dlist_head *coordinators)
{
	dlist_iter iter;
	SwitcherNodeWrapper *node;
	dlist_foreach(iter, coordinators)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		if (node->mgrNode->form.nodetype == CNDN_TYPE_COORDINATOR_MASTER)
		{
			return node->mgrNode;
		}
	}
	return NULL;
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

static void refreshMgrNodeBeforeRepair(MgrNodeWrapper *mgrNode,
									   MemoryContext spiContext)
{
	int rows;
	rows = updateMgrNodeCurestatus(mgrNode, CURE_STATUS_CURING, spiContext);
	if (rows != 1)
	{
		ereport(ERROR,
				(errmsg("%s, can not transit to curestatus:%s",
						NameStr(mgrNode->form.nodename),
						CURE_STATUS_CURING)));
	}
	else
	{
		namestrcpy(&mgrNode->form.curestatus, CURE_STATUS_CURING);
	}
}

static void refreshMgrNodeAfterRepair(MgrNodeWrapper *mgrNode,
									  MemoryContext spiContext)
{
	int rows;
	rows = updateMgrNodeToUnIsolate(mgrNode, spiContext);
	if (rows != 1)
	{
		ereport(ERROR,
				(errmsg("%s, can not transit to curestatus:%s",
						NameStr(mgrNode->form.nodename),
						CURE_STATUS_CURING)));
	}
}

static void selectActiveCoordinators(dlist_head *resultList)
{
	int spiRes;
	MemoryContext oldContext;
	MemoryContext spiContext;
	StringInfoData sql;

	oldContext = CurrentMemoryContext;
	SPI_CONNECT_TRANSACTIONAL_START(spiRes, true);
	spiContext = CurrentMemoryContext;
	MemoryContextSwitchTo(oldContext);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT * \n"
					 "FROM pg_catalog.mgr_node \n"
					 "WHERE nodetype in ('%c', '%c') \n"
					 "AND nodeinited = %d::boolean \n"
					 "AND nodeincluster = %d::boolean \n"
					 "AND curestatus != '%s' \n",
					 CNDN_TYPE_COORDINATOR_MASTER,
					 CNDN_TYPE_GTM_COOR_MASTER,
					 true,
					 true,
					 CURE_STATUS_ISOLATED);
	selectMgrNodes(sql.data, spiContext, resultList);
	pfree(sql.data);

	SPI_FINISH_TRANSACTIONAL_COMMIT();
}

static void checkGetActiveCoordinators(dlist_head *coordinators)
{
	dlist_head mgrNodes = DLIST_STATIC_INIT(mgrNodes);
	SwitcherNodeWrapper *node;
	dlist_iter iter;

	selectActiveCoordinators(&mgrNodes);

	if (dlist_is_empty(&mgrNodes))
	{
		ereport(ERROR,
				(errmsg("can't find any active coordinators")));
	}
	mgrNodesToSwitcherNodes(&mgrNodes, coordinators);

	dlist_foreach(iter, coordinators)
	{
		node = dlist_container(SwitcherNodeWrapper, link, iter.cur);
		node->pgConn = getNodeDefaultDBConnection(node->mgrNode, 10);
		if (node->pgConn == NULL)
		{
			ereport(ERROR,
					(errmsg("connect to coordinator %s failed",
							NameStr(node->mgrNode->form.nodename))));
		}
		node->runningMode = getNodeRunningMode(node->pgConn);
		if (node->runningMode != NODE_RUNNING_MODE_MASTER)
		{
			ereport(ERROR,
					(errmsg("coordinator %s configured as master, "
							"but actually did not running on that status",
							NameStr(node->mgrNode->form.nodename))));
		}
	}
}

static void dropFaultNodeFromActiveCoordinators(dlist_head *activeCoordinators,
												MgrNodeWrapper *faultNode,
												bool complain)
{
	dlist_mutable_iter coordIter;
	SwitcherNodeWrapper *coordinator;

	dlist_foreach_modify(coordIter, activeCoordinators)
	{
		coordinator = dlist_container(SwitcherNodeWrapper, link, coordIter.cur);
		if (nodeExistsInPgxcNode(coordinator->pgConn,
								 NameStr(coordinator->mgrNode->form.nodename),
								 true,
								 NameStr(faultNode->form.nodename),
								 getMappedPgxcNodetype(coordinator->mgrNode->form.nodetype),
								 complain))
		{
			ereport(LOG,
					(errmsg("drop node %s in table pgxc_node of %s begin",
							NameStr(faultNode->form.nodename),
							NameStr(coordinator->mgrNode->form.nodename))));
			dropNodeFromPgxcNode(coordinator->pgConn,
								 NameStr(coordinator->mgrNode->form.nodename),
								 NameStr(faultNode->form.nodename),
								 complain);
			exec_pgxc_pool_reload(coordinator->pgConn,
								  complain);
			ereport(LOG,
					(errmsg("drop node %s in table pgxc_node of %s successed",
							NameStr(faultNode->form.nodename),
							NameStr(coordinator->mgrNode->form.nodename))));
		}
		else
		{
			ereport(LOG,
					(errmsg("%s not exist in table pgxc_node of %s, skip",
							NameStr(faultNode->form.nodename),
							NameStr(coordinator->mgrNode->form.nodename))));
		}
	}
	ereport(LOG,
			(errmsg("drop %s in table pgxc_node of all coordinators successed",
					NameStr(faultNode->form.nodename))));
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