#include "postgres.h"

#include "access/parallel.h"
#include "common/ip.h"
#include "lib/ilist.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"
#include "utils/dynamicreduce.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include <unistd.h>

#include "utils/dr_private.h"
#ifdef DR_USING_EPOLL
#include "postmaster/postmaster.h"
#endif /* DR_USING_EPOLL */

DRLatchEventData *dr_latch_data = NULL;

static BackgroundWorker *dr_bgworker = NULL;
static BackgroundWorkerHandle *dr_bghandle = NULL;

#ifdef DR_USING_EPOLL
int					dr_epoll_fd = PGINVALID_SOCKET;
struct epoll_event  *dr_epoll_events = NULL;
#else
WaitEventSet   *dr_wait_event_set = NULL;
WaitEvent	   *dr_wait_event = NULL;
#endif /* DR_USING_EPOLL */
Size			dr_wait_count = 0;
Size			dr_wait_max = 0;
#define DrTopMemoryContext TopMemoryContext
pid_t			dr_reduce_pid = 0;
DR_STATUS		dr_status;
bool			is_reduce_worker = false;

/* clear network when reset */
//static bool dr_clear_network = false;
/* keep error message, but don't report it immediately */
//static bool dr_keep_error = false;

static void dr_start_event(void);

static void handle_sigterm(SIGNAL_ARGS);

/* event functions */
static void OnPostmasterEvent(DROnEventArgs);
static void OnLatchEvent(DROnEventArgs);
static void OnLatchPreWait(DROnPreWaitArgs);
static void TryBackendMessage(void);
static void DRReset(void);

#ifdef DR_USING_EPOLL
static inline void DRSetupSignal(void)
{
	sigset_t	sigs;

	/* block SIGUSR1 and SIGUSR2 */
	if (sigemptyset(&sigs) < 0 ||
		sigaddset(&sigs, SIGUSR1) < 0 ||
		sigaddset(&sigs, SIGUSR2) < 0 ||
		sigprocmask(SIG_SETMASK, &sigs, NULL) < 0 ||
		raise(SIGUSR2) < 0)
	{
		elog(ERROR, "block signal failed: %m");
	}
}
#endif
void DynamicReduceWorkerMain(Datum main_arg)
{
	DREventData *base;
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext	loop_context;
	HASH_SEQ_STATUS	seq_state;
	PlanInfo	   *pi;
#ifdef DR_USING_EPOLL
	sigset_t		unblock_sigs;
	int				nevent;
#else
	Size nevent;
	bool pre_check_latch = false;
#endif /* DR_USING_EPOLL */

	is_reduce_worker = true;
	ParallelWorkerNumber = 0;

	/*
	 * Establish signal handlers.
	 *
	 * We want CHECK_FOR_INTERRUPTS() to kill off this worker process just as
	 * it would a normal user backend.  To make that happen, we establish a
	 * signal handler that is a stripped-down version of die().
	 */
	pqsignal(SIGTERM, handle_sigterm);
#ifdef DR_USING_EPOLL
	DRSetupSignal();
	sigemptyset(&unblock_sigs);
#else
	BackgroundWorkerUnblockSignals();
#endif

	dr_status = DRS_STARTUPED;

	DRAttachShmem(main_arg, true);

	dr_start_event();
	DRInitNodeSearch();
	DRInitPlanSearch();
	loop_context = AllocSetContextCreate(TopMemoryContext,
										 "DynamicReduceLoop",
										 ALLOCSET_DEFAULT_SIZES);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		DRPlanSeqInit(&seq_state);
		while ((pi=hash_seq_search(&seq_state)) != NULL)
			(*pi->OnPlanError)(pi);

		/* reset hash_seq_search */
		AtEOXact_HashTables(false);

		/* Report the error to the server log */
		EmitErrorReport();
		FlushErrorState();

		DRUtilsAbort();

#ifdef DR_USING_EPOLL
		CallConnectingOnError();
		DRNodeSeqInit(&seq_state);
		while ((base=hash_seq_search(&seq_state)) != NULL)
		{
			Assert(base->type == DR_EVENT_DATA_NODE);
			if (base->OnError)
				(*base->OnError)(base);
		}
#else /* DR_USING_EPOLL */
		for (nevent=dr_wait_count;nevent>0;)
		{
			--nevent;
			base = GetWaitEventData(dr_wait_event_set, nevent);
			if (base->OnError)
				(*base->OnError)(base, (int)nevent);
		}
#endif /* DR_USING_EPOLL */
	}
	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	for(;;)
	{
		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(loop_context);
		MemoryContextResetAndDeleteChildren(loop_context);

#ifdef DR_USING_EPOLL
		DRNodeSeqInit(&seq_state);
		while((base=hash_seq_search(&seq_state)) != NULL)
		{
			if (base->OnPreWait)
				(*base->OnPreWait)(base);
		}

		DRPlanSeqInit(&seq_state);
		while ((pi=hash_seq_search(&seq_state)) != NULL)
		{
			if (pi->OnPreWait)
				(*pi->OnPreWait)(pi);
		}

		nevent = epoll_pwait(dr_epoll_fd, dr_epoll_events, (int)dr_wait_count, 100, &unblock_sigs);
		if (nevent == 0 &&	/* timeout */
			MyLatch->is_set == false)
		{
			/*
			 * sometime shm_mq can send/receive, but we not get latch event,
			 * We don't no why, maybe shm_mq has a bug.
			 * For now, we also using timeout(0.1 second) process latch event,
			 * I think this is not a good idea
			 */
			OnLatchEvent(NULL, 0);
		}
		while (nevent>0)
		{
			--nevent;
			base = dr_epoll_events[nevent].data.ptr;
			(*base->OnEvent)(base, dr_epoll_events[nevent].events);
		}
		if (MyLatch->is_set)
			OnLatchEvent(NULL, 0);
#else /* DR_USING_EPOLL */
		for (nevent=dr_wait_count;nevent>0;)
		{
			--nevent;
			base = GetWaitEventData(dr_wait_event_set, nevent);
			if (base->OnPreWait)
				(*base->OnPreWait)(base, (int)nevent);
		}

		CHECK_FOR_INTERRUPTS();

re_wait_:
		pre_check_latch = (!pre_check_latch);
		SetWaitPreCheckLatch(dr_wait_event_set, pre_check_latch);

		nevent = WaitEventSetWait(dr_wait_event_set,
								  pre_check_latch ? 100:0,
								  dr_wait_event,
								  dr_wait_count,
								  PG_WAIT_IPC);
		if (nevent == 0)
		{
			if (pre_check_latch)
				SetLatch(MyLatch);
			goto re_wait_;
		}

		while (nevent > 0)
		{
			WaitEvent *we = &dr_wait_event[--nevent];
			base = we->user_data;
			(*base->OnEvent)(we);
		}
#endif /* DR_USING_EPOLL */
	}
}

uint16 StartDynamicReduceWorker(void)
{
	Size			size;
	StringInfoData	buf;
	int				msgtype;
	uint32			i;
	Oid				auth_user_id;
	uint16			result;

	ResetDynamicReduceWork();

	if (dr_mem_seg == NULL)
	{
		DRSetupShmem();
		Assert(dr_mem_seg != NULL);
	}

#ifndef DR_USING_EPOLL
	if (dr_wait_event_set == NULL)
	{
		dr_wait_event_set = CreateWaitEventSet(TopMemoryContext, 2);
		AddWaitEventToSet(dr_wait_event_set,
						  WL_LATCH_SET,
						  PGINVALID_SOCKET,
						  &MyProc->procLatch,
						  NULL);
		AddWaitEventToSet(dr_wait_event_set,
						  WL_POSTMASTER_DEATH,
						  PGINVALID_SOCKET,
						  NULL,
						  NULL);
	}
#endif /* DR_USING_EPOLL */

	if (dr_bgworker == NULL)
	{
		dr_bgworker = MemoryContextAllocZero(TopMemoryContext, sizeof(*dr_bgworker));
		dr_bgworker->bgw_flags = BGWORKER_SHMEM_ACCESS|BGWORKER_BACKEND_DATABASE_CONNECTION;
		dr_bgworker->bgw_start_time = BgWorkerStart_ConsistentState;
		dr_bgworker->bgw_restart_time = BGW_NEVER_RESTART;
		strcpy(dr_bgworker->bgw_library_name, "postgres");
		strcpy(dr_bgworker->bgw_function_name, "DynamicReduceWorkerMain");
		strcpy(dr_bgworker->bgw_type, "dynamic reduce");
		snprintf(dr_bgworker->bgw_name, BGW_MAXLEN, "dynamic reduce for PID %d", MyProcPid);
		dr_bgworker->bgw_notify_pid = MyProcPid;
	}

	for(i=0;;++i)
	{
		BgwHandleStatus status;

		if (dr_bghandle == NULL)
		{
			MemoryContext oldcontext = MemoryContextSwitchTo(DrTopMemoryContext);
			dr_bgworker->bgw_main_arg = UInt32GetDatum(dsm_segment_handle(dr_mem_seg));
			if (!RegisterDynamicBackgroundWorker(dr_bgworker, &dr_bghandle))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("could not register background process"),
						 errhint("You may need to increase max_worker_processes.")));
			}
			MemoryContextSwitchTo(oldcontext);
			shm_mq_set_handle(dr_mq_backend_sender, dr_bghandle);
			shm_mq_set_handle(dr_mq_worker_sender, dr_bghandle);
		}

		status = WaitForBackgroundWorkerStartup(dr_bghandle, &dr_reduce_pid);
		if (status == BGWH_STARTED)
		{
			break;
		}else if (status == BGWH_STOPPED && i > 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("dynamic reduce restart failed")));
		}

		TerminateBackgroundWorker(dr_bghandle);
		DRResetShmem();
		pfree(dr_bghandle);
		dr_bghandle = NULL;
	}

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_STARTUP);

	appendBinaryStringInfoNT(&buf, (char*)&PGXCNodeOid, sizeof(PGXCNodeOid));
	appendBinaryStringInfoNT(&buf, (char*)&PGXCNodeIdentifier, sizeof(PGXCNodeIdentifier));
	appendBinaryStringInfoNT(&buf, (char*)&MyDatabaseId, sizeof(MyDatabaseId));
	auth_user_id = GetAuthenticatedUserId();
	appendBinaryStringInfoNT(&buf, (char*)&auth_user_id, sizeof(auth_user_id));

	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	DRRecvMsgFromReduce(&size, (void**)&buf.data, false);
	buf.len = buf.maxlen = (int)size;
	buf.cursor = 0;

	msgtype = pq_getmsgbyte(&buf);
	if (msgtype != ADB_DR_MQ_MSG_PORT)
	{
		shm_mq_set_handle(dr_mq_backend_sender, NULL);
		shm_mq_set_handle(dr_mq_worker_sender, NULL);

		TerminateBackgroundWorker(dr_bghandle);
		pfree(dr_bghandle);
		dr_bghandle = NULL;

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid message type %d from backend", msgtype),
				 errhint("expect message %d", ADB_DR_MQ_MSG_PORT)));
	}
	pq_copymsgbytes(&buf, (char*)&result, sizeof(result));
	pq_getmsgend(&buf);

	return result;
}

void StopDynamicReduceWorker(void)
{
	HASH_SEQ_STATUS	seq;
	PlanInfo	   *pi;

	if (is_reduce_worker)
	{
#ifdef DR_USING_EPOLL
		DREventData		   *base;

		DRNodeSeqInit(&seq);
		while ((base=hash_seq_search(&seq)) != NULL)
		{
			if (base->fd != PGINVALID_SOCKET)
				closesocket(base->fd);
		}
#else
		pgsocket	fd;
		Size		i = dr_wait_count;
		while (i>0)
		{
			fd = GetWaitEventSocket(dr_wait_event_set, --i);
			if (fd != PGINVALID_SOCKET)
				closesocket(fd);
		}
#endif

		DRPlanSeqInit(&seq);
		while ((pi=hash_seq_search(&seq)) != NULL)
			(*pi->OnDestroy)(pi);
	}

	if (dr_bghandle)
	{
		TerminateBackgroundWorker(dr_bghandle);
		pfree(dr_bghandle);
		dr_bghandle = NULL;
	}
	DRDetachShmem();
}

void DynamicReduceStartParallel(void)
{
	Assert(IsParallelWorker());

#ifndef DR_USING_EPOLL
	if (dr_wait_event_set == NULL)
	{
		dr_wait_event_set = CreateWaitEventSet(TopMemoryContext, 2);
		AddWaitEventToSet(dr_wait_event_set,
						  WL_LATCH_SET,
						  PGINVALID_SOCKET,
						  &MyProc->procLatch,
						  NULL);
		AddWaitEventToSet(dr_wait_event_set,
						  WL_POSTMASTER_DEATH,
						  PGINVALID_SOCKET,
						  NULL,
						  NULL);
	}
#endif /* DR_USING_EPOLL */
}

void DRCheckStarted(void)
{
	if (dr_bghandle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("dynamic reduce not started")));
}

static void dr_start_event(void)
{
	MemoryContext oldcontext;
	oldcontext = MemoryContextSwitchTo(DrTopMemoryContext);

#ifdef DR_USING_EPOLL
	if (dr_epoll_fd == PGINVALID_SOCKET &&
		(dr_epoll_fd = epoll_create1(EPOLL_CLOEXEC)) == PGINVALID_SOCKET)
	{
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("epoll_create1 failed: %m")));
	}
	if (dr_epoll_events == NULL)
	{
		dr_epoll_events = MemoryContextAlloc(DrTopMemoryContext,
											 sizeof(dr_epoll_events[0]) * DR_WAIT_EVENT_SIZE_STEP);
		dr_wait_max = DR_WAIT_EVENT_SIZE_STEP;
	}
#else /* DR_USING_EPOLL */
	dr_wait_event_set = CreateWaitEventSet(DrTopMemoryContext, DR_WAIT_EVENT_SIZE_STEP);
	dr_wait_event = MemoryContextAlloc(DrTopMemoryContext, sizeof(dr_wait_event[0]) * DR_WAIT_EVENT_SIZE_STEP);
	dr_wait_max = DR_WAIT_EVENT_SIZE_STEP;
#endif /*  DR_USING_EPOLL */

	/* postmaster death event */
	{
		DRPostmasterEventData *ped;
		ped = MemoryContextAllocZero(DrTopMemoryContext, sizeof(*ped));
		ped->type = DR_EVENT_DATA_POSTMASTER;
		ped->OnEvent = OnPostmasterEvent;
#ifdef DR_USING_EPOLL
		DRCtlWaitEvent(postmaster_alive_fds[POSTMASTER_FD_WATCH],
					   EPOLLIN,
					   ped,
					   EPOLL_CTL_ADD);
#else
		AddWaitEventToSet(dr_wait_event_set, WL_POSTMASTER_DEATH, PGINVALID_SOCKET, NULL, ped);
#endif
		++dr_wait_count;
	}

	/* latch set event */
	dr_latch_data = MemoryContextAllocZero(DrTopMemoryContext, sizeof(*dr_latch_data));
	dr_latch_data->base.type = DR_EVENT_DATA_LATCH;
	dr_latch_data->base.OnEvent = OnLatchEvent;
	dr_latch_data->base.OnPreWait = OnLatchPreWait;
	initOidBufferEx(&dr_latch_data->net_oid_buf, OID_BUF_DEF_SIZE, DrTopMemoryContext);
	initOidBufferEx(&dr_latch_data->work_oid_buf, OID_BUF_DEF_SIZE, DrTopMemoryContext);
	initOidBufferEx(&dr_latch_data->work_pid_buf, OID_BUF_DEF_SIZE, DrTopMemoryContext);
#ifndef DR_USING_EPOLL
	AddWaitEventToSet(dr_wait_event_set,
					  WL_LATCH_SET,
					  PGINVALID_SOCKET,
					  MyLatch,
					  dr_latch_data);
	++dr_wait_count;
#endif /* DR_USING_EPOLL */

	MemoryContextSwitchTo(oldcontext);
}

/* event functions */
static void OnPostmasterEvent(DROnEventArgs)
{
	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}
}

static void OnLatchEvent(DROnEventArgs)
{
	PlanInfo *pi;
	HASH_SEQ_STATUS seq_status;

	ResetLatch(MyLatch);

	TryBackendMessage();

	DRPlanSeqInit(&seq_status);
	while ((pi=hash_seq_search(&seq_status)) != NULL)
	{
		(*pi->OnLatchSet)(pi);
	}
}

static void OnLatchPreWait(DROnPreWaitArgs)
{
	PlanInfo	   *pi;
	HASH_SEQ_STATUS seq_state;

	DRPlanSeqInit(&seq_state);
	while ((pi=hash_seq_search(&seq_state)) != NULL)
	{
		if (pi->OnPreWait)
			(*pi->OnPreWait)(pi);
	}
}

static void TryBackendMessage(void)
{
	StringInfoData		buf;
	Size				size;
	int					msgtype;

	if (DRRecvMsgFromBackend(&size, (void**)&buf.data, true) == false)
		return;
	buf.cursor = 0;
	buf.len = buf.maxlen = (int)size;

	msgtype = pq_getmsgbyte(&buf);
	if (msgtype == ADB_DR_MQ_MSG_STARTUP)
	{
		DRListenEventData  *listen_event;
		MemoryContext		oldcontext;
		Oid					dboid;
		Oid					auth_user_id;
		char 				port_msg[3];

		/* reset first */
		DRReset();

		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(dr_latch_data));
		pq_copymsgbytes(&buf, (char*)&PGXCNodeOid, sizeof(PGXCNodeOid));
		pq_copymsgbytes(&buf, (char*)&PGXCNodeIdentifier, sizeof(PGXCNodeIdentifier));
		pq_copymsgbytes(&buf, (char*)&dboid, sizeof(dboid));
		pq_copymsgbytes(&buf, (char*)&auth_user_id, sizeof(auth_user_id));
		pq_getmsgend(&buf);

		if (OidIsValid(MyDatabaseId))
		{
			if (MyDatabaseId != dboid)
			{
				ereport(ERROR,
						(errmsg("diffent database OID as last"),
						 errdetail("last is %u, current is %u", MyDatabaseId, dboid)));
			}
		}else
		{
			InitializingParallelWorker = true;
			BackgroundWorkerInitializeConnectionByOid(dboid, auth_user_id, 0);
			Assert(MyDatabaseId == dboid);
			SetClientEncoding(GetDatabaseEncoding());
			InitializingParallelWorker = false;
		}

		listen_event = GetListenEventData();
		dr_status = DRS_LISTENED;

		/* send port to backend */
		port_msg[0] = ADB_DR_MQ_MSG_PORT;
		memcpy(&port_msg[1], &listen_event->port, 2);
		DRSendMsgToBackend(port_msg, sizeof(port_msg), false);
		MemoryContextSwitchTo(oldcontext);
	}else if (msgtype == ADB_DR_MQ_MSG_CONNECT)
	{
		DRConnectNetMsg(&buf);
		DRSendConfirmToBackend(false);
	}else if (msgtype == ADB_DR_MQ_MSG_RESET)
	{
		DRReset();
		dr_status = DRS_RESET;
		DRSendMsgToBackend(buf.data, buf.len, false);
	}else if (msgtype == ADB_DR_MQ_MSG_START_PLAN_NORMAL)
	{
		DRStartNormalPlanMessage(&buf);
		DRSendConfirmToBackend(false);
	}else if (msgtype == ADB_DR_MQ_MSG_START_PLAN_SFS)
	{
		DRStartSFSPlanMessage(&buf);
		DRSendConfirmToBackend(false);
	}else if (msgtype == ADB_DR_MQ_MSG_START_PLAN_PARALLEL)
	{
		DRStartParallelPlanMessage(&buf);
		DRSendConfirmToBackend(false);
	}else if (msgtype == ADB_DR_MQ_MSG_START_PLAN_STS)
	{
		DRStartSTSPlanMessage(&buf);
		DRSendConfirmToBackend(false);
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unknown message type %d from backend", msgtype)));
	}
}

static void DRReset(void)
{
	HASH_SEQ_STATUS		status;
	Size				nevent pg_attribute_unused();
	PlanInfo		   *pi;
	DREventData		   *base;

	DRPlanSeqInit(&status);
	while ((pi=hash_seq_search(&status)) != NULL)
		(*pi->OnDestroy)(pi);

#ifdef DR_USING_EPOLL
	DRNodeSeqInit(&status);
	while ((base=hash_seq_search(&status)) != NULL)
	{
		if (base->type == DR_EVENT_DATA_NODE)
			DRNodeReset((DRNodeEventData*)base);
	}
#else
	for (nevent=dr_wait_count;nevent>0;)
	{
		--nevent;
		base = GetWaitEventData(dr_wait_event_set, nevent);
		if (base->type == DR_EVENT_DATA_NODE)
			DRNodeReset((DRNodeEventData*)base);
	}
#endif
	DRUtilsReset();
}

#ifndef DR_USING_EPOLL
void DREnlargeWaitEventSet(void)
{
	if (dr_wait_count == dr_wait_max)
	{
		dr_wait_event_set = EnlargeWaitEventSet(dr_wait_event_set,
												dr_wait_max + DR_WAIT_EVENT_SIZE_STEP);
		dr_wait_max += DR_WAIT_EVENT_SIZE_STEP;
	}
}
#endif /* DR_USING_EPOLL */

void DRGetEndOfPlanMessage(PlanInfo *pi, PlanWorkerInfo *pwi)
{
	pwi->last_msg_type = ADB_DR_MSG_END_OF_PLAN;
	pwi->last_size = 0;
	pwi->last_data = NULL;
	pwi->dest_oids = pi->working_nodes.oids;
	pwi->dest_count = pi->working_nodes.len;
	pwi->dest_cursor = 0;
}

/*
 * When we receive a SIGTERM, we set InterruptPending and ProcDiePending just
 * like a normal backend.  The next CHECK_FOR_INTERRUPTS() will do the right
 * thing.
 */
static void handle_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	SetLatch(MyLatch);

	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	errno = save_errno;
}
