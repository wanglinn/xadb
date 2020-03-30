#include "postgres.h"

#include "access/parallel.h"
#include "catalog/namespace.h"
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
#include <sys/signalfd.h>

#include "utils/dr_private.h"
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
#include "postmaster/postmaster.h"
#endif /* DR_USING_EPOLL */

typedef struct DRExtraInfo
{
	Oid		PGXCNodeOid;
	uint32	PGXCNodeIdentifier;
	Oid		temp_namespace_id;
	Oid		temp_toast_namespace_id;
}DRExtraInfo;

DRLatchEventData *dr_latch_data = NULL;

static BackgroundWorker *dr_bgworker = NULL;
static BackgroundWorkerHandle *dr_bghandle = NULL;

#ifdef WITH_REDUCE_RDMA
volatile Size poll_max;
Size poll_count;
struct pollfd * volatile poll_fd;
#elif defined DR_USING_EPOLL
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
static bool		dr_backend_is_query_error = false;

/* keep error message, but don't report it immediately */
static bool dr_keep_error = false;
static ErrorData *dr_error_data = NULL;

static void dr_start_event(void);

static void handle_sigterm(SIGNAL_ARGS);

/* event functions */
static void OnPostmasterEvent(DROnEventArgs);
static void OnLatchEvent(DROnEventArgs);
static void OnLatchPreWait(DROnPreWaitArgs);
static void TryBackendMessage(void);
static void DRReset(void);
static bool DRIsIdleStatus(void);

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

#ifdef WITH_REDUCE_RDMA
static inline int setup_signalfd(void)
{
	int sfd;
	sigset_t sigset;

	sigemptyset(&sigset);
	sigaddset(&sigset, SIGUSR1);

	if (sigprocmask(SIG_SETMASK, &sigset, NULL) == -1)
		elog(ERROR, "sigprocmask SIG_BLOCK failed: %m");

	sfd = signalfd(-1, &sigset, 0);
	if (sfd < 0)
		elog(ERROR, "signalfd sigset failed: %m");

	return sfd;
}
#endif

static void DynamicReduceRestoreExtra(void)
{
	DRExtraInfo *extra = (DRExtraInfo*)MyBgworkerEntry->bgw_extra;
	PGXCNodeOid = extra->PGXCNodeOid;
	PGXCNodeIdentifier = extra->PGXCNodeIdentifier;
	SetTempNamespaceState(extra->temp_namespace_id, extra->temp_toast_namespace_id);
}

void DynamicReduceWorkerMain(Datum main_arg)
{
	DREventData *base;
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext	loop_context;
	HASH_SEQ_STATUS	seq_state;
	PlanInfo	   *pi;

#ifdef WITH_REDUCE_RDMA
	Size nevent;
	DRNodeEventData *nedinfo;
	DRNodeEventData *newdata;
	struct 			signalfd_siginfo info;
	int				sfd, i, ret;
	time_t			time_now,time_last_latch = 0;
#elif defined DR_USING_EPOLL
	sigset_t		unblock_sigs;
	int				nevent;
	time_t			time_now,time_last_latch = 0;
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

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "reduce toplevel");
	DynamicReduceRestoreExtra();
	DRAttachShmem(main_arg, true);

	dr_start_event();
	DRInitNodeSearch();
	DRInitPlanSearch();

#ifdef WITH_REDUCE_RDMA
	sfd = setup_signalfd();
	newdata = MemoryContextAllocZero(DrTopMemoryContext, sizeof(*newdata));
	newdata->base.type = DR_EVENT_DATA_LATCH;
	newdata->base.OnEvent = OnLatchEvent;
	newdata->base.OnPreWait = OnLatchPreWait;
	newdata->waiting_plan_id = INVALID_PLAN_ID;
	//initStringInfoExtend(&newdata->sendBuf, DR_SOCKET_BUF_SIZE_START);
	//initStringInfoExtend(&newdata->recvBuf, DR_SOCKET_BUF_SIZE_START);
	newdata->nodeoid = InvalidOid;
	newdata->status = DRN_ACCEPTED;

	RDRCtlWaitEvent(sfd, POLLIN, newdata, RPOLL_EVENT_ADD);
#endif

	loop_context = AllocSetContextCreate(TopMemoryContext,
										 "DynamicReduceLoop",
										 ALLOCSET_DEFAULT_SIZES);

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		if (dr_keep_error == false ||
			dr_backend_is_query_error)
		{
			/* Report the error to the server log and exit */
			EmitErrorReport();
			FlushErrorState();
			return;
		}

		HOLD_INTERRUPTS();

		dr_keep_error = false;	/* reset keep error */
		if (dr_error_data == NULL)
		{
			MemoryContextSwitchTo(DrTopMemoryContext);
			dr_error_data = CopyErrorData();
			MemoryContextSwitchTo(loop_context);
		}

		FlushErrorState();
		dr_status = DRS_FAILED;

		MemoryContextSwitchTo(loop_context);

		/* reset hash_seq_search */
		AtEOXact_HashTables(false);

		DRPlanSeqInit(&seq_state);
		while ((pi=hash_seq_search(&seq_state)) != NULL)
			(*pi->OnPlanError)(pi);

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
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

		/* reset hash_seq_search again */
		AtEOXact_HashTables(false);
	}
	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;
	InterruptHoldoffCount = 0;

	while(!ProcDiePending)
	{
		CHECK_FOR_INTERRUPTS();

		MemoryContextSwitchTo(loop_context);
		MemoryContextResetAndDeleteChildren(loop_context);

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
		DRNodeSeqInit(&seq_state);
		while((base=hash_seq_search(&seq_state)) != NULL)
		{
			if (base->OnPreWait)
				(*base->OnPreWait)(base);
		}
		CallConnectiongPreWait();

		DRPlanSeqInit(&seq_state);
		while ((pi=hash_seq_search(&seq_state)) != NULL)
		{
			if (pi->OnPreWait)
				(*pi->OnPreWait)(pi);
		}

		if (dr_backend_is_query_error &&
			DRIsIdleStatus())
		{
			DRSendConfirmToBackend(false);
			dr_backend_is_query_error = false;
		}
#ifdef WITH_REDUCE_RDMA
		if (poll_count > poll_max)
		{
			Size new_size = poll_max+STEP_POLL_ALLOC;
			while (new_size < poll_max)
				new_size += STEP_POLL_ALLOC;
			poll_fd = repalloc(poll_fd, new_size*sizeof(*poll_fd));
			poll_max+= new_size;
		}
		nevent = adb_rpoll(poll_fd, poll_count, 100);
		time_now = time(NULL);
		if (nevent == 0 ||	/* timeout */
			time_now != time_last_latch)
		{
			/*
			 * sometime shm_mq can send/receive, but we not get latch event,
			 * We don't no why, maybe shm_mq has a bug.
			 * For now, we also using timeout(0.1 second) process latch event,
			 * I think this is not a good idea
			 */
			pg_memory_barrier();
			MyLatch->is_set = true;
		}
		if (nevent>0)
		{
			for (i = 0; i < poll_count && nevent > 0; i++)
			{
				if (poll_fd[i].fd > 0 && poll_fd[i].revents)
				{
					if (poll_fd[i].fd == sfd)
					{
						ret = read(sfd, &info, sizeof info);
						if (ret < 0)
							elog(ERROR, "read signal ret %d failed: %m", ret);
					}
					nedinfo = (DRNodeEventData*)dr_rhandle_find(poll_fd[i].fd);
					base = &nedinfo->base;
					Assert(base->fd == poll_fd[i].fd);
					(*base->OnEvent)(base, poll_fd[i].revents);
					--nevent;
				}
			}
		}
#elif defined DR_USING_EPOLL
		if (dr_wait_count > dr_wait_max)
		{
			Size new_size = dr_wait_max + DR_WAIT_EVENT_SIZE_STEP;
			while (new_size < dr_wait_max)
				new_size += DR_WAIT_EVENT_SIZE_STEP;
			dr_epoll_events = repalloc(dr_epoll_events, sizeof(dr_epoll_events[0]) * new_size);
			dr_wait_max = new_size;
		}
		nevent = epoll_pwait(dr_epoll_fd, dr_epoll_events, (int)dr_wait_count, 100, &unblock_sigs);
		time_now = time(NULL);
		if (nevent == 0 ||	/* timeout */
			time_now != time_last_latch)
		{
			/*
			 * sometime shm_mq can send/receive, but we not get latch event,
			 * We don't no why, maybe shm_mq has a bug.
			 * For now, we also using timeout(0.1 second) process latch event,
			 * I think this is not a good idea
			 */
			pg_memory_barrier();
			MyLatch->is_set = true;
		}
		while (nevent>0)
		{
			--nevent;
			base = dr_epoll_events[nevent].data.ptr;
			(*base->OnEvent)(base, dr_epoll_events[nevent].events);
		}
#endif
		if (MyLatch->is_set)
		{
			OnLatchEvent(NULL, 0);
			time_last_latch = time_now;
		}
#else /* DR_USING_EPOLL */
		for (nevent=dr_wait_count;nevent>0;)
		{
			--nevent;
			base = GetWaitEventData(dr_wait_event_set, nevent);
			if (base->OnPreWait)
				(*base->OnPreWait)(base, (int)nevent);
		}

		if (dr_backend_is_query_error &&
			DRIsIdleStatus())
		{
			DRSendConfirmToBackend(false);
			dr_backend_is_query_error = false;
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
	ereport(DEBUG1, (errmsg("dynamic reduce shutting down")));
}

uint16 StartDynamicReduceWorker(void)
{
	Size			size;
	StringInfoData	buf;
	int				msgtype;
	uint32			i;
	uint16			result;

	ResetDynamicReduceWork();

	if (dr_mem_seg == NULL)
	{
		DRSetupShmem();
		Assert(dr_mem_seg != NULL);
	}

#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
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
		DRExtraInfo *extra;
		dr_bgworker = MemoryContextAllocZero(TopMemoryContext, sizeof(*dr_bgworker));
		dr_bgworker->bgw_flags = BGWORKER_SHMEM_ACCESS;
		dr_bgworker->bgw_start_time = BgWorkerStart_ConsistentState;
		dr_bgworker->bgw_restart_time = BGW_NEVER_RESTART;
		strcpy(dr_bgworker->bgw_library_name, "postgres");
		strcpy(dr_bgworker->bgw_function_name, "DynamicReduceWorkerMain");
		strcpy(dr_bgworker->bgw_type, "dynamic reduce");
		snprintf(dr_bgworker->bgw_name, BGW_MAXLEN, "dynamic reduce for PID %d", MyProcPid);
		dr_bgworker->bgw_notify_pid = MyProcPid;
		extra = (DRExtraInfo*)dr_bgworker->bgw_extra;
		extra->PGXCNodeOid = PGXCNodeOid;
		extra->PGXCNodeIdentifier = PGXCNodeIdentifier;
		GetTempNamespaceState(&extra->temp_namespace_id, &extra->temp_toast_namespace_id);
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
		WaitForBackgroundWorkerShutdown(dr_bghandle);
		DRResetShmem();
		pfree(dr_bghandle);
		dr_bghandle = NULL;
	}

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_STARTUP);

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
		WaitForBackgroundWorkerShutdown(dr_bghandle);
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
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
		DREventData		   *base;

		if (DRNodeSeqInit(&seq))
		{
			while ((base=hash_seq_search(&seq)) != NULL)
			{
				if (base->fd != PGINVALID_SOCKET)
#ifdef WITH_REDUCE_RDMA
					adb_rclose(base->fd);
#else
					closesocket(base->fd);
#endif
			}
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

		if (DRPlanSeqInit(&seq))
		{
			while ((pi=hash_seq_search(&seq)) != NULL)
				(*pi->OnDestroy)(pi);
		}
	}

	if (dr_bghandle)
	{
		TerminateBackgroundWorker(dr_bghandle);
		WaitForBackgroundWorkerShutdown(dr_bghandle);
		pfree(dr_bghandle);
		dr_bghandle = NULL;
	}
	DRDetachShmem();
}

void TerminateDynamicReduceWorker(void)
{
	if (dr_bghandle)
		TerminateBackgroundWorker(dr_bghandle);
}

void DynamicReduceStartParallel(void)
{
	Assert(IsParallelWorker());

#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
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
#ifdef WITH_REDUCE_RDMA
	oldcontext = MemoryContextSwitchTo(DrTopMemoryContext);
	poll_max = START_POOL_ALLOC;
	poll_count = 0;
	poll_fd = MemoryContextAlloc(DrTopMemoryContext,
								 sizeof(struct pollfd) * poll_max);
	dr_create_rhandle_list();
#elif defined DR_USING_EPOLL
	oldcontext = MemoryContextSwitchTo(DrTopMemoryContext);
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
#ifdef WITH_REDUCE_RDMA
		RDRCtlWaitEvent(postmaster_alive_fds[POSTMASTER_FD_WATCH],
					   POLLIN,
					   ped,
					   RPOLL_EVENT_ADD);
#elif defined DR_USING_EPOLL
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
#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
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

	DRPlanSeqInit(&seq_status);
	while ((pi=hash_seq_search(&seq_status)) != NULL)
	{
		(*pi->OnLatchSet)(pi);
	}

	TryBackendMessage();
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

	if (dr_backend_is_query_error ||
		DRRecvMsgFromBackend(&size, (void**)&buf.data, true) == false)
		return;
	buf.cursor = 0;
	buf.len = buf.maxlen = (int)size;

	msgtype = pq_getmsgbyte(&buf);
	if (dr_status == DRS_FAILED)
	{
		Assert(dr_error_data != NULL);
		Assert(dr_keep_error == false);
		switch(msgtype)
		{
		case ADB_DR_MQ_MSG_STARTUP:
		case ADB_DR_MQ_MSG_QUERY_ERROR:
		case ADB_DR_MQ_MSG_CONNECT:
		case ADB_DR_MQ_MSG_RESET:
			ReThrowError(dr_error_data);
			abort();	/* should not run to here */
		default:
			break;
		}
	}
	if (msgtype == ADB_DR_MQ_MSG_STARTUP)
	{
		DRListenEventData  *listen_event;
		MemoryContext		oldcontext;
		char 				port_msg[3];

		/* reset first */
		DRReset();

		oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(dr_latch_data));
		pq_getmsgend(&buf);

		listen_event = GetListenEventData();
		dr_status = DRS_WORKING;

		/* send port to backend */
		port_msg[0] = ADB_DR_MQ_MSG_PORT;
		memcpy(&port_msg[1], &listen_event->port, 2);
		DRSendMsgToBackend(port_msg, sizeof(port_msg), false);
		MemoryContextSwitchTo(oldcontext);
	}else if (msgtype == ADB_DR_MQ_MSG_QUERY_ERROR)
	{
		Assert(dr_error_data == NULL);
		if (DRIsIdleStatus())
			DRSendConfirmToBackend(false);
		else
			dr_backend_is_query_error = true;
	}else if (msgtype == ADB_DR_MQ_MSG_CONNECT)
	{
		DRConnectNetMsg(&buf);
		DRSendConfirmToBackend(false);
	}else if (msgtype == ADB_DR_MQ_MSG_RESET)
	{
		Assert(dr_status != DRS_FAILED);
		DRReset();
		DRSendMsgToBackend(buf.data, buf.len, false);
		dr_status = DRS_STARTUPED;
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
	PlanInfo		   *pi;
	DREventData		   *base;
#if !defined (DR_USING_EPOLL) && !defined(WITH_REDUCE_RDMA)
	Size				nevent;
#endif

	DRPlanSeqInit(&status);
	while ((pi=hash_seq_search(&status)) != NULL)
		(*pi->OnDestroy)(pi);

#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA) 
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
	DRShmemResetSharedFile();
}

#if (!defined DR_USING_EPOLL) && (!defined WITH_REDUCE_RDMA)
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

/*
 * keep error, but not report, let dynamic reduce in failed state,
 * until reset dynamic reduce report error
 * can be call in function ereport()
 */
int DRKeepError(void)
{
	dr_keep_error = true;
	return 0;
}

static bool DRIsIdleStatus(void)
{
	DRNodeEventData *ned;
#if (defined DR_USING_EPOLL) || (defined WITH_REDUCE_RDMA)
	HASH_SEQ_STATUS seq;
	if (HaveConnectingNode())
		return false;
	DRNodeSeqInit(&seq);
	while ((ned=hash_seq_search(&seq)) != NULL)
	{
		Assert(ned->base.type == DR_EVENT_DATA_NODE);
		if (ned->status == DRN_ACCEPTED ||
			ned->status == DRN_CONNECTING ||
			ned->sendBuf.len > 0 ||
			ned->recvBuf.len > 0)
		{
			hash_seq_term(&seq);
			return false;
		}
	}
#else
	Size nevent;
	for (nevent=dr_wait_count;nevent>0;)
	{
		--nevent;
		ned = GetWaitEventData(dr_wait_event_set, nevent);
		if (ned->base.type == DR_EVENT_DATA_NODE)
		{
			if (ned->status == DRN_ACCEPTED ||
				ned->status == DRN_CONNECTING ||
				ned->sendBuf.len > 0 ||
				ned->recvBuf.len > 0)
				return false;
		}
	}
#endif
	if (DRCurrentPlanCount() > 0)
		return false;

	return true;
}
