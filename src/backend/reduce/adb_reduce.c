/*-------------------------------------------------------------------------
 *
 * adb_reduce.c
 *	  interface for communication between backend process and its self reduce
 *	  process.
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/backend/reduce/adb_reduce.c
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "postgres.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/parallel.h"
#include "executor/clusterReceiver.h"
#include "libpq/pqsignal.h"
#include "pgxc/pgxc.h"
#include "postmaster/fork_process.h"
#include "postmaster/syslogger.h"
#include "reduce/adb_reduce.h"
#include "reduce/rdc_msg.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

extern bool redirection_done;
extern bool print_reduce_debug_log;

#ifndef WIN32
static int backend_reduce_fds[2] = {-1, -1};
#else
/* Process handle of backend used for the same purpose on Windows */
static HANDLE	BackendHandle;
#endif	/* WIN32 */

static char			my_reduce_path[MAXPGPATH] = {0};

static RdcPortId	SelfReduceID = InvalidOid;
static pid_t		SelfReducePID = 0;
static int			SelfReduceListenPort = 0;
static RdcPort	   *SelfReducePort = NULL;
static List		   *GroupReduceList = NIL;

#define RDC_BACKEND_HOLD	0
#define RDC_REDUCE_HOLD		1

static void ResetSelfReduce(void);
static void InitCommunicationChannel(void);
static void CloseBackendPort(void);
static void CloseReducePort(void);
static int  GetReduceListenPort(void);
#ifndef WIN32
static void AdbReduceLauncherMain(char *exec_path, int rid);
#endif
static int  SendPlanMsgToRemote(RdcPort *port, char msg_type, List *dest_nodes);

static void
ResetSelfReduce(void)
{
	rdc_freeport(SelfReducePort);
	SelfReducePort = NULL;
	SelfReduceListenPort = 0;
	SelfReduceID = InvalidOid;
	list_free(GroupReduceList);
	GroupReduceList = NIL;
	cancel_before_shmem_exit(EndSelfReduce, 0);
}

void
AtEOXact_Reduce(void)
{
	if (SelfReducePort && IsCoordMaster())
	{
		StringInfo msg = RdcMsgBuf(SelfReducePort);

		resetStringInfo(msg);
		rdc_beginmessage(msg, MSG_BACKEND_CLOSE);
		rdc_endmessage(SelfReducePort, msg);
		(void) rdc_flush(SelfReducePort);

		ResetSelfReduce();
	}
}

void
EndSelfReduce(int code, Datum arg)
{
	if (!IsParallelWorker() && SelfReducePID != 0)
	{
		int ret = kill(SelfReducePID, SIGTERM);
		bool no_error = DatumGetBool(arg);
		if (!(ret == 0 || errno == ESRCH))
		{
			if (no_error)
			{
				ereport(ERROR,
					(errmsg("fail to terminate adb reduce subprocess")));
			}
		}
		adb_elog(print_reduce_debug_log, LOG,
			"[proc %d] kill SIGTERM to [proc %d]", MyProcPid, SelfReducePID);
		SelfReducePID = 0;
 	}
	ResetSelfReduce();
}

static void
SigChldHandler(SIGNAL_ARGS)
{
	int		status;

	wait(&status);
}

/*
 * Main entry point for adb reduce launcher process, to be called from the
 * backend.
 *
 * return reduce listen port if OK.
 * return 0 if trouble.
 */
int
StartSelfReduceLauncher(RdcPortId rid)
{
	MemoryContext	old_context;

	if (my_reduce_path[0] == '\0')
	{
		int ret;

		sigaddset(&BlockSig, SIGCHLD);
		PG_SETMASK(&BlockSig);
		PG_TRY();
		{
			if ((ret = find_other_exec(my_exec_path, "adb_reduce",
#ifdef ADB
									   "adb_reduce (" ADB_VERSION " based on PostgreSQL) " PG_VERSION"\n",
#else
									"adb_reduce based on (PG " PG_VERSION ")\n",
#endif
									   my_reduce_path)) < 0)
			{
				if (ret == -1)
					ereport(ERROR,
							(errmsg("The program \"adb_reduce\" was not found in the "
									"same directory as \"%s\".\n"
									"Please check your installation.",
									my_exec_path)));
				else
					ereport(ERROR,
							(errmsg("The program \"adb_reduce\" was found by \"%s\" "
									"but was not the expected version.\n"
									"Please check your installation.",
									my_exec_path)));
			}
		} PG_CATCH();
		{
			my_reduce_path[0] = '\0';
			PG_SETMASK(&UnBlockSig);
			PG_RE_THROW();
		} PG_END_TRY();
		PG_SETMASK(&UnBlockSig);
	}

	pqsignal(SIGCHLD, SigChldHandler);
	EndSelfReduce(0, 0);
	InitCommunicationChannel();
	before_shmem_exit(EndSelfReduce, 0);

	SelfReduceID = rid;
	Assert(OidIsValid(rid));
#ifndef WIN32
	switch ((SelfReducePID = fork_process()))
	{
		case -1:
			ereport(LOG,
				 (errmsg("could not fork adb reduce launcher process: %m")));
			return 0;

		case 0:
			/* Lose the backend's on-exit routines */
			on_exit_reset();
			CloseBackendPort();
			AdbReduceLauncherMain(my_reduce_path, rid);
			break;

		default:
			CloseReducePort();
			old_context = MemoryContextSwitchTo(TopMemoryContext);
			SelfReducePort = rdc_newport(backend_reduce_fds[RDC_BACKEND_HOLD],
										 TYPE_REDUCE, SelfReduceID,
										 TYPE_BACKEND, InvalidPortId,
										 MyProcPid, NULL);
			if (GroupReduceList != NIL)
				list_free(GroupReduceList);
			GroupReduceList = NIL;
			if (!rdc_set_noblock(SelfReducePort))
				ereport(ERROR,
						(errmsg("%s", RdcError(SelfReducePort))));
			(void) MemoryContextSwitchTo(old_context);

			return GetReduceListenPort();
	}
#else
#error "Does not support fork adb_reduce on WIN32 platforms"
#endif

	/* shouldn't get here */
	return 0;
}

RdcPort *
ConnectSelfReduce(RdcPortType self_type, RdcPortId self_id,
				  RdcPortPID self_pid, RdcExtra self_extra)
{
	Assert(IsParallelWorker() || SelfReducePID != 0);
	Assert(SelfReduceListenPort != 0);
	Assert(SelfReduceID != InvalidOid);
	return rdc_connect("127.0.0.1", SelfReduceListenPort,
					   TYPE_REDUCE, SelfReduceID,
					   self_type, self_id,
					   self_pid, self_extra);
}

/*
 * Initialize socketpair for communication between backend and reduce
 *
 * Called once in the backend.
 */
static void
InitCommunicationChannel(void)
{
#ifndef WIN32
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, backend_reduce_fds))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not create socketpair to monitor backend "
				 				 "death: %m")));
#else
	/*
	 * On Windows, we use a process handle for the same purpose.
	 */
	if (DuplicateHandle(GetCurrentProcess(),
						GetCurrentProcess(),
						GetCurrentProcess(),
						&BackendHandle,
						0,
						TRUE,
						DUPLICATE_SAME_ACCESS) == 0)
		ereport(FATAL,
				(errmsg_internal("could not duplicate backend handle: error code %lu",
								 GetLastError())));
#endif	/* WIN32 */
}

static void
CloseBackendPort(void)
{
#ifndef WIN32
	if (close(backend_reduce_fds[RDC_BACKEND_HOLD]))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not close backend port in reduce process: %m")));
	backend_reduce_fds[RDC_BACKEND_HOLD] = -1;
#endif	/* WIN32 */
}

static void
CloseReducePort(void)
{
#ifndef WIN32
	if (close(backend_reduce_fds[RDC_REDUCE_HOLD]))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not close reduce in backend process: %m")));
	backend_reduce_fds[RDC_REDUCE_HOLD] = -1;
#endif	/* WIN32 */
}

static int
GetReduceListenPort(void)
{
	const char *error_msg = NULL;
	StringInfo	msg_buf;
	char		msg_type;
	int			port;

	msg_type = rdc_getmessage(SelfReducePort, 0);
	msg_buf = RdcInBuf(SelfReducePort);
	switch (msg_type)
	{
		case MSG_LISTEN_PORT:
			port = rdc_getmsgint(msg_buf, sizeof(port));
			rdc_getmsgend(msg_buf);
			break;
		case MSG_ERROR:
			error_msg = rdc_getmsgstring(msg_buf);
			rdc_getmsgend(msg_buf);
		default:
			ereport(ERROR,
					(errmsg("fail to get reduce listen port"),
					 errdetail("%s", error_msg ? error_msg : RdcError(SelfReducePort))));
			break;
	}
	SelfReduceListenPort = port;
	return port;
}

#ifndef WIN32
static void
AdbReduceLauncherMain(char *exec_path, int rid)
{
	StringInfoData	cmd;
	int				fd = 3;
	const char	   *rid_ptr = NULL,		/* -n */
				   *wfd_ptr = NULL,		/* -W */
				   *ext_ptr = NULL;		/* -E */

	/* close already opened fd as much as possible */
	while (fd < backend_reduce_fds[RDC_REDUCE_HOLD])
	{
		close(fd);
		fd++;
	}

	initStringInfo(&cmd);
	rid_ptr = cmd.data;
	appendStringInfo(&cmd, "%d", rid);
	appendStringInfoChar(&cmd, '\0');
	wfd_ptr = cmd.data + cmd.len;
	appendStringInfo(&cmd, "%d", backend_reduce_fds[RDC_REDUCE_HOLD]);
	appendStringInfoChar(&cmd, '\0');
	ext_ptr = cmd.data + cmd.len;
	appendStringInfo(&cmd, "work_mem=%d "
						   "log_min_messages=%d "
						   "log_destination=%d "
						   "redirection_done=%d "
						   "print_reduce_debug_log=%d",
						   work_mem,
						   log_min_messages,
						   Log_destination,
						   redirection_done,
						   print_reduce_debug_log);
	(void) execl(exec_path, exec_path, "-n", rid_ptr,
									   "-W", wfd_ptr,
									   "-E", ext_ptr,
									   (char *) NULL);

	fprintf(stderr, "fail to start adb_reduce: %m\n");
	exit(EXIT_FAILURE);
}
#endif

void
StartSelfReduceGroup(RdcMask *rdc_masks, int num)
{
	MemoryContext	oldcontext;
	int				i;

	Assert(GroupReduceList == NIL);
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	for (i = 0; i < num; i++)
		GroupReduceList = lappend_oid(GroupReduceList, (Oid) rdc_masks[i].rdc_rpid);
	(void) MemoryContextSwitchTo(oldcontext);

	if (rdc_send_group_rqt(SelfReducePort, rdc_masks, num) == EOF)
		ereport(ERROR,
				(errmsg("fail to send reduce group message"),
				 errdetail("%s", RdcError(SelfReducePort))));
}

void
EndSelfReduceGroup(void)
{
	if (rdc_recv_group_rsp(SelfReducePort) == EOF)
		ereport(ERROR,
				(errmsg("fail to receive reduce group response"),
				 errdetail("%s", RdcError(SelfReducePort))));
}

List *
GetReduceGroup(void)
{
	Assert(GroupReduceList != NIL);
	return GroupReduceList;
}

static int
SendPlanMsgToRemote(RdcPort *port, char msg_type, List *dest_nodes)
{
	StringInfo	msg;
	int			num;
	ListCell   *lc;

	Assert(port);
	msg = RdcMsgBuf(port);

	resetStringInfo(msg);
	rdc_beginmessage(msg, msg_type);
	num = list_length(dest_nodes);
	rdc_sendint(msg, num, sizeof(num));
	foreach (lc, dest_nodes)
		rdc_sendRdcPortID(msg, lfirst_oid(lc));
	rdc_endmessage(port, msg);

	return rdc_flush(port);
}

void
SendRejectToRemote(RdcPort *port, List *dest_nodes)
{
	AssertArg(dest_nodes);

	if (SendPlanMsgToRemote(port, MSG_PLAN_REJECT, dest_nodes) == EOF)
		ereport(ERROR,
				(errmsg("fail to send REJECT message to remote"),
				 errdetail("%s", RdcError(port))));

	adb_elog(print_reduce_debug_log, LOG,
		 "Backend send REJECT message of" PLAN_PORT_PRINT_FORMAT,
		 RdcSelfID(port));

	port->send_num++;
}

void
SendCloseToRemote(RdcPort *port, List *dest_nodes)
{
	ssize_t	rsz;
	char	buf[1];

	if (!RdcSockIsValid(port))
		return ;

	/* check validation of socket */
	rsz = recv(RdcSocket(port), buf, 1, MSG_PEEK);
	/* the peer has performed an orderly shutdown */
	if (rsz == 0)
		return ;
	else if (rsz < 0)
	{
		/* return when socket is invalid */
		if (errno != EAGAIN && errno != EWOULDBLOCK)
			return ;
	}

	if (SendPlanMsgToRemote(port, MSG_PLAN_CLOSE, dest_nodes) == EOF)
		ereport(ERROR,
				(errmsg("fail to send CLOSE message to remote"),
				 errdetail("%s", RdcError(port))));

	 adb_elog(print_reduce_debug_log, LOG,
		  "Backend send CLOSE message of" PLAN_PORT_PRINT_FORMAT,
		  RdcSelfID(port));

	port->send_num++;
	RdcEndStatus(port) |= RDC_END_CLOSE;
}

void
SendEofToRemote(RdcPort *port, List *dest_nodes)
{
	AssertArg(dest_nodes);

	if (SendPlanMsgToRemote(port, MSG_EOF, dest_nodes) == EOF)
		ereport(ERROR,
				(errmsg("fail to send EOF message to remote"),
				 errdetail("%s", RdcError(port))));

	adb_elog(print_reduce_debug_log, LOG,
		 "Backend send EOF message of" PLAN_PORT_PRINT_FORMAT,
		 RdcSelfID(port));

	port->send_num++;
	RdcEndStatus(port) |= RDC_END_EOF;
}

void
SendSlotToRemote(RdcPort *port, List *dest_nodes, TupleTableSlot *slot)
{
	StringInfo		msg;
	ListCell	   *lc;
	int				num;
	MinimalTuple	tup;
	char		   *tupbody;
	unsigned int	tupbodylen;
	bool			need_free_tuple;

	AssertArg(port);
	if (!dest_nodes)
		return ;

	AssertArg(slot);

	tup = fetch_slot_message(slot, &need_free_tuple);
	/* the part of the MinimalTuple we'll write: */
	tupbody = (char *) tup + MINIMAL_TUPLE_DATA_OFFSET;
	tupbodylen = tup->t_len - MINIMAL_TUPLE_DATA_OFFSET;
	msg = RdcMsgBuf(port);

	resetStringInfo(msg);
	rdc_beginmessage(msg, MSG_P2R_DATA);
	rdc_sendint(msg, tupbodylen, sizeof(tupbodylen));
	rdc_sendbytes(msg, (const char * ) tupbody, tupbodylen);
	num = list_length(dest_nodes);
	rdc_sendint(msg, num, sizeof(num));
	foreach (lc, dest_nodes)
		rdc_sendRdcPortID(msg, lfirst_oid(lc));
	rdc_endmessage(port, msg);

	if (rdc_flush(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to send tuple to remote"),
				 errdetail("%s", RdcError(port))));

	if (need_free_tuple)
		pfree(tup);

	port->send_num++;
}

TupleTableSlot *
GetSlotFromRemote(RdcPort *port, TupleTableSlot *slot,
				  Oid *slot_oid, Oid *eof_oid,
				  List **closed_remote)
{
	StringInfo	msg;
	int			msg_type;
	int			msg_len;
	int			sv_cursor;
	bool		sv_noblock;
	RdcPortId	rid;

	AssertArg(port);
	AssertArg(slot);

	msg = RdcInBuf(port);
	sv_noblock = port->noblock;
	sv_cursor = msg->cursor;

	if ((msg_type = rdc_getbyte(port)) == EOF ||
		rdc_getbytes(port, sizeof(msg_len)) == EOF)
		goto _eof_got;
	msg_len = rdc_getmsgint(msg, sizeof(msg_len));
	/* total data */
	msg_len -= sizeof(msg_len);
	if (rdc_getbytes(port, msg_len) == EOF)
		goto _eof_got;

	port->recv_num++;
	switch (msg_type)
	{
		case MSG_R2P_DATA:
			{
				const char	   *data;
				char		   *tupbody;
				MinimalTuple	tuple;
				unsigned int	tuplen;

				/* reduce id while slot come */
				rid = rdc_getmsgRdcPortID(msg);
				elog(DEBUG1, "fetch tuple from REDUCE " PORTID_FORMAT, rid);
				msg_len -= sizeof(rid);
				data = rdc_getmsgbytes(msg, msg_len);
				rdc_getmsgend(msg);

				tuplen = msg_len + MINIMAL_TUPLE_DATA_OFFSET;
				tuple = (MinimalTuple) MemoryContextAlloc(slot->tts_mcxt, tuplen);
				tupbody = (char *) tuple + MINIMAL_TUPLE_DATA_OFFSET;
				tuple->t_len = tuplen;
				memcpy(tupbody, data, msg_len);

				if (slot_oid)
					*slot_oid = (Oid) rid;
				return ExecStoreMinimalTuple(tuple, slot, true);
			}
		case MSG_EOF:
			{
				/* reduce id while EOF message come */
				rid = rdc_getmsgRdcPortID(msg);
				rdc_getmsgend(msg);
				if (eof_oid)
					*eof_oid = (Oid) rid;
			}
			break;
		case MSG_PLAN_REJECT:
		case MSG_PLAN_CLOSE:
			{
				rid = rdc_getmsgRdcPortID(msg);
				rdc_getmsgend(msg);

				if (closed_remote)
					*closed_remote = list_append_unique_oid(*closed_remote, (Oid) rid);
			}
			break;
		default:
			ereport(ERROR,
					(errmsg("unexpected message type '%d' from self reduce",
							msg_type),
					 errdetail("%s", RdcError(port))));
	}

	return ExecClearTuple(slot);

_eof_got:
	if (sv_noblock)
	{
		msg->cursor = sv_cursor;
		return NULL;		/* not enough data */
	}

	ereport(ERROR,
			(errmsg("fail to fetch slot from self reduce"),
			 errdetail("%s", RdcError(port))));
	return NULL;	/* keep compiler quiet */
}

Size EstimateReduceInfoSpace(void)
{
	return sizeof(SelfReduceID) +
		   sizeof(SelfReduceListenPort) +
		   sizeof(int) +
		   sizeof(Oid)*list_length(GroupReduceList);
}

void SerializeReduceInfo(Size maxsize, char *ptr)
{
	ListCell *lc;
	if(maxsize < sizeof(SelfReduceID) +
				 sizeof(SelfReduceListenPort) +
				 sizeof(int) +
				 sizeof(Oid)*list_length(GroupReduceList))
	{
		elog(ERROR, "not enough space to serialize reduce info");
	}
	*(RdcPortId*)ptr = SelfReduceID;			ptr += sizeof(SelfReduceID);
	*(int*)ptr = SelfReduceListenPort;			ptr += sizeof(SelfReduceListenPort);
	*(int*)ptr = list_length(GroupReduceList);	ptr += sizeof(int);
	foreach(lc, GroupReduceList)
	{
		*(Oid*)ptr = lfirst_oid(lc);
		ptr += sizeof(Oid);
	}
}

void RestoreReduceInfo(char *start_addr)
{
	int count;
	SelfReduceID = *(RdcPortId*)start_addr;			start_addr += sizeof(SelfReduceID);
	SelfReduceListenPort = *((int*)start_addr);		start_addr += sizeof(SelfReduceListenPort);

	count = *(int*)start_addr;						start_addr += sizeof(int);
	GroupReduceList = NIL;
	for(;count>0;--count)
	{
		GroupReduceList = lappend_oid(GroupReduceList, *(Oid*)start_addr);
		start_addr += sizeof(Oid);
	}
}
