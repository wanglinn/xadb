#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "postgres.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "postmaster/fork_process.h"
#include "postmaster/syslogger.h"
#include "reduce/adb_reduce.h"
#include "reduce/rdc_msg.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

extern bool redirection_done;

#ifndef WIN32
static int backend_reduce_fds[2] = {-1, -1};
#else
/* Process handle of backend used for the same purpose on Windows */
static HANDLE	BackendHandle;
#endif	/* WIN32 */

static RdcPortId	SelfReduceID = InvalidOid;
static int			RdcListenPort = 0;
static pid_t		AdbReducePID = 0;
static RdcPort	   *backend_hold_port = NULL;

#define RDC_BACKEND_HOLD	0
#define RDC_REDUCE_HOLD		1

static void InitCommunicationChannel(void);
static void CloseBackendPort(void);
static void CloseReducePort(void);
static int  GetReduceListenPort(void);
static void AdbReduceLauncherMain(int rid);

void
EndSelfReduce(int code, Datum arg)
{
	if (AdbReducePID != 0)
	{
		int ret = kill(AdbReducePID, SIGTERM);
		bool no_error = DatumGetBool(arg);
		if (!(ret == 0 || errno == ESRCH))
		{
			if (no_error)
			{
				ereport(ERROR,
					(errmsg("fail to terminate adb reduce subprocess")));
			}
		}
		AdbReducePID = 0;
 	}
	rdc_freeport(backend_hold_port);
	backend_hold_port = NULL;
	RdcListenPort = 0;
	SelfReduceID = InvalidOid;
	cancel_before_shmem_exit(EndSelfReduce, 0);
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

	pqsignal(SIGCHLD, SigChldHandler);
	EndSelfReduce(0, 0);
	InitCommunicationChannel();
	before_shmem_exit(EndSelfReduce, 0);

	SelfReduceID = rid;
	Assert(OidIsValid(rid));
	switch ((AdbReducePID = fork_process()))
	{
		case -1:
			ereport(LOG,
				 (errmsg("could not fork adb reduce launcher process: %m")));
			return 0;

		case 0:
			/* Lose the backend's on-exit routines */
			on_exit_reset();
			CloseBackendPort();
			AdbReduceLauncherMain(rid);
			break;

		default:
			CloseReducePort();
			old_context = MemoryContextSwitchTo(TopMemoryContext);
			backend_hold_port = rdc_newport(backend_reduce_fds[RDC_BACKEND_HOLD],
											TYPE_REDUCE, SelfReduceID,
											TYPE_BACKEND, InvalidPortId);
			(void) MemoryContextSwitchTo(old_context);

			return GetReduceListenPort();
	}

	/* shouldn't get here */
	return 0;
}

RdcPort *
ConnectSelfReduce(RdcPortType self_type, RdcPortId self_id)
{
	Assert(AdbReducePID != 0);
	Assert(RdcListenPort != 0);
	Assert(SelfReduceID != InvalidOid);
	return rdc_connect("127.0.0.1", RdcListenPort,
					   TYPE_REDUCE, SelfReduceID,
					   self_type, self_id);
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
	int		port = 0;
	char	firstchar;
	const char *error_msg = NULL;

	firstchar = rdc_getmessage(backend_hold_port, 0);
	switch (firstchar)
	{
		case RDC_LISTEN_PORT:
			port = rdc_getmsgint(RdcInBuf(backend_hold_port), sizeof(port));
			rdc_getmsgend(RdcInBuf(backend_hold_port));
			break;
		case RDC_ERROR_MSG:
			error_msg = rdc_getmsgstring(RdcInBuf(backend_hold_port));
			rdc_getmsgend(RdcInBuf(backend_hold_port));
		default:
			ereport(ERROR,
					(errmsg("fail to get reduce listen port"),
					 errhint("%s", error_msg ? error_msg : RdcError(backend_hold_port))));
			break;
	}
	RdcListenPort = port;
	return port;
}

static void
AdbReduceLauncherMain(int rid)
{
	StringInfoData	cmd;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "exec \"adb_reduce\" -n %d -W %d",
		rid, backend_reduce_fds[RDC_REDUCE_HOLD]);

	appendStringInfo(&cmd, " -E \""
						   "work_mem=%d "
						   "log_min_messages=%d "
						   "log_destination=%d "
						   "redirection_done=%d\"",
						   work_mem,
						   log_min_messages,
						   Log_destination,
						   redirection_done);

	(void) execl("/bin/sh", "/bin/sh", "-c", cmd.data, (char *) NULL);

	ereport(ERROR,
			(errmsg("fail to start adb_reduce: %m")));
}

void
StartSelfReduceGroup(RdcMask *rdc_masks, int num)
{
	if (rdc_send_group_rqt(backend_hold_port, rdc_masks, num) == EOF)
		ereport(ERROR,
				(errmsg("fail to send reduce group message"),
				 errdetail("%s", RdcError(backend_hold_port))));
}

void
EndSelfReduceGroup(void)
{
	if (rdc_recv_group_rsp(backend_hold_port) == EOF)
		ereport(ERROR,
				(errmsg("fail to receive reduce group response"),
				 errdetail("%s", RdcError(backend_hold_port))));
}

void
SendSlotToRemote(RdcPort *port, List *destNodes, TupleTableSlot *slot)
{
	StringInfoData  msg;

	AssertArg(port);
	if (TupIsNull(slot))
	{
#ifdef DEBUG_ADB
		elog(LOG, "Backend send EOF message of plan %ld", RdcSelfID(port));
#endif
		rdc_beginmessage(&msg, RDC_EOF_MSG);
	} else if (destNodes)
	{
		ListCell	   *lc;
		int				num;
		MinimalTuple	tup;
		int				len;

		AssertArg(slot);
		tup = ExecFetchSlotMinimalTuple(slot);
		len = (int) GetMemoryChunkSpace(tup);
		rdc_beginmessage(&msg, RDC_P2R_DATA);
		rdc_sendint(&msg, len, sizeof(len));
		rdc_sendbytes(&msg, (const char * ) tup, len);
		num = list_length(destNodes);
		rdc_sendint(&msg, num, sizeof(num));
		foreach (lc, destNodes)
			rdc_sendRdcPortID(&msg, lfirst_oid(lc));
	} else
	{
		return ;
	}
	rdc_endmessage(port, &msg);
	if (rdc_flush(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to send tuple to remote"),
				 errdetail("%s", RdcError(port))));
}

TupleTableSlot *
GetSlotFromRemote(RdcPort *port, TupleTableSlot *slot, bool *eof)
{
	int			msg_type;
	StringInfo	msg;
	int			sv_cursor;
	bool		sv_noblock;

	AssertArg(port);
	AssertArg(slot);

	msg = RdcInBuf(port);
	sv_noblock = port->noblock;
	sv_cursor = msg->cursor;

	msg_type = rdc_getbyte(port);
	if (msg_type == EOF)
		goto _eof_got;

	switch (msg_type)
	{
		case RDC_R2P_DATA:
			{
				const char	   *data;
				int				datalen;
				MinimalTuple	tup;
#ifdef DEBUG_ADB
				RdcPortId		rid;
#endif

				/* data length */
				if (rdc_getbytes(port, sizeof(datalen)) == EOF)
					goto _eof_got;
				datalen = rdc_getmsgint(msg, sizeof(datalen));

				/* total data */
				datalen -= sizeof(datalen);
				if (rdc_getbytes(port, datalen) == EOF)
					goto _eof_got;

#ifdef DEBUG_ADB
				/* reduce id while slot comes from */
				rid = rdc_getmsgRdcPortID(msg);
				elog(LOG, "Fetch tuple from REDUCE %ld", rid);
				datalen -= sizeof(rid);
#endif
				data = rdc_getmsgbytes(msg, datalen);
				rdc_getmsgend(msg);

				tup = (MinimalTuple) MemoryContextAlloc(slot->tts_mcxt, datalen);
				memcpy(tup, data, datalen);
				return ExecStoreMinimalTuple(tup, slot, true);
			}
		case RDC_EOF_MSG:
			if (eof)
				*eof = true;
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
