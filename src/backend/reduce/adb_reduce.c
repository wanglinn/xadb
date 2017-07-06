#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/fork_process.h"
#include "postmaster/syslogger.h"
#include "reduce/adb_reduce.h"
#include "reduce/rdc_msg.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

extern bool redirection_done;

/*
 * 1. Used to connect local Reduce for local plan node.
 * 2. Used to be connected for other Reduces.
 */
int		AdbRdcListenPort = 0;

#ifndef WIN32
static int backend_reduce_fds[2] = {-1, -1};
#else
/* Process handle of backend used for the same purpose on Windows */
static HANDLE	BackendHandle;
#endif	/* WIN32 */

static pid_t	AdbReducePID = 0;
static RdcPort *backend_hold_port = NULL;

#define RDC_BACKEND_HOLD	0
#define RDC_REDUCE_HOLD		1

static void CleanUpReduce(int code, Datum arg);
static void InitCommunicationChannel(void);
static void CloseBackendPort(void);
static void CloseReducePort(void);
static int  GetReduceListenPort(void);
static void AdbReduceLauncherMain(int rid);

static void
CleanUpReduce(int code, Datum arg)
{
	if (AdbReducePID != 0)
	{
		int ret = kill(AdbReducePID, SIGTERM);
		if (!(ret == 0 || errno == ESRCH))
			ereport(ERROR,
					(errmsg("fail to terminate adb reduce subprocess")));
		AdbReducePID = 0;
 	}
	rdc_freeport(backend_hold_port);
	backend_hold_port = NULL;
	AdbRdcListenPort = 0;
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
StartAdbReduceLauncher(int rid)
{
	MemoryContext	old_context;

	pqsignal(SIGCHLD, SigChldHandler);
	CleanUpReduce(0, 0);
	InitCommunicationChannel();
	on_proc_exit(CleanUpReduce, 0);

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
			/* TODO: be sure ID of local Reduce */
			backend_hold_port = rdc_newport(backend_reduce_fds[RDC_BACKEND_HOLD],
											TYPE_REDUCE, InvalidPortId,
											TYPE_BACKEND, InvalidPortId);
			(void) MemoryContextSwitchTo(old_context);
			AdbRdcListenPort = GetReduceListenPort();
			return AdbRdcListenPort;
	}

	/* shouldn't get here */
	return 0;
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

int
StartAdbReduceGroup(const char *hosts[], int ports[], int num)
{
	if (rdc_send_group_rqt(backend_hold_port, num, hosts, ports) == EOF ||
		rdc_recv_group_rsp(backend_hold_port) == EOF)
		ereport(ERROR,
				(errmsg("fail to start adb reduce group"),
				 errhint("%s", RdcError(backend_hold_port))));
	return 0;
}

int
SendTupleToReduce(RdcPort *port, const Oid nodeIds[], int num,
				  const char *data, int datalen)
{
	StringInfoData	buf;
	int				i;

	AssertArg(port);
	Assert(num > 0);
	if (data)
	{
		Assert(datalen > 0);
		rdc_beginmessage(&buf, RDC_P2R_DATA);
	} else
	{
		Assert(datalen == 0);
		rdc_beginmessage(&buf, RDC_EOF_MSG);
	}
	rdc_sendint(&buf, num, sizeof(num));
	for (i = 0; i < num; i++)
		rdc_sendint(&buf, nodeIds[i], sizeof(nodeIds[i]));
	rdc_sendbytes(&buf, data, datalen);
	rdc_endmessage(port, &buf);

	if (rdc_flush(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to send tuple to adb reduce"),
				 errhint("%s", RdcError(port))));

	return 0;
}

void*
GetTupleFromReduce(RdcPort *port)
{
	return NULL;
}

