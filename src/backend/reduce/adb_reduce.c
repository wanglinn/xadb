#include <unistd.h>
#include <fcntl.h>

#include "postgres.h"
#include "postmaster/fork_process.h"
#include "reduce/adb_reduce.h"
#include "storage/ipc.h"


#ifndef WIN32
static int backend_reduce_fds[2] = {-1, -1};
#else
/* Process handle of backend used for the same purpose on Windows */
static HANDLE	BackendHandle;
#endif	/* WIN32 */

#define RDC_BACKEND_HOLD	0
#define RDC_REDUCE_HOLD		1

static void InitCommunicationChannel(void);
static void CloseBackendPort(void);
static void CloseReducePort(void);
static int  GetReduceListenPort(void);
static void AdbReduceLauncherMain(int argc, const char *argv[]);

/*
 * Main entry point for adb reduce launcher process, to be called from the
 * backend.
 */
int
StartAdbReduceLauncher(void)
{
	pid_t		AdbReducePID;

	InitCommunicationChannel();
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
			AdbReduceLauncherMain(0, NULL);
			break;

		default:
			CloseReducePort();
			return GetReduceListenPort();
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

	/*
	 * Create a socket pair. Backend holds the write end of the pipe open
	 * (RDC_BACKEND_HOLD), and adb reduce hold the read end. Broker can pass
	 * the read file descriptor to select() to wake up in case backend
	 * dies, or check for backend death with a (read() == 0). Broker must
	 * close the write end as soon as possible after forking, because EOF
	 * won't be signaled in the read end until all processes have closed the
	 * write fd. That is taken care of in CloseBackendPorts().
	 */
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, backend_reduce_fds))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not create socketpair to monitor backend "
				 				 "death: %m")));

	/*
	 * Set O_NONBLOCK to allow testing for the fd's presence with a read()
	 * call.
	 */
	if (fcntl(backend_reduce_fds[RDC_REDUCE_HOLD], F_SETFL, O_NONBLOCK))
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg_internal("could not set backend death monitoring socketpair "
				 				 "to nonblocking mode: %m")));
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

	return port;
}

static void
AdbReduceLauncherMain(int argc, const char *argv[])
{

}

