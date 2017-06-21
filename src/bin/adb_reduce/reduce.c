#include "reduce.h"

#include <sys/types.h>
#include <sys/socket.h>

#define RDC_PARENT_HOLD		0
#define RDC_CHILD_HOLD		1

static int sv[2];			/* for socketpair */
static int listend_port;	/* get from Reduce process */

int StartAdbReduce(void)
{
	pid_t		AdbReducePID;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
	{
		fprintf(stderr, "Fail to create a socketpair: %m");
		exit(EXIT_FAILURE);
	}

	switch ((AdbReducePID = fork_process()))
	{
		case -1:
			fprintf(stderr, "could not fork adb reduce process: %m");
			return 0;

		case 0:
			closesocket(sv[RDC_PARENT_HOLD]);
			AdbReduceMain(sv[RDC_CHILD_HOLD]);
			break;

		default:
			closesocket(sv[1]);
			return (int) AdbReducePID;
	}

	/* shouldn't get here */
	return 0;
}

static void
AdbReduceMain(int parent_sock)
{
	/* TODO: */
}

/*
 * GetReduceListenPort -- Get reduce listen port from socketpair sv[0]
 */
int
GetReduceListenPort(void)
{
	return 0;
}

/*
 * SendStartupPacket -- Send start packet which contains whole Reduce
 * cluster info.
 */
int
SendStartupPacket(void)
{
	return 0;
}

/*
 * IsAdbReduceReadyRun -- Check adb Reduce is ready or not
 */
bool
IsAdbReduceReadyRun(void)
{
	return false;
}

/*
 *
 */
int
ConnectAdbReduce(void)
{
	int		fd = PGINVALID_SOCKET;

	if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		return PGINVALID_SOCKET;
}
