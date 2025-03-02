/*-------------------------------------------------------------------------
 *
 * noblock.c
 *	  set a file descriptor as blocking or non-blocking
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/port/noblock.c
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"

#include <fcntl.h>
#if defined (WITH_RDMA) || defined(WITH_REDUCE_RDMA)
#include "rdma/adb_rsocket.h"
#endif

/*
 * Put socket into nonblock mode.
 * Returns true on success, false on failure.
 */
bool
pg_set_noblock(pgsocket sock)
{
#if !defined(WIN32)
	int			flags;

	flags = fcntl(sock, F_GETFL);
	if (flags < 0)
		return false;
	if (fcntl(sock, F_SETFL, (flags | O_NONBLOCK)) == -1)
		return false;
	return true;
#else
	unsigned long ioctlsocket_ret = 1;

	/* Returns non-0 on failure, while fcntl() returns -1 on failure */
	return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}

#if defined (WITH_RDMA) || defined(WITH_REDUCE_RDMA)
bool
pg_set_rnoblock(pgsocket sock)
{
	int			flags;

	flags = adb_rfcntl(sock, F_GETFL);
	if (flags < 0)
		return false;
	if (adb_rfcntl(sock, F_SETFL, (flags | O_NONBLOCK)) == -1)
		return false;
	return true;
}

bool
pg_set_rblock(pgsocket sock)
{
#if !defined(WIN32)
	int			flags;

	flags = adb_rfcntl(sock, F_GETFL);
	if (flags < 0)
		return false;
	if (adb_rfcntl(sock, F_SETFL, (flags & ~O_NONBLOCK)) == -1)
		return false;
	return true;
#else
	unsigned long ioctlsocket_ret = 0;

	/* Returns non-0 on failure, while fcntl() returns -1 on failure */
	return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}
#endif

/*
 * Put socket into blocking mode.
 * Returns true on success, false on failure.
 */
bool
pg_set_block(pgsocket sock)
{
#if !defined(WIN32)
	int			flags;

	flags = fcntl(sock, F_GETFL);
	if (flags < 0)
		return false;
	if (fcntl(sock, F_SETFL, (flags & ~O_NONBLOCK)) == -1)
		return false;
	return true;
#else
	unsigned long ioctlsocket_ret = 0;

	/* Returns non-0 on failure, while fcntl() returns -1 on failure */
	return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}
