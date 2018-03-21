/*-------------------------------------------------------------------------
 *
 * rdc_comm.c
 *	  Communication functions for Reduce
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/reduce/rdc_comm.c
 *
 * NOTES
 *	  RdcPort is port for communication between Reduce and other Reduce,
 *	  generate a new RdcPort if connect to or be connected from other Reduce.
 *	  In this case, RdcPort will have null "next" brother RdcPort.
 *
 *	  RdcPort is also for communication between Reduce and Plan node, in this
 *	  case, RdcPort is contained by PlanPort with the same RdcPortId. Sometimes,
 *	  RdcPort has one or more brother with the same RdcPortId, it means there
 *	  are parallel-works for the Plan node.
 *-------------------------------------------------------------------------
 */
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#if defined(RDC_FRONTEND)
#include "rdc_globals.h"
#else
#include "postgres.h"
#include "miscadmin.h"

extern bool print_reduce_debug_log;
#endif

#include "reduce/rdc_comm.h"
#include "reduce/rdc_msg.h"
#include "utils/memutils.h"

pgsocket MyBossSock = PGINVALID_SOCKET;

static WaitEVSet RdcWaitSet = NULL;

#define RDC_EXTRA_SIZE		64
#define RDC_ERROR_SIZE		128
#define RDC_MSG_SIZE		256
#define RDC_BUFFER_SIZE		8192

#define IdxIsEven(idx)		((idx) % 2 == 0)		/* even number */
#define IdxIsOdd(idx)		((idx) % 2 == 1)		/* odd nnumber */

static int rdc_wait_timed(int forRead, int forWrite, RdcPort *port, int timeout);
static RdcPort *rdc_connect_start(const char *host, uint32 port,
					RdcPortType peer_type, RdcPortId peer_id,
					RdcPortType self_type, RdcPortId self_id,
					RdcPortPID self_pid, RdcExtra self_extra);
static int rdc_connect_complete(RdcPort *port);
static ssize_t rdc_secure_read(RdcPort *port, void *ptr, size_t len, int flags);
static int rdc_flush_buffer(RdcPort *port, StringInfo buf, bool block);
static int internal_put_buffer(RdcPort *port, const char *s, size_t len, bool enlarge);
static int internal_puterror(RdcPort *port, const char *s, size_t len, bool replace);

static RdcPollingStatusType internal_recv_startup_rqt(RdcPort *port, int expected_ver);
static RdcPollingStatusType internal_recv_startup_rsp(RdcPort *port, RdcPortType expceted_type,
					RdcPortId expceted_id);
static int connect_nodelay(RdcPort *port);
static int connect_keepalive(RdcPort *port);
static int connect_close_on_exec(RdcPort *port);
static void drop_connection(RdcPort *port, bool flushInput);

const char *
rdc_type2string(RdcPortType type)
{
	switch (type)
	{
		case TYPE_LOCAL:
			return "LOCAL";

		case TYPE_BACKEND:
			return "BACKEND";

		case TYPE_PLAN:
			return "PLAN";

		case TYPE_REDUCE:
			return "REDUCE";

		default:
			return "UNKNOWN";
	}
	return "UNKNOWN";
}

BossStatus
BossNowStatus(void)
{
	char		c;
	ssize_t		rc;

	/*
	 * Always return BOSS_IS_WORKING if MyBossSock
	 * is not set
	 */
	if (MyBossSock == PGINVALID_SOCKET)
		return BOSS_IS_WORKING;

_re_recv:
	rc = recv(MyBossSock, &c, 1, MSG_PEEK);
	/* the peer has performed an orderly shutdown */
	if (rc == 0)
		return BOSS_IS_SHUTDOWN;
	else
	/* receive CLOSE message request */
	if (rc > 0 && c == MSG_BACKEND_CLOSE)
		return BOSS_WANT_QUIT;
	else
	if (rc < 0)
	{
		if (errno == EINTR)
			goto _re_recv;

		if (errno != EAGAIN &&
			errno != EWOULDBLOCK)
			return BOSS_IN_TROUBLE;
	}

	return BOSS_IS_WORKING;
}

void
RdcPortStats(RdcPort *port)
{
#if !defined(RDC_FRONTEND)
	if (port)
	{
		adb_elog(print_reduce_debug_log, LOG,
			 "[%s " PORTID_FORMAT"] -> [%s " PORTID_FORMAT "] statistics:"
			 "time to live " INT64_FORMAT
			 " seconds, send " UINT64_FORMAT
			 ", recv " UINT64_FORMAT,
			 RdcSelfTypeStr(port), RdcSelfID(port),
			 RdcPeerTypeStr(port), RdcPeerID(port),
			 time(NULL) - port->create_time,
			 port->send_num,
			 port->recv_num);
	}
#endif
}

RdcPort *
rdc_newport(pgsocket sock,
			RdcPortType peer_type, RdcPortId peer_id,
			RdcPortType self_type, RdcPortId self_id,
			RdcPortPID self_pid, RdcExtra self_extra)
{
	RdcPort		   *rdc_port = NULL;

	rdc_port = (RdcPort *) palloc0(sizeof(*rdc_port));
	RdcSocket(rdc_port) = sock;
	RdcPeerType(rdc_port) = peer_type;
	RdcPeerID(rdc_port) = peer_id;
	RdcPeerPID(rdc_port) = -1;
	RdcSelfType(rdc_port) = self_type;
	RdcSelfID(rdc_port) = self_id;
	RdcSelfPID(rdc_port) = self_pid;
#if !defined(RDC_FRONTEND)
	rdc_port->create_time = time(NULL);
#endif
#ifdef DEBUG_ADB
	RdcPeerHost(rdc_port) = NULL;
	RdcPeerPort(rdc_port) = NULL;
	RdcSelfHost(rdc_port) = NULL;
	RdcSelfPort(rdc_port) = NULL;
 #endif
	initStringInfoExtend(RdcMsgBuf(rdc_port), RDC_MSG_SIZE);
	initStringInfoExtend(RdcPeerExtra(rdc_port), RDC_EXTRA_SIZE);
	initStringInfoExtend(RdcSelfExtra(rdc_port), RDC_EXTRA_SIZE);
	initStringInfoExtend(RdcInBuf(rdc_port), RDC_BUFFER_SIZE);
	initStringInfoExtend(RdcOutBuf(rdc_port), RDC_BUFFER_SIZE);
	initStringInfoExtend(RdcOutBuf2(rdc_port), RDC_BUFFER_SIZE);
	initStringInfoExtend(RdcErrBuf(rdc_port), RDC_ERROR_SIZE);

	appendStringInfoStringInfo(RdcSelfExtra(rdc_port), self_extra);

	return rdc_port;
}

void
rdc_freeport(RdcPort *port)
{
	RdcPort *next = NULL;

	while (port)
	{
		next = RdcNext(port);

		RdcPortStats(port);
		if (RdcSockIsValid(port))
		{
			shutdown(RdcSocket(port), SHUT_RDWR);
			closesocket(RdcSocket(port));
		}
		if (port->addrs)
		{
			freeaddrinfo(port->addrs);
			port->addrs = port->addr_cur = NULL;
		}
		pfree(port->self_attr.rpa_extra.data);
		pfree(port->peer_attr.rpa_extra.data);
		pfree(port->msg_buf.data);
		pfree(port->in_buf.data);
		pfree(port->out_buf.data);
		pfree(port->out_buf2.data);
		pfree(port->err_buf.data);
#ifdef DEBUG_ADB
		safe_pfree(RdcPeerHost(port));
		safe_pfree(RdcPeerPort(port));
		safe_pfree(RdcSelfHost(port));
		safe_pfree(RdcSelfPort(port));
#endif
		pfree(port);
		port = next;
	}
}

void
rdc_resetport(RdcPort *port)
{
	if (port)
	{
		resetStringInfo(RdcInBuf(port));
		rdc_flush(port);
		resetStringInfo(RdcOutBuf(port));
		resetStringInfo(RdcErrBuf(port));
	}
}

static int
rdc_wait_timed(int forRead, int forWrite, RdcPort *port, int timeout)
{
	int			nready;
	EventType	wait_events = WAIT_NONE;
	WaitEventElt *wee = NULL;

	if (RdcWaitSet == NULL)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		RdcWaitSet = makeWaitEVSetExtend(2);
		(void) MemoryContextSwitchTo(oldcontext);
	}

	resetWaitEVSet(RdcWaitSet);
	if (forRead)
		wait_events |= WT_SOCK_READABLE;
	if (forWrite)
		wait_events |= WT_SOCK_WRITEABLE;
	addWaitEventBySock(RdcWaitSet, RdcSocket(port), wait_events);
	nready = execWaitEVSet(RdcWaitSet, timeout);
	wee = nextWaitEventElt(RdcWaitSet);
	if (nready < 0)
	{
		rdc_puterror(port,
					 "something wrong while waiting for read/write "
					 "event on socket of" RDC_PORT_PRINT_FORMAT ": %m",
					 RDC_PORT_PRINT_VALUE(port));
		return EOF;
	} else if (WEEHasError(wee))
	{
		char *err_msg = "null";
#if defined(WAIT_USE_POLL)
		err_msg = (WEERetEvent(wee) & POLLERR) ? "POLLERR" :
				  (WEERetEvent(wee) & POLLHUP) ? "POLLHUP":
				  "POLLNVAL";
#endif
		rdc_puterror(port,
					 "something wrong while waiting for read/write "
					 "event on socket of" RDC_PORT_PRINT_FORMAT ": %s",
					 RDC_PORT_PRINT_VALUE(port), err_msg);

		return EOF;
	}

	return 0;
}

static RdcPort *
rdc_connect_start(const char *host, uint32 port,
				  RdcPortType peer_type, RdcPortId peer_id,
				  RdcPortType self_type, RdcPortId self_id,
				  RdcPortPID self_pid, RdcExtra self_extra)
{
	RdcPort			   *rdc_port = NULL;
	int					ret;
	struct addrinfo		hint;
	char				portstr[NI_MAXSERV];

	rdc_port = rdc_newport(PGINVALID_SOCKET,
						   peer_type, peer_id,
						   self_type, self_id,
						   self_pid, self_extra);

	snprintf(portstr, sizeof(portstr), "%u", port);

	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_INET;
	hint.ai_flags = AI_PASSIVE;

	/* Use getaddrinfo() to resolve the address */
	ret = getaddrinfo(host, portstr, &hint, &(rdc_port->addrs));
	if (ret || !rdc_port->addrs)
	{
		if (rdc_port->addrs)
			freeaddrinfo(rdc_port->addrs);

		rdc_puterror(rdc_port,
					 "could not resolve address %s:%d: %s",
					 host, port, gai_strerror(ret));
		return rdc_port;
	}
	rdc_port->addr_cur = rdc_port->addrs;
	RdcStatus(rdc_port) = RDC_CONNECTION_NEEDED;
#ifdef DEBUG_ADB
	RdcPeerHost(rdc_port) = pstrdup(host);
	RdcPeerPort(rdc_port) = pstrdup(portstr);
#endif

	(void) rdc_connect_poll(rdc_port);

	return rdc_port;
}

static int
rdc_connect_complete(RdcPort *port)
{
	RdcPollingStatusType	flag = RDC_POLLING_WRITING;
	int						finish_time = -1;

	for (;;)
	{
		switch (flag)
		{
			case RDC_POLLING_OK:
				return 1;		/* success! */

			case RDC_POLLING_READING:
				if (rdc_wait_timed(1, 0, port, finish_time))
				{
					RdcStatus(port) = RDC_CONNECTION_BAD;
					return 0;
				}
				break;

			case RDC_POLLING_WRITING:
				if (rdc_wait_timed(0, 1, port, finish_time))
				{
					RdcStatus(port) = RDC_CONNECTION_BAD;
					return 0;
				}
				break;

			default:
				/* Just in case we failed to set it in rdc_connect_poll */
				RdcStatus(port) = RDC_CONNECTION_BAD;
				return 0;
		}

		/*
		 * Now try to advance the state machine.
		 */
		flag = rdc_connect_poll(port);
	}
}

RdcPort *
rdc_connect(const char *host, uint32 port,
			RdcPortType peer_type, RdcPortId peer_id,
			RdcPortType self_type, RdcPortId self_id,
			RdcPortPID self_pid, RdcExtra self_extra)
{
	RdcPort *rdc_port = NULL;

	rdc_port = rdc_connect_start(host, port,
								 peer_type, peer_id,
								 self_type, self_id,
								 self_pid, self_extra);
	if (!IsRdcPortError(rdc_port))
		(void) rdc_connect_complete(rdc_port);

	return rdc_port;
}

/*
 * rdc_connect_poll -- poll an asynchronous connection.
 *
 * Returns a RdcPollingStatusType.
 * Before calling this function, use select(2)/poll(2) to determine when data
 * has arrived.
 */
RdcPollingStatusType
rdc_connect_poll(RdcPort *port)
{
	int 		optval;

	if (port == NULL)
		return RDC_POLLING_FAILED;

	/* Get the new data */
	switch (RdcStatus(port))
	{
		/*
		 * We really shouldn't have been polled in these two cases, but we
		 * can handle it.
		 */
		case RDC_CONNECTION_BAD:
			return RDC_POLLING_FAILED;
		case RDC_CONNECTION_OK:
			return RDC_POLLING_OK;

		/* These are reading states */
		case RDC_CONNECTION_AWAITING_RESPONSE:
		case RDC_CONNECTION_ACCEPT:
		case RDC_CONNECTION_AUTH_OK:
			{
				/* Load waiting data */
				int 		n = rdc_recv(port);

				if (n == EOF)
					goto error_return;
				if (n == 0)
					return RDC_POLLING_READING;

				break;
			}

			/* These are writing states, so we just proceed. */
		case RDC_CONNECTION_STARTED:
		case RDC_CONNECTION_MADE:
		case RDC_CONNECTION_SENDING_RESPONSE:
			break;

		case RDC_CONNECTION_ACCEPT_NEED:
		case RDC_CONNECTION_NEEDED:
			break;

		default:
			rdc_puterror(port,
						 "invalid connection state, "
						 "probably indicative of memory corruption");
			goto error_return;
	}

keep_going: 					/* We will come back to here until there is
								 * nothing left to do. */
	switch (RdcStatus(port))
	{
		case RDC_CONNECTION_NEEDED:
			{
				struct addrinfo    *addr_cur = NULL;

				/*
				 * Try to initiate a connection to one of the addresses
				 * returned by pg_getaddrinfo_all().  conn->addr_cur is the
				 * next one to try. We fail when we run out of addresses.
				 */
				while (port->addr_cur != NULL)
				{
					addr_cur = port->addr_cur;

					/* skip not INET address */
					if (!IS_AF_INET(addr_cur->ai_family))
					{
						port->addr_cur = addr_cur->ai_next;
						continue;
					}

					/* Remember current address for possible error msg */
					memcpy(&port->raddr, addr_cur->ai_addr, addr_cur->ai_addrlen);

					/* Open a stream socket */
					RdcSocket(port) = socket(addr_cur->ai_family, SOCK_STREAM, 0);
					if (RdcSocket(port) == PGINVALID_SOCKET)
					{
						/*
						 * ignore socket() failure if we have more addresses
						 * to try
						 */
						if (addr_cur->ai_next != NULL)
						{
							port->addr_cur = addr_cur->ai_next;
							continue;
						}

						rdc_puterror(port, "could not create socket: %m");
						break;
					}

					/*
					 * Select socket options: no delay of outgoing data for
					 * TCP sockets, nonblock mode, keepalive. Fail if any
					 * ofthis fails.
					 */
					if (!rdc_set_noblock(port) ||
						!connect_nodelay(port) ||
						!connect_keepalive(port) ||
						!connect_close_on_exec(port))
					{
						drop_connection(port, true);
						port->addr_cur = addr_cur->ai_next;
						continue;
					}
#ifdef DEBUG_ADB
					elog(LOG,
						 "try to connect" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(port));
#endif
					/*
					 * Start/make connection.  This should not block, since we
					 * are in nonblock mode.  If it does, well, too bad.
					 */
					if (connect(RdcSocket(port), addr_cur->ai_addr, addr_cur->ai_addrlen) < 0)
					{
						if (errno == EINPROGRESS ||
							errno == EINTR)
						{
							/*
							 * This is fine - we're in non-blocking mode, and
							 * the connection is in progress.  Tell caller to
							 * wait for write-ready on socket.
							 */
							RdcStatus(port) = RDC_CONNECTION_STARTED;
							RdcWaitEvents(port) = WT_SOCK_WRITEABLE;
							return RDC_POLLING_WRITING;
						}

						rdc_puterror(port,
									 "fail to connect" RDC_PORT_PRINT_FORMAT ":%m",
									 RDC_PORT_PRINT_VALUE(port));
					}
					else
					{
						/*
						 * Hm, we're connected already --- seems the "nonblock
						 * connection" wasn't.	Advance the state machine and
						 * go do the next stuff.
						 */
						RdcStatus(port) = RDC_CONNECTION_STARTED;
						goto keep_going;
					}

					drop_connection(port, true);
					/*
					 * Try the next address, if any.
					 */
					port->addr_cur = addr_cur->ai_next;
				}

				/*
				 * Ooops, no more addresses.  An appropriate error message is
				 * already set up, so just set the right status.
				 */
				goto error_return;
			}

		case RDC_CONNECTION_STARTED:
			{
				socklen_t	optlen = sizeof(optval);
				socklen_t	addrlen;

				if (getsockopt(RdcSocket(port), SOL_SOCKET, SO_ERROR,
							   (char *) &optval, &optlen) == -1)
				{
					rdc_puterror(port, "could not get socket error status: %m");
					goto error_return;
				}
				else if (optval != 0)
				{
					/*
					 * When using a nonblocking connect, we will typically see
					 * connect failures at this point, so provide a friendly
					 * error message.
					 */
					rdc_puterror(port, "some error occurred when connect: %s", strerror(optval));
					drop_connection(port, true);

					/*
					 * If more addresses remain, keep trying, just as in the
					 * case where connect() returned failure immediately.
					 */
					if (port->addr_cur->ai_next != NULL)
					{
						port->addr_cur = port->addr_cur->ai_next;
						RdcStatus(port) = RDC_CONNECTION_NEEDED;
						goto keep_going;
					}
					goto error_return;
				}

				/* Fill in the client address */
				addrlen = sizeof(port->laddr);
				if (getsockname(RdcSocket(port),
								(struct sockaddr *) &(port->laddr),
								&addrlen) < 0)
				{
					rdc_puterror(port, "could not get client address from socket: %m");
					goto error_return;
				}
#ifdef DEBUG_ADB
				{
					char   *self_host = inet_ntoa(((struct sockaddr_in *) &(port->laddr))->sin_addr);
					int		rprt = (int) ntohs(((struct sockaddr_in *) &(port->laddr))->sin_port);
					char	portstr[NI_MAXSERV];

					snprintf(portstr, NI_MAXSERV, "%d", rprt);
					RdcSelfHost(port) = self_host ? pstrdup(self_host) : pstrdup("???");
					RdcSelfPort(port) = pstrdup(portstr);
				}
#endif

				/*
				 * Make sure we can write before advancing to next step.
				 */
				RdcStatus(port) = RDC_CONNECTION_MADE;
				RdcWaitEvents(port) = WT_SOCK_WRITEABLE;
				return RDC_POLLING_WRITING;
			}

		case RDC_CONNECTION_MADE:
			{
				if (rdc_send_startup_rqt(port, RdcSelfType(port), RdcSelfID(port),
										 RdcSelfPID(port), RdcSelfExtra(port)))
				{
					drop_connection(port, true);
					rdc_puterror(port, "could not send startup packet: %m");
					goto error_return;
				}
				RdcStatus(port) = RDC_CONNECTION_AWAITING_RESPONSE;
				RdcWaitEvents(port) = WT_SOCK_READABLE;
				return RDC_POLLING_READING;
			}

		case RDC_CONNECTION_AWAITING_RESPONSE:
			{
				RdcPollingStatusType status;

				status = internal_recv_startup_rsp(port, RdcSelfType(port), RdcSelfID(port));
				switch (status)
				{
					case RDC_POLLING_FAILED:
						goto error_return;
						break;
					case RDC_POLLING_OK:
						goto keep_going;
						break;
					case RDC_POLLING_READING:
						RdcWaitEvents(port) = WT_SOCK_READABLE;
						return status;
					case RDC_POLLING_WRITING:
					default:
						Assert(0);
						break;
				}
			}

		case RDC_CONNECTION_ACCEPT_NEED:
			break;

		case RDC_CONNECTION_ACCEPT:
			{
				RdcPollingStatusType status;

				status = internal_recv_startup_rqt(port, RDC_VERSION_NUM);
				switch (status)
				{
					case RDC_POLLING_FAILED:
						goto error_return;
						break;
					case RDC_POLLING_READING:
						RdcWaitEvents(port) = WT_SOCK_READABLE;
						return status;
					case RDC_POLLING_WRITING:
						RdcWaitEvents(port) = WT_SOCK_WRITEABLE;
						return status;
					case RDC_POLLING_OK:
					default:
						Assert(0);
						break;
				}
			}

		case RDC_CONNECTION_SENDING_RESPONSE:
			{
				if (rdc_send_startup_rsp(port, RdcPeerType(port), RdcPeerID(port), MyProcPid))
				{
					drop_connection(port, true);
					rdc_puterror(port, "could not send startup response: %m");
					goto error_return;
				}

				RdcStatus(port) = RDC_CONNECTION_AUTH_OK;
				goto keep_going;
			}

		case RDC_CONNECTION_AUTH_OK:
			{
				if (port->addrs)
				{
					freeaddrinfo(port->addrs);
					port->addrs = port->addr_cur = NULL;
				}
				resetStringInfo(RdcOutBuf(port));
				resetStringInfo(RdcErrBuf(port));
				RdcWaitEvents(port) = WT_SOCK_READABLE;
				RdcStatus(port) = RDC_CONNECTION_OK;
				if (port->hook)
					(*port->hook)(port);
				return RDC_POLLING_OK;
			}

		default:
			rdc_puterror(port,
						 "invalid connection state %d, "
						 "probably indicative of memory corruption",
						 RdcStatus(port));
			goto error_return;
	}

error_return:
	RdcStatus(port) = RDC_CONNECTION_BAD;
	if (port->hook)
		(*port->hook)(port);
	return RDC_POLLING_FAILED;
}

/*
 * rdc_accept -- accept a connection from the socket
 *
 * returns RdcPort if OK.
 * returns NULL if no connection to accept in noblocking mode.
 * otherwise ereport error.
 */
RdcPort *
rdc_accept(pgsocket sock,
		   RdcPortType self_type, RdcPortId self_id,
		   RdcPortPID self_pid, RdcExtra self_extra)
{
	RdcPort	   *port = NULL;
	socklen_t	addrlen;

	port = rdc_newport(PGINVALID_SOCKET,
					   /* the identity of peer will be set by Startup Packet */
					   InvalidPortType, InvalidPortId,
					   self_type, self_id, self_pid, self_extra);
	addrlen = sizeof(port->raddr);
_re_accept:
	RdcSocket(port) = accept(sock, &port->raddr, &addrlen);
	if (RdcSocket(port) < 0)
	{
		if (errno == EINTR)
			goto _re_accept;

		rdc_freeport(port);
		if (errno == EAGAIN ||
			errno == EWOULDBLOCK)
			return NULL;

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("fail to accept: %m")));
	}

	if (!rdc_set_noblock(port) ||
		!connect_nodelay(port) ||
		!connect_keepalive(port))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("fail to set socket options while accept: %s",
				 RdcError(port))));
	RdcStatus(port) = RDC_CONNECTION_ACCEPT;
	RdcWaitEvents(port) = WT_SOCK_READABLE;

#ifdef DEBUG_ADB
	{
		int		ret;
		char	hbuf[NI_MAXHOST];
		char	sbuf[NI_MAXSERV];

		ret = getnameinfo(&port->raddr, addrlen, hbuf, sizeof(hbuf), sbuf, sizeof(sbuf), 0);
		if (ret != 0)
		{
			rdc_freeport(port);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("fail to getnameinfo while accept: %s",
					 gai_strerror(ret))));
		}
		RdcPeerHost(port) = pstrdup(hbuf);
		RdcPeerPort(port) = pstrdup(sbuf);
	}
#endif

	return port;
}

static int
rdc_idx(RdcNode *rdc_nodes, int num, RdcPortId rpid)
{
	int i;

	if (rdc_nodes == NULL)
		return -1;

	for (i = 0; i < num; i++)
	{
		if (rdc_nodes[i].mask.rdc_rpid == rpid)
			return i;
	}

	return -1;
}


/*
 * rdc_parse_group -- parse group message from client
 *
 * rdc_num      output reduce group number
 *
 * returns RdcNode if OK.
 * returns NULL if trouble.
 */
RdcNode *
rdc_parse_group(RdcPort *port, int *rdc_num, RdcConnHook hook)
{
	RdcPortId			rpid;
	int					rprt;
	const char		   *rhst;
	int					num;
	int					i;
	StringInfo			msg;
	int					ret;
	struct addrinfo		hint;
	char				portstr[NI_MAXSERV];
	List			   *clist = NIL;
	ListCell		   *cell = NULL;
	RdcPort			   *rdc_port = NULL;
	RdcNode			   *rdc_nodes = NULL;
	RdcNode			   *rdc_node = NULL;
	int					self_idx;
	int					othr_idx;

	if (port == NULL)
		return NULL;

	msg = RdcInBuf(port);
	num = rdc_getmsgint(msg, sizeof(num));
	Assert(num > 0);
	rdc_nodes = (RdcNode *) palloc0(num * sizeof(RdcNode));

	/* parse group message */
	for (i = 0; i < num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		rdc_node->mask.rdc_rpid = rdc_getmsgRdcPortID(msg);
		rdc_node->mask.rdc_port = rdc_getmsgint(msg, sizeof(rdc_node->mask.rdc_port));
		rdc_node->mask.rdc_host = pstrdup(rdc_getmsgstring(msg));
		rdc_node->port = NULL;
	}

	for (i = 0; i < num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		rpid = rdc_node->mask.rdc_rpid;
		rhst = rdc_node->mask.rdc_host;
		rprt = rdc_node->mask.rdc_port;

		/* skip self Reduce */
		if (RdcIdIsSelfID(rpid))
			continue;

		self_idx = rdc_idx(rdc_nodes, num, MyReduceId);
		othr_idx = rdc_idx(rdc_nodes, num, rpid);
		Assert(self_idx >= 0);
		Assert(othr_idx >= 0);

		rdc_port = NULL;
		/*
		 * If MyReduceId is even, i will connect with Reduce node whose id is
		 * even and bigger than me. also connect Reduce node whose id is odd
		 * and smaller than me.
		 *
		 * If MyReduceId is odd, i will connect with Reduce node whose id is
		 * odd and bigger than me. also connect Reduce node whose id is even
		 * and smaller than me.
		 *
		 * for example:
		 *		0	1	2	3	4	5	6	7	8	9
		 *----------------------------------------------
		 *		2	3	4	5	6	7	8	9	7	8
		 *		4	5	6	7	8	9	5	6	5	6
		 *		6	7	8	9	3	4	3	4	3	4
		 *		8	9	1	2	1	2	1	2	1	2
		 *			0		0		0		0		0
		 *----------------------------------------------
		 *		4	5	4	5	4	5	4	5	4	5
		 *	total: 45 connects
		 */
		if ((IdxIsEven(self_idx) && IdxIsEven(othr_idx) && othr_idx > self_idx) ||
			(IdxIsEven(self_idx) && IdxIsOdd(othr_idx) && othr_idx < self_idx) ||
			(IdxIsOdd(self_idx) && IdxIsOdd(othr_idx) && othr_idx > self_idx) ||
			(IdxIsOdd(self_idx) && IdxIsEven(othr_idx) && othr_idx < self_idx))
		{
			rdc_port = rdc_newport(PGINVALID_SOCKET,
								   TYPE_REDUCE, rpid,
								   TYPE_REDUCE, MyReduceId,
								   MyProcPid, NULL);

			MemSet(&hint, 0, sizeof(hint));
			hint.ai_socktype = SOCK_STREAM;
			hint.ai_family = AF_INET;
			hint.ai_flags = AI_PASSIVE;

			snprintf(portstr, sizeof(portstr), "%d", rprt);
			/* Use getaddrinfo() to resolve the address */
			ret = getaddrinfo(rhst, portstr, &hint, &(rdc_port->addrs));
			if (ret || !rdc_port->addrs)
			{
				rdc_puterror(port,
							 "could not resolve address %s:%d: %s",
							 rhst, rprt, gai_strerror(ret));
				goto _err_parse;
			}
			rdc_port->addr_cur = rdc_port->addrs;
			RdcStatus(rdc_port) = RDC_CONNECTION_NEEDED;
			RdcPositive(rdc_port) = true;
			RdcHook(rdc_port) = hook;
#ifdef DEBUG_ADB
			RdcPeerHost(rdc_port) = pstrdup(rhst);
			RdcPeerPort(rdc_port) = pstrdup(portstr);
#endif
			/* try to poll connect once */
			if (rdc_connect_poll(rdc_port) != RDC_POLLING_WRITING)
			{
				rdc_puterror(port,
							 "fail to connect with" RDC_PORT_PRINT_FORMAT ": %s",
							 RDC_PORT_PRINT_VALUE(rdc_port),
							 RdcError(rdc_port));
				goto _err_parse;
			}
			/* OK and add it */
			rdc_node->port = rdc_port;
			clist = lappend(clist, rdc_port);
		}
		/*
		 * Wait for other Reduce to connect with me.
		 */
	}

	list_free(clist);
	if (rdc_num)
		*rdc_num = num;

	return rdc_nodes;

_err_parse:
	if (rdc_port != NULL)
		rdc_freeport(rdc_port);
	foreach (cell, clist)
		rdc_freeport((RdcPort *) lfirst(cell));
	pfree(rdc_nodes);

	return NULL;
}

/*
 * rdc_puterror_binary -- put error message in error buffer
 */
int
rdc_puterror_binary(RdcPort *port, const char *s, size_t len)
{
	return internal_puterror(port, s, len, false);
}

/*
 * rdc_puterror_format -- put error message in error buffer
 */
int
rdc_puterror(RdcPort *port, const char *fmt, ...)
{
	va_list			args;
	int				needed;
	bool			replace = true;

	AssertArg(port && fmt);

	if (replace || port->err_buf.len == 0)
		resetStringInfo(RdcErrBuf(port));

	for(;;)
	{
		va_start(args, fmt);
		needed = appendStringInfoVA(RdcErrBuf(port), _(fmt), args);
		va_end(args);
		if(needed == 0)
			break;
		enlargeStringInfo(RdcErrBuf(port), needed);
	}

	return 0;
}

/*
 * rdc_putmessage -- put message in out buffer
 */
int
rdc_putmessage(RdcPort *port, const char *s, size_t len)
{
	return internal_put_buffer(port, s, len, false);
}

/*
 * rdc_putmessage_extend -- put message in out buffer
 */
int
rdc_putmessage_extend(RdcPort *port, const char *s, size_t len, bool enlarge)
{
	return internal_put_buffer(port, s, len, enlarge);
}

/*
 * rdc_flush - flush pending data or error.
 *
 * returns 0 if OK, EOF if trouble
 */
int
rdc_flush(RdcPort *port)
{
	return rdc_flush_buffer(port, RdcOutBuf(port), true);
}

/*
 * rdc_try_flush - try flush data or error.
 *
 * returns 0 if OK
 * returns 1 if some data left
 * returns EOF if trouble
 */
int
rdc_try_flush(RdcPort *port)
{
	return rdc_flush_buffer(port, RdcOutBuf(port), false);
}

/*
 *	Read data from a secure connection.
 */
static ssize_t
rdc_secure_read(RdcPort *port, void *ptr, size_t len, int flags)
{
	ssize_t		n;
	int			nready;
	int			save_errno;
	WaitEventElt *wee = NULL;

	if (RdcWaitSet == NULL)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		RdcWaitSet = makeWaitEVSetExtend(2);
		(void) MemoryContextSwitchTo(oldcontext);
	}

_retry_recv:
	n = recv(RdcSocket(port), ptr, len, flags);
	/* keep save the errno, it maybe changed by other actions */
	save_errno = errno;

	/* In blocking mode, wait until the socket is ready */
	if (n < 0 && !port->noblock && (errno == EWOULDBLOCK || errno == EAGAIN))
	{
		resetWaitEVSet(RdcWaitSet);
		addWaitEventBySock(RdcWaitSet, MyBossSock, WT_SOCK_READABLE);
		addWaitEventBySock(RdcWaitSet, RdcSocket(port), WT_SOCK_READABLE);
		nready = execWaitEVSet(RdcWaitSet, -1);
		if (nready < 0)
			ereport(ERROR,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("fail to wait read/write event for socket of" RDC_PORT_PRINT_FORMAT,
				 		RDC_PORT_PRINT_VALUE(port))));
		if (MyBossSock != PGINVALID_SOCKET)
		{
			wee = nextWaitEventElt(RdcWaitSet);
			if (WEEHasError(wee) ||
				(WEECanRead(wee) && BossNowStatus() != BOSS_IS_WORKING))
				ereport(ERROR,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("terminating connection due to unexpected backend exit")));
		}
		goto _retry_recv;
	}

	/*
	 * Process interrupts that happened while (or before) receiving. Note that
	 * we signal that we're not blocking, which will prevent some types of
	 * interrupts from being processed.
	 */
	CHECK_FOR_INTERRUPTS();

	/* it is the real errno */
	errno = save_errno;

	return n;
}

/*
 * rdc_recv - receive data
 *
 * returns 0 if nothing is received in noblocking mode
 * returns 1 if receive some data
 * returns EOF if trouble
 */
int
rdc_recv(RdcPort *port)
{
	StringInfo	buf;

	AssertArg(port);
	buf = RdcInBuf(port);
	Assert(RdcSockIsValid(port));

	if (buf->cursor > 0)
	{
		if (buf->cursor < buf->len)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(buf->data, buf->data + buf->cursor, buf->len - buf->cursor);
			buf->len -= buf->cursor;
			buf->cursor = 0;
		} else
			buf->len = buf->cursor = 0;
	}

	/* double enlarge the buffer if it is full */
	if (buf->maxlen == buf->len)
		enlargeStringInfo(buf, buf->maxlen - 1);

	/* Can fill buffer from buf->len and upwards */
	for (;;)
	{
		int 		r;

		r = rdc_secure_read(port, buf->data + buf->len, buf->maxlen - buf->len, 0);

		if (r < 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if interrupted */

			if (errno == EAGAIN ||
				errno == EWOULDBLOCK)
				return 0;		/* Ok in noblocking mode */

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 */
			ereport(COMMERROR,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));

			rdc_puterror(port,
						 "could not receive data from" RDC_PORT_PRINT_FORMAT ": %m",
						 RDC_PORT_PRINT_VALUE(port));
			return EOF;
		}
		if (r == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			rdc_puterror(port,
						 "the peer of" RDC_PORT_PRINT_FORMAT " has performed an orderly shutdown",
						 RDC_PORT_PRINT_VALUE(port));
			return EOF;
		}
		/* r contains number of bytes read, so just increase length */
		buf->len += r;

		return 1;
	}
}

/*
 * rdc_try_read_some
 *
 * set noblock and try to read some data.
 *
 * return 0 if receive would block.
 * return 1 if receive some data.
 */
int
rdc_try_read_some(RdcPort *port)
{
	int res;

	if (!PortIsValid(port))
		return 0;

	if (!rdc_set_noblock(port))
		ereport(ERROR,
				(errmsg("fail to set noblocking mode for" RDC_PORT_PRINT_FORMAT,
						RDC_PORT_PRINT_VALUE(port))));
	res = rdc_recv(port);
	if (res == EOF)
		ereport(ERROR,
				(errmsg("fail to read some from" RDC_PORT_PRINT_FORMAT,
						RDC_PORT_PRINT_VALUE(port)),
				 errdetail("%s", RdcError(port))));
	return res;
}

/*
 * rdc_getbyte - get one byte
 *
 * returns EOF if not enough data in noblocking mode
 * returns EOF if trouble in blocking mode
 * returns the first byte
 */
int
rdc_getbyte(RdcPort *port)
{
	StringInfo	buf;

	AssertArg(port);
	buf = RdcInBuf(port);
	if (buf->cursor >= buf->len)
	{
		/* noblock mode */
		if (port->noblock)
			return EOF;			/* try to read */

		/* block mode */
		if (rdc_recv(port) == EOF)
			return EOF;			/* fail to receive */
	}
	return rdc_getmsgbyte(buf);

}

/*
 * rdc_getbytes - get a certain length of data
 *
 * returns EOF if not enough data in noblocking mode
 * returns EOF if trouble in blocking mode
 * returns 0 if OK
 */
int
rdc_getbytes(RdcPort *port, size_t len)
{
	StringInfo	buf;
	size_t		amount;

	AssertArg(port);

	/* nothing needed to get */
	if (len <= 0)
		return 0;

	buf = RdcInBuf(port);
	while (len > 0)
	{
		while (buf->cursor + len > buf->len)
		{
			/* return EOF if in noblocking mode */
			if (port->noblock)
				return EOF;		/* try to read next time */

			PG_TRY();
			{
				/*
				 * in blocking mode, we still have space as below:
				 * (buf->maxlen - buf->len -1 + buf->cursor).
				 */
				if (buf->maxlen - buf->len - 1 + buf->cursor < len)
					enlargeStringInfo(buf, len);
			} PG_CATCH();
			{
				if (rdc_discardbytes(port, len))
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("incomplete message from client")));
				PG_RE_THROW();
			} PG_END_TRY();

			/* If not enough in buffer, then recv some in blocking mode */
			if (rdc_recv(port) == EOF)
				return EOF; 	/* Failed to recv data */
		}
		amount = buf->len - buf->cursor;
		if (amount >= len)
			len = 0;
		else
			len -= amount;
	}
	return 0;

}

/*
 * rdc_discardbytes - discard a certain length of data
 *
 * returns EOF if error.
 * returns 0 if OK.
 */
int
rdc_discardbytes(RdcPort *port, size_t len)
{
	size_t		amount;
	StringInfo	buf;

	/* nothing needed to discard */
	if (len <= 0)
		return 0;

	AssertArg(port);

	buf = RdcInBuf(port);
	while (len > 0)
	{
		while (buf->cursor >= buf->len)
		{
			if (rdc_recv(port) == EOF)	/* If nothing in buffer, then recv some */
				return EOF; 	/* Failed to recv data */
		}
		amount = buf->len - buf->cursor;
		if (amount > len)
			amount = len;
		buf->cursor += amount;
		len -= amount;
	}
	return 0;
}

/*
 * rdc_getmessage - get one whole message
 *
 * returns EOF if error.
 * returns message type if OK.
 */
int
rdc_getmessage(RdcPort *port, size_t maxlen)
{
	char	rdctype = '\0';
	uint32	len;
	bool	sv_noblock;

	AssertArg(port);

	/* set in blocking mode */
	sv_noblock = port->noblock;
	if (sv_noblock)
		rdc_set_block(port);
	PG_TRY();
	{
		rdctype = rdc_getbyte(port);
		if (rdctype == EOF)
		{
			rdc_puterror(port, "unexpected EOF on client connection: %m");
			goto eof_end;
		}

		if (rdc_getbytes(port, sizeof(len)) == EOF)
		{
			rdc_puterror(port, "unexpected EOF within message length word: %m");
			goto eof_end;
		}

		len = rdc_getmsgint(RdcInBuf(port), sizeof(len));
		if (len < 4 ||
			(maxlen > 0 && len > maxlen))
		{
			rdc_puterror(port, "invalid message length");
			goto eof_end;
		}

		len -= 4;					/* discount length itself */
		if (len > 0)
		{
			if (rdc_getbytes(port, len) == EOF)
			{
				rdc_puterror(port, "incomplete message from client");
				goto eof_end;
			}
		}
	} PG_CATCH();
	{
		/* set in noblocking mode */
		if (sv_noblock)
			rdc_set_noblock(port);
		PG_RE_THROW();
	} PG_END_TRY();

	/* already parse firstchar and length, just parse other data left */
	return rdctype;

eof_end:
	/* set in noblocking mode */
	if (sv_noblock)
		rdc_set_noblock(port);
	return EOF;
}

/*
 * rdc_geterror - get error message from a port
 */
const char *
rdc_geterror(RdcPort *port)
{
	if (!port || port->err_buf.len <= 0)
		return "missing error message";

	return port->err_buf.data;
}

/*
 * rdc_set_block
 *
 * Only mark false for RdcPort->noblock,but do
 * not set block for the socket of RdcPort actually.
 */
int
rdc_set_block(RdcPort *port)
{
	if (port == NULL)
		return 0;

	/*if (port->noblock == false)
		return 1;

	if (!pg_set_block(RdcSocket(port)))
	{
		rdc_puterror(port, "could set socket to blocking mode: %m");
		return 0;
	}*/
	port->noblock = false;

	return 1;
}

/*
 * rdc_set_noblock
 *
 * Set noblock for socket of RdcPort, and mark true
 * for RdcPort->noblock.
 */
int
rdc_set_noblock(RdcPort *port)
{
	if (port == NULL)
		return 0;

	if (port->noblock == true)
		return 1;

	if (!pg_set_noblock(RdcSocket(port)))
	{
		rdc_puterror(port, "could set socket to noblocking mode: %m");
		return 0;
	}
	port->noblock = true;

	return 1;
}

void
rdc_set_opt(RdcPort *port, int level, int optname, const void *optval, socklen_t optlen)
{
	if (port == NULL)
	{
		rdc_puterror(port, "port is invalid");
		return ;
	}

	if (setsockopt(RdcSocket(port), level, optname, optval, optlen) != 0)
		rdc_puterror(port, "could set socket option: %m");
}

void
rdc_get_opt(RdcPort *port, int level, int optname, void *optval, socklen_t *optlen)
{
	if (port == NULL)
	{
		rdc_puterror(port, "port is invalid");
		return ;
	}

	if (getsockopt(RdcSocket(port), level, optname, optval, optlen) != 0)
		rdc_puterror(port, "could get socket option: %m");
}

static int
internal_puterror(RdcPort *port, const char *s, size_t len, bool replace)
{
	StringInfo	errbuf;

	AssertArg(port);
	errbuf = RdcErrBuf(port);

	/* empty error buffer, just fill in */
	if (errbuf->len == 0)
	{
		appendBinaryStringInfo(errbuf, s, len);
		return 0;
	}

	/* already have error message */
	if (replace)
	{
		resetStringInfo(errbuf);
		appendBinaryStringInfo(errbuf, s, len);
	}

	return 0;
}

static int
rdc_flush_buffer(RdcPort *port, StringInfo buf, bool block)
{
	int			r;
	static int	last_reported_send_errno = 0;
	pgsocket	sock = PGINVALID_SOCKET;

	AssertArg(port);
	AssertArg(buf);
	Assert(RdcSockIsValid(port));

	sock = RdcSocket(port);
	while (buf->cursor < buf->len)
	{
		r = send(sock, buf->data + buf->cursor, buf->len - buf->cursor, 0);
		if (r <= 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if we were interrupted */

			/*
			 * Ok if no data writable without blocking, and the socket is in
			 * non-blocking mode.
			 */
			if (errno == EAGAIN ||
				errno == EWOULDBLOCK)
			{
				if (block)
					continue;
				else
					return 1;	/* some data left to be sent */
			}

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If a client disconnects while we're in the midst of output, we
			 * might write quite a bit of data before we get to a safe query
			 * abort point.  So, suppress duplicate log messages.
			 */
			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;
				ereport(COMMERROR,
						(errcode_for_socket_access(),
						 errmsg("could not send data to client: %m")));
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon. We also set a
			 * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
			 * the connection.
			 */
			buf->cursor = buf->len = 0;
#if defined(RDC_FRONTEND)
			ClientConnectionLostType = RdcPeerType(port);
			ClientConnectionLostID = RdcPeerID(port);
			ClientConnectionLost = 1;
			InterruptPending = 1;
#endif
			return EOF;
		}

		last_reported_send_errno = 0;	/* reset after any successful send */
		buf->cursor += r;
	}

	buf->cursor = buf->len = 0;

	return 0;
}

static int
internal_put_buffer(RdcPort *port, const char *s, size_t len, bool enlarge)
{
	size_t		amount;
	StringInfo	buf;

	AssertArg(port);

	buf = RdcOutBuf(port);
	if (enlarge)
	{
		/* If buffer is full, then flush it out */
		if (buf->len + len >= buf->maxlen)
		{
			if (rdc_flush(port))
				return EOF;
		}
		appendBinaryStringInfo(buf, s, len);
	} else
	{
		while (len > 0)
		{
			/* If buffer is full, then flush it out */
			if (buf->len + len >= buf->maxlen)
			{
				if (rdc_flush(port))
					return EOF;
			}
			amount = buf->maxlen - buf->len;
			if (amount > len)
				amount = len;
			appendBinaryStringInfo(buf, s, amount);
			s += amount;
			len -= amount;
		}
	}

	return 0;
}

/*
 * internal_recv_startup_rqt - receive and parse startup request message
 *
 * returns RDC_POLLING_FAILED if trouble
 * returns RDC_POLLING_READING if not enough data
 * returns RDC_POLLING_WRITING if receive and parse startup request OK
 *
 * NOTE
 *		this is used to get the startup infomation after accepting a new
 *		connection.
 */
static RdcPollingStatusType
internal_recv_startup_rqt(RdcPort *port, int expected_ver)
{
	char		beresp;
	uint32		length;
	size_t		len;
	RdcPortType	rqt_type = InvalidPortType;
	RdcPortId	rqt_id = InvalidPortId;
	RdcPortPID	rqt_pid = InvalidPortPID;
	int			rqt_ver;
	StringInfo	msg;
	int			sv_cursor;

	AssertArg(port);

	/* set in noblocking mode */
	if (!rdc_set_noblock(port))
	{
		drop_connection(port, true);
		return RDC_POLLING_FAILED;
	}

	msg = RdcInBuf(port);
	sv_cursor = msg->cursor;

	/* Read type byte */
	beresp = rdc_getbyte(port);
	if (beresp == EOF)
	{
		msg->cursor = sv_cursor;
		/* We'll come back when there is more data */
		return RDC_POLLING_READING;
	}

	if (beresp != MSG_START_RQT)
	{
		rdc_puterror(port,
					 "expected startup request from client, "
					 "but received %c", beresp);
		return RDC_POLLING_FAILED;
	}

	/* Read message length word */
	if (rdc_getbytes(port, sizeof(length))== EOF)
	{
		msg->cursor = sv_cursor;
		/* We'll come back when there is more data */
		return RDC_POLLING_READING;
	}
	length = rdc_getmsgint(msg, sizeof(length));
	if (length < 4)
	{
		rdc_puterror(port, "invalid message length");
		return RDC_POLLING_FAILED;
	}

	/* Read message */
	length -= 4;
	if (rdc_getbytes(port, length) == EOF)
	{
		msg->cursor = sv_cursor;
		/* We'll come back when there is more data */
		return RDC_POLLING_READING;
	}

	/* check request version */
	len = sizeof(rqt_ver);
	rqt_ver = rdc_getmsgint(msg, len);
	RdcVersion(port) = rqt_ver;
	length -= len;

	/* check request type */
	len = sizeof(rqt_type);
	rqt_type = rdc_getmsgint(msg, len);
	RdcPeerType(port) = rqt_type;
	length -= len;

	/* check request id */
	len = sizeof(rqt_id);
	rqt_id = rdc_getmsgRdcPortID(msg);
	RdcPeerID(port) = rqt_id;
	length -= len;

	/* check request pid */
	len = sizeof(rqt_pid);
	rqt_pid = rdc_getmsgint(msg, len);
	RdcPeerPID(port) = rqt_pid;
	length -= len;

	/* check request extra */
	if (length > 0)
		appendBinaryStringInfo(RdcPeerExtra(port),
							   rdc_getmsgbytes(msg, length),
							   length);

	rdc_getmsgend(msg);

	Assert(PortTypeIDIsValid(port));

	if (rqt_ver != expected_ver)
	{
		rdc_puterror(port,
					 "expected Reduce version '%d' from client, "
					 "but received request version '%d'",
					 expected_ver, rqt_ver);
		return RDC_POLLING_FAILED;
	}

#ifdef DEBUG_ADB
	elog(LOG,
		 "recv startup request from" RDC_PORT_PRINT_FORMAT,
		 RDC_PORT_PRINT_VALUE(port));
#endif

	/* We are done with authentication exchange */
	RdcStatus(port) = RDC_CONNECTION_SENDING_RESPONSE;
	return RDC_POLLING_WRITING;
}

/*
 * internal_recv_startup_rsp - receive and parse startup response message
 *
 * returns RDC_POLLING_FAILED if trouble
 * returns RDC_POLLING_READING if not enough data
 * returns RDC_POLLING_OK if authentication success
 *
 * NOTE
 *		this is used to get the response from server after sending startup
 *		request message.
 */
static RdcPollingStatusType
internal_recv_startup_rsp(RdcPort *port, RdcPortType expected_type, RdcPortId expected_id)
{
	char		beresp;
	uint32		length;
	int			sv_cursor;
	StringInfo	msg;

	AssertArg(port);

	msg = RdcInBuf(port);
	sv_cursor = msg->cursor;

	/* Read type byte */
	beresp = rdc_getbyte(port);
	if (beresp == EOF)
	{
		msg->cursor = sv_cursor;
		/* We'll come back when there is more data */
		return RDC_POLLING_READING;
	}

	/*
	 * Validate message type: we expect only an authentication
	 * request or an error here.  Anything else probably means
	 * it's not Reduce on the other end at all.
	 */
	if (!(beresp == MSG_START_RSP || beresp == MSG_ERROR))
	{
		rdc_puterror(port,
					 "expected startup response from "
					 "server, but received %c", beresp);
		return RDC_POLLING_FAILED;
	}

	/* Read message length word */
	if (rdc_getbytes(port, sizeof(length))== EOF)
	{
		msg->cursor = sv_cursor;
		/* We'll come back when there is more data */
		return RDC_POLLING_READING;
	}
	length = rdc_getmsgint(msg, sizeof(length));
	if (length < 4)
	{
		rdc_puterror(port, "invalid message length");
		return RDC_POLLING_FAILED;
	}

	/* Read message */
	length -= 4;
	if (rdc_getbytes(port, length) == EOF)
	{
		msg->cursor = sv_cursor;
		/* We'll come back when there is more data */
		return RDC_POLLING_READING;
	}

	/* Check message */
	if (beresp == MSG_ERROR)
	{
		if (length > 0)
		{
			const char *errmsg;

			errmsg = rdc_getmsgstring(msg);
			rdc_getmsgend(msg);
			rdc_puterror(port, "%s", errmsg);
		} else
		{
			rdc_getmsgend(msg);
			rdc_puterror(port, "received error response from server");
		}
		return RDC_POLLING_FAILED;
	} else
	{
		RdcPortType	rsp_type = InvalidPortType;
		RdcPortId	rsp_id = InvalidPortId;
		RdcPortPID	rsp_pid = InvalidPortPID;
		int			rsp_ver;

		rsp_ver = rdc_getmsgint(msg, sizeof(rsp_ver));
		if (rsp_ver != RDC_VERSION_NUM)
		{
			rdc_puterror(port,
						 "expected Reduce version '%d' from server, "
						 "but received response type '%d'",
						 RDC_VERSION_NUM,
						 rsp_ver);
			return RDC_POLLING_FAILED;
		}
		rsp_type = rdc_getmsgint(msg, sizeof(rsp_type));
		if (rsp_type != expected_type)
		{
			rdc_puterror(port,
						 "expected port type '%s' from server, "
						 "but received response type '%s'",
						 rdc_type2string(expected_type),
						 rdc_type2string(rsp_type));
			return RDC_POLLING_FAILED;
		}
		rsp_id = rdc_getmsgRdcPortID(msg);
		if (rsp_id != expected_id)
		{
			rdc_puterror(port,
						 "expected port id '" PORTID_FORMAT "' from server, "
						 "but received response id '" PORTID_FORMAT "'",
						 expected_id, rsp_id);
			return RDC_POLLING_FAILED;
		}
		rsp_pid = rdc_getmsgint(msg, sizeof(rsp_pid));
		RdcPeerPID(port) = rsp_pid;
		rdc_getmsgend(msg);

#ifdef DEBUG_ADB
		elog(LOG,
			 "recv startup response from" RDC_PORT_PRINT_FORMAT,
			 RDC_PORT_PRINT_VALUE(port));
#endif

		/* We are done with authentication exchange */
		RdcStatus(port) = RDC_CONNECTION_AUTH_OK;
		return RDC_POLLING_OK;
	}
}

/*
 * connect_nodelay - set socket TCP_NODELAY option
 *
 * returns 1 if OK
 * returns 0 if trouble
 */
static int
connect_nodelay(RdcPort *port)
{
#ifdef	TCP_NODELAY
	int			on = 1;

	if (setsockopt(RdcSocket(port), IPPROTO_TCP, TCP_NODELAY,
				   (char *) &on,
				   sizeof(on)) < 0)
	{
		rdc_puterror(port, "could not set socket to TCP no delay mode: %m"));
		return 0;
	}
#endif

	return 1;
}

/*
 * connect_keepalive - set socket SO_KEEPALIVE option
 *
 * returns 1 if OK
 * returns 0 if trouble
 */
static int
connect_keepalive(RdcPort *port)
{
#ifndef WIN32
	int			on = 1;
	if (setsockopt(RdcSocket(port), SOL_SOCKET, SO_KEEPALIVE,
				   (char *) &on, sizeof(on)) < 0)
	{
		rdc_puterror(port, "setsockopt(SO_KEEPALIVE) failed: %m");
		return 0;
	}
#else	/* WIN32 */
	/* TODO on WIN32 platform */
#endif	/* WIN32 */

	return 1;
}

/*
 * connect_close_on_exec - set socket FD_CLOEXEC option
 *
 * returns 1 if OK
 * returns 0 if trouble
 */
static int
connect_close_on_exec(RdcPort *port)
{
#ifdef F_SETFD
	if (fcntl(RdcSocket(port), F_SETFD, FD_CLOEXEC) == -1)
	{
		rdc_puterror("could not set socket to close-on-exec mode: %m");
		return 0;
	}
#endif   /* F_SETFD */
	return 1;
}

/*
 * drop_connection - drop connection if fail to poll connect
 */
static void
drop_connection(RdcPort *port, bool flushInput)
{
	if (RdcSocket(port) >= 0)
		closesocket(RdcSocket(port));
	RdcSocket(port) = PGINVALID_SOCKET;
	/* Optionally discard any unread data */
	if (flushInput)
		resetStringInfo(RdcInBuf(port));
	/* Always discard any unsent data */
	resetStringInfo(RdcOutBuf(port));
	/* resetStringInfo(RdcErrBuf(port)); */
}
