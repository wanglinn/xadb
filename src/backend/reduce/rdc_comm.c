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

#if defined(RDC_FRONTEND)
#include "rdc_globals.h"
#else
#include "postgres.h"
#include "miscadmin.h"
#endif

#include "reduce/rdc_comm.h"
#include "reduce/rdc_msg.h"
#include "utils/memutils.h"

#define RDC_BUFFER_SIZE		8192

static ssize_t rdc_secure_read(RdcPort *port, void *ptr, size_t len, int flags);
static int internal_flush_buffer(pgsocket sock, StringInfo buf, bool block);
static int internal_put_buffer(RdcPort *port, const char *s, size_t len, bool enlarge);
static int internal_puterror(RdcPort *port, const char *s, size_t len, bool replace);

static RdcPollingStatusType internal_recv_startup_rqt(RdcPort *port, int expected_ver);
static RdcPollingStatusType internal_recv_startup_rsp(RdcPort *port, RdcPortType expceted_type, RdcPortId expceted_id);
static int connect_nodelay(RdcPort *port);
static int connect_keepalive(RdcPort *port);
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

RdcPort *
rdc_newport(pgsocket sock, RdcPortType type, RdcPortId id)
{
	RdcPort		   *rdc_port = NULL;

	rdc_port = (RdcPort *) palloc0(sizeof(*rdc_port));
	rdc_port->next = NULL;
	rdc_port->sock = sock;
	rdc_port->noblock = false;
	rdc_port->type = type;
	rdc_port->from_to = id;
	rdc_port->version = 0;
	rdc_port->wait_events = WAIT_NONE;
	rdc_port->got_eof = false;
#ifdef DEBUG_ADB
	rdc_port->hoststr = NULL;
	rdc_port->portstr = NULL;
#endif
	rdc_port->addrs = NULL;
	rdc_port->addr_cur = NULL;
	initStringInfoExtend(RdcInBuf(rdc_port), RDC_BUFFER_SIZE);
	initStringInfoExtend(RdcOutBuf(rdc_port), RDC_BUFFER_SIZE);
	initStringInfoExtend(RdcErrBuf(rdc_port), RDC_BUFFER_SIZE);

	return rdc_port;
}

void
rdc_freeport(RdcPort *port)
{
	RdcPort *next = NULL;

	while (port)
	{
		next = RdcNext(port);

		if (RdcSockIsValid(port))
			closesocket(RdcSocket(port));
		if (port->addrs)
		{
			freeaddrinfo(port->addrs);
			port->addrs = port->addr_cur = NULL;
		}
		pfree(port->in_buf.data);
		pfree(port->out_buf.data);
		pfree(port->err_buf.data);
#ifdef DEBUG_ADB
		safe_pfree(RdcHostStr(port));
		safe_pfree(RdcPortStr(port));
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

RdcPort *
rdc_connect(const char *host, uint32 port, RdcPortType type, RdcPortId id)
{
	RdcPort			   *rdc_port = NULL;
	struct addrinfo	   *addr = NULL;
	int					ret;
	struct addrinfo		hint;
	char				portstr[MAXPGPATH];
	socklen_t			addrlen;

	rdc_port = rdc_newport(PGINVALID_SOCKET, type, id);

	snprintf(portstr, sizeof(portstr), "%u", port);

	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;
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

	for (addr = rdc_port->addrs; addr != NULL; addr = addr->ai_next)
	{
		/* skip not INET address */
		if (!IS_AF_INET(addr->ai_family))
			continue;

		/* create a socket */
		RdcSocket(rdc_port) = socket(addr->ai_family, SOCK_STREAM, 0);
		if (RdcSocket(rdc_port) == PGINVALID_SOCKET)
		{
			if (addr->ai_next)
				continue;
			rdc_puterror(rdc_port,
						 "fail to create a socket for %s:%u: %m",
						 host, port);
			return rdc_port;
		}

		/* set socket block */
		rdc_set_block(rdc_port);

reconnect:
		/* try to connect */
		ret = connect(RdcSocket(rdc_port), addr->ai_addr, addr->ai_addrlen);
		if (ret < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if (errno == EINTR)
				goto reconnect;
			closesocket(RdcSocket(rdc_port));
			RdcSocket(rdc_port) = PGINVALID_SOCKET;

			if (addr->ai_next)
				continue;

			rdc_puterror(rdc_port,
						 "fail to connect %s:%u: %m",
						 host, port);
			return rdc_port;
		}

		/* connect OK. send startup request */
		if (rdc_send_startup_rqt(rdc_port, type, id))
		{
			CHECK_FOR_INTERRUPTS();
			closesocket(RdcSocket(rdc_port));
			RdcSocket(rdc_port) = PGINVALID_SOCKET;

			if (addr->ai_next)
				continue;

			rdc_puterror(rdc_port,
						 "fail to send to %s:%u startup request: %m",
						 host, port);
			return rdc_port;
		}

		/* receive statup response */
		if (rdc_recv_startup_rsp(rdc_port, type, id))
		{
			CHECK_FOR_INTERRUPTS();
			closesocket(RdcSocket(rdc_port));
			RdcSocket(rdc_port) = PGINVALID_SOCKET;

			if (addr->ai_next)
				continue;

			rdc_puterror(rdc_port,
						 "fail to receive from %s:%u startup response: %m",
						 host, port);
			return rdc_port;
		}

		/* Remember client and server address for possible error msg */
		memcpy(&rdc_port->raddr, addr->ai_addr, addr->ai_addrlen);
		addrlen = sizeof(rdc_port->laddr);
		if (getsockname(RdcSocket(rdc_port), (struct sockaddr *) &(rdc_port->laddr),
			&addrlen) < 0)
		{
			CHECK_FOR_INTERRUPTS();
			closesocket(RdcSocket(rdc_port));
			RdcSocket(rdc_port) = PGINVALID_SOCKET;

			if (addr->ai_next)
				continue;

			rdc_puterror(rdc_port,
						 "could not get client address from socket: %m");
			return rdc_port;
		}

		/* connect and authentication OK */
		break;
	}

	freeaddrinfo(rdc_port->addrs);
	rdc_port->addrs = NULL;

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
		case CONNECTION_BAD:
			return RDC_POLLING_FAILED;
		case CONNECTION_OK:
			return RDC_POLLING_OK;

		/* These are reading states */
		case CONNECTION_AWAITING_RESPONSE:
		case CONNECTION_ACCEPT:
		case CONNECTION_AUTH_OK:
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
		case CONNECTION_STARTED:
		case CONNECTION_MADE:
		case CONNECTION_SENDING_RESPONSE:
			break;

		case CONNECTION_ACCEPT_NEED:
		case CONNECTION_NEEDED:
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
		case CONNECTION_NEEDED:
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
						!connect_keepalive(port))
					{
						drop_connection(port, true);
						port->addr_cur = addr_cur->ai_next;
						continue;
					}
#ifdef DEBUG_ADB
					elog(LOG,
						 "Try to connect [%s %d] {%s:%s}",
						 RdcTypeStr(port), RdcID(port),
						 RdcHostStr(port), RdcPortStr(port));
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
							RdcStatus(port) = CONNECTION_STARTED;
							RdcWaitEvents(port) = WAIT_SOCKET_WRITEABLE;
							return RDC_POLLING_WRITING;
						}

						rdc_puterror(port,
									 "fail to connect [%s:%d] {%s:%s}: %m",
									 RdcTypeStr(port), RdcID(port),
									 RdcHostStr(port), RdcPortStr(port));
					}
					else
					{
						/*
						 * Hm, we're connected already --- seems the "nonblock
						 * connection" wasn't.	Advance the state machine and
						 * go do the next stuff.
						 */
						RdcStatus(port) = CONNECTION_STARTED;
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

		case CONNECTION_STARTED:
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
						RdcStatus(port) = CONNECTION_NEEDED;
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

				/*
				 * Make sure we can write before advancing to next step.
				 */
				RdcStatus(port) = CONNECTION_MADE;
				RdcWaitEvents(port) = WAIT_SOCKET_WRITEABLE;
				return RDC_POLLING_WRITING;
			}

		case CONNECTION_MADE:
			{
				if (rdc_send_startup_rqt(port, TYPE_REDUCE, MyReduceId))
				{
					drop_connection(port, true);
					rdc_puterror(port, "could not send startup packet: %m");
					goto error_return;
				}
				RdcStatus(port) = CONNECTION_AWAITING_RESPONSE;
				RdcWaitEvents(port) = WAIT_SOCKET_READABLE;
				return RDC_POLLING_READING;
			}

		case CONNECTION_AWAITING_RESPONSE:
			{
				RdcPollingStatusType status;

				status = internal_recv_startup_rsp(port, TYPE_REDUCE, MyReduceId);
				switch (status)
				{
					case RDC_POLLING_FAILED:
						goto error_return;
						break;
					case RDC_POLLING_OK:
						goto keep_going;
						break;
					case RDC_POLLING_READING:
						RdcWaitEvents(port) = WAIT_SOCKET_READABLE;
						return status;
					case RDC_POLLING_WRITING:
					default:
						Assert(0);
						break;
				}
			}

		case CONNECTION_ACCEPT_NEED:
			break;

		case CONNECTION_ACCEPT:
			{
				RdcPollingStatusType status;

				status = internal_recv_startup_rqt(port, RDC_VERSION_NUM);
				switch (status)
				{
					case RDC_POLLING_FAILED:
						goto error_return;
						break;
					case RDC_POLLING_READING:
						RdcWaitEvents(port) = WAIT_SOCKET_READABLE;
						return status;
					case RDC_POLLING_WRITING:
						RdcWaitEvents(port) = WAIT_SOCKET_WRITEABLE;
						return status;
					case RDC_POLLING_OK:
					default:
						Assert(0);
						break;
				}
			}

		case CONNECTION_SENDING_RESPONSE:
			{
				if (rdc_send_startup_rsp(port, port->type, port->from_to))
				{
					drop_connection(port, true);
					rdc_puterror(port, "could not send startup response: %m");
					goto error_return;
				}

				RdcStatus(port) = CONNECTION_AUTH_OK;
				goto keep_going;
			}

		case CONNECTION_AUTH_OK:
			{
				if (port->addrs)
				{
					freeaddrinfo(port->addrs);
					port->addrs = port->addr_cur = NULL;
				}
				resetStringInfo(RdcOutBuf(port));
				resetStringInfo(RdcErrBuf(port));
				RdcWaitEvents(port) = WAIT_SOCKET_READABLE;
				RdcStatus(port) = CONNECTION_OK;
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
	RdcStatus(port) = CONNECTION_BAD;
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
rdc_accept(pgsocket sock)
{
	RdcPort	   *port = NULL;
	socklen_t	addrlen;

	port = rdc_newport(PGINVALID_SOCKET, InvalidPortType, InvalidPortId);
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
	RdcStatus(port) = CONNECTION_ACCEPT;
	RdcWaitEvents(port) = WAIT_SOCKET_READABLE;

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
		RdcHostStr(port) = pstrdup(hbuf);
		RdcPortStr(port) = pstrdup(sbuf);
	}
#endif

	return port;
}

/*
 * rdc_parse_group -- parse group message from client
 *
 * rdc_num      output reduce group number
 * nodeinfos    output reduce group node infomation
 * connect_list output a list which contains RdcPort connect to.
 *
 * returns STATUS_OK if OK.
 * returns STATUS_ERROR if trouble.
 */
int
rdc_parse_group(RdcPort *port,				/* IN */
				int *rdc_num,				/* OUT */
#ifdef DEBUG_ADB
				ReduceInfo **nodeinfos,		/* OUT */
#endif
				List **connect_list)		/* OUT */
{
	const char		   *host = NULL;
	int					num;
	int					i;
	uint32				portnum;
	StringInfo			msg;
	int					ret;
	struct addrinfo		hint;
	char				portstr[32];
	List			   *clist = NIL;
	ListCell		   *cell = NULL;
	RdcPort			   *rdc_port = NULL;

	if (port == NULL)
		return STATUS_ERROR;

	msg = RdcInBuf(port);
	num = rdc_getmsgint(msg, sizeof(num));
	Assert(num > 0);
#ifdef DEBUG_ADB
	if (nodeinfos)
		*nodeinfos = (ReduceInfo *) palloc0(num * sizeof(ReduceInfo));
#endif

	for (i = 0; i < num; i++)
	{
		host = rdc_getmsgstring(msg);
		portnum = rdc_getmsgint(msg, sizeof(portnum));

#ifdef DEBUG_ADB
		if (nodeinfos)
		{
			(*nodeinfos)[i].rnid = i;
			(*nodeinfos)[i].host = pstrdup(host);
			(*nodeinfos)[i].port = portnum;
		}
#endif

		/* skip self Reduce */
		if (i == MyReduceId)
			continue;

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
		if ((PortIdIsEven(MyReduceId) && PortIdIsEven(i) && i > MyReduceId) ||
			(PortIdIsEven(MyReduceId) && PortIdIsOdd(i) && i < MyReduceId) ||
			(PortIdIsOdd(MyReduceId) && PortIdIsOdd(i) && i > MyReduceId) ||
			(PortIdIsOdd(MyReduceId) && PortIdIsEven(i) && i < MyReduceId))
		{
			rdc_port = rdc_newport(PGINVALID_SOCKET, TYPE_REDUCE, i);

			MemSet(&hint, 0, sizeof(hint));
			hint.ai_socktype = SOCK_STREAM;
			hint.ai_family = AF_INET;
			hint.ai_flags = AI_PASSIVE;

			snprintf(portstr, sizeof(portstr), "%u", portnum);
			/* Use getaddrinfo() to resolve the address */
			ret = getaddrinfo(host, portstr, &hint, &(rdc_port->addrs));
			if (ret || !rdc_port->addrs)
			{
				rdc_freeport(rdc_port);
				foreach (cell, clist)
					rdc_freeport((RdcPort *) lfirst(cell));

				rdc_puterror(port,
							 "could not resolve address %s:%u: %s",
							 host, portnum, gai_strerror(ret));
				return STATUS_ERROR;
			}
			rdc_port->addr_cur = rdc_port->addrs;
			RdcStatus(rdc_port) = CONNECTION_NEEDED;
#ifdef DEBUG_ADB
			RdcHostStr(rdc_port) = pstrdup(host);
			RdcPortStr(rdc_port) = pstrdup(portstr);
#endif
			/* try to poll connect once */
			if (rdc_connect_poll(rdc_port) != RDC_POLLING_WRITING)
			{
				rdc_freeport(rdc_port);
				foreach (cell, clist)
					rdc_freeport((RdcPort *) lfirst(cell));

				rdc_puterror(port,
							 "fail to connect with [%s %d] {%s:%s}: %s",
							 RdcTypeStr(rdc_port), RdcID(rdc_port),
							 RdcHostStr(rdc_port), RdcPortStr(rdc_port),
							 RdcError(rdc_port));
				return STATUS_ERROR;
			}
			/* OK and add it */
			clist = lappend(clist, rdc_port);
		}
		/*
		 * Wait for other Reduce to connect with me.
		 */
	}

	if (rdc_num)
		*rdc_num = num;
	if (connect_list)
		*connect_list = clist;
	else
	{
		foreach (cell, clist)
			rdc_freeport((RdcPort *) lfirst(cell));
	}

	return STATUS_OK;
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
	AssertArg(port);

	/* flush out buffer */
	if (internal_flush_buffer(RdcSocket(port), RdcOutBuf(port), true))
		return EOF;

	/* just return if no error */
	//if (!IsRdcPortError(port))
	//	return 0;

	/* flush error buffer */
	//if (internal_flush_buffer(RdcSocket(port), &(port->err_buf), true))
	//	return EOF;

	return 0;
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
	AssertArg(port);

	return internal_flush_buffer(RdcSocket(port), RdcOutBuf(port), false);
}

/*
 *	Read data from a secure connection.
 */
static ssize_t
rdc_secure_read(RdcPort *port, void *ptr, size_t len, int flags)
{
	ssize_t		n;
	int			nready;
	uint32		waitfor;

retry:
	n = recv(RdcSocket(port), ptr, len, flags);
	waitfor = WAIT_SOCKET_READABLE;

	/* In blocking mode, wait until the socket is ready */
	if (n < 0 && !port->noblock && (errno == EWOULDBLOCK || errno == EAGAIN))
	{
		PG_TRY();
		{
			begin_wait_events();
			add_wait_events_sock(RdcSocket(port), waitfor);
			nready = exec_wait_events(-1);
			if (nready < 0)
				ereport(ERROR,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("fail to wait read/write event for socket of [%s %d]",
					 		RdcTypeStr(port), RdcID(port))));
			end_wait_events();
			goto retry;
		} PG_CATCH();
		{
			end_wait_events();
			PG_RE_THROW();
		} PG_END_TRY();
	}

	/*
	 * Process interrupts that happened while (or before) receiving. Note that
	 * we signal that we're not blocking, which will prevent some types of
	 * interrupts from being processed.
	 */
	CHECK_FOR_INTERRUPTS();

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
						 "could not receive data from [%s %d] {%s:%s}: %m",
						 RdcTypeStr(port), RdcID(port),
						 RdcHostStr(port), RdcPortStr(port));
			return EOF;
		}
		if (r == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			rdc_puterror(port,
						 "the peer of [%s %d] {%s:%s} has performed an orderly shutdown",
						 RdcTypeStr(port), RdcID(port),
						 RdcHostStr(port), RdcPortStr(port));
			return EOF;
		}
		/* r contains number of bytes read, so just increase length */
		buf->len += r;

		return 1;
	}
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
		PG_TRY();
		{
			enlargeStringInfo(buf, len);
		} PG_CATCH();
		{
			if (rdc_discardbytes(port, len))
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("incomplete message from client")));
			PG_RE_THROW();
		} PG_END_TRY();

		while (buf->cursor >= buf->len)
		{
			/* return EOF if in noblocking mode */
			if (port->noblock)
				return EOF;		/* try to read next time */

			/* If nothing in buffer, then recv some in blocking mode */
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
internal_flush_buffer(pgsocket sock, StringInfo buf, bool block)
{
	int			r;
	static int	last_reported_send_errno = 0;

	AssertArg(sock != PGINVALID_SOCKET);
	AssertArg(buf);

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
			ClientConnectionLost = 1;
			InterruptPending = 1;
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
	RdcPortType	rqt_type = InvalidPortType;
	RdcPortId	rqt_id = InvalidPortId;
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

	if (beresp != RDC_START_RQT)
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

	/* check message */
	rqt_ver = rdc_getmsgint(msg, sizeof(rqt_type));
	RdcVersion(port) = rqt_ver;

	rqt_type = rdc_getmsgint(msg, sizeof(rqt_type));
	Assert(PortTypeIsValid(rqt_type));
	RdcType(port) = rqt_type;

	rqt_id = rdc_getmsgint(msg, sizeof(rqt_id));
	Assert(PortIdIsValid(rqt_id));
	RdcID(port) = rqt_id;

	rdc_getmsgend(msg);

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
		 "recv startup request from [%s %d] {%s:%s}",
		 rdc_type2string(rqt_type), RdcID(port),
		 RdcHostStr(port), RdcPortStr(port));
#endif

	/* We are done with authentication exchange */
	RdcStatus(port) = CONNECTION_SENDING_RESPONSE;
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
	if (!(beresp == RDC_START_RSP || beresp == RDC_ERROR_MSG))
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
	if (beresp == RDC_ERROR_MSG)
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
		rsp_id = rdc_getmsgint(msg, sizeof(rsp_id));
		if (rsp_id != expected_id)
		{
			rdc_puterror(port,
						 "expected port id '%d' from server, "
						 "but received response id '%d'",
						 expected_id, rsp_id);
			return RDC_POLLING_FAILED;
		}
		rdc_getmsgend(msg);

#ifdef DEBUG_ADB
		elog(LOG,
			 "recv startup response from [%s %d] {%s:%s}",
			 rdc_type2string(rsp_type), RdcID(port),
			 RdcHostStr(port), RdcPortStr(port));
#endif

		/* We are done with authentication exchange */
		RdcStatus(port) = CONNECTION_AUTH_OK;
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
