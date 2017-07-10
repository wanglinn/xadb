/*-------------------------------------------------------------------------
 *
 * rdc_handler.c
 *	  interface for handling messages between Reduce and Reduce,
 *	  also for handling messages between Reduce and Plan node.
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/bin/adb_reduce/rdc_handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "rdc_globals.h"

#include "rdc_handler.h"
#include "rdc_plan.h"
#include "reduce/rdc_msg.h"

static int  try_read_some(RdcPort *port);
static bool try_read_from_reduce(RdcPort *port, List **pln_list);
static void try_write_to_reduce(RdcPort *port);
static void try_read_from_plan(PlanPort *pln_port, RdcPort **rdc_nodes, int rdc_num);
static void try_write_to_plan(PlanPort *pln_port);
static void build_packet_rdc2plan(StringInfo buf, RdcPortId rdc_id, const char *data, int datalen);
static void send_rdc2plan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen);
static int  send_rdc2rdc(RdcPort *port, RdcPortId planid, const char *data, int datalen);

/*
 * rdc_handle_plannode - handle port for Plan node
 */
void
rdc_handle_plannode(RdcPort **rdc_nodes, int rdc_num, List *pln_list)
{
	ListCell	   *cell = NULL;
	PlanPort	   *pln_port = NULL;

	foreach (cell, pln_list)
	{
		pln_port = (PlanPort *) lfirst(cell);
		try_read_from_plan(pln_port, rdc_nodes, rdc_num);
		try_write_to_plan(pln_port);
	}
}

/*
 * rdc_handle_reduce - handle port for Reduce
 */
void
rdc_handle_reduce(RdcPort **rdc_nodes, int rdc_num, List **pln_list)
{
	RdcPort		   *port = NULL;
	int				i;

	for (i = 0; i < rdc_num; i++)
	{
		port = rdc_nodes[i];
		/* skip if null (MyReduceId or be free) */
		if (port == NULL)
			continue;
		if (RdcWaitRead(port))
		{
			if (try_read_from_reduce(port, pln_list))
				rdc_nodes[i] = NULL;
		}
		if (RdcWaitWrite(port))
			try_write_to_reduce(port);
	}
}

/*
 * try_read_some - set noblock and try to read some data
 *
 * returns 0 if OK
 * returns EOF if trouble
 */
static int
try_read_some(RdcPort *port)
{
	if (!rdc_set_noblock(port))
		ereport(ERROR,
				(errmsg("fail to set noblocking mode for [%s %ld] {%s:%s}",
						RdcPeerTypeStr(port), RdcPeerID(port),
						RdcPeerHost(port), RdcPeerPort(port))));

	if (rdc_recv(port) == EOF)
		return EOF;

	return 0;
}

/*
 * try_read_from_plan - handle message from Plan node
 */
static void
try_read_from_plan(PlanPort *pln_port, RdcPort **rdc_nodes, int rdc_num)
{
	StringInfo		msg;
	int				sv_cursor;
	char			firstchar;
	bool			port_free;
	bool			quit;
	RdcPort		   *port;
	RdcPort		   *next;
	RdcPort		   *prev = NULL;

	AssertArg(pln_port && rdc_nodes);
	Assert(rdc_num > 0);

	port = pln_port->port;
	while (port != NULL)
	{
		Assert(PlanPortIsValid(port));
		Assert(RdcSockIsValid(port));

		next = RdcNext(port);

		/* set true if got RDC_CLOSE_MSG */
		port_free = false;

		/* skip if do not care about READ event */
		if (!RdcWaitRead(port))
		{
			port = next;
			continue;
		}

		/* try to read as much as possible */
		quit = false;
		while (!quit)
		{
			/* set noblocking mode and read some data */
			if (try_read_some(port) == EOF)
				ereport(ERROR,
						(errmsg("fail to read some from [%s %ld] {%s:%s}",
								RdcPeerTypeStr(port), RdcPeerID(port),
								RdcPeerHost(port), RdcPeerPort(port)),
						 errhint("connection to client lost")));

			msg = RdcInBuf(port);
			sv_cursor = msg->cursor;
			firstchar = rdc_getbyte(port);
			if (firstchar == EOF)
			{
				port->wait_events |= WAIT_SOCKET_READABLE;
				break;		/* break while */
			}

			switch (firstchar)
			{
				case RDC_P2R_DATA:
				case RDC_EOF_MSG:
					{
						RdcPort		   *rdc_port;
						RdcPortId		rid;
						int				ridx;
						const char	   *data;
						int				num, i;
						int				totallen, datalen;

						/* total data length */
						if (rdc_getbytes(port, sizeof(totallen)) == EOF)
						{
							msg->cursor = sv_cursor;
							port->wait_events |= WAIT_SOCKET_READABLE;
							quit = true;	/* break while */
							break;			/* break case */
						}
						totallen = rdc_getmsgint(msg, sizeof(totallen));
						totallen -= sizeof(totallen);
						if (rdc_getbytes(port, totallen) == EOF)
						{
							msg->cursor = sv_cursor;
							port->wait_events |= WAIT_SOCKET_READABLE;
							quit = true;	/* break while */
							break;			/* break case */
						}
						/* data length and data */
						datalen = rdc_getmsgint(msg, sizeof(datalen));
						data = rdc_getmsgbytes(msg, datalen);
						/* reduce number */
						num = rdc_getmsgint(msg, sizeof(num));
						/* reduce id */
						for (i = 0; i < num; i++)
						{
							rid = rdc_getmsgint64(msg);
							Assert(rid != MyReduceId);
							Assert(rid >= 0 && rid < rdc_num);
							/* reduce port */
							ridx = rdc_portidx(MyReduceOpts->rdc_masks, MyReduceOpts->rdc_num, rid);
							Assert(ridx >= 0);
							rdc_port = rdc_nodes[ridx];
							if (firstchar == RDC_P2R_DATA)
							{
								/* send data to reduce */
								if (send_rdc2rdc(rdc_port, RdcPeerID(port), data, datalen))
									quit = true;	/* break while */
							} else
							{
								/* send eof to reduce */
								RdcGotEof(port) = true;
								RdcWaitEvents(port) &= ~WAIT_SOCKET_READABLE;
								if (send_rdc2rdc(rdc_port, RdcPeerID(port), NULL, 0))
									quit = true;	/* break while */
							}
						}
						rdc_getmsgend(msg);
					}
					break;
				case RDC_CLOSE_MSG:
					{
						/* TODO: how to close? */
						elog(LOG,
							 "Receive close message from [%s %ld] {%s:%s}",
							 RdcPeerTypeStr(port), RdcPeerID(port),
							 RdcPeerHost(port), RdcPeerPort(port));

						/* close the first RdcPort of PlanPort */
						if (prev == NULL)
							pln_port->port = next;
						else
							RdcNext(prev) = next;
						rdc_freeport(port);
						port_free = true;
						quit = true;	/* break while */
					}
					break;
				case RDC_ERROR_MSG:
					break;
				default:
					ereport(ERROR,
							(errmsg("unexpected message type %c of Plan port",
									firstchar)));
			}
		}

		if (!port_free)
			prev = port;
		port = next;
	}

	/* The worker RdcPort of PlanPort is all free */
	if (pln_port->port == NULL)
		plan_freeport(pln_port);
}

static void
try_write_to_plan(PlanPort *pln_port)
{
	StringInfo		buf;
	RdcPort		   *port;
	RSstate		   *rdcstore;
	int				r;

	AssertArg(pln_port);

	rdcstore = pln_port->rdcstore;
	port = pln_port->port;
	while (port != NULL)
	{
		Assert(PlanPortIsValid(port));
		Assert(RdcSockIsValid(port));

		/* skip if do not care about WRITE event */
		if (!RdcWaitWrite(port))
		{
			port = RdcNext(port);
			continue;
		}

		/* set in noblocking mode */
		if (!rdc_set_noblock(port))
			ereport(ERROR,
					(errmsg("fail to set noblocking mode for [%s %ld] {%s:%s}",
							RdcPeerTypeStr(port), RdcPeerID(port),
							RdcPeerHost(port), RdcPeerPort(port))));

		buf = RdcOutBuf(port);
		for (;;)
		{
			/* output buffer has unsent data, try to send them first */
			while (buf->cursor < buf->len)
			{
				r = send(RdcSocket(port), buf->data + buf->cursor, buf->len - buf->cursor, 0);
				if (r <= 0)
				{
					if (errno == EINTR)
						continue;

					if (errno == EAGAIN ||
						errno == EWOULDBLOCK)
						break;		/* break while */

					buf->cursor = buf->len = 0;
					ClientConnectionLost = 1;
					InterruptPending = 1;
					CHECK_FOR_INTERRUPTS();		/* fail to send */
				}
				buf->cursor += r;
			}

			/* break and wait for next time if can't continue sending */
			if (buf->cursor < buf->len)
			{
				RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
				break;		/* break for */
			}
			/* output buffer is empty, continue trying rdcstore */
			else
			{
				bool	hasData = false;

				/* it is safe to reset output buffer */
				resetStringInfo(buf);
				Assert(rdcstore);

				/* try to get one tuple from rdcstore */
				rdcstore_gettuple(rdcstore, buf, &hasData);
				/* break rdcstore is also empty */
				if (!hasData)
				{
					RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;
					break;	/* break for */
				}
			}
		}

		port = RdcNext(port);
	}
}

static bool
try_read_from_reduce(RdcPort *port, List **pln_list)
{
	StringInfo		msg;
	int				sv_cursor;
	char			firstchar;
	bool			quit;
	bool			port_free;

	AssertArg(port && pln_list);
	Assert(ReducePortIsValid(port));
	Assert(RdcPeerID(port) != MyReduceId);

	quit = false;
	port_free = false;
	msg = RdcInBuf(port);
	while (!quit)
	{
		/* set noblocking mode and read some data */
		if (try_read_some(port) == EOF)
			ereport(ERROR,
					(errmsg("fail to read some from [%s %ld] {%s:%s}",
							RdcPeerTypeStr(port), RdcPeerID(port),
							RdcPeerHost(port), RdcPeerPort(port)),
					 errhint("connection to client lost")));

		sv_cursor = msg->cursor;
		firstchar = rdc_getbyte(port);
		if (firstchar == EOF)
		{
			port->wait_events |= WAIT_SOCKET_READABLE;
			break;		/* break while */
		}

		switch (firstchar)
		{
			case RDC_R2R_DATA:
			case RDC_EOF_MSG:
				{
					PlanPort	   *pln_port;
					const char	   *data;
					RdcPortId		planid;
					int				datalen;

					/* length and planid */
					if (rdc_getbytes(port, sizeof(datalen)) == EOF ||
						rdc_getbytes(port, sizeof(planid)) == EOF)
					{
						msg->cursor = sv_cursor;
						port->wait_events |= WAIT_SOCKET_READABLE;
						quit = true;	/* break while */
						break;			/* break case */
					}
					datalen = rdc_getmsgint(msg, sizeof(datalen));
					datalen -= sizeof(datalen);
					planid = rdc_getmsgint64(msg);
					datalen -= sizeof(planid);
					/* find RdcPort of plan */
					pln_port = find_plan_port(*pln_list, planid);
					if (pln_port == NULL)
					{
						pln_port = plan_newport(planid);
						*pln_list = lappend(*pln_list, pln_port);
					}
					/* got eof */
					if (firstchar == RDC_EOF_MSG)
					{
						rdc_getmsgend(msg);
						RdcGotEof(port) = true;
						/* fill in eof */
						send_rdc2plan(pln_port, RdcPeerID(port), NULL, 0);
						break ;			/* break case */
					}
					/* got data */
					if (rdc_getbytes(port, datalen) == EOF)
					{
						msg->cursor = sv_cursor;
						port->wait_events |= WAIT_SOCKET_READABLE;
						quit = true;	/* break while */
						break ;			/* break case */
					}
					data = rdc_getmsgbytes(msg, datalen);
					rdc_getmsgend(msg);

					/* fill in data */
					send_rdc2plan(pln_port, RdcPeerID(port), data, datalen);
				}
				break;
			case RDC_CLOSE_MSG:
				{
					/* TODO: how to close? */
					elog(LOG,
						 "Receive close message from [%s %ld] {%s:%s}",
						 RdcPeerTypeStr(port), RdcPeerID(port),
						 RdcPeerHost(port), RdcPeerPort(port));
					rdc_freeport(port);
					port_free = true;
					quit = true;		/* break while */
				}
				break;
			case RDC_ERROR_MSG:
				break;
			default:
				ereport(ERROR,
						(errmsg("unexpected message type %c of Reduce port",
								firstchar)));
				break;
		}
	}

	return port_free;
}

static void
try_write_to_reduce(RdcPort *port)
{
	int ret;
	ret = rdc_try_flush(port);
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
	else
		RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;
}

static void
build_packet_rdc2plan(StringInfo buf, RdcPortId rdc_id, const char *data, int datalen)
{
	AssertArg(buf);

	if (data != NULL)
	{
		Assert(datalen > 0);
		rdc_beginmessage(buf, RDC_R2P_DATA);
	} else
	{
		Assert(datalen == 0);
		rdc_beginmessage(buf, RDC_EOF_MSG);
	}
	rdc_sendint64(buf, rdc_id);
	if (data)
		rdc_sendbytes(buf, data, datalen);
	rdc_sendlength(buf);
}

static void
send_rdc2plan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen)
{
	RSstate			   *rdcstore = NULL;
	RdcPort			   *port = NULL;
	StringInfoData		buf;

	AssertArg(pln_port);
	Assert(pln_port->rdcstore);
	rdcstore = pln_port->rdcstore;

	build_packet_rdc2plan(&buf, rdc_id, data, datalen);
	rdcstore_puttuple(rdcstore, buf.data, buf.len);
	pfree(buf.data);
	buf.data = NULL;

	port = pln_port->port;
	while (port != NULL)
	{
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
		port = RdcNext(port);
	}
	try_write_to_plan(pln_port);
}

static int
send_rdc2rdc(RdcPort *port, RdcPortId planid, const char *data, int datalen)
{
	StringInfoData		buf;
	int					ret;

	AssertArg(port);
	Assert(ReducePortIsValid(port));

	if (data != NULL)
	{
		Assert(datalen > 0);
		rdc_beginmessage(&buf, RDC_R2R_DATA);
	} else
	{
		Assert(datalen == 0);
		rdc_beginmessage(&buf, RDC_EOF_MSG);
	}
	rdc_sendint64(&buf, planid);
	if (data)
		rdc_sendbytes(&buf, data, datalen);
	rdc_endmessage(port, &buf);

	ret = rdc_try_flush(port);
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
	else
		RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;

	return ret;
}

