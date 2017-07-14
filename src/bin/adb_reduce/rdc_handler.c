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
static bool try_read_from_reduce(RdcPort *port, List **pln_nodes);
static void try_write_to_reduce(RdcPort *port);
static void try_read_from_plan(PlanPort *pln_port);
static void try_write_to_plan(PlanPort *pln_port, bool flush);
static void send_rdc2plan_data(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen);
static void send_rdc2plan_eof(PlanPort *pln_port, RdcPortId rdc_id);
static int  send_rdc2rdc_data(RdcPort *port, RdcPortId planid, const char *data, int datalen);
static int  send_rdc2rdc_eof(RdcPortId planid);
static RdcPort *find_rdc_port(RdcPortId rpid);

/*
 * rdc_handle_plannode - handle port for Plan node
 */
void
rdc_handle_plannode(List **pln_nodes)
{
	ListCell	   *cell = NULL;
	PlanPort	   *pln_port = NULL;

	AssertArg(pln_nodes);
	foreach (cell, *pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(cell);
		try_read_from_plan(pln_port);
		try_write_to_plan(pln_port, false);
	}
}

/*
 * rdc_handle_reduce - handle port for Reduce
 */
void
rdc_handle_reduce(List **pln_nodes)
{
	RdcNode		   *rdc_nodes = NULL;
	RdcNode		   *rdc_node = NULL;
	RdcPort		   *port = NULL;
	int				rdc_num;
	int				i;

	AssertArg(pln_nodes);
	rdc_nodes = MyRdcOpts->rdc_nodes;
	rdc_num = MyRdcOpts->rdc_num;
	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		port = rdc_node->port;
		if (rdc_node->mask.rdc_rpid == MyReduceId ||
			port == NULL)
			continue;
		if (RdcWaitRead(port))
		{
			if (try_read_from_reduce(port, pln_nodes))
				rdc_node->port = NULL;
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
try_read_from_plan(PlanPort *pln_port)
{
	StringInfo		msg;
	int				sv_cursor;
	char			firstchar;
	bool			port_free;
	bool			quit;
	RdcPort		   *port;
	RdcPort		   *next;
	RdcPort		   *prev = NULL;

	AssertArg(pln_port);

	SetRdcPsStatus(" reading from plan %ld", pln_port->pln_id);
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
						/* total data */
						totallen -= sizeof(totallen);
						if (rdc_getbytes(port, totallen) == EOF)
						{
							msg->cursor = sv_cursor;
							port->wait_events |= WAIT_SOCKET_READABLE;
							quit = true;	/* break while */
							break;			/* break case */
						}
						if (firstchar == RDC_P2R_DATA)
						{
							/* data length and data */
							datalen = rdc_getmsgint(msg, sizeof(datalen));
							data = rdc_getmsgbytes(msg, datalen);
							/* reduce number */
							num = rdc_getmsgint(msg, sizeof(num));
							/* reduce id */
							for (i = 0; i < num; i++)
							{
								rid = rdc_getmsgRdcPortID(msg);
								Assert(rid != MyReduceId);
								/* reduce port */
								rdc_port = find_rdc_port(rid);
								Assert(rdc_port);
								/* send data to reduce */
								if (send_rdc2rdc_data(rdc_port, RdcPeerID(port), data, datalen))
									quit = true;	/* break while */
							}
						} else
						{
							if (send_rdc2rdc_eof(RdcPeerID(port)))
								quit = true;	/* break while */
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
}

static void
try_write_to_plan(PlanPort *pln_port, bool flush)
{
	StringInfo		buf;
	RdcPort		   *port;
	RSstate		   *rdcstore;
	int				r;
	volatile bool	eof;

	AssertArg(pln_port);

	SetRdcPsStatus(" writing to plan %ld", pln_port->pln_id);
	rdcstore = pln_port->rdcstore;
	port = pln_port->port;
	eof = (pln_port->eof_num == pln_port->rdc_num - 1);

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
					{
						if (flush)
							continue;
						break;		/* break while */
					}

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
					if (eof && !RdcSendEOF(port))
					{
						rdc_beginmessage(buf, RDC_EOF_MSG);
						rdc_sendlength(buf);
						RdcSendEOF(port) = true;
					} else
					{
						if (RdcSendEOF(port))
							RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;
						break;	/* break for */
					}
				}
			}
		}

		port = RdcNext(port);
	}
}

static bool
try_read_from_reduce(RdcPort *port, List **pln_nodes)
{
	StringInfo		msg;
	int				sv_cursor;
	char			firstchar;
	bool			quit;
	bool			port_free;

	AssertArg(port && pln_nodes);
	Assert(ReducePortIsValid(port));
	Assert(RdcPeerID(port) != MyReduceId);

	SetRdcPsStatus(" reading from reduce %ld", RdcPeerID(port));
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
					int				totalen;

					/* total data length*/
					if (rdc_getbytes(port, sizeof(totalen)) == EOF)
					{
						msg->cursor = sv_cursor;
						port->wait_events |= WAIT_SOCKET_READABLE;
						quit = true;	/* break while */
						break;
					}
					totalen = rdc_getmsgint(msg, sizeof(totalen));
					/* total data */
					totalen -= sizeof(totalen);
					if (rdc_getbytes(port, totalen) == EOF)
					{
						msg->cursor = sv_cursor;
						port->wait_events |= WAIT_SOCKET_READABLE;
						quit = true;	/* break while */
						break;
					}
					/* plan node id */
					planid = rdc_getmsgRdcPortID(msg);
					/* find RdcPort of plan */
					pln_port = find_plan_port(*pln_nodes, planid);
					if (pln_port == NULL)
					{
						pln_port = plan_newport(planid);
						*pln_nodes = lappend(*pln_nodes, pln_port);
					}
					/* data */
					if (firstchar == RDC_R2R_DATA)
					{
						datalen = totalen - sizeof(planid);
						data = rdc_getmsgbytes(msg, datalen);
						rdc_getmsgend(msg);
						/* fill in data */
						send_rdc2plan_data(pln_port, RdcPeerID(port), data, datalen);
					} else
					{
						rdc_getmsgend(msg);
						/* fill in eof */
						send_rdc2plan_eof(pln_port, RdcPeerID(port));
					}
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

	AssertArg(port);
	Assert(ReducePortIsValid(port));

	SetRdcPsStatus(" writing to reduce %ld", RdcPeerID(port));;
	ret = rdc_try_flush(port);
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
	else
		RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;
}

static void
send_rdc2plan_data(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen)
{
	RSstate			   *rdcstore = NULL;
	RdcPort			   *port = NULL;
	StringInfoData		buf;

	AssertArg(pln_port);
	AssertArg(data);
	Assert(datalen > 0);
	Assert(pln_port->rdcstore);
	rdcstore = pln_port->rdcstore;

	rdc_beginmessage(&buf, RDC_R2P_DATA);
#ifdef DEBUG_ADB
	rdc_sendRdcPortID(&buf, rdc_id);
#endif
	rdc_sendbytes(&buf, data, datalen);
	rdc_sendlength(&buf);

	rdcstore_puttuple(rdcstore, buf.data, buf.len);
	pfree(buf.data);
	buf.data = NULL;

	port = pln_port->port;
	while (port != NULL)
	{
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
		port = RdcNext(port);
	}
	try_write_to_plan(pln_port, false);
}

static void
send_rdc2plan_eof(PlanPort *pln_port, RdcPortId rdc_id)
{
	RdcPort	   *port;
	int			i;
	bool		exists;

	AssertArg(pln_port);

#ifdef DEBUG_ADB
	elog(LOG,
		 "receive EOF message of plan %ld from reduce %ld",
		 pln_port->pln_id, rdc_id);
#endif

	exists = false;
	for (i = 0; i < pln_port->eof_num; i++)
	{
		if (pln_port->rdc_eofs[i] == rdc_id)
		{
			exists = true;
			break;
		}
	}
	if (exists)
		ereport(ERROR,
				(errmsg("receive EOF message of plan %ld from REDUCE %ld once again",
				 pln_port->pln_id, rdc_id)));
	pln_port->rdc_eofs[pln_port->eof_num++] = rdc_id;

	port = pln_port->port;
	while (port != NULL)
	{
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
		port = RdcNext(port);
	}
	try_write_to_plan(pln_port, true);
}

static int
send_rdc2rdc_data(RdcPort *port, RdcPortId planid, const char *data, int datalen)
{
	StringInfoData		buf;
	int					ret;

	AssertArg(port);
	AssertArg(data);
	Assert(datalen > 0);
	Assert(ReducePortIsValid(port));

	rdc_beginmessage(&buf, RDC_R2R_DATA);
	rdc_sendRdcPortID(&buf, planid);
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

static int
send_rdc2rdc_eof(RdcPortId planid)
{
	int				i;
	int				rdc_num = MyRdcOpts->rdc_num;
	RdcNode		   *rdc_nodes = MyRdcOpts->rdc_nodes;
	RdcNode		   *rdc_node = NULL;
	RdcPort		   *rdc_port;
	StringInfoData	buf;
	int				ret;
	int				res = 0;

	rdc_beginmessage(&buf, RDC_EOF_MSG);
	rdc_sendRdcPortID(&buf, planid);
	rdc_sendlength(&buf);

	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		rdc_port = rdc_node->port;
		if (rdc_node->mask.rdc_rpid == MyReduceId)
			continue;

		Assert(RdcPeerID(rdc_port) != MyReduceId);
#ifdef DEBUG_ADB
		elog(LOG,
			 "Send EOF message of plan %ld to reduce %ld",
			 planid, RdcPeerID(rdc_port));
#endif
		rdc_putmessage(rdc_port, buf.data, buf.len);

		ret = rdc_try_flush(rdc_port);
		CHECK_FOR_INTERRUPTS();
		if (ret != 0)
		{
			res = ret;
			RdcWaitEvents(rdc_port) |= WAIT_SOCKET_WRITEABLE;
		}
		else
			RdcWaitEvents(rdc_port) &= ~WAIT_SOCKET_WRITEABLE;
	}
	pfree(buf.data);

	return res;
}

static RdcPort *
find_rdc_port(RdcPortId rpid)
{
	int			i;
	int			rdc_num = MyRdcOpts->rdc_num;
	RdcNode	   *rdc_nodes = MyRdcOpts->rdc_nodes;
	RdcNode	   *rdc_node = NULL;

	Assert(rpid != MyReduceId);
	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		if (rdc_node->mask.rdc_rpid == rpid)
			return rdc_node->port;
	}

	return NULL;
}
