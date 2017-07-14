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

static void TryReadSome(RdcPort *port);
static bool HandleReadFromReduce(RdcPort *port, List **pln_nodes);
static void HandleWriteToReduce(RdcPort *port);
static void HandleReadFromPlan(PlanPort *pln_port);
static void HandleWriteToPlan(PlanPort *pln_port, bool flush);
static void SendDataRdcToPlan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen);
static void SendEofRdcToPlan(PlanPort *pln_port, RdcPortId rdc_id);
static int  SendDataRdcToRdc(RdcPort *port, RdcPortId planid, const char *data, int datalen);
static int  SendEofRdcToRdc(RdcPortId planid);
static RdcPort *LookUpReducePort(RdcPortId rpid);

/*
 * HandlePlanIO
 *
 * handle I/O of port for Plan node.
 */
void
HandlePlanIO(List **pln_nodes)
{
	ListCell	   *cell = NULL;
	PlanPort	   *pln_port = NULL;

	AssertArg(pln_nodes);
	foreach (cell, *pln_nodes)
	{
		pln_port = (PlanPort *) lfirst(cell);
		HandleReadFromPlan(pln_port);
		HandleWriteToPlan(pln_port, false);
	}
}

/*
 * HandleReduceIO
 *
 * handle I/O of port for Reduce.
 */
void
HandleReduceIO(List **pln_nodes)
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
			if (HandleReadFromReduce(port, pln_nodes))
				rdc_node->port = NULL;
		}
		if (RdcWaitWrite(port))
			HandleWriteToReduce(port);
	}
}

/*
 * TryReadSome
 *
 * set noblock and try to read some data.
 */
static void
TryReadSome(RdcPort *port)
{
	if (!rdc_set_noblock(port))
		ereport(ERROR,
				(errmsg("fail to set noblocking mode for [%s %ld] {%s:%s}",
						RdcPeerTypeStr(port), RdcPeerID(port),
						RdcPeerHost(port), RdcPeerPort(port))));

	if (rdc_recv(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to read some from [%s %ld] {%s:%s}",
						RdcPeerTypeStr(port), RdcPeerID(port),
						RdcPeerHost(port), RdcPeerPort(port)),
				 errdetail("connection to client lost: %s", RdcError(port))));
}

/*
 * HandleReadFromPlan - handle message from Plan node
 */
static void
HandleReadFromPlan(PlanPort *pln_port)
{
	StringInfo		msg;
	int				sv_cursor;
	char			mtype;
	bool			port_free;
	bool			quit;
	RdcPort		   *port;
	RdcPort		   *next;
	RdcPort		   *prev = NULL;

	AssertArg(pln_port);

	SetRdcPsStatus(" reading from plan %ld", PlanID(pln_port));
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
			TryReadSome(port);

			msg = RdcInBuf(port);
			sv_cursor = msg->cursor;
			mtype = rdc_getbyte(port);
			if (mtype == EOF)
			{
				RdcWaitEvents(port) |= WT_SOCK_READABLE;
				break;		/* break while */
			}

			switch (mtype)
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
							RdcWaitEvents(port) |= WT_SOCK_READABLE;
							quit = true;	/* break while */
							break;			/* break case */
						}
						totallen = rdc_getmsgint(msg, sizeof(totallen));
						/* total data */
						totallen -= sizeof(totallen);
						if (rdc_getbytes(port, totallen) == EOF)
						{
							msg->cursor = sv_cursor;
							RdcWaitEvents(port) |= WT_SOCK_READABLE;
							quit = true;	/* break while */
							break;			/* break case */
						}
						if (mtype == RDC_P2R_DATA)
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
								rdc_port = LookUpReducePort(rid);
								/* send data to reduce */
								if (SendDataRdcToRdc(rdc_port, RdcPeerID(port), data, datalen))
									quit = true;	/* break while */
							}
						} else
						{
							if (SendEofRdcToRdc(RdcPeerID(port)))
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
							(errmsg("unexpected message type %d of Plan port",
									mtype)));
			}
		}

		if (!port_free)
			prev = port;
		port = next;
	}
}

static void
HandleWriteToPlan(PlanPort *pln_port, bool flush)
{
	StringInfo		buf;
	RdcPort		   *port;
	RSstate		   *rdcstore;
	int				r;
	volatile bool	eof;

	AssertArg(pln_port);

	SetRdcPsStatus(" writing to plan %ld", PlanID(pln_port));
	rdcstore = pln_port->rdcstore;
	port = pln_port->port;
	eof = (pln_port->eof_num == (pln_port->rdc_num - 1));

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
				RdcWaitEvents(port) |= WT_SOCK_WRITEABLE;
				break;		/* break for */
			}
			/* output buffer is empty, try to read from rdcstore */
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
							RdcWaitEvents(port) &= ~WT_SOCK_WRITEABLE;
						break;	/* break for */
					}
				}
			}
		}

		port = RdcNext(port);
	}
}

static bool
HandleReadFromReduce(RdcPort *port, List **pln_nodes)
{
	StringInfo		msg;
	int				sv_cursor;
	char			mtype;
	bool			quit;
	bool			port_free;

	AssertArg(pln_nodes);
	Assert(ReducePortIsValid(port));
	Assert(RdcPeerID(port) != MyReduceId);

	SetRdcPsStatus(" reading from reduce %ld", RdcPeerID(port));
	quit = false;
	port_free = false;
	msg = RdcInBuf(port);
	while (!quit)
	{
		/* set noblocking mode and read some data */
		TryReadSome(port);

		sv_cursor = msg->cursor;
		mtype = rdc_getbyte(port);
		if (mtype == EOF)
		{
			RdcWaitEvents(port) |= WT_SOCK_READABLE;
			break;		/* break while */
		}

		switch (mtype)
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
						port->wait_events |= WT_SOCK_READABLE;
						quit = true;	/* break while */
						break;
					}
					totalen = rdc_getmsgint(msg, sizeof(totalen));
					/* total data */
					totalen -= sizeof(totalen);
					if (rdc_getbytes(port, totalen) == EOF)
					{
						msg->cursor = sv_cursor;
						port->wait_events |= WT_SOCK_READABLE;
						quit = true;	/* break while */
						break;
					}
					/* plan node id */
					planid = rdc_getmsgRdcPortID(msg);
					/* find RdcPort of plan */
					pln_port = LookUpPlanPort(*pln_nodes, planid);
					if (pln_port == NULL)
					{
						pln_port = plan_newport(planid);
						*pln_nodes = lappend(*pln_nodes, pln_port);
					}
					/* data */
					if (mtype == RDC_R2R_DATA)
					{
						datalen = totalen - sizeof(planid);
						data = rdc_getmsgbytes(msg, datalen);
						rdc_getmsgend(msg);
						/* fill in data */
						SendDataRdcToPlan(pln_port, RdcPeerID(port), data, datalen);
					} else
					{
						rdc_getmsgend(msg);
						/* fill in eof */
						SendEofRdcToPlan(pln_port, RdcPeerID(port));
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
								mtype)));
				break;
		}
	}

	return port_free;
}

static void
HandleWriteToReduce(RdcPort *rdc_port)
{
	int ret;

	Assert(ReducePortIsValid(rdc_port));

	SetRdcPsStatus(" writing to reduce %ld", RdcPeerID(rdc_port));;
	ret = rdc_try_flush(rdc_port);
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(rdc_port) |= WT_SOCK_WRITEABLE;
	else
		RdcWaitEvents(rdc_port) &= ~WT_SOCK_WRITEABLE;
}

static void
SendDataRdcToPlan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen)
{
	RSstate			   *rdcstore = NULL;
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

	PlanPortAddEvents(pln_port, WT_SOCK_WRITEABLE);
	HandleWriteToPlan(pln_port, false);
}

static void
SendEofRdcToPlan(PlanPort *pln_port, RdcPortId rdc_id)
{
	int			i;
	bool		exists;

	AssertArg(pln_port);

#ifdef DEBUG_ADB
	elog(LOG,
		 "receive EOF message of plan %ld from reduce %ld",
		 PlanID(pln_port), rdc_id);
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
				 PlanID(pln_port), rdc_id)));
	pln_port->rdc_eofs[pln_port->eof_num++] = rdc_id;

	PlanPortAddEvents(pln_port, WT_SOCK_WRITEABLE);
	HandleWriteToPlan(pln_port, true);
}

static int
SendDataRdcToRdc(RdcPort *port, RdcPortId planid, const char *data, int datalen)
{
	StringInfoData		buf;
	int					ret;

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
		RdcWaitEvents(port) |= WT_SOCK_WRITEABLE;
	else
		RdcWaitEvents(port) &= ~WT_SOCK_WRITEABLE;

	return ret;
}

static int
SendEofRdcToRdc(RdcPortId planid)
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
		if (rdc_node->mask.rdc_rpid == MyReduceId ||
			rdc_port == NULL)
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
			RdcWaitEvents(rdc_port) |= WT_SOCK_WRITEABLE;
		}
		else
			RdcWaitEvents(rdc_port) &= ~WT_SOCK_WRITEABLE;
	}
	pfree(buf.data);

	return res;
}

static RdcPort *
LookUpReducePort(RdcPortId rpid)
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

	ereport(ERROR,
			(errmsg("REDUCE %ld doesn't exists", rpid)));

	return NULL;
}
