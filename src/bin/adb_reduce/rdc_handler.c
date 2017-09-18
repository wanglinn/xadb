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

#include "rdc_exit.h"
#include "rdc_handler.h"
#include "rdc_plan.h"
#include "reduce/rdc_msg.h"
#include "utils/memutils.h"		/* for MemoryContext */

static int  HandlePlanMsg(RdcPort *work_port, PlanPort *pln_port);
static void HandleRdcMsg(RdcPort *rdc_port, List **pln_nodes);
static void HandleReadFromReduce(RdcPort *port, List **pln_nodes);
static void HandleWriteToReduce(RdcPort *port);
static void HandleReadFromPlan(PlanPort *pln_port);
static void HandleWriteToPlan(PlanPort *pln_port);
static bool WritePlanEndToPlanHook(const char *data, int datalen, void *context);
static void SendPlanMsgToPlan(PlanPort *pln_port, char msg_type, RdcPortId rdc_id, const char *data, int datalen);
static void SendPlanDataToPlan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen);
static void SendPlanEofToPlan(PlanPort *pln_port, RdcPortId rdc_id, bool error_if_exists);
static void SendPlanCloseToPlan(PlanPort *pln_port, RdcPortId rdc_id);
static void SendPlanRejectToPlan(PlanPort *pln_port, RdcPortId rdc_id);
static int  SendPlanDataToRdc(StringInfo msg, PlanPort *pln_port);
static int  SendPlanEofToRdc(StringInfo msg, PlanPort *pln_port);
static int  SendPlanCloseToRdc(StringInfo msg, PlanPort *pln_port);
static int  SendPlanRejectToRdc(StringInfo msg, PlanPort *pln_port);
static int  BroadcastDataToRdc(StringInfo msg,
							   PlanPort *pln_port,
							   char msg_type,
							   const char *msg_data,
							   int msg_len,
							   bool flush);
static RdcPort *LookupReducePort(RdcPortId rpid);

/*
 * HandlePlanIO
 *
 * handle I/O of port for Plan node.
 */
void
HandlePlanIO(List **pln_nodes)
{
	ListCell	   *cell = NULL;
	ListCell	   *next = NULL;
	PlanPort	   *pln_port = NULL;

	AssertArg(pln_nodes);
	for (cell = list_head(*pln_nodes); cell != NULL; cell = next)
	{
		pln_port = (PlanPort *) lfirst(cell);
		next = lnext(cell);
		Assert(pln_port);

		if (!PlanPortIsValid(pln_port))
			continue;

		if (PlanPortIsValid(pln_port))
		{
			HandleReadFromPlan(pln_port);
			HandleWriteToPlan(pln_port);
		}

		/*
		 * PlanPort may be invalid after reading from plan,
		 * so release it.
		 *
		 * Here is we truly free sources of a PlanPort,
		 * but we do not delete from PlanPort list, as it
		 * is already marked invalid and no use anymore.
		 */
		FreeInvalidPlanPort(pln_port);
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
	RdcPort		   *rdc_port = NULL;
	int				rdc_num;
	int				i;

	AssertArg(pln_nodes);
	rdc_nodes = MyRdcOpts->rdc_nodes;
	rdc_num = MyRdcOpts->rdc_num;
	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		rdc_port = rdc_node->port;

		if (!PortIsValid(rdc_port))
			continue;

		if (RdcWaitRead(rdc_port))
			HandleReadFromReduce(rdc_port, pln_nodes);
		if (RdcWaitWrite(rdc_port))
			HandleWriteToReduce(rdc_port);

		if (!PortIsValid(rdc_port))
		{
			/*
			 * Here is we truly close port of reduce
			 * rdc_freeport(rdc_port);
			 * rdc_node->port = NULL;
			 */
		}
	}
}

/*
 * HandlePlanMsg
 *
 * Handle message from plan node.
 *
 * return 0 if the message hasn't enough length.
 * return 1 if flush to other reduce would block.
 * return EOF if receive CLOSE message from plan node.
 */
static int
HandlePlanMsg(RdcPort *work_port, PlanPort *pln_port)
{
	bool			quit;
	StringInfo		msg;
	char			msg_type;
	int				msg_len;
	int				sv_cursor;
	int				res = 0;

	Assert(RdcPeerID(work_port) == PlanID(pln_port));

	quit = false;
	msg = RdcInBuf(work_port);
	while (!quit)
	{
		sv_cursor = msg->cursor;
		if ((msg_type = rdc_getbyte(work_port)) == EOF ||
			rdc_getbytes(work_port, sizeof(msg_len)) == EOF)
		{
			msg->cursor = sv_cursor;
			RdcWaitEvents(work_port) |= WT_SOCK_READABLE;
			break;		/* break while */
		}
		msg_len = rdc_getmsgint(msg, sizeof(msg_len));
		/* total data */
		msg_len -= sizeof(msg_len);
		if (rdc_getbytes(work_port, msg_len) == EOF)
		{
			msg->cursor = sv_cursor;
			RdcWaitEvents(work_port) |= WT_SOCK_READABLE;
			break;		/* break while */
		}

		/*
		 * enough length of buffer for one whole message from PLAN,
		 * so increase the number of receiving from PLAN.
		 */
		pln_port->recv_from_pln++;

		switch (msg_type)
		{
			case MSG_P2R_DATA:
				{
					if (SendPlanDataToRdc(msg, pln_port))
					{
						/*
						 * flush to other reduce would block,
						 * and we try to read from plan next time.
						 */
						res = 1;
						quit = true;	/* break while */
					}
				}
				break;
			case MSG_EOF:
				{
					elog(LOG,
						 "recv EOF message from" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(work_port));

					/* msg contains the target nodes(number and RdcPortIds) */
					if (SendPlanEofToRdc(msg, pln_port))
					{
						/*
						 * flush to other reduce would block,
						 * and we try to read from plan next time.
						 */
						res = 1;
						quit = true;	/* break while */
					}
				}
				break;
			case MSG_PLAN_CLOSE:
				{
					elog(LOG,
						 "recv PLAN CLOSE message from" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(work_port));

					/*
					 * do not wait read and write events on socket of port as
					 * it would be "CLOSEd".
					 */
					RdcWaitEvents(work_port) &= ~WT_SOCK_READABLE;
					RdcWaitEvents(work_port) &= ~WT_SOCK_WRITEABLE;
					RdcFlags(work_port) = RDC_FLAG_CLOSED;
					pln_port->work_num--;
					/* this mark the PlanPort is invalid */
					if (PlanWorkNum(pln_port) == 0)
						PlanFlags(pln_port) = PLAN_FLAG_CLOSED;
					quit = true;	/* break while */
					res = EOF;

					(void) SendPlanCloseToRdc(msg, pln_port);
				}
				break;
			case MSG_PLAN_REJECT:
				{
					elog(LOG,
						 "recv PLAN REJECT message from" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(work_port));

					/*
					 * do not wait write envents on socket of port as
					 * it will reject.
					 */
					PlanFlags(pln_port) |= PLAN_FLAG_REJECT;

					if (SendPlanRejectToRdc(msg, pln_port))
					{
						/*
						 * flush to other reduce would block,
						 * and we try to read from plan next time.
						 */
						res = 1;
						quit = true;	/* break while */
					}
				}
				break;
			case MSG_ERROR:
				break;
			default:
				ereport(ERROR,
						(errmsg("unexpected message type %d of Plan port",
								msg_type)));
		}
	}

	return res;
}

/*
 * HandleReadFromPlan
 *
 * handle message from Plan node.
 */
static void
HandleReadFromPlan(PlanPort *pln_port)
{
	RdcPort		   *work_port;

	AssertArg(pln_port);
	if (!PlanPortIsValid(pln_port))
		return ;

	SetRdcPsStatus(" reading from plan " PORTID_FORMAT, PlanID(pln_port));
	for (work_port = pln_port->work_port;
		 work_port != NULL;
		 work_port= RdcNext(work_port))
	{
		Assert(PlanTypeIDIsValid(work_port));
		Assert(RdcSockIsValid(work_port));

		/* skip if do not care about READ event */
		if (!PortIsValid(work_port) ||
			!RdcWaitRead(work_port))
			continue;

		/* try to read as mush as possible */
		while (HandlePlanMsg(work_port, pln_port) == 0)
		{
			if (!PortIsValid(work_port) ||		/* break if work port is invalid */
				rdc_try_read_some(work_port) == 0)	/* break if read would block */
				break;
		}
	}
}

/*
 * HandleWriteToPlan
 *
 * send data to plan node.
 */
static void
HandleWriteToPlan(PlanPort *pln_port)
{
	StringInfo		buf;
	StringInfo		buf2;
	RdcPort		   *work_port;
	RSstate		   *rdcstore;
	int				r;

	AssertArg(pln_port);
	if (!PlanPortIsValid(pln_port) ||
		PlanPortIsReject(pln_port))
		return ;

	SetRdcPsStatus(" writing to plan " PORTID_FORMAT, PlanID(pln_port));
	rdcstore = pln_port->rdcstore;
	work_port = pln_port->work_port;

	/*
	 * To avoid forgetting to send data, add wait events again
	 * for PlanPort.
	 */
	if (!rdcstore_ateof(rdcstore))
		PlanPortAddEvents(pln_port, WT_SOCK_WRITEABLE);

	while (work_port != NULL)
	{
		Assert(PlanTypeIDIsValid(work_port));
		Assert(RdcSockIsValid(work_port));

		/* skip if do not care about WRITE event */
		if (!PortIsValid(work_port) ||
			!RdcWaitWrite(work_port))
		{
			work_port = RdcNext(work_port);
			continue;
		}

		/* set in noblocking mode */
		if (!rdc_set_noblock(work_port))
			ereport(ERROR,
					(errmsg("fail to set noblocking mode for" RDC_PORT_PRINT_FORMAT,
							RDC_PORT_PRINT_VALUE(work_port))));

		buf = RdcOutBuf(work_port);
		buf2 = RdcOutBuf2(work_port);
		for (;;)
		{
			/* output buffer has unsent data, try to send them first */
			while (buf->cursor < buf->len)
			{
				r = send(RdcSocket(work_port), buf->data + buf->cursor, buf->len - buf->cursor, 0);
				if (r <= 0)
				{
					if (errno == EINTR)
						continue;

					if (errno == EAGAIN ||
						errno == EWOULDBLOCK)
						break;		/* break while */

					buf->cursor = buf->len = 0;
					ClientConnectionLostType = RdcPeerType(work_port);
					ClientConnectionLostID = RdcPeerID(work_port);
					ClientConnectionLost = 1;
					InterruptPending = 1;
					CHECK_FOR_INTERRUPTS();		/* fail to send */
				}
				buf->cursor += r;
			}

			/* break and wait for next time if can't continue sending */
			if (buf->cursor < buf->len)
			{
				RdcWaitEvents(work_port) |= WT_SOCK_WRITEABLE;
				break;		/* break for */
			}
			/* output buffer is empty, try to read from rdcstore */
			else
			{
				int		count;

				/* it is safe to reset output buffer */
				resetStringInfo(buf);
				Assert(rdcstore);
				appendStringInfoStringInfo(buf, buf2);
				resetStringInfo(buf2);

				count = rdcstore_gettuple_multi(rdcstore, buf, buf2, WritePlanEndToPlanHook, pln_port);
				Assert(count >= 0 && buf->len >= 0 && buf2->len >= 0);
				pln_port->send_to_pln += count;

				/* nothing to send, remove write event and break */
				if (buf->len == 0 && buf2->len == 0)
				{
					RdcWaitEvents(work_port) &= ~WT_SOCK_WRITEABLE;
					break;	/* break for */
				}
			}
		}

		work_port = RdcNext(work_port);
	}
}

static bool
WritePlanEndToPlanHook(const char *data, int datalen, void *context)
{
	bool is_plan_end = false;

	Assert(data && context && data > 0);
	if (data[0] == MSG_EOF || data[0] == MSG_PLAN_CLOSE)
	{
		PlanPort   *pln_port = (PlanPort *) context;
		RdcPort	   *wrk_port;
		StringInfo	buf2;

		wrk_port = pln_port->work_port;
		while (wrk_port)
		{
			buf2 = RdcOutBuf2(wrk_port);
			appendBinaryStringInfo(buf2, data, datalen);
			wrk_port = RdcNext(wrk_port);
		}

		is_plan_end = true;
	}

	return is_plan_end;
}


/*
 * HandleRdcMsg
 *
 * handle message from other reduce
 */
static void
HandleRdcMsg(RdcPort *rdc_port, List **pln_nodes)
{
	StringInfo		msg;
	char			msg_type;
	int				msg_len;
	int				sv_cursor;

	Assert(ReduceTypeIDIsValid(rdc_port));
	Assert(PortIsValid(rdc_port));
	Assert(pln_nodes);

	msg = RdcInBuf(rdc_port);
	while (true)
	{
		sv_cursor = msg->cursor;
		if ((msg_type = rdc_getbyte(rdc_port)) == EOF ||
			rdc_getbytes(rdc_port, sizeof(msg_len)) == EOF)
		{
			msg->cursor = sv_cursor;
			RdcWaitEvents(rdc_port) |= WT_SOCK_READABLE;
			break;		/* break while */
		}
		msg_len = rdc_getmsgint(msg, sizeof(msg_len));
		/* total data */
		msg_len -= sizeof(msg_len);
		if (rdc_getbytes(rdc_port, msg_len) == EOF)
		{
			msg->cursor = sv_cursor;
			RdcWaitEvents(rdc_port) |= WT_SOCK_READABLE;
			break;		/* break while */
		}

		switch (msg_type)
		{
			case MSG_R2R_DATA:
			case MSG_EOF:
			case MSG_PLAN_CLOSE:
			case MSG_PLAN_REJECT:
				{
					PlanPort	   *pln_port;
					const char	   *data;
					RdcPortId		planid;
					int 			datalen;

					/* plan node id */
					planid = rdc_getmsgRdcPortID(msg);
					/* find RdcPort of plan */
					pln_port = LookupPlanPort(*pln_nodes, planid);
					if (pln_port == NULL)
					{
						pln_port = plan_newport(planid);
						*pln_nodes = lappend(*pln_nodes, pln_port);
					}
					/* data */
					if (msg_type == MSG_R2R_DATA)
					{
						datalen = msg_len - sizeof(planid);
						data = rdc_getmsgbytes(msg, datalen);
						rdc_getmsgend(msg);
						/* fill in data */
						SendPlanDataToPlan(pln_port, RdcPeerID(rdc_port), data, datalen);
					} else
					/* EOF message */
					if (msg_type == MSG_EOF)
					{
						rdc_getmsgend(msg);
						elog(LOG,
							 "recv EOF message of" PLAN_PORT_PRINT_FORMAT
							 " from" RDC_PORT_PRINT_FORMAT,
							 planid, RDC_PORT_PRINT_VALUE(rdc_port));
						/* fill in EOF message */
						SendPlanEofToPlan(pln_port, RdcPeerID(rdc_port), true);
					} else
					/* PLAN CLOSE message */
					if (msg_type == MSG_PLAN_CLOSE)
					{
						rdc_getmsgend(msg);
						elog(LOG,
							 "recv CLOSE message of" PLAN_PORT_PRINT_FORMAT
							 " from" RDC_PORT_PRINT_FORMAT,
							 planid, RDC_PORT_PRINT_VALUE(rdc_port));
						/* fill in PLAN CLOSE message */
						SendPlanCloseToPlan(pln_port, RdcPeerID(rdc_port));
					} else
					/* PLAN REJECT message */
					{
						rdc_getmsgend(msg);
						elog(LOG,
							 "recv REJECT message of" PLAN_PORT_PRINT_FORMAT
							 " from" RDC_PORT_PRINT_FORMAT,
							 planid, RDC_PORT_PRINT_VALUE(rdc_port));
						/* fill in PLAN CLOSE message */
						SendPlanRejectToPlan(pln_port, RdcPeerID(rdc_port));
					}
				}
				break;
			case MSG_RDC_CLOSE:
				{
					rdc_getmsgend(msg);
					elog(LOG,
						 "recv REDUCE CLOSE message from" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(rdc_port));

					/*
					 * mark the RdcPort of Reduce CLOSED and we will never
					 * send/recv any data.
					 */
					RdcWaitEvents(rdc_port) &= ~WT_SOCK_READABLE;
					RdcWaitEvents(rdc_port) &= ~WT_SOCK_WRITEABLE;
					RdcFlags(rdc_port) = RDC_FLAG_CLOSED;

					/*
					 * notify other Reduce to quit by sending CLOSE
					 * message.
					 */
					BroadcastRdcClose();
				}
				break;
			case MSG_ERROR:
				break;
			default:
				ereport(ERROR,
						(errmsg("unexpected message type %c from" RDC_PORT_PRINT_FORMAT,
								msg_type, RDC_PORT_PRINT_VALUE(rdc_port))));
				break;
		}
	}
}

/*
 * HandleReadFromReduce
 *
 * handle message from other reduce
 */
static void
HandleReadFromReduce(RdcPort *rdc_port, List **pln_nodes)
{
	AssertArg(pln_nodes);
	Assert(ReduceTypeIDIsValid(rdc_port));

	/* return if reduce port is invalid */
	if (!PortIsValid(rdc_port))
		return ;

	SetRdcPsStatus(" reading from reduce " PORTID_FORMAT, RdcPeerID(rdc_port));
	do {
		HandleRdcMsg(rdc_port, pln_nodes);
		if (!PortIsValid(rdc_port))		/* break if port is invalid */
			break;
	} while (rdc_try_read_some(rdc_port));
}

/*
 * HandleWriteToReduce
 *
 * send data to other reduce
 */
static void
HandleWriteToReduce(RdcPort *rdc_port)
{
	int ret;

	Assert(ReduceTypeIDIsValid(rdc_port));
	if (!PortIsValid(rdc_port))
		return ;

	SetRdcPsStatus(" writing to reduce " PORTID_FORMAT, RdcPeerID(rdc_port));;
	ret = rdc_try_flush(rdc_port);
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(rdc_port) |= WT_SOCK_WRITEABLE;
	else
		RdcWaitEvents(rdc_port) &= ~WT_SOCK_WRITEABLE;
}

static void
SendPlanMsgToPlan(PlanPort *pln_port, char msg_type, RdcPortId rdc_id, const char *data, int datalen)
{
	RSstate	   *rdcstore;
	StringInfo	msg;

	Assert(pln_port);

	/*
	 * return if there is no worker of PlanPort.
	 * discard this data.
	 */
	if (!PlanPortIsValid(pln_port) ||
		PlanPortIsReject(pln_port))
	{
		/*
		 * PlanPort is invalid, the message will be discarded,
		 * so increase the number of discarding.
		 */
		pln_port->dscd_from_rdc++;
		return ;
	}

	/*
	 * the data received from other reduce will be put in RdcStore,
	 * so increase the number of receiving from reduce.
	 */
	pln_port->recv_from_rdc++;

	Assert(pln_port->rdcstore);

	rdcstore = pln_port->rdcstore;
	msg = PlanMsgBuf(pln_port);

	resetStringInfo(msg);
	rdc_beginmessage(msg, msg_type);
	rdc_sendRdcPortID(msg, rdc_id);
	if (data)
	{
		Assert(datalen > 0);
		rdc_sendbytes(msg, data, datalen);
	}
	rdc_sendlength(msg);

	rdcstore_puttuple(rdcstore, msg->data, msg->len);

	/*
	 * It may be not useful, because "port" of "pln_port" may be
	 * NULL until now. so try to add wait events for PlanPort again
	 * see in HandleWriteToPlan.
	 */
	PlanPortAddEvents(pln_port, WT_SOCK_WRITEABLE);
}

/*
 * SendPlanDataToPlan
 *
 * send data to plan node
 */
static void
SendPlanDataToPlan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen)
{
	Assert(data && datalen > 0);

	SendPlanMsgToPlan(pln_port, MSG_R2P_DATA, rdc_id, data, datalen);
}

/*
 * SendPlanEofToPlan
 *
 * send EOF to plan node
 */
static void
SendPlanEofToPlan(PlanPort *pln_port, RdcPortId rdc_id, bool error_if_exists)
{
	int				i;
	bool			found;

	Assert(pln_port);

	found = false;
	for (i = 0; i < pln_port->eof_num; i++)
	{
		if (pln_port->rdc_eofs[i] == rdc_id)
		{
			found = true;
			break;
		}
	}
	if (found)
	{
		if (error_if_exists)
			ereport(ERROR,
				(errmsg("recv EOF message of" PLAN_PORT_PRINT_FORMAT
						" from [REDUCE " PORTID_FORMAT "] once again",
				 PlanID(pln_port), rdc_id)));

		return ;
	} else
		pln_port->rdc_eofs[pln_port->eof_num++] = rdc_id;

	SendPlanMsgToPlan(pln_port, MSG_EOF, rdc_id, NULL, 0);
}

/*
 * SendPlanCloseToPlan
 *
 * send CLOSE to plan node
 */
static void
SendPlanCloseToPlan(PlanPort *pln_port, RdcPortId rdc_id)
{
	SendPlanMsgToPlan(pln_port, MSG_PLAN_CLOSE, rdc_id, NULL, 0);

	SendPlanEofToPlan(pln_port, rdc_id, false);
}

static void
SendPlanRejectToPlan(PlanPort *pln_port, RdcPortId rdc_id)
{
	SendPlanMsgToPlan(pln_port, MSG_PLAN_REJECT, rdc_id, NULL, 0);
}

/*
 * SendPlanDataToRdc
 *
 * send data from plan node to other reduce.
 *
 * return 0 if flush OK.
 * return 1 if some data unsent.
 */
static int
SendPlanDataToRdc(StringInfo msg, PlanPort *pln_port)
{
	int			datalen;
	const char *data;

	AssertArg(msg);

	/* data length and data */
	datalen = rdc_getmsgint(msg, sizeof(datalen));
	data = rdc_getmsgbytes(msg, datalen);

	return BroadcastDataToRdc(msg, pln_port, MSG_R2R_DATA, data, datalen, false);
}

/*
 * SendPlanEofToRdc
 *
 * send EOF of plan node to other reduce.
 *
 * return 0 if flush OK.
 * return 1 if some data unsent.
 */
static int
SendPlanEofToRdc(StringInfo msg, PlanPort *pln_port)
{
	return BroadcastDataToRdc(msg, pln_port, MSG_EOF, NULL, 0, false);
}

/*
 * SendPlanCloseToRdc
 *
 * send CLOSE of plan node to other reduce.
 *
 * return 0 if flush OK.
 * return 1 if some data unsent.
 */
static int
SendPlanCloseToRdc(StringInfo msg, PlanPort *pln_port)
{
	return BroadcastDataToRdc(msg, pln_port, MSG_PLAN_CLOSE, NULL, 0, false);
}

/*
 * SendPlanRejectToRdc
 *
 * send REJECT of plan node to other reduce.
 *
 * return 0 if flush OK.
 * return 1 if some data unsent.
 */
static int
SendPlanRejectToRdc(StringInfo msg, PlanPort *pln_port)
{
	return BroadcastDataToRdc(msg, pln_port, MSG_PLAN_REJECT, NULL, 0, false);
}

/*
 * BroadcastDataToRdc
 *
 * broadcast message of plan node to other reduce.
 *
 * return 0 if flush OK.
 * return 1 if some data unsent.
 */
static int
BroadcastDataToRdc(StringInfo msg,
				   PlanPort *pln_port,
				   char msg_type,
				   const char *msg_data,
				   int msg_len,
				   bool flush)
{
	RdcPortId		rid;
	RdcPort		   *rdc_port;
	int				num, i;
	int				ret;
	int				res = 0;
	const char	   *log_str;
	RdcPortId		planid;
	StringInfo		rdc_buf;

	AssertArg(msg && pln_port);
	planid = PlanID(pln_port);
	rdc_buf = PlanMsgBuf(pln_port);

	resetStringInfo(rdc_buf);

	/* makeup packet to broadcast */
	rdc_beginmessage(rdc_buf, msg_type);
	rdc_sendRdcPortID(rdc_buf, planid);
	switch (msg_type)
	{
		case MSG_EOF:
			log_str = "EOF message";
			Assert(!msg_data && !msg_len);
			break;
		case MSG_PLAN_CLOSE:
			log_str = "PLAN CLOSE message";
			Assert(!msg_data && !msg_len);
			break;
		case MSG_PLAN_REJECT:
			log_str = "PLAN REJECT message";
			Assert(!msg_data && !msg_len);
			break;
		case MSG_R2R_DATA:
			log_str = NULL;
			Assert(msg_data && msg_len > 0);
			rdc_sendbytes(rdc_buf, msg_data, msg_len);
			break;
		default:
			Assert(false);
			break;
	}
	rdc_sendlength(rdc_buf);

	/* parse reduce nodes which will be broadcasted */
	num = rdc_getmsgint(msg, sizeof(num));
	for (i = 0; i < num; i++)
	{
		rid = rdc_getmsgRdcPortID(msg);
		if (rid == MyReduceId)
			continue;
		rdc_port = LookupReducePort(rid);

		/*
		 * return if port is marked invalid
		 * (flag of port is not RDC_FLAG_VALID)
		 */
		if (!PortIsValid(rdc_port))
			continue;

		rdc_putmessage(rdc_port, rdc_buf->data, rdc_buf->len);

		if (log_str)
			elog(LOG,
				 "send %s of" PLAN_PORT_PRINT_FORMAT " to" RDC_PORT_PRINT_FORMAT,
				 log_str, planid, RDC_PORT_PRINT_VALUE(rdc_port));

		if (flush)
			ret = rdc_flush(rdc_port);
		else
		{
			ret = rdc_try_flush(rdc_port);
			/* trouble will be checked */
			CHECK_FOR_INTERRUPTS();
			if (ret != 0)
			{
				res = ret;
				RdcWaitEvents(rdc_port) |= WT_SOCK_WRITEABLE;
			}
			else
				RdcWaitEvents(rdc_port) &= ~WT_SOCK_WRITEABLE;
		}
	}
	rdc_getmsgend(msg);

	return res;
}

void
BroadcastRdcClose(void)
{
	int			i;
	int			rdc_num = MyRdcOpts->rdc_num;
	RdcNode	   *rdc_nodes = MyRdcOpts->rdc_nodes;
	RdcNode	   *rdc_node = NULL;
	RdcPort	   *rdc_port = NULL;
	StringInfoData msg;

	initStringInfo(&msg);
	rdc_beginmessage(&msg, MSG_RDC_CLOSE);
	rdc_sendlength(&msg);

	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		rdc_port = rdc_node->port;

		if (!rdc_port || RdcSendCLOSE(rdc_port))
			continue;

		rdc_putmessage(rdc_port, msg.data, msg.len);
		(void) rdc_flush(rdc_port);
		/* no need to check trouble? */

		elog(LOG,
			 "send REDUCE CLOSE message to" RDC_PORT_PRINT_FORMAT,
			 RDC_PORT_PRINT_VALUE(rdc_port));

		RdcEndStatus(rdc_port) |= RDC_END_CLOSE;
	}
	pfree(msg.data);
	msg.data = NULL;
}

/*
 * LookupReducePort
 *
 * find a valid reduce port by RdcPortId.
 *
 */
static RdcPort *
LookupReducePort(RdcPortId rpid)
{
	int			i;
	int			rdc_num = MyRdcOpts->rdc_num;
	RdcNode	   *rdc_nodes = MyRdcOpts->rdc_nodes;
	RdcNode	   *rdc_node = NULL;

	Assert(rpid != MyReduceId);
	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		if (RdcNodeID(rdc_node) == rpid)
			return rdc_node->port;
	}

	ereport(ERROR,
			(errmsg("REDUCE " PORTID_FORMAT " doesn't exists", rpid)));

	return NULL;
}
