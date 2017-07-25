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

static int  TryReadSome(RdcPort *port);
static int  HandlePlanMsg(RdcPort *work_port, PlanPort *pln_port);
static void HandleRdcMsg(RdcPort *rdc_port, List **pln_nodes);
static void HandleReadFromReduce(RdcPort *port, List **pln_nodes);
static void HandleWriteToReduce(RdcPort *port);
static void HandleReadFromPlan(PlanPort *pln_port);
static void HandleWriteToPlan(PlanPort *pln_port);
static void SendRdcDataToPlan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen);
static void SendRdcEofToPlan(PlanPort *pln_port, RdcPortId rdc_id, bool error_if_exists);
static void SendPlanCloseToPlan(PlanPort *pln_port, RdcPortId rdc_id);
static int  SendPlanDataToRdc(RdcPort *rdc_port, RdcPortId planid, const char *data, int datalen);
static int  SendPlanEofToRdc(RdcPortId planid);
static int  SendPlanCloseToRdc(RdcPortId planid);
static int  BroadcastDataToRdc(StringInfo buf, bool flush);
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
	ListCell	   *next = NULL;
	PlanPort	   *pln_port = NULL;

	AssertArg(pln_nodes);
	for (cell = list_head(*pln_nodes); cell != NULL; cell = next)
	{
		pln_port = (PlanPort *) lfirst(cell);
		next = lnext(cell);

		if (PlanPortIsValid(pln_port))
		{
			HandleReadFromPlan(pln_port);
			HandleWriteToPlan(pln_port);
		}

		/*
		 * PlanPort may be invalid after reading from plan,
		 * so close it.
		 */
		if (!PlanPortIsValid(pln_port))
		{
			/* Here is we truly close port of plan */
			*pln_nodes = list_delete_ptr(*pln_nodes, pln_port);
			plan_freeport(pln_port);
		}
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
		if (RdcNodeID(rdc_node) == MyReduceId ||
			rdc_port == NULL)
			continue;

		if (PortIsValid(rdc_port))
		{
			if (RdcWaitRead(rdc_port))
				HandleReadFromReduce(rdc_port, pln_nodes);
			if (RdcWaitWrite(rdc_port))
				HandleWriteToReduce(rdc_port);
		}

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
 * TryReadSome
 *
 * set noblock and try to read some data.
 *
 * return 0 if receive would block.
 * return 1 if receive some data.
 */
static int
TryReadSome(RdcPort *port)
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

		switch (msg_type)
		{
			case MSG_P2R_DATA:
				{
					RdcPort		   *rdc_port;
					RdcPortId		rid;
					const char	   *data;
					int				num, i;
					int				datalen;

					/*
					 * get one whole data from PLAN, so increase the
					 * number of receiving from PLAN.
					 */
					pln_port->recv_from_pln++;

					/* data length and data */
					datalen = rdc_getmsgint(msg, sizeof(datalen));
					data = rdc_getmsgbytes(msg, datalen);
					/* reduce number */
					num = rdc_getmsgint(msg, sizeof(num));
					/* reduce id */
					for (i = 0; i < num; i++)
					{
						rid = rdc_getmsgRdcPortID(msg);
						Assert(!RdcIdIsSelfID(rid));
						/* reduce port */
						rdc_port = LookUpReducePort(rid);
						/* send data to reduce */
						if (SendPlanDataToRdc(rdc_port, RdcPeerID(work_port), data, datalen))
						{
							/*
							 * flush to other reduce would block,
							 * and we try to read from plan next time.
							 */
							res = 1;
							quit = true;	/* break while */
						}
					}
					rdc_getmsgend(msg);
				}
				break;
			case MSG_EOF:
				{
					rdc_getmsgend(msg);
					elog(LOG,
						 "receive EOF message from" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(work_port));

					/*
					 * get EOF message from PLAN, so increase the
					 * number of receiving from PLAN.
					 */
					pln_port->recv_from_pln++;

					if (SendPlanEofToRdc(RdcPeerID(work_port)))
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
					bool broadcast = rdc_getmsgint(msg, sizeof(broadcast));
					rdc_getmsgend(msg);
					elog(LOG,
						 "receive CLOSE message from" RDC_PORT_PRINT_FORMAT,
						 RDC_PORT_PRINT_VALUE(work_port));

					/*
					 * get CLOSE message from PLAN, so increase the
					 * number of receiving from PLAN.
					 */
					pln_port->recv_from_pln++;

					/*
					 * do not wait read events on socket of port as
					 * it would "CLOSEd".
					 */
					RdcWaitEvents(work_port) &= ~WT_SOCK_READABLE;
					RdcWaitEvents(work_port) &= ~WT_SOCK_WRITEABLE;
					RdcFlags(work_port) = RDC_FLAG_CLOSED;
					pln_port->work_num--;
					/* this mark the PlanPort is invalid */
					if (PlanWorkNum(pln_port) == 0)
						PlanWorkNum(pln_port) = -1;
					quit = true;	/* break while */
					res = EOF;

					if (broadcast)
						(void) SendPlanCloseToRdc(RdcPeerID(work_port));
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
	for (work_port = pln_port->port;
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
				TryReadSome(work_port) == 0)	/* break if read would block */
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
	RdcPort		   *port;
	RSstate		   *rdcstore;
	int				r;
	volatile bool	eof;

	AssertArg(pln_port);
	if (!PlanPortIsValid(pln_port))
		return ;

	SetRdcPsStatus(" writing to plan " PORTID_FORMAT, PlanID(pln_port));
	rdcstore = pln_port->rdcstore;
	port = pln_port->port;
	eof = (pln_port->eof_num == (pln_port->rdc_num - 1));

	while (port != NULL)
	{
		Assert(PlanTypeIDIsValid(port));
		Assert(RdcSockIsValid(port));

		/* skip if do not care about WRITE event */
		if (!PortIsValid(port) ||
			!RdcWaitWrite(port))
		{
			port = RdcNext(port);
			continue;
		}

		/* set in noblocking mode */
		if (!rdc_set_noblock(port))
			ereport(ERROR,
					(errmsg("fail to set noblocking mode for" RDC_PORT_PRINT_FORMAT,
							RDC_PORT_PRINT_VALUE(port))));

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
#ifdef RDC_FRONTEND
					ClientConnectionLostType = RdcPeerType(port);
					ClientConnectionLostID = RdcPeerID(port);
#endif
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
						rdc_beginmessage(buf, MSG_EOF);
						rdc_sendlength(buf);
						RdcEndStatus(port) |= RDC_END_EOF;
					} else
					{
						if (RdcSendEOF(port))
							RdcWaitEvents(port) &= ~WT_SOCK_WRITEABLE;
						break;	/* break for */
					}
				} else
				{
					/*
					 * OK to get tuple from rdcstore, so increase the
					 * number of sending to PLAN.
					 */
					pln_port->send_to_pln++;
				}
			}
		}

		port = RdcNext(port);
	}
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
				{
					PlanPort	   *pln_port;
					const char	   *data;
					RdcPortId		planid;
					int 			datalen;

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
					if (msg_type == MSG_R2R_DATA)
					{
						datalen = msg_len - sizeof(planid);
						data = rdc_getmsgbytes(msg, datalen);
						rdc_getmsgend(msg);
						/* fill in data */
						SendRdcDataToPlan(pln_port, RdcPeerID(rdc_port), data, datalen);
					} else
					/* EOF message */
					if (msg_type == MSG_EOF)
					{
						rdc_getmsgend(msg);
						elog(LOG,
							 "receive EOF message of PLAN " PORTID_FORMAT
							 " from" RDC_PORT_PRINT_FORMAT,
							 planid, RDC_PORT_PRINT_VALUE(rdc_port));
						/* fill in EOF message */
						SendRdcEofToPlan(pln_port, RdcPeerID(rdc_port), true);
					} else
					/* PLAN CLOSE message */
					{
						rdc_getmsgend(msg);
						elog(LOG,
							 "receive CLOSE message of PLAN " PORTID_FORMAT
							 " from" RDC_PORT_PRINT_FORMAT,
							 planid, RDC_PORT_PRINT_VALUE(rdc_port));
						/* fill in PLAN CLOSE message */
						SendPlanCloseToPlan(pln_port, RdcPeerID(rdc_port));
					}
				}
				break;
			case MSG_ERROR:
				break;
			default:
				ereport(ERROR,
						(errmsg("unexpected message type %c of Reduce port",
								msg_type)));
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
	} while (TryReadSome(rdc_port));
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

/*
 * SendRdcDataToPlan
 *
 * send data to plan node
 */
static void
SendRdcDataToPlan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen)
{
	RSstate			   *rdcstore = NULL;
	StringInfoData		buf;

	AssertArg(pln_port);
	AssertArg(data);
	Assert(datalen > 0);
	Assert(pln_port->rdcstore);
	rdcstore = pln_port->rdcstore;

	/*
	 * return if there is no worker of PlanPort.
	 * discard this data.
	 */
	if (!PlanPortIsValid(pln_port))
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

	rdc_beginmessage(&buf, MSG_R2P_DATA);
#ifdef DEBUG_ADB
	rdc_sendRdcPortID(&buf, rdc_id);
#endif
	rdc_sendbytes(&buf, data, datalen);
	rdc_sendlength(&buf);

	rdcstore_puttuple(rdcstore, buf.data, buf.len);
	pfree(buf.data);
	buf.data = NULL;

	PlanPortAddEvents(pln_port, WT_SOCK_WRITEABLE);
	//HandleWriteToPlan(pln_port);
}

/*
 * SendRdcEofToPlan
 *
 * send EOF to plan node
 */
static void
SendRdcEofToPlan(PlanPort *pln_port, RdcPortId rdc_id, bool error_if_exists)
{
	int			i;
	bool		found;

	AssertArg(pln_port);

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
				(errmsg("receive EOF message of plan " PORTID_FORMAT
						" from REDUCE " PORTID_FORMAT " once again",
				 PlanID(pln_port), rdc_id)));
	} else
		pln_port->rdc_eofs[pln_port->eof_num++] = rdc_id;

	PlanPortAddEvents(pln_port, WT_SOCK_WRITEABLE);
	//HandleWriteToPlan(pln_port);
}

/*
 * SendPlanCloseToPlan
 *
 * send CLOSE to plan node
 */
static void
SendPlanCloseToPlan(PlanPort *pln_port, RdcPortId rdc_id)
{
	RSstate			   *rdcstore = NULL;
	StringInfoData		buf;

	AssertArg(pln_port);
	Assert(pln_port->rdcstore);
	rdcstore = pln_port->rdcstore;

	/*
	 * return if there is no worker of PlanPort.
	 * discard this data.
	 */
	if (!PlanPortIsValid(pln_port))
	{
		/*
		 * PlanPort is invalid, the message will be discarded,
		 * so increase the number of discarding.
		 */
		pln_port->dscd_from_rdc++;
		return ;
	}

	/*
	 * the CLOSE message received from other reduce will be put in RdcStore,
	 * so increase the number of receiving from reduce.
	 */
	pln_port->recv_from_rdc++;

	rdc_beginmessage(&buf, MSG_PLAN_CLOSE);
	rdc_sendRdcPortID(&buf, rdc_id);
	rdc_sendlength(&buf);

	rdcstore_puttuple(rdcstore, buf.data, buf.len);
	pfree(buf.data);
	buf.data = NULL;

	SendRdcEofToPlan(pln_port, rdc_id, false);
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
SendPlanDataToRdc(RdcPort *rdc_port, RdcPortId planid, const char *data, int datalen)
{
	StringInfoData		buf;
	int					ret;

	AssertArg(data);
	Assert(datalen > 0);
	Assert(ReduceTypeIDIsValid(rdc_port));

	/*
	 * return if port is marked invalid
	 * (flag of port is not RDC_FLAG_VALID)
	 */
	if (!PortIsValid(rdc_port))
		return 0;

	rdc_beginmessage(&buf, MSG_R2R_DATA);
	rdc_sendRdcPortID(&buf, planid);
	rdc_sendbytes(&buf, data, datalen);
	rdc_endmessage(rdc_port, &buf);

	ret = rdc_try_flush(rdc_port);
	/* trouble will be checked */
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(rdc_port) |= WT_SOCK_WRITEABLE;
	else
		RdcWaitEvents(rdc_port) &= ~WT_SOCK_WRITEABLE;

	return ret;
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
SendPlanEofToRdc(RdcPortId planid)
{
	StringInfoData	buf;

	rdc_beginmessage(&buf, MSG_EOF);
	rdc_sendRdcPortID(&buf, planid);
	rdc_sendlength(&buf);

	elog(LOG,
		 "broadcast EOF message of plan " PORTID_FORMAT " to reduce group",
		 planid);

	return BroadcastDataToRdc(&buf, false);
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
SendPlanCloseToRdc(RdcPortId planid)
{
	StringInfoData	buf;

	rdc_beginmessage(&buf, MSG_PLAN_CLOSE);
	rdc_sendRdcPortID(&buf, planid);
	rdc_sendlength(&buf);

	elog(LOG,
		 "broadcast CLOSE message of plan " PORTID_FORMAT " to reduce group",
		 planid);

	return BroadcastDataToRdc(&buf, false);
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
BroadcastDataToRdc(StringInfo buf, bool flush)
{
	int				i;
	int				rdc_num = MyRdcOpts->rdc_num;
	RdcNode		   *rdc_nodes = MyRdcOpts->rdc_nodes;
	RdcNode		   *rdc_node = NULL;
	RdcPort		   *rdc_port;
	int				ret;
	int				res = 0;

	for (i = 0; i < rdc_num; i++)
	{
		rdc_node = &(rdc_nodes[i]);
		rdc_port = rdc_node->port;

		if (RdcNodeID(rdc_node) == MyReduceId ||	/* skip self reduce */
			rdc_port == NULL ||						/* skip null reduce port */
			!PortIsValid(rdc_port))					/* skip invalid reduce port */
			continue;

		Assert(RdcPeerID(rdc_port) != MyReduceId);
		rdc_putmessage(rdc_port, buf->data, buf->len);

		if (flush)
			ret = rdc_flush(rdc_port);
		else
		{
			ret = rdc_try_flush(rdc_port);
			/* touble will be checked */
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
	pfree(buf->data);
	buf->data = NULL;

	return res;
}

/*
 * LookUpReducePort
 *
 * find a valid reduce port by RdcPortId.
 *
 */
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
		if (RdcNodeID(rdc_node) == rpid)
			return rdc_node->port;
	}

	ereport(ERROR,
			(errmsg("REDUCE " PORTID_FORMAT " doesn't exists", rpid)));

	return NULL;
}
