/*-------------------------------------------------------------------------
 *
 * rdc_msg.c
 *	  interface for message between Reduce and Reduce,
 *	  also for message between Reduce and Plan node.
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/bin/adb_reduce/rdc_msg.h
 *
 *-------------------------------------------------------------------------
 */
#include "rdc_globals.h"
#include "rdc_msg.h"

static int  try_read_some(RdcPort *port);
static void try_handle_reduce_read(RdcPort *port, List **pln_list);
static void try_handle_reduce_write(RdcPort *port);

static void try_handle_plan_read(PlanPort *pln_port, RdcPort **rdc_nodes, int rdc_num);
static void try_handle_plan_write(PlanPort *pln_port);
static void build_packet_rdc2plan(StringInfo buf, RdcPortId rdc_id, const char *data, int datalen);
static void send_rdc2plan(PlanPort *pln_port, RdcPortId rdc_id, const char *data, int datalen);
static void send_rdc2rdc(RdcPort *port, RdcPortId planid, const char *data, int datalen);

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
		try_handle_plan_read(pln_port, rdc_nodes, rdc_num);
		try_handle_plan_write(pln_port);
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
		if (RdcWaitRead(port))
			try_handle_reduce_read(port, pln_list);
		if (RdcWaitWrite(port))
			try_handle_reduce_write(port);
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
				(errmsg("fail to set noblocking mode for [%s %d] {%s:%s}",
						RdcTypeStr(port), RdcID(port),
						RdcHostStr(port), RdcPortStr(port))));

	if (rdc_recv(port) == EOF)
		return EOF;

	return 0;
}

/*
 * try_handle_plan_read - handle message from Plan node
 */
static void
try_handle_plan_read(PlanPort *pln_port, RdcPort **rdc_nodes, int rdc_num)
{
	StringInfo		msg;
	int				sv_cursor;
	char			firstchar;
	RdcPort		   *port;

	AssertArg(pln_port && rdc_nodes);
	Assert(rdc_num > 0);

	port = pln_port->port;
	while (port != NULL)
	{
		Assert(PlanPortIsValid(port));
		Assert(RdcSockIsValid(port));

		/* skip if do not care about READ event */
		if (!RdcWaitRead(port))
			continue;

		/* set noblocking mode and read some data */
		if (try_read_some(port) == EOF)
			ereport(ERROR,
					(errmsg("fail to read some from [%s %d] {%s:%s}",
							RdcTypeStr(port), RdcID(port),
							RdcHostStr(port), RdcPortStr(port)),
					 errhint("connection to client lost")));

		msg = RdcInBuf(port);
		sv_cursor = msg->cursor;
		firstchar = rdc_getbyte(port);
		if (firstchar == EOF)
		{
			port->wait_events |= WAIT_SOCKET_READABLE;
			continue;
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
						break;
					}
					totallen = rdc_getmsgint(msg, sizeof(totallen));
					totallen -= sizeof(totallen);
					if (rdc_getbytes(port, totallen) == EOF)
					{
						msg->cursor = sv_cursor;
						port->wait_events |= WAIT_SOCKET_READABLE;
						break;
					}
					/* data length and data */
					datalen = rdc_getmsgint(msg, sizeof(datalen));
					data = rdc_getmsgbytes(msg, datalen);
					/* reduce number */
					num = rdc_getmsgint(msg, sizeof(num));
					/* reduce id */
					for (i = 0; i < num; i++)
					{
						rid = rdc_getmsgint(msg, sizeof(rid));
						Assert(rid != MyReduceId);
						Assert(rid >= 0 && rid < rdc_num);
						/* reduce port */
						rdc_port = rdc_nodes[rid];
						if (firstchar == RDC_P2R_DATA)
						{
							/* send data to reduce */
							send_rdc2rdc(rdc_port, RdcID(port), data, datalen);
						} else
						{
							/* send eof to reduce */
							RdcWaitEvents(port) &= ~WAIT_SOCKET_READABLE;
							send_rdc2rdc(rdc_port, RdcID(port), NULL, 0);
						}
					}
					rdc_getmsgend(msg);
				}
				break;
			case RDC_P2R_CLOSE:
				{
					/* TODO: how to close? */
				}
				break;
			case RDC_ERROR_MSG:
				break;
			default:
				ereport(ERROR,
						(errmsg("unexpected message type %c of Plan port",
								firstchar)));
		}

		port = RdcNext(port);
	}
}

static void
try_handle_plan_write(PlanPort *pln_port)
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

		if (!RdcWaitWrite(port))
			continue;

		if (!rdc_set_noblock(port))
			ereport(ERROR,
					(errmsg("fail to set noblocking mode for [%s %d] {%s:%s}",
							RdcTypeStr(port), RdcID(port),
							RdcHostStr(port), RdcPortStr(port))));

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
						break;

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
				break;
			}
			/* output buffer is empty, continue trying rdcstore */
			else
			{
				bool	hasData = false;

				resetStringInfo(buf);
				Assert(rdcstore);

				/* get one tuple from rdcstore */
				rdcstore_gettuple(rdcstore, buf, &hasData);
				if (!hasData)
				{
					RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;
					break;
				}
			}
		}

		port = RdcNext(port);
	}
}

static void
try_handle_reduce_read(RdcPort *port, List **pln_list)
{
	StringInfo		msg;
	int				sv_cursor;
	char			firstchar;

	AssertArg(port && pln_list);
	Assert(ReducePortIsValid(port));
	Assert(RdcID(port) != MyReduceId);

	/* set noblocking mode and read some data */
	if (try_read_some(port) == EOF)
		ereport(ERROR,
				(errmsg("fail to read some from [%s %d] {%s:%s}",
						RdcTypeStr(port), RdcID(port),
						RdcHostStr(port), RdcPortStr(port)),
				 errhint("connection to client lost")));

	msg = RdcInBuf(port);
	sv_cursor = msg->cursor;
	firstchar = rdc_getbyte(port);
	if (firstchar == EOF)
	{
		port->wait_events |= WAIT_SOCKET_READABLE;
		return ;
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
					break ;
				}
				datalen = rdc_getmsgint(msg, sizeof(datalen));
				datalen -= sizeof(datalen);
				planid = rdc_getmsgint(msg, sizeof(planid));
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
					/* fill in eof */
					send_rdc2plan(pln_port, RdcID(port), NULL, 0);
					break ;
				}
				/* got data */
				if (rdc_getbytes(port, datalen) == EOF)
				{
					msg->cursor = sv_cursor;
					port->wait_events |= WAIT_SOCKET_READABLE;
					break ;
				}
				data = rdc_getmsgbytes(msg, datalen);
				rdc_getmsgend(msg);

				/* fill in data */
				send_rdc2plan(pln_port, RdcID(port), data, datalen);
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

static void
try_handle_reduce_write(RdcPort *port)
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
	rdc_sendint(buf, rdc_id, sizeof(rdc_id));
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
	try_handle_plan_write(pln_port);
}

static void
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
	rdc_sendint(&buf, planid, sizeof(RdcPortId));
	if (data)
		rdc_sendbytes(&buf, data, datalen);
	rdc_endmessage(port, &buf);

	ret = rdc_try_flush(port);
	CHECK_FOR_INTERRUPTS();
	if (ret != 0)
		RdcWaitEvents(port) |= WAIT_SOCKET_WRITEABLE;
	else
		RdcWaitEvents(port) &= ~WAIT_SOCKET_WRITEABLE;
}

int
rdc_send_startup_rqt(RdcPort *port, RdcPortType type, RdcPortId id)
{
	StringInfoData	buf;

	AssertArg(port);

#ifdef DEBUG_ADB
	elog(LOG,
		 "send startup request to [%s %d] {%s:%s}",
		 RdcTypeStr(port), RdcID(port),
		 RdcHostStr(port), RdcPortStr(port));
#endif

	initStringInfo(&buf);
	rdc_beginmessage(&buf, RDC_START_RQT);
	rdc_sendint(&buf, RDC_VERSION_NUM, sizeof(int));		/* version */
	rdc_sendint(&buf, type, sizeof(type));
	rdc_sendint(&buf, id, sizeof(id));
	rdc_endmessage(port, &buf);

	return rdc_flush(port);
}

int
rdc_send_startup_rsp(RdcPort *port, RdcPortType type, RdcPortId id)
{
	StringInfoData	buf;

	AssertArg(port);

#ifdef DEBUG_ADB
	elog(LOG,
		 "send startup response to [%s %d] {%s:%s}",
		 RdcTypeStr(port), RdcID(port),
		 RdcHostStr(port), RdcPortStr(port));
#endif

	initStringInfo(&buf);
	rdc_beginmessage(&buf, RDC_START_RSP);
	rdc_sendint(&buf, RDC_VERSION_NUM, sizeof(int));
	rdc_sendint(&buf, type, sizeof(type));
	rdc_sendint(&buf, id, sizeof(id));
	rdc_endmessage(port, &buf);

	return rdc_flush(port);
}

int
rdc_send_group_rqt(RdcPort *port, int num, const char *hosts[], int ports[])
{
	StringInfoData	buf;
	int				i = 0;

	AssertArg(port);
	Assert(num > 1);

	initStringInfo(&buf);
	rdc_beginmessage(&buf, RDC_GROUP_RQT);
	rdc_sendint(&buf, num, sizeof(num));
	for (i = 0; i < num; i++)
	{
		Assert(hosts[i] && hosts[i][0]);
		Assert(ports[i] > 1024 && ports[i] < 65535);
		rdc_sendstring(&buf, hosts[i]);
		rdc_sendint(&buf, ports[i], sizeof(ports[i]));
	}
	rdc_endmessage(port, &buf);

	return rdc_flush(port);
}

int
rdc_send_group_rsp(RdcPort *port)
{
	StringInfoData	buf;

	AssertArg(port);
	initStringInfo(&buf);
	rdc_beginmessage(&buf, RDC_GROUP_RSP);
	rdc_endmessage(port, &buf);

	return rdc_flush(port);
}

int
rdc_recv_startup_rsp(RdcPort *port, RdcPortType expected_type, RdcPortId expected_id)
{
	char		firstchar;
	StringInfo	msg;

	AssertArg(port);

	msg = RdcInBuf(port);

	firstchar = rdc_getmessage(port, 0);
	if (!(firstchar == RDC_START_RSP || firstchar == RDC_ERROR_MSG))
	{
		rdc_puterror(port,
					 "expected startup response from server, "
					 "but received %c", firstchar);
		return EOF;
	}

	/* error response */
	if (firstchar == RDC_ERROR_MSG)
	{
		const char *errmsg = rdc_getmsgstring(RdcInBuf(port));
		rdc_puterror(port, "%s", errmsg);
		return EOF;
	} else
	{
		RdcPortType	rsp_type = InvalidPortType;
		RdcPortId	rsp_id = InvalidPortId;
		int			rsp_ver;

		rsp_ver = rdc_getmsgint(msg, sizeof(rsp_ver));
		if (rsp_ver != RDC_VERSION_NUM)
		{
			rdc_puterror(port,
						 "expected version '%d' from server, "
						 "but received response version '%d'",
						 RDC_VERSION_NUM,
						 rsp_ver);
			return EOF;
		}

		rsp_type = rdc_getmsgint(RdcInBuf(port), sizeof(rsp_type));
		if (rsp_type != expected_type)
		{
			rdc_puterror(port,
						 "expected port type '%s' from server, "
						 "but received response type '%s'",
						 rdc_type2string(expected_type),
						 rdc_type2string(rsp_type));
			return EOF;
		}

		rsp_id = rdc_getmsgint(RdcInBuf(port), sizeof(rsp_id));
		if (rsp_id != expected_id)
		{
			rdc_puterror(port,
						 "expected port id '%d' from server, "
						 "but received response id '%d'",
						 expected_id, rsp_id);
			return EOF;
		}

		rdc_getmsgend(RdcInBuf(port));
	}

	return 0;
}

int
rdc_recv_group_rsp(RdcPort *port)
{
	char		firstchar;

	AssertArg(port);
	firstchar = rdc_getmessage(port, 0);
	if (firstchar != RDC_GROUP_RSP)
	{
		rdc_puterror(port,
					 "expected group response from server, "
					 "but received %c", firstchar);
		return EOF;
	}
	rdc_getmsgend(RdcInBuf(port));

	return 0;
}
