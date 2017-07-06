/*-------------------------------------------------------------------------
 *
 * rdc_msg.c
 *	  interface for messages between Reduce and Reduce,
 *	  also for messages between Reduce and Plan node.
 *
 * Copyright (c) 2016-2017, ADB Development Group
 *
 * IDENTIFICATION
 *		src/backend/reduce/rdc_msg.c
 *
 *-------------------------------------------------------------------------
 */
#include "reduce/rdc_comm.h"
#include "reduce/rdc_msg.h"

int		MyReduceId = -1;

int
rdc_send_startup_rqt(RdcPort *port, RdcPortType type, RdcPortId id)
{
	StringInfoData	buf;

	AssertArg(port);

#ifdef DEBUG_ADB
	elog(LOG,
		 "send startup request to [%s %d] {%s:%s}",
		 RdcPeerTypeStr(port), RdcPeerID(port),
		 RdcPeerHost(port), RdcPeerPort(port));
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
		 RdcPeerTypeStr(port), RdcPeerID(port),
		 RdcPeerHost(port), RdcPeerPort(port));
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

int
rdc_send_close_rqt(RdcPort *port)
{
	StringInfoData	buf;

	AssertArg(port);
	rdc_beginmessage(&buf, RDC_CLOSE_MSG);
	rdc_endmessage(port, &buf);

	return rdc_flush(port);
}
