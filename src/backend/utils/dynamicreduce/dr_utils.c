#include "postgres.h"

#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

static DynamicReduceNodeInfo *cur_net_info = NULL;
static uint32		cur_net_count = 0;
static OidBuffer	cur_working_nodes = NULL;

bool DynamicReduceHandleMessage(void *data, Size len)
{
	ErrorData		edata;
	StringInfoData	msg;
	char			msgtype;

	msg.data = data;
	msg.maxlen = msg.len = len;
	msg.cursor = 0;

	msgtype = pq_getmsgbyte(&msg);
	switch(msgtype)
	{
	case 'E':	/* ErrorResponse */
	case 'N':	/* NoticeResponse */
		/* Parse ErrorResponse or NoticeResponse. */
		pq_parse_errornotice(&msg, &edata);

		/* Death of a worker isn't enough justification for suicide. */
		edata.elevel = Min(edata.elevel, ERROR);

		if (edata.context)
			edata.context = psprintf("%s\n%s", edata.context, _("dynamic reduce"));
		else
			edata.context = pstrdup(_("dynamic reduce"));
		
		ThrowErrorData(&edata);
		return true;
	default:
		/* ignore other message for now */
		break;
	}
	return false;
}
/* ============ serialize and restore struct DynamicReduceNodeInfo ============= */
static void SerializeOneDynamicReduceNodeInfo(StringInfo buf, const DynamicReduceNodeInfo *info)
{
	appendBinaryStringInfoNT(buf, (const char*)info, offsetof(DynamicReduceNodeInfo, host));
	appendStringInfoString(buf, NameStr(info->host));
	buf->len++;	/* include '\0' */
	appendStringInfoString(buf, NameStr(info->name));
	buf->len++; /* same idea include '\0' */
}

static void RestoreOneDynamicReduceNodeInfo(StringInfo buf, DynamicReduceNodeInfo *info)
{
	pq_copymsgbytes(buf, (char*)info, offsetof(DynamicReduceNodeInfo, host));
	namestrcpy(&info->host, pq_getmsgrawstring(buf));
	namestrcpy(&info->name, pq_getmsgrawstring(buf));
}

void SerializeDynamicReduceNodeInfo(StringInfo buf, const DynamicReduceNodeInfo *info, uint32 count)
{
	uint32 i;
	if (count == 0)
		return;

	appendBinaryStringInfoNT(buf, (char*)&count, sizeof(count));
	for (i=0;i<count;++i)
		SerializeOneDynamicReduceNodeInfo(buf, &info[i]);
}

uint32 RestoreDynamicReduceNodeInfo(StringInfo buf, DynamicReduceNodeInfo **info)
{
	DynamicReduceNodeInfo *pinfo;
	uint32 count;
	uint32 i;

	pq_copymsgbytes(buf, (char*)&count, sizeof(count));
	pinfo = palloc0(sizeof(*pinfo)*count);

	for(i=0;i<count;++i)
		RestoreOneDynamicReduceNodeInfo(buf, &pinfo[i]);
	*info = pinfo;

	return count;
}

/* ===================== connect message =============================== */
void DynamicReduceConnectNet(const DynamicReduceNodeInfo *info, uint32 count)
{
	StringInfoData		buf;
	uint32				i;

	DRCheckStarted();

	if (count < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("dynamic reduce got %u node info,"
						"there should be at least 2", count)));

	initStringInfo(&buf);
	pq_sendbyte(&buf, ADB_DR_MQ_MSG_CONNECT);
	SerializeDynamicReduceNodeInfo(&buf, info, count);
	DRSendMsgToReduce(buf.data, buf.len, false);
	pfree(buf.data);

	if (cur_working_nodes == NULL)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		cur_working_nodes = makeOidBuffer(OID_BUF_DEF_SIZE);
		MemoryContextSwitchTo(oldcontext);
	}
	resetOidBuffer(cur_working_nodes);
	for (i=0;i<count;++i)
		appendOidBufferOid(cur_working_nodes, info[i].node_oid);

	DRRecvConfirmFromReduce(false);
}

void DRConnectNetMsg(StringInfo msg)
{
	MemoryContext	oldcontext;

	if (cur_net_count != 0)
	{
		ereport(ERROR,
				(errmsg("last connected dynamic info in dynamic reduce is valid")));
	}

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	cur_net_count = RestoreDynamicReduceNodeInfo(msg, &cur_net_info);
	MemoryContextSwitchTo(oldcontext);
	pq_getmsgend(msg);
	if (cur_net_count < 2)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("dynamic reduce got %u node info,"
						"there should be at least 2", cur_net_count)));

	ConnectToAllNode(cur_net_info, cur_net_count);
}

const Oid* DynamicReduceGetCurrentWorkingNodes(uint32 *count)
{
	if (cur_working_nodes == NULL)
	{
		if (count)
			*count = 0;
		return NULL;
	}

	if (count)
		*count = cur_working_nodes->len;
	return cur_working_nodes->oids;
}

void DRUtilsReset(void)
{
	if (cur_net_count > 0)
	{
		pfree(cur_net_info);
		cur_net_info = NULL;
		cur_net_count = 0;
	}

	if (cur_working_nodes)
		resetOidBuffer(cur_working_nodes);
}
