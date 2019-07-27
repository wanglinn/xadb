#include "postgres.h"

#include "libpq/pqformat.h"
#include "storage/latch.h"
#include "utils/memutils.h"

#include "utils/dynamicreduce.h"
#include "utils/dr_private.h"

/*
 * sizeof(msg len) = 4 bytes
 * sizeof(msg type) = 1 byte
 * sizeof(plan id) = 4 bytes
 */
#define NODE_MSG_HEAD_LEN	9

static HTAB		   *htab_node_info = NULL;

static void OnNodeEvent(WaitEvent *ev);
static void OnNodeError(DREventData *base, int pos);
static void OnPreWaitNode(struct DREventData *base, int pos);

static int PorcessNodeEventData(DRNodeEventData *ned, WaitEvent *ev);
static void OnNodeSendMessage(DRNodeEventData *ned, int latch_pos);
static void OnNodeRecvMessage(DRNodeEventData *ned, WaitEvent *ev);

void DROnNodeConectSuccess(DRNodeEventData *ned, WaitEvent *ev)
{
	ned->base.OnEvent = OnNodeEvent;
	ned->base.OnPreWait = OnPreWaitNode;
	ned->base.OnError = OnNodeError;
	ned->status = DRN_WORKING;

	DR_NODE_DEBUG((errmsg("node %u(%p) connect successed", ned->nodeoid, ned)));
	if (ned->recvBuf.cursor < ned->recvBuf.len)
	{
		/* process other message(s) */
		PorcessNodeEventData(ned, ev);
	}else
	{
		/* reset receive buffer */
		ned->recvBuf.cursor = ned->recvBuf.len = 0;
	}

	ActiveWaitingPlan(ned);
}

bool PutMessageToNode(DRNodeEventData *ned, char msg_type, const char *data, uint32 len, int plan_id)
{
	uint32				free_space;
	uint32				need_space;
	bool				is_empty;
	Assert(len >= 0);
	Assert(plan_id >= -1);

	Assert(ned && ned->base.type == DR_EVENT_DATA_NODE);
	Assert(OidIsValid(ned->nodeoid));
	if (ned->status == DRN_WAIT_CLOSE)
	{
		/* just return success */
		return true;
	}

	if (ned->sendBuf.cursor == ned->sendBuf.len)
	{
		is_empty = true;
		ned->sendBuf.cursor = ned->sendBuf.len = 0;
		free_space = ned->sendBuf.maxlen;
	}else
	{
		is_empty = false;

		if (ned->sendBuf.cursor > 0 &&
			ned->sendBuf.len - ned->sendBuf.cursor <= (ned->sendBuf.maxlen >> 2))
		{
			memmove(ned->sendBuf.data,
					ned->sendBuf.data + ned->sendBuf.cursor,
					ned->sendBuf.len - ned->sendBuf.cursor);
			ned->sendBuf.len -= ned->sendBuf.cursor;
			ned->sendBuf.cursor = 0;
		}

		free_space = ned->sendBuf.maxlen - ned->sendBuf.len;
	}

	need_space = NODE_MSG_HEAD_LEN + len;

	if (is_empty &&
		free_space < need_space)
	{
		/*
		 * need enlarge space
		 * now free_space equal ned->sendBuf.maxlen
		 */
		int max_len = ned->sendBuf.maxlen + DR_SOCKET_BUF_SIZE_STEP;
		while (max_len < need_space)
			max_len += DR_SOCKET_BUF_SIZE_STEP;
		enlargeStringInfo(&ned->sendBuf, max_len);
		free_space = ned->sendBuf.maxlen;
		Assert(free_space >= need_space);
	}

	if (free_space < need_space)
	{
		DR_NODE_DEBUG((errmsg("PutMessageToNode(node=%u, type=%d, len=%u, plan=%d) == false",
							  ned->nodeoid, msg_type, len, plan_id)));
		return false;
	}

	appendBinaryStringInfoNT(&ned->sendBuf, (char*)&len, sizeof(len));					/* message length */
	appendStringInfoCharMacro(&ned->sendBuf, msg_type);									/* message type */
	appendBinaryStringInfoNT(&ned->sendBuf, (char*)&plan_id, sizeof(plan_id));			/* plan ID */
	if (len > 0)
		appendBinaryStringInfoNT(&ned->sendBuf, data, len);								/* message data */

	DR_NODE_DEBUG((errmsg("PutMessageToNode(node=%u, type=%d, len=%u, plan=%d) == true",
						  ned->nodeoid, msg_type, len, plan_id)));

	return true;
}

static void OnNodeEvent(WaitEvent *ev)
{
	DRNodeEventData *ned = ev->user_data;
	Assert(ned->base.type == DR_EVENT_DATA_NODE);
	DR_NODE_DEBUG((errmsg("node %d got events %d", ned->nodeoid, ev->events)));
	if (ev->events & WL_SOCKET_READABLE)
		OnNodeRecvMessage(ned, ev);
	if (ev->events & WL_SOCKET_WRITEABLE)
		OnNodeSendMessage(ned, ev->pos);
}

static void OnNodeError(DREventData *base, int pos)
{
	DRNodeEventData *ned = (DRNodeEventData*)base;
	Assert(base->type == DR_EVENT_DATA_NODE);

	if (ned->status == DRN_WAIT_CLOSE ||
		ned->sendBuf.len > ned->sendBuf.cursor ||
		ned->recvBuf.len > ned->recvBuf.cursor)
		FreeNodeEventInfo(ned);
}

static void OnPreWaitNode(struct DREventData *base, int pos)
{
	DRNodeEventData *ned = (DRNodeEventData*)base;
	uint32 need_event;
	Assert(base->type == DR_EVENT_DATA_NODE);
	Assert(GetWaitEventData(dr_wait_event_set, pos) == base);
	if (ned->status == DRN_WAIT_CLOSE)
	{
		if (dr_status == DRS_RESET)
			FreeNodeEventInfo(ned);
		return;
	}

	if (OidIsValid(ned->nodeoid))
	{
		need_event = 0;
		if (ned->recvBuf.maxlen > ned->recvBuf.len)
			need_event |= WL_SOCKET_READABLE;
		if (ned->sendBuf.len > ned->sendBuf.cursor)
			need_event |= WL_SOCKET_WRITEABLE;
		DR_NODE_DEBUG((errmsg("node %d set wait events %d", ned->nodeoid, need_event)));
		ModifyWaitEvent(dr_wait_event_set, pos, need_event, NULL);
	}
}

ssize_t RecvMessageFromNode(DRNodeEventData *ned, WaitEvent *ev)
{
	ssize_t size;
	int space;
	if (ned->recvBuf.cursor != 0 &&
		ned->recvBuf.len == ned->recvBuf.cursor)
	{
		ned->recvBuf.cursor = ned->recvBuf.len = 0;
		space = ned->recvBuf.maxlen;
	}else
	{
		space = ned->recvBuf.maxlen - ned->recvBuf.len;

		if (space == 0)
		{
			if (ned->recvBuf.cursor)
			{
				memmove(ned->recvBuf.data,
						ned->recvBuf.data + ned->recvBuf.cursor,
						ned->recvBuf.len - ned->recvBuf.cursor);
				ned->recvBuf.len -= ned->recvBuf.cursor;
				ned->recvBuf.cursor = 0;
			}else
			{
				enlargeStringInfo(&ned->recvBuf, ned->recvBuf.maxlen + 4096);
			}
			space = ned->recvBuf.maxlen - ned->recvBuf.len;
		}
	}
	Assert(space > 0);

rerecv_:
	size = recv(ev->fd,
				ned->recvBuf.data + ned->recvBuf.len,
				space,
				0);
	if (size < 0)
	{
		if (errno == EINTR)
			goto rerecv_;
		if (errno == EWOULDBLOCK)
			return 0;
		ned->status = DRN_WAIT_CLOSE;
		//dr_keep_error = true;
		if (dr_status == DRS_RESET)
			return 0;
		ereport(ERROR,
				(errmsg("could not recv message from node %u:%m", ned->nodeoid)));
	}else if (size == 0)
	{
		ned->status = DRN_WAIT_CLOSE;
		//dr_keep_error = true;
		if (dr_status == DRS_RESET)
			return 0;
		ereport(ERROR,
				(errmsg("remote node %u closed socket", ned->nodeoid)));
	}
	ned->recvBuf.len += size;
	DR_NODE_DEBUG((errmsg("node %u got message of length %zd from remote", ned->nodeoid, size)));
	return size;
}

static int PorcessNodeEventData(DRNodeEventData *ned, WaitEvent *ev)
{
	PlanInfo	   *pi;
	StringInfoData	buf;
	uint32			msglen;
	int				msgtype;
	int				plan_id;
	int				msg_count = 0;
	Assert(OidIsValid(ned->nodeoid));

	ned->waiting_plan_id = INVALID_PLAN_ID;
	for (;;)
	{
		buf = ned->recvBuf;
		if (buf.len - buf.cursor < NODE_MSG_HEAD_LEN)
			break;

		pq_copymsgbytes(&buf, (char*)&msglen, sizeof(msglen));
		msgtype = pq_getmsgbyte(&buf);
		pq_copymsgbytes(&buf, (char*)&plan_id, sizeof(plan_id));

		if (buf.len - buf.cursor < msglen)
		{
			DR_NODE_DEBUG((errmsg("node %u need message data length %u, but now is only %d",
								  ned->nodeoid, msglen, buf.len-buf.cursor)));
			break;
		}

		pi = DRPlanSearch(plan_id, HASH_FIND, NULL);
		DR_NODE_DEBUG((errmsg("node %u processing message %d plan %d(%p) length %u",
					   ned->nodeoid, msgtype, plan_id, pi, msglen)));

		if (msgtype == ADB_DR_MSG_TUPLE)
		{
			if (pi == NULL ||
				(*pi->OnNodeRecvedData)(pi, buf.data+buf.cursor, msglen, ned->nodeoid) == false)
			{
				DR_NODE_DEBUG((errmsg("node %u put tuple to plan %d(%p) return false", ned->nodeoid, plan_id, pi)));
				ned->waiting_plan_id = plan_id;
				return msg_count;
			}
		}else if (msgtype == ADB_DR_MSG_END_OF_PLAN)
		{
			if (pi == NULL ||
				(*pi->OnNodeEndOfPlan)(pi, ned->nodeoid) == false)
			{
				DR_NODE_DEBUG((errmsg("node %u put end of plan %d(%p) return false", ned->nodeoid, plan_id, pi)));
				ned->waiting_plan_id = plan_id;
				return msg_count;
			}
		}else
		{
			ned->status = DRN_WAIT_CLOSE;
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unknown message type %d from node %u", msgtype, ned->nodeoid)));
		}

		buf.cursor += msglen;
		++msg_count;

		/* update buffer */
		ned->recvBuf.cursor = buf.cursor;
	}

	if (msg_count == 0 &&
		ned->waiting_plan_id == INVALID_PLAN_ID &&
		ned->recvBuf.len == ned->recvBuf.maxlen)
	{
		/* enlarge free space */
		if (ned->recvBuf.cursor > 0)
		{
			memmove(ned->recvBuf.data,
					ned->recvBuf.data + ned->recvBuf.cursor,
					ned->recvBuf.len - ned->recvBuf.cursor);
			ned->recvBuf.len -= ned->recvBuf.cursor;
			ned->recvBuf.cursor = 0;
		}else
		{
			enlargeStringInfo(&ned->recvBuf,
							  ned->recvBuf.maxlen + DR_SOCKET_BUF_SIZE_STEP);
		}
	}

	if (msg_count > 0)
	{
		if (ned->recvBuf.cursor == ned->recvBuf.len)
		{
			ned->recvBuf.cursor = ned->recvBuf.len = 0;
		}else if (ned->recvBuf.len - ned->recvBuf.cursor <= (ned->recvBuf.maxlen >> 2))
		{
			memmove(ned->recvBuf.data,
					ned->recvBuf.data + ned->recvBuf.cursor,
					ned->recvBuf.len - ned->recvBuf.cursor);
			ned->recvBuf.len -= ned->recvBuf.cursor;
			ned->recvBuf.cursor = 0;
		}
	}

	return msg_count;
}

static void OnNodeRecvMessage(DRNodeEventData *ned, WaitEvent *ev)
{
	ssize_t result;

	Assert(ned->base.type == DR_EVENT_DATA_NODE);
	Assert(ned->recvBuf.maxlen > ned->recvBuf.len);
	if (ned->status == DRN_WAIT_CLOSE)
		return;

	result = RecvMessageFromNode(ned, ev);
	if (result > 0)
	{
		PorcessNodeEventData(ned, ev);
	}
}

static void OnNodeSendMessage(DRNodeEventData *ned, int latch_pos)
{
	ssize_t result;

	Assert(ned->base.type == DR_EVENT_DATA_NODE);
	Assert(ned->sendBuf.len > ned->sendBuf.cursor);
	if (ned->status == DRN_WAIT_CLOSE)
		return;

resend_:
	result = send(GetWaitEventSocket(dr_wait_event_set, latch_pos),
				  ned->sendBuf.data + ned->sendBuf.cursor,
				  ned->sendBuf.len - ned->sendBuf.cursor,
				  0);
	if (result >= 0)
	{
		DR_NODE_DEBUG((errmsg("node %u send message of length %zd to remote success", ned->nodeoid, result)));
		ned->sendBuf.cursor += result;
		if (ned->sendBuf.cursor == ned->sendBuf.len)
			ned->sendBuf.cursor = ned->sendBuf.len = 0;
		ActiveWaitingPlan(ned);
	}else
	{
		if (errno == EINTR)
			goto resend_;
		ereport(ERROR,
				(errmsg("can not send message to node %u: %m", ned->nodeoid)));
	}
}

static uint32 hash_node_event_data(const void *key, Size keysize)
{
	Assert(keysize == sizeof(DRNodeEventData));

	return ((DRNodeEventData*)key)->nodeoid;
}

static int compare_node_event_data(const void *key1, const void *key2, Size keysize)
{
	Assert(keysize == sizeof(DRNodeEventData));

	return (int)((DRNodeEventData*)key1)->nodeoid - (int)((DRNodeEventData*)key2)->nodeoid;
}

static void* copy_node_event_key(void *dest, const void *src, Size keysize)
{
	MemSet(dest, 0, sizeof(DRNodeEventData));
	((DRNodeEventData*)dest)->nodeoid = ((DRNodeEventData*)src)->nodeoid;

	return dest;
}

void DRNodeReset(DRNodeEventData *ned)
{
	if (ned->status != DRN_WORKING ||
		ned->waiting_plan_id != INVALID_PLAN_ID ||
		ned->recvBuf.len != ned->recvBuf.cursor ||
		ned->sendBuf.len != ned->sendBuf.cursor)
		FreeNodeEventInfo(ned);
}

void DRActiveNode(int planid)
{
	DRNodeEventData	   *ned;
	Size				i;

	for (i=0;i<dr_wait_count;++i)
	{
		ned = GetWaitEventData(dr_wait_event_set, i);
		if (ned->base.type == DR_EVENT_DATA_NODE &&
			ned->waiting_plan_id == planid)
		{
			DR_NODE_DEBUG((errmsg("plan %d activing node %u", planid, ned->nodeoid)));
			PorcessNodeEventData(ned, NULL);
		}
	}
}

void DRInitNodeSearch(void)
{
	HASHCTL ctl;
	if (htab_node_info == NULL)
	{
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(DRNodeEventData);
		ctl.entrysize = sizeof(DRNodeEventData);
		ctl.hash = hash_node_event_data;
		ctl.match = compare_node_event_data;
		ctl.keycopy = copy_node_event_key;
		ctl.hcxt = TopMemoryContext;
		htab_node_info = hash_create("Dynamic reduce node info",
									 DR_HTAB_DEFAULT_SIZE,
									 &ctl,
									 HASH_ELEM|HASH_CONTEXT|HASH_FUNCTION|HASH_COMPARE|HASH_KEYCOPY);
	}
}

DRNodeEventData* DRSearchNodeEventData(Oid nodeoid, HASHACTION action, bool *found)
{
	DRNodeEventData ned;
	Assert(OidIsValid(nodeoid));

	ned.nodeoid = nodeoid;
	return hash_search_with_hash_value(htab_node_info, &ned, nodeoid, action, found);
}
