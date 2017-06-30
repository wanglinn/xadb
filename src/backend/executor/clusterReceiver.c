
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "nodes/execnodes.h"
#include "executor/clusterReceiver.h"
#include "executor/tuptable.h"
#include "libpq/libpq.h"
#include "libpq/libpq-node.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "storage/ipc.h"
#include "tcop/dest.h"

#include <time.h>

#define CLUSTER_MSG_TUPLE_DESC	'T'
#define CLUSTER_MSG_TUPLE_DATA	'D'
#define CLUSTER_MSG_INSTRUMENT	'I'
#define CLUSTER_MSG_PROCESSED	'P'

typedef struct ClusterPlanReceiver
{
	DestReceiver pub;
	StringInfoData buf;
	time_t lastCheckTime;	/* last check client message time */
	bool check_end_msg;
}ClusterPlanReceiver;

typedef struct RestoreInstrumentContext
{
	StringInfoData buf;
	Oid		nodeOid;
	int		plan_id;
}RestoreInstrumentContext;

static bool cluster_receive_slot(TupleTableSlot *slot, DestReceiver *self);
static void cluster_receive_startup(DestReceiver *self,int operation,TupleDesc typeinfo);
static void cluster_receive_shutdown(DestReceiver *self);
static void cluster_receive_destroy(DestReceiver *self);
static bool serialize_instrument_walker(PlanState *ps, StringInfo buf);
static void restore_instrument_message(PlanState *ps, const char *msg, int len, struct pg_conn *conn);
static bool restore_instrument_walker(PlanState *ps, RestoreInstrumentContext *context);

/*
 * return ture if got data
 * false is other data
 */
bool clusterRecvTuple(TupleTableSlot *slot, const char *msg, int len, PlanState *ps, struct pg_conn *conn)
{
	if(*msg == CLUSTER_MSG_TUPLE_DATA)
	{
		uint32 t_len = offsetof(MinimalTupleData, t_infomask2) - 1 + len;
		MinimalTuple tup = palloc(t_len);
		MemSet(tup, 0, offsetof(MinimalTupleData, t_infomask2) - offsetof(MinimalTupleData, t_len));
		tup->t_len = t_len;
		memcpy(&tup->t_infomask2, msg + 1, len-1);
		ExecStoreMinimalTuple(tup, slot, true);
		return true;
	}else if(*msg == CLUSTER_MSG_TUPLE_DESC)
	{
		TupleDesc desc = slot->tts_tupleDescriptor;
		StringInfoData buf;
		int i;
		Oid oid;
		if(desc->tdhasoid != (bool)msg[1])
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("diffent TupleDesc of has Oid"),
				errdetail("local is %d, remote is %d", desc->tdhasoid, msg[1])));
		}
		i = *(int*)(&msg[2]);
		if(i != desc->natts)
		{
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
				errmsg("diffent TupleDesc of number attribute"),
				errdetail("local is %d, remote is %d", desc->natts, i)));
		}
		buf.data = (char*)msg;
		buf.maxlen = buf.len = len;
		buf.cursor = 6;
		for(i=0;i<desc->natts;++i)
		{
			oid = load_oid_type(&buf);
			if(oid != desc->attrs[i]->atttypid)
			{
				ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("diffent TupleDesc of attribute[%d]", i),
					errdetail("local is %u, remote is %u", desc->attrs[i]->atttypid, oid)));
			}
		}
		return false;
	}else if(*msg == CLUSTER_MSG_INSTRUMENT)
	{
		if(ps != NULL)
			restore_instrument_message(ps, msg+1, len-1, conn);
		return false;
	}else if(*msg == CLUSTER_MSG_PROCESSED)
	{
		if(ps != NULL)
		{
			uint64 processed;
			memcpy(&processed, msg+1, sizeof(processed));
			ps->state->es_processed += processed;
		}
		return false;
	}else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("unknown cluster message type %d", msg[0])));
	}
	return false; /* keep compiler quiet */
}

static bool cluster_receive_slot(TupleTableSlot *slot, DestReceiver *self)
{
	ClusterPlanReceiver * r = (ClusterPlanReceiver*)self;
	time_t time_now;
	bool need_more_slot;

	resetStringInfo(&r->buf);
	serialize_slot_message(&r->buf, slot);
	pq_putmessage('d', r->buf.data, r->buf.len);
	pq_flush();

	/* check client message */
	need_more_slot = true;
	if(r->check_end_msg && (time_now = time(NULL)) != r->lastCheckTime)
	{
		int n;
		unsigned char first_char;
		r->lastCheckTime = time_now;
		pq_startmsgread();
		n = pq_getbyte_if_available(&first_char);
		if(n == 0)
		{
			/* no message from client */
			pq_endmsgread();
			need_more_slot = true;
		}else if(n < 0)
		{
			/* eof, we don't need more slot */
			pq_endmsgread();
			need_more_slot = false;
		}else
		{
			resetStringInfo(&r->buf);
			if(pq_getmessage(&r->buf, 0))
			{
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("unexpected EOF on coordinator connection")));
				proc_exit(0);
			}

			if(first_char == 'c'
				|| first_char == 'X')
			{
				/* copy end */
				need_more_slot = false;
			}else if(first_char == 'd')
			{
				/* message */
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						errmsg("not support copy data in yet")));
			}else
			{
				ereport(FATAL,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("invalid coordinator message type \"%c\"",
								first_char)));
			}
		}
	}
	return need_more_slot;
}

static void cluster_receive_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	ClusterPlanReceiver * r = (ClusterPlanReceiver*)self;

	initStringInfo(&r->buf);

	/* send tuple desc */
	serialize_slot_head_message(&r->buf, typeinfo);
	pq_putmessage('d', r->buf.data, r->buf.len);
	pq_flush();
}

static void cluster_receive_shutdown(DestReceiver *self)
{
}

static void cluster_receive_destroy(DestReceiver *self)
{
	ClusterPlanReceiver * r = (ClusterPlanReceiver*)self;
	if(r->buf.data)
		pfree(r->buf.data);
	pfree(r);
}

DestReceiver *createClusterReceiver(void)
{
	ClusterPlanReceiver *self;

	self = palloc0(sizeof(*self));
	self->pub.mydest = DestClusterOut;
	self->pub.receiveSlot = cluster_receive_slot;
	self->pub.rStartup = cluster_receive_startup;
	self->pub.rShutdown = cluster_receive_shutdown;
	self->pub.rDestroy = cluster_receive_destroy;
	self->check_end_msg = true;
	/* self->lastCheckTime = 0; */

	return &self->pub;
}

bool clusterRecvSetCheckEndMsg(DestReceiver *r, bool check)
{
	ClusterPlanReceiver *self;
	bool old_check;
	Assert(r->mydest == DestClusterOut);

	self = (ClusterPlanReceiver*)r;
	old_check = self->check_end_msg;
	self->check_end_msg = check ? true:false;

	return old_check;
}

void serialize_instrument_message(PlanState *ps, StringInfo buf)
{
	appendStringInfoChar(buf, CLUSTER_MSG_INSTRUMENT);
	serialize_instrument_walker(ps, buf);
}

void serialize_processed_message(StringInfo buf, uint64 processed)
{
	appendStringInfoChar(buf, CLUSTER_MSG_PROCESSED);
	appendBinaryStringInfo(buf, (char*)&processed, sizeof(processed));
}

void serialize_slot_head_message(StringInfo buf, TupleDesc desc)
{
	int i;
	AssertArg(buf && desc);

	appendStringInfoChar(buf, CLUSTER_MSG_TUPLE_DESC);
	appendStringInfoChar(buf, desc->tdhasoid);
	appendBinaryStringInfo(buf, (char*)&(desc->natts), sizeof(desc->natts));
	for(i=0;i<desc->natts;++i)
		save_oid_type(buf, desc->attrs[i]->atttypid);
}

void serialize_slot_message(StringInfo buf, TupleTableSlot *slot)
{
	MinimalTuple tup;
	AssertArg(buf && !TupIsNull(slot));

	tup = ExecFetchSlotMinimalTuple(slot);
	appendStringInfoChar(buf, CLUSTER_MSG_TUPLE_DATA);
	appendBinaryStringInfo(buf, (char*)&(tup->t_infomask2),
						   tup->t_len - offsetof(MinimalTupleData, t_infomask2));
}

static bool serialize_instrument_walker(PlanState *ps, StringInfo buf)
{
	int num_worker;

	if(ps == NULL)
		return false;

	/* plan ID */
	appendBinaryStringInfo(buf,
						   (char*)&(ps->plan->plan_node_id),
						   sizeof(ps->plan->plan_node_id));

	if(ps->worker_instrument && ps->worker_instrument->num_workers)
	{
		num_worker = ps->worker_instrument->num_workers;
	}else
	{
		num_worker = 0;
	}
	/* worker instrument */
	appendBinaryStringInfo(buf, (char*)&num_worker, sizeof(num_worker));

	appendBinaryStringInfo(buf,
						   (char*)ps->instrument,
						   sizeof(*(ps->instrument)));
	if(num_worker)
		appendBinaryStringInfo(buf,
							   (char*)(ps->worker_instrument->instrument),
							   sizeof(Instrumentation) * num_worker);

	return planstate_tree_walker(ps, serialize_instrument_walker, buf);
}

static bool restore_instrument_walker(PlanState *ps, RestoreInstrumentContext *context)
{
	if(ps == NULL)
		return false;

	if(context->plan_id == ps->plan->plan_node_id)
	{
		MemoryContext oldcontext;
		ClusterInstrumentation *ci;
		int n;

		pq_copymsgbytes(&(context->buf), (char*)&n, sizeof(n));	/* count of worker */

		oldcontext = MemoryContextSwitchTo(ps->state->es_query_cxt);
		ci = palloc(sizeof(*ci) + sizeof(ci->instrument[0]) * n);
		ci->num_workers = n;
		ci->nodeOid = context->nodeOid;
		ps->list_cluster_instrument = lappend(ps->list_cluster_instrument, ci);
		MemoryContextSwitchTo(oldcontext);

		pq_copymsgbytes(&(context->buf),
						(char*)&(ci->instrument[0]),
						sizeof(ci->instrument[0]) * (n+1));
		return true;
	}
	return planstate_tree_walker(ps, restore_instrument_walker, context);
}

static void restore_instrument_message(PlanState *ps, const char *msg, int len, struct pg_conn *conn)
{
	RestoreInstrumentContext context;
	context.buf.data = (char*)msg;
	context.buf.cursor = 0;
	context.buf.len = context.buf.maxlen = len;
	if(conn)
		context.nodeOid = PQNConnectOid(conn);

	while(context.buf.cursor < context.buf.len)
	{
		if(context.buf.len-context.buf.cursor < sizeof(context.plan_id)+sizeof(Instrumentation))
			ereport(ERROR,
					(errmsg("invalid instrumentation message length"),
					errcode(ERRCODE_INTERNAL_ERROR)));
		pq_copymsgbytes(&context.buf, (char*)&(context.plan_id), sizeof(context.plan_id));
		if(planstate_tree_walker(ps, restore_instrument_walker, &context) == false)
		{
			ereport(ERROR, (errmsg("plan node %d not found", context.plan_id)));
		}
	}
}
