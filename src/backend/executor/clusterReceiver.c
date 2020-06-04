
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/tuptoaster.h"
#include "access/tuptypeconvert.h"
#include "access/xact.h"
#include "nodes/execnodes.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "libpq/libpq.h"
#include "libpq/libpq-node.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "storage/ipc.h"
#include "tcop/dest.h"
#include "utils/memutils.h"

#include <time.h>

typedef struct ClusterPlanReceiver
{
	DestReceiver pub;
	StringInfoData buf;
	time_t lastCheckTime;	/* last check client message time */
	TupleTableSlot *convert_slot;
	TupleTypeConvert *convert;
	bool check_end_msg;
}ClusterPlanReceiver;

typedef struct RestoreInstrumentContext
{
	StringInfoData buf;
	Oid		nodeOid;
	int		plan_id;
}RestoreInstrumentContext;

typedef struct SerializeInstrumentContext
{
	StringInfo buf;
	Bitmapset *serialized;
}SerializeInstrumentContext;

static bool cluster_receive_slot(TupleTableSlot *slot, DestReceiver *self);
static void cluster_receive_startup(DestReceiver *self,int operation,TupleDesc typeinfo);
static void cluster_receive_shutdown(DestReceiver *self);
static void cluster_receive_destroy(DestReceiver *self);
static bool serialize_instrument_walker(PlanState *ps, SerializeInstrumentContext *context);
static void restore_instrument_message(PlanState *ps, const char *msg, int len, struct pg_conn *conn);
static bool restore_instrument_walker(PlanState *ps, RestoreInstrumentContext *context);
static void process_transaction_message(const char *msg, int len, struct pg_conn *conn);

void serialize_rdc_listen_port_message(StringInfo buf, uint16 port)
{
	initStringInfo(buf);
	appendStringInfoChar(buf, CLUSTER_MSG_RDC_PORT);
	appendBinaryStringInfo(buf, (char *) &port, sizeof(port));
}

bool clusterRecvRdcListenPort(struct pg_conn *conn, const char *msg, int len, uint16 *port)
{
	const char *nodename;
	if (*msg == CLUSTER_MSG_RDC_PORT)
	{
		Assert(len == sizeof(*port) + 1);
		if (port)
			memcpy(port, msg + 1, sizeof(*port));
		return true;
	}
	nodename = PQNConnectName(conn);
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("fail to get reduce listen port"),
			 errdetail("unexpected cluster message type %d", msg[0]),
			 nodename ? errnode(nodename) : 0));
	return false;
}

/*
 * return ture if got data
 * false is other data
 */
bool clusterRecvTuple(TupleTableSlot *slot, const char *msg, int len, PlanState *ps, struct pg_conn *conn)
{
	if(*msg == CLUSTER_MSG_TUPLE_DATA)
	{
		if (slot)
		{
			restore_slot_message(msg+1, len-1, slot);
			return true;
		}
	}else if(*msg == CLUSTER_MSG_TUPLE_DESC)
	{
		if (slot)
			compare_slot_head_message(msg+1, len-1, slot->tts_tupleDescriptor);
		return false;
	}else if(*msg == CLUSTER_MSG_INSTRUMENT)
	{
		if(ps != NULL)
			restore_instrument_message(ps, msg+1, len-1, conn);
		return false;
	}else if(*msg == CLUSTER_MSG_PROCESSED)
	{
		if(ps != NULL)
			ps->state->es_processed += restore_processed_message(msg+1, len-1);
		return false;
	}else if (*msg == CLUSTER_MSG_EXECUTOR_RUN_END)
	{
		return false;
	}else if (*msg == CLUSTER_MSG_TABLE_STAT)
	{
		ClusterRecvTableStat(msg+1, len-1);
		return false;
	}else if (*msg == CLUSTER_MSG_TRANSACTION_ID)
	{
		process_transaction_message(msg+1, len-1, conn);
		return false;
	}else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("unknown cluster message type %d", msg[0])));
	}
	return false; /* keep compiler quiet */
}

bool clusterRecvTupleEx(ClusterRecvState *state, const char *msg, int len, struct pg_conn *conn)
{
	switch(*msg)
	{
	case CLUSTER_MSG_TUPLE_DATA:
		restore_slot_message(msg+1, len-1, state->base_slot);
		return true;
	case CLUSTER_MSG_TUPLE_DESC:
		compare_slot_head_message(msg+1, len-1, state->base_slot->tts_tupleDescriptor);
		break;
	case CLUSTER_MSG_INSTRUMENT:
		if(state->ps != NULL)
			restore_instrument_message(state->ps, msg+1, len-1, conn);
		break;
	case CLUSTER_MSG_PROCESSED:
		if(state->ps != NULL)
			state->ps->state->es_processed += restore_processed_message(msg+1, len-1);
		break;
	case CLUSTER_MSG_CONVERT_DESC:
		if(state->convert_slot)
		{
			compare_slot_head_message(msg+1, len-1, state->convert_slot->tts_tupleDescriptor);
		}else
		{
			TupleDesc desc;
			MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state->base_slot));

			desc = restore_slot_head_message(msg+1, len-1);
			if(state->ps)
			{
				state->convert_slot = ExecInitExtraTupleSlot(state->ps->state, desc);
			}else
			{
				state->convert_slot = MakeSingleTupleTableSlot(desc);
				state->convert_slot_is_single = true;
			}
			state->convert = create_type_convert(state->base_slot->tts_tupleDescriptor,
												 false,
												 true);

			MemoryContextSwitchTo(oldcontext);
		}
		break;
	case CLUSTER_MSG_CONVERT_TUPLE:
		if(state->convert)
		{
			restore_slot_message(msg+1, len-1, state->convert_slot);
			do_type_convert_slot_in(state->convert, state->convert_slot, state->base_slot, state->slot_need_copy_datum);
			return true;
		}else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("con not parse convert tuple")));
		}
		break;
	case CLUSTER_MSG_EXECUTOR_RUN_END:
		break;
	case CLUSTER_MSG_TABLE_STAT:
		ClusterRecvTableStat(msg+1, len-1);
		break;
	case CLUSTER_MSG_TRANSACTION_ID:
		process_transaction_message(msg+1, len-1, conn);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("unknown cluster message type %d", msg[0])));
	}
	return false;
}

static bool cluster_receive_slot(TupleTableSlot *slot, DestReceiver *self)
{
	ClusterPlanReceiver * r = (ClusterPlanReceiver*)self;
	time_t time_now;
	bool need_more_slot;

	resetStringInfo(&r->buf);
	if(r->convert)
	{
		do_type_convert_slot_out(r->convert, slot, r->convert_slot, false);
		serialize_slot_message(&r->buf, r->convert_slot, CLUSTER_MSG_CONVERT_TUPLE);
	}else
	{
		serialize_slot_message(&r->buf, slot, CLUSTER_MSG_TUPLE_DATA);
	}
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

	/* need convert ? */
	r->convert = create_type_convert(typeinfo, true, false);
	if(r->convert)
	{
		r->convert_slot = MakeSingleTupleTableSlot(r->convert->out_desc);
		resetStringInfo(&r->buf);
		serialize_slot_convert_head(&r->buf, r->convert->out_desc);
		pq_putmessage('d', r->buf.data, r->buf.len);
	}

	pq_flush();
}

static void cluster_receive_shutdown(DestReceiver *self)
{
	ClusterPlanReceiver * r = (ClusterPlanReceiver*)self;
	if(r->convert_slot)
		ExecDropSingleTupleTableSlot(r->convert_slot);
	if(r->convert)
		free_type_convert(r->convert);
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

static ClusterRecvState *createClusterRecvStateEx(TupleTableSlot *slot, PlanState *ps, bool need_copy)
{
	ClusterRecvState *state;

	state = palloc0(sizeof(*state));
	state->base_slot = slot;
	state->ps = ps;

	state->convert = create_type_convert(slot->tts_tupleDescriptor, false, true);
	if(state->convert != NULL)
	{
		state->slot_need_copy_datum = need_copy;
		if (ps)
		{
			state->convert_slot = ExecInitExtraTupleSlot(ps->state, state->convert->out_desc);
			state->convert_slot_is_single = false;
		}else
		{
			state->convert_slot = MakeSingleTupleTableSlot(state->convert->out_desc);
			state->convert_slot_is_single = true;
		}
	}

	return state;
}

ClusterRecvState *createClusterRecvState(PlanState *ps, bool need_copy)
{

	return createClusterRecvStateEx(ps->ps_ResultTupleSlot, ps, need_copy);
}

ClusterRecvState *createClusterRecvStateFromSlot(TupleTableSlot *slot, bool need_copy)
{
	return createClusterRecvStateEx(slot, NULL, need_copy);
}

void freeClusterRecvState(ClusterRecvState *state)
{
	if(state)
	{
		if (state->convert_slot &&
			state->convert_slot_is_single)
			ExecDropSingleTupleTableSlot(state->convert_slot);
		if(state->convert)
			free_type_convert(state->convert);
		pfree(state);
	}
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
	SerializeInstrumentContext context;
	appendStringInfoChar(buf, CLUSTER_MSG_INSTRUMENT);
	context.buf = buf;
	context.serialized = NULL;
	serialize_instrument_walker(ps, &context);
	bms_free(context.serialized);
}

void serialize_processed_message(StringInfo buf, uint64 processed)
{
	appendStringInfoChar(buf, CLUSTER_MSG_PROCESSED);
	appendBinaryStringInfo(buf, (char*)&processed, sizeof(processed));
}

uint64 restore_processed_message(const char *msg, int len)
{
	uint64 processed;
	if (len != sizeof(processed))
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("Invalid processed message length")));
	memcpy(&processed, msg, sizeof(processed));

	return processed;
}

void serialize_tuple_desc(StringInfo buf, TupleDesc desc, char msg_type)
{
	int32 atttypmod;
	int32 attndims;
	int i,natts;

	AssertArg(buf && desc);

	natts = desc->natts;
	appendStringInfoChar(buf, msg_type);
	appendStringInfoChar(buf, desc->tdhasoid);
	appendBinaryStringInfo(buf, (char*)&desc->natts, sizeof(desc->natts));
	for(i=0;i<natts;++i)
	{
		if (TupleDescAttr(desc, i)->attisdropped)
		{
			appendStringInfoChar(buf, true);
		}else
		{
			appendStringInfoChar(buf, false);
			/* attname */
			save_node_string(buf, NameStr(TupleDescAttr(desc, i)->attname));
			/* atttypmod */
			atttypmod = TupleDescAttr(desc, i)->atttypmod;
			appendBinaryStringInfo(buf, (const char *)&atttypmod, sizeof(atttypmod));
			/* attndims */
			attndims = TupleDescAttr(desc, i)->attndims;
			appendBinaryStringInfo(buf, (const char *) &attndims, sizeof(attndims));
			/* save oid type */
			save_oid_type(buf, TupleDescAttr(desc, i)->atttypid);
		}
	}
}

void compare_slot_head_message(const char *msg, int len, TupleDesc desc)
{
	StringInfoData buf;
	int i,nattr;
	Oid oid;
	bool isdropped;

	if(len < (sizeof(bool) + sizeof(int)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid message length")));
	}

	if(desc->tdhasoid != (bool)msg[0])
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("diffent TupleDesc of has Oid"),
				 errdetail("local is %d, remote is %d", desc->tdhasoid, msg[0])));
	}

	memcpy(&nattr, msg+1, sizeof(nattr));
	if (desc->natts != nattr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("diffent TupleDesc of number attribute"),
				 errdetail("local is %d, remote is %d", desc->natts, nattr)));
	}

	buf.data = (char*)msg;
	buf.maxlen = buf.len = len;
	buf.cursor = (sizeof(bool)+sizeof(int));
	for(i=0;i<desc->natts;++i)
	{
		isdropped = (bool)pq_getmsgbyte(&buf);
		if (TupleDescAttr(desc,i)->attisdropped != isdropped)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("diffent TupleDesc attribute isdropped of number %d", i+1),
					 errdetail("local is %d, remote is %d", TupleDescAttr(desc,i)->attisdropped, isdropped)));
		}

		if (isdropped == false)
		{
			/* attname ignore */
			(void) load_node_string(&buf, false);
			/* atttypmod ignore */
			buf.cursor += sizeof(int32);
			/* attndims */
			buf.cursor += sizeof(int32);
			/* load oid type */
			oid = load_oid_type(&buf);
			if(oid != TupleDescAttr(desc, i)->atttypid)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("diffent TupleDesc of attribute[%d]", i),
						 errdetail("local is %u, remote is %u", TupleDescAttr(desc, i)->atttypid, oid)));
			}
		}
	}
}

TupleDesc restore_slot_head_message(const char *msg, int len)
{
	StringInfoData buf;
	buf.cursor = 0;
	buf.len = buf.maxlen = len;
	buf.data = (char*)msg;
	return restore_slot_head_message_str(&buf);
}

TupleDesc restore_slot_head_message_str(StringInfo buf)
{
	TupleDesc desc;
	int i;
	Oid oid;
	const char *attributeName;
	int32 typmod;
	int attdim;
	bool bval;

	bval = (bool)pq_getmsgbyte(buf);
	pq_copymsgbytes(buf, (char*)&i, sizeof(i));

	desc = CreateTemplateTupleDesc(i, bval);

	for(i=1;i<=desc->natts;++i)
	{
		bval = (bool)pq_getmsgbyte(buf);

		if (bval)
		{
			/* like function RemoveAttributeById */
			Form_pg_attribute attr = TupleDescAttr(desc, i-1);
			attr->attisdropped = true;
			attr->atttypid = InvalidOid;
			attr->attnotnull = false;
			attr->attstattarget = 0;
			attr->atthasmissing = false;
		}else
		{
			/* attname ignore */
			attributeName = load_node_string(buf, false);
			/* atttypmod ignore */
			pq_copymsgbytes(buf, (char *) &typmod, sizeof(typmod));
			/* attndims */
			pq_copymsgbytes(buf, (char *) &attdim, sizeof(attdim));
			/* load oid type */
			oid = load_oid_type(buf);
			TupleDescInitEntry(desc,
							   i,
							   attributeName,
							   oid,
							   typmod,
							   attdim);
		}
	}
	return desc;
}

MinimalTuple fetch_slot_message(TupleTableSlot *slot, bool *need_free_tup)
{
	MinimalTuple tup;
	TupleDesc desc;
	int i;
	bool have_external;
	AssertArg(!TupIsNull(slot));

	slot_getallattrs(slot);
	have_external = false;
	desc = slot->tts_tupleDescriptor;
	for(i=desc->natts;(--i)>=0;)
	{
		Form_pg_attribute attr=TupleDescAttr(desc, i);

		if (slot->tts_isnull[i] == false &&
			attr->attlen == -1 &&
			attr->attbyval == false &&
			VARATT_IS_EXTERNAL(DatumGetPointer(slot->tts_values[i])))
		{
			/* bytea */
			have_external = true;
			break;
		}
	}

	if(have_external)
	{
		MemoryContext old_context = MemoryContextSwitchTo(slot->tts_mcxt);
		Datum *values = palloc(sizeof(Datum) * desc->natts);
		for(i=desc->natts;(--i)>=0;)
		{
			Form_pg_attribute attr = TupleDescAttr(desc, i);
			if (slot->tts_isnull[i] == false &&
				attr->attlen == -1 &&
				attr->attbyval == false &&
				VARATT_IS_EXTERNAL(DatumGetPointer(slot->tts_values[i])))
			{
				values[i] = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)DatumGetPointer(slot->tts_values[i])));
#ifdef NOT_USED
				/* try compress it */
				if(!VARATT_IS_COMPRESSED(DatumGetPointer(values[i])))
				{
					Datum tmp = toast_compress_datum(values[i]);
					if(tmp)
					{
						pfree(DatumGetPointer(values[i]));
						values[i] = tmp;
					}
				}
#endif /* NOT_USED */
			}else
			{
				values[i] = slot->tts_values[i];
			}
		}
		tup = heap_form_minimal_tuple(desc, values, slot->tts_isnull);
		if (slot->tts_tupleDescriptor->tdhasoid)
		{
			HeapTupleHeader header;
			Oid oid = ExecFetchSlotTupleOid(slot);
			if (OidIsValid(oid))
			{
				header = (HeapTupleHeader)((char*)tup - MINIMAL_TUPLE_OFFSET);
				HeapTupleHeaderSetOid(header, oid);
			}
		}
		*need_free_tup = true;

		/* clear resource */
		for(i=desc->natts;(--i)>=0;)
		{
			if(values[i] != slot->tts_values[i])
				pfree(DatumGetPointer(values[i]));
		}
		pfree(values);
		MemoryContextSwitchTo(old_context);
	}else
	{
		tup = ExecFetchSlotMinimalTuple(slot);
		*need_free_tup = false;
	}
	return tup;
}

void serialize_slot_message(StringInfo buf, TupleTableSlot *slot, char msg_type)
{
	MinimalTuple tup;
	bool need_free_tup;
	AssertArg(buf && !TupIsNull(slot));

	tup = fetch_slot_message(slot, &need_free_tup);

	appendStringInfoChar(buf, msg_type);
	appendBinaryStringInfo(buf, (char*)tup, tup->t_len);
	if(need_free_tup)
		pfree(tup);
}

TupleTableSlot* restore_slot_message(const char *msg, int len, TupleTableSlot *slot)
{
	MinimalTuple tup;
	uint32 t_len = *(uint32*)msg;
	if(t_len > len)
		ereport(ERROR, (errmsg("invalid tuple message length")));
	tup = MemoryContextAlloc(slot->tts_mcxt, t_len);
	memcpy(tup, msg, t_len);
	return ExecStoreMinimalTuple(tup, slot, true);
}

static bool serialize_instrument_walker(PlanState *ps, SerializeInstrumentContext *context)
{
	int num_worker;

	if (ps == NULL ||
		bms_is_member(ps->plan->plan_node_id, context->serialized))
		return false;

	context->serialized = bms_add_member(context->serialized, ps->plan->plan_node_id);

	/* plan ID */
	appendBinaryStringInfo(context->buf,
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
	appendBinaryStringInfo(context->buf, (char*)&num_worker, sizeof(num_worker));

	appendBinaryStringInfo(context->buf,
						   (char*)ps->instrument,
						   sizeof(*(ps->instrument)));
	if(num_worker)
		appendBinaryStringInfo(context->buf,
							   (char*)(ps->worker_instrument->instrument),
							   sizeof(Instrumentation) * num_worker);

	return planstate_tree_walker(ps, serialize_instrument_walker, context);
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

static void process_transaction_message(const char *msg, int len, struct pg_conn *conn)
{
	List *list;
	StringInfoData buf;
	int level;
	TransactionId xid;
	char send_msg[5];

	buf.data = (char*)msg;
	buf.len = buf.maxlen = len;
	buf.cursor = 0;

	pq_copymsgbytes(&buf, (char*)&level, sizeof(level));
	if (level != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("not support sub transaction yet!")));
	}

	xid = GetTopTransactionId();
	send_msg[0] = CLUSTER_MSG_TRANSACTION_ID;
	memcpy(&send_msg[1], &xid, sizeof(xid));

	list = list_make1(conn);
	PQNputCopyData(list, send_msg, sizeof(send_msg));
	PQNFlush(list, true);
	list_free(list);
}

void put_executor_end_msg(bool flush)
{
	static const char run_end_msg[] = {CLUSTER_MSG_EXECUTOR_RUN_END};
	pq_putmessage('d', run_end_msg, sizeof(run_end_msg));
	if (flush)
		pq_flush();
}

static bool wait_exec_end_msg_call_back(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	if (*buf == CLUSTER_MSG_EXECUTOR_RUN_END)
	{
		return true;
	}else if (clusterRecvTuple(NULL, buf, len, NULL, conn))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message type %d from %s, except %d",
				 		*buf, PQNConnectName(conn), CLUSTER_MSG_EXECUTOR_RUN_END)));
	}
	return false;
}

void wait_executor_end_msg(struct pg_conn *conn)
{
	PQNHookFunctions funcs = PQNDefaultHookFunctions;
	funcs.HookCopyOut = wait_exec_end_msg_call_back;
	PQNOneExecFinish(conn, &funcs, true);
}
