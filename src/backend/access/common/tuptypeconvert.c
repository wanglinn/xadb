#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/tuptypeconvert.h"
#include "catalog/pg_type.h"
#include "executor/clusterReceiver.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

typedef struct ConvertIO
{
	FmgrInfo	in_func;
	FmgrInfo	out_func;
	Oid			base_type;
	Oid			io_param;
	bool		bin_type;
}ConvertIO;

typedef struct RecordConvert
{
	TupleTypeConvert *convert;
	TupleTableSlot *slot_base;
	TupleTableSlot *slot_temp;
}RecordConvert;

static bool type_need_convert(Oid typeoid, bool *binary);
static TupleDesc create_convert_desc_if_need(TupleDesc indesc);
static void free_record_convert(RecordConvert *rc);

static Datum convert_record_recv(PG_FUNCTION_ARGS);
static Datum convert_record_send(PG_FUNCTION_ARGS);
static RecordConvert* set_record_convert_tuple_desc(RecordConvert *rc, TupleDesc desc, MemoryContext context, bool is_send);

TupleTypeConvert* create_type_convert(TupleDesc base_desc, bool need_out, bool need_in)
{
	TupleTypeConvert *convert;
	TupleDesc out_desc;
	Form_pg_attribute attr;
	ConvertIO *io;
	int i;
	Oid func;
	AssertArg(need_out || need_in);

	out_desc = create_convert_desc_if_need(base_desc);
	if(out_desc == NULL)
		return NULL;
	Assert(base_desc->natts == out_desc->natts);

	convert = palloc(sizeof(*convert));
	convert->base_desc = base_desc;
	convert->out_desc = out_desc;
	convert->io_state = NIL;

	for(i=0;i<base_desc->natts;++i)
	{
		bool use_binary;
		attr = base_desc->attrs[i];
		if(type_need_convert(attr->atttypid, &use_binary))
		{
			io = palloc0(sizeof(*io));
			io->bin_type = use_binary;
			io->base_type = getBaseType(attr->atttypid);
			if(need_in)
			{
				if(use_binary)
					getTypeBinaryInputInfo(attr->atttypid, &func, &io->io_param);
				else
					getTypeInputInfo(attr->atttypid, &func, &io->io_param);
				fmgr_info(func, &io->in_func);
				if (io->in_func.fn_addr == anyarray_recv ||
					io->in_func.fn_addr == array_recv)
				{
					io->in_func.fn_addr = array_recv_str_type;
				}else if(io->in_func.fn_addr == record_recv)
				{
					Assert(io->base_type == RECORDOID);
					io->in_func.fn_addr = convert_record_recv;
				}
			}
			if(need_out)
			{
				bool is_varian;
				if(use_binary)
					getTypeBinaryOutputInfo(attr->atttypid, &func, &is_varian);
				else
					getTypeOutputInfo(attr->atttypid, &func, &is_varian);
				fmgr_info(func, &io->out_func);
				if (io->out_func.fn_addr == anyarray_send ||
					io->out_func.fn_addr == array_send)
				{
					io->out_func.fn_addr = array_send_str_type;
				}else if(io->out_func.fn_addr == record_send)
				{
					io->out_func.fn_addr = convert_record_send;
				}
			}
		}else
		{
			io = NULL;
		}
		convert->io_state = lappend(convert->io_state, io);
	}
	return convert;
}

TupleTableSlot* do_type_convert_slot_in(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy)
{
	int i;
	ListCell *lc;
	ConvertIO *io;
	StringInfoData buf;

	Assert(list_length(convert->io_state) == src->tts_tupleDescriptor->natts);
	Assert(src->tts_tupleDescriptor->natts == dest->tts_tupleDescriptor->natts);
	Assert(src->tts_tupleDescriptor->natts == convert->base_desc->natts);

	ExecClearTuple(dest);
	if(TupIsNull(src))
		return dest;
	slot_getallattrs(src);

	push_client_encoding(GetDatabaseEncoding());
	PG_TRY();
	{
		i=0;
		memcpy(dest->tts_isnull, src->tts_isnull, sizeof(src->tts_isnull[0]) * src->tts_tupleDescriptor->natts);
		foreach(lc, convert->io_state)
		{
			if(!src->tts_isnull[i])
			{
				io = lfirst(lc);
				if(io == NULL)
				{
					Form_pg_attribute attr = src->tts_tupleDescriptor->attrs[i];
					if (need_copy && !attr->attbyval)
						dest->tts_values[i] = datumCopy(src->tts_values[i], false, attr->attlen);
					else
						dest->tts_values[i] = src->tts_values[i];
				}else
				{
					if(io->bin_type)
					{
						bytea *p = DatumGetByteaP(src->tts_values[i]);
						buf.data = VARDATA_ANY(p);
						buf.len = buf.maxlen = VARSIZE_ANY_EXHDR(p);
						buf.cursor = 0;
						dest->tts_values[i] = ReceiveFunctionCall(&io->in_func, &buf, io->io_param, -1);
					}else
					{
						dest->tts_values[i] = InputFunctionCall(&io->in_func, DatumGetPointer(src->tts_values[i]), io->io_param, -1);
					}
				}
			}
			++i;
		}
	}PG_CATCH();
	{
		pop_client_encoding();
		PG_RE_THROW();
	}PG_END_TRY();
	pop_client_encoding();

	return ExecStoreVirtualTuple(dest);
}

TupleTableSlot* do_type_convert_slot_out(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy)
{
	ListCell *lc;
	ConvertIO *io;
	int i;

	Assert(list_length(convert->io_state) == src->tts_tupleDescriptor->natts);
	Assert(src->tts_tupleDescriptor->natts == dest->tts_tupleDescriptor->natts);
	Assert(src->tts_tupleDescriptor->natts == convert->base_desc->natts);

	ExecClearTuple(dest);
	if(TupIsNull(src))
		return dest;
	slot_getallattrs(src);

	i=0;
	memcpy(dest->tts_isnull, src->tts_isnull, sizeof(src->tts_isnull[0]) * src->tts_tupleDescriptor->natts);
	foreach(lc, convert->io_state)
	{
		if (!src->tts_isnull[i])
		{
			io = lfirst(lc);
			if(io == NULL)
			{
				Form_pg_attribute attr = src->tts_tupleDescriptor->attrs[i];
				if (need_copy && !attr->attbyval)
					dest->tts_values[i] = datumCopy(src->tts_values[i], false, attr->attlen);
				else
					dest->tts_values[i] = src->tts_values[i];
			}else if(!src->tts_isnull[i])
			{
				if(io->bin_type)
				{
					bytea *p = SendFunctionCall(&io->out_func, src->tts_values[i]);
					dest->tts_values[i] = PointerGetDatum(p);
				}else
				{
					char *str = OutputFunctionCall(&io->out_func, src->tts_values[i]);
					dest->tts_values[i] = PointerGetDatum(str);
				}
			}
		}
		++i;
	}

	return ExecStoreVirtualTuple(dest);
}

static TupleDesc create_convert_desc_if_need(TupleDesc indesc)
{
	TupleDesc outdesc;
	Form_pg_attribute attr;
	int i;
	Oid type;

	for(i=0;i<indesc->natts;++i)
	{
		if (type_need_convert(indesc->attrs[i]->atttypid, NULL))
			break;
	}
	if(i>=indesc->natts)
		return NULL;	/* don't need convert */

	outdesc = CreateTemplateTupleDesc(indesc->natts, false);
	for(i=0;i<indesc->natts;++i)
	{
		bool use_binary;
		attr = indesc->attrs[i];
		Assert(attr->attisdropped == false);

		type = attr->atttypid;
		if(type_need_convert(attr->atttypid, &use_binary))
			type = use_binary ? BYTEAOID:UNKNOWNOID;
		else
			type = attr->atttypid;

		TupleDescInitEntry(outdesc,
						   i+1,
						   NULL,
						   type,
						   -1,
						   0);
	}
	return outdesc;
}

static bool type_need_convert(Oid typeoid, bool *binary)
{
	Oid base_oid;
	char type;

	type = get_typtype(typeoid);
	if (type == TYPTYPE_PSEUDO)
	{
		if(binary)
		{
			if (typeoid == ANYARRAYOID ||
				(base_oid = getBaseType(typeoid)) == ANYARRAYOID ||
				base_oid == RECORDOID)
				*binary = true;
			else
				*binary = false;
		}
		return true;
	}else if(type == TYPTYPE_ENUM)
	{
		if (binary)
			*binary = true;
		return true;
	}

	base_oid = getBaseType(typeoid);
	if (typeoid == REGCLASSOID ||
		base_oid == REGCLASSOID ||
		typeoid == REGPROCOID ||
		base_oid == REGPROCOID)
	{
		if(binary)
			*binary = false;
		return true;
	}else if(typeoid < FirstNormalObjectId ||
			 base_oid < FirstNormalObjectId)
	{
		if(binary)
			*binary = false;
		return false;
	}
	if (binary)
		*binary = false;
	return true;
}

void free_type_convert(TupleTypeConvert *convert)
{
	ListCell *lc;
	if(convert)
	{
		foreach(lc,convert->io_state)
		{
			ConvertIO *io = lfirst(lc);
			if(io)
			{
				if(io->base_type == RECORDOID)
				{
					free_record_convert(io->in_func.fn_extra);
					free_record_convert(io->out_func.fn_extra);
				}
				pfree(io);
			}
		}
		list_free(convert->io_state);
		if(convert->out_desc)
			FreeTupleDesc(convert->out_desc);
		pfree(convert);
	}
}

static void free_record_convert(RecordConvert *rc)
{
	if(rc)
	{
		if(rc->slot_temp)
			ExecDropSingleTupleTableSlot(rc->slot_temp);
		if(rc->slot_base)
			ExecDropSingleTupleTableSlot(rc->slot_base);
		free_type_convert(rc->convert);
		pfree(rc);
	}
}

static Datum convert_record_recv(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	RecordConvert *my_extra;
	HeapTuple tuple;
	StringInfo buf;

	buf = (StringInfo)PG_GETARG_POINTER(0);

	if(pq_getmsgbyte(buf) != CLUSTER_MSG_TUPLE_DESC)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid cluster record data")));
	}

	tupdesc = restore_slot_head_message_str(buf);

	my_extra = (RecordConvert *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL ||
		!equalTupleDescs(my_extra->slot_base->tts_tupleDescriptor, tupdesc))
	{
		TupleDesc desc;
		BlessTupleDesc(tupdesc);
		desc = lookup_rowtype_tupdesc(tupdesc->tdtypeid, tupdesc->tdtypmod);
		my_extra = fcinfo->flinfo->fn_extra
				 = set_record_convert_tuple_desc(my_extra,
												 desc,
												 fcinfo->flinfo->fn_mcxt,
												 false);
		ReleaseTupleDesc(desc);
	}
	FreeTupleDesc(tupdesc);

	if(pq_getmsgbyte(buf) != (my_extra->convert ? CLUSTER_MSG_CONVERT_TUPLE:CLUSTER_MSG_TUPLE_DATA))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid cluster record data")));
	}
	if(my_extra->convert)
	{
		restore_slot_message(buf->data + buf->cursor, buf->len - buf->cursor, my_extra->slot_temp);
		do_type_convert_slot_in(my_extra->convert, my_extra->slot_temp, my_extra->slot_base, false);
		tuple = ExecFetchSlotTuple(my_extra->slot_base);
	}else
	{
		restore_slot_message(buf->data + buf->cursor, buf->len - buf->cursor, my_extra->slot_base);
		tuple = ExecFetchSlotTuple(my_extra->slot_base);
	}

	PG_RETURN_HEAPTUPLEHEADER(tuple->t_data);
}

static Datum convert_record_send(PG_FUNCTION_ARGS)
{
	HeapTupleHeader		record;
	TupleDesc			tupdesc;
	HeapTupleData		tuple;
	RecordConvert	   *my_extra;
	Oid					tupType;
	int32				tupTypmod;
	StringInfoData		buf;

	record = PG_GETARG_HEAPTUPLEHEADER(0);
	tupType = HeapTupleHeaderGetTypeId(record);
	tupTypmod = HeapTupleHeaderGetTypMod(record);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	my_extra = fcinfo->flinfo->fn_extra
			 = set_record_convert_tuple_desc(fcinfo->flinfo->fn_extra,
											 tupdesc,
											 fcinfo->flinfo->fn_mcxt,
											 true);

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(record);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_xc_node_id = 0;
	tuple.t_data = record;

	ExecStoreTuple(&tuple, my_extra->slot_base, InvalidBuffer, false);

	pq_begintypsend(&buf);
	serialize_slot_head_message(&buf, tupdesc);
	if(my_extra->convert)
	{
		do_type_convert_slot_out(my_extra->convert, my_extra->slot_base, my_extra->slot_temp, false);
		serialize_slot_message(&buf, my_extra->slot_temp, CLUSTER_MSG_CONVERT_TUPLE);
	}else
	{
		serialize_slot_message(&buf, my_extra->slot_base, CLUSTER_MSG_TUPLE_DATA);
	}

	ReleaseTupleDesc(tupdesc);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static RecordConvert* set_record_convert_tuple_desc(RecordConvert *rc, TupleDesc desc, MemoryContext context, bool is_send)
{
	if (rc == NULL ||
		!equalTupleDescs(rc->slot_base->tts_tupleDescriptor, desc))
	{
		MemoryContext old_context = MemoryContextSwitchTo(context);
		if(rc == NULL)
		{
			rc = palloc(sizeof(*rc));
			rc->slot_base = MakeSingleTupleTableSlot(desc);
			rc->slot_temp = NULL;
		}else
		{
			if(rc->slot_temp)
			{
				ExecDropSingleTupleTableSlot(rc->slot_temp);
				rc->slot_temp = NULL;
			}
			free_type_convert(rc->convert);
			ExecSetSlotDescriptor(rc->slot_base, desc);
		}

		if(is_send)
			rc->convert = create_type_convert(desc, true, false);
		else
			rc->convert = create_type_convert(desc, false, true);

		if (rc->convert)
		{
			if(rc->slot_temp == NULL)
				rc->slot_temp = MakeSingleTupleTableSlot(rc->convert->out_desc);
			else
				ExecSetSlotDescriptor(rc->slot_temp, rc->convert->out_desc);
		}
		MemoryContextSwitchTo(old_context);
	}
	return rc;
}
