#include "postgres.h"

#include "access/htup_details.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/tuptypeconvert.h"
#include "catalog/pg_type.h"
#include "executor/clusterReceiver.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/typcache.h"
#include "utils/rangetypes.h"
#include "miscadmin.h"

typedef struct ConvertIO
{
	FmgrInfo	in_func;
	FmgrInfo	out_func;
	Oid			io_param;
	bool		bin_type;
	bool		should_free;	/* when is true, in_func or/and out_func.fn_expr is point to free function, arg is FmgrInfo::fn_extra */
}ConvertIO;

typedef struct RecordConvert
{
	TupleTypeConvert *convert;
	TupleTableSlot *slot_base;
	TupleTableSlot *slot_temp;
	char send_msg_type;
}RecordConvert;

typedef struct ArrayConvert
{
	ConvertIO io;
	Oid last_type;
	int16 base_typlen;
	int16 convert_typlen;
	bool base_typbyval;
	bool convert_typbyval;
	char base_typalign;
	bool need_convert;
}ArrayConvert;

typedef struct RangeConvert
{
	TypeCacheEntry *typcache;
	ConvertIO io;
	Oid last_type;
	int16 base_typlen;
	int16 convert_typlen;
	bool base_typbyval;
	bool convert_typbyval;
	char base_typalign;
	bool need_convert;
}RangeConvert;

typedef void (*clean_function)(void *ptr);

static TupleDesc create_convert_desc_if_need(TupleDesc indesc);
static void free_record_convert(RecordConvert *rc);

static bool setup_convert_io(ConvertIO *io, Oid typid, bool need_out, bool need_in);
static void clean_convert_io(ConvertIO *io);
static bool have_type_convert(const TupleTypeConvert *convert);

static void append_stringinfo_datum(StringInfo buf, Datum datum, int16 typlen, bool typbyval);
static Datum load_stringinfo_datum(StringInfo buf, int16 typlen, bool typbyval);
static Datum load_stringinfo_datum_io(StringInfo buf, ConvertIO *io);

static Datum convert_record_recv(PG_FUNCTION_ARGS);
static Datum convert_record_send(PG_FUNCTION_ARGS);
static RecordConvert* set_record_convert_tuple_desc(RecordConvert *rc, TupleDesc desc, MemoryContext context, bool is_send);

static Datum convert_array_recv(PG_FUNCTION_ARGS);
static Datum convert_array_send(PG_FUNCTION_ARGS);
static ArrayConvert* set_array_convert(ArrayConvert *ac, Oid element_type, MemoryContext context, bool is_send);
static void free_array_convert(ArrayConvert *ac);

static Datum convert_range_send(PG_FUNCTION_ARGS);
static Datum convert_range_recv(PG_FUNCTION_ARGS);
static RangeConvert *get_convert_range_io_data(RangeConvert *ac, Oid rngtypid, MemoryContext context, bool is_send);
static void free_range_convert(RangeConvert *ac);

static Datum convert_ts_config_send(PG_FUNCTION_ARGS);
static Datum convert_ts_config_recv(PG_FUNCTION_ARGS);

static TupleTableSlot* convert_copy_tuple_oid(TupleTableSlot *dest, TupleTableSlot *src, bool use_min);

#ifdef USE_ASSERT_CHECKING
static bool convert_equal_tuple_desc(TupleDesc desc1, TupleDesc desc2);
#endif /* USE_ASSERT_CHECKING */

TupleTypeConvert* create_type_convert(TupleDesc base_desc, bool need_out, bool need_in)
{
	TupleTypeConvert *convert;
	TupleDesc out_desc;
	Form_pg_attribute attr;
	ConvertIO *io,tmp_io;
	int i;
	AttrNumber natts;
	AssertArg(need_out || need_in);

	out_desc = create_convert_desc_if_need(base_desc);
	if(out_desc == NULL)
		return NULL;
	Assert(base_desc->natts >= out_desc->natts);

	convert = palloc(sizeof(*convert));
	convert->base_desc = base_desc;
	PinTupleDesc(base_desc);
	convert->out_desc = out_desc;
	convert->io_state = NIL;

	for(i=natts=0;i<base_desc->natts;++i)
	{
		attr = TupleDescAttr(base_desc, i);

		if (attr->attisdropped == false &&
			setup_convert_io(&tmp_io, attr->atttypid, need_out, need_in))
		{
			io = palloc(sizeof(*io));
			memcpy(io, &tmp_io, sizeof(*io));
		}else
		{
			io = NULL;
		}
		convert->io_state = lappend(convert->io_state, io);
	}
	return convert;
}

static bool have_type_convert(const TupleTypeConvert *convert)
{
	if (convert && convert->io_state != NIL)
	{
		ListCell *lc;
		foreach(lc, convert->io_state)
		{
			if (lfirst(lc) != NULL)
				return true;
		}
	}
	return false;
}

TupleTableSlot* do_type_convert_slot_in(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy)
{
	ListCell *lc;
	ConvertIO *io;
	StringInfoData buf;
	int i,natts;

	AssertArg(list_length(convert->io_state) == src->tts_tupleDescriptor->natts);
	AssertArg(convert_equal_tuple_desc(src->tts_tupleDescriptor, convert->out_desc));
	AssertArg(convert_equal_tuple_desc(dest->tts_tupleDescriptor, convert->base_desc));
	AssertArg(dest->tts_tupleDescriptor->natts == src->tts_tupleDescriptor->natts);

	ExecClearTuple(dest);
	if(TupIsNull(src))
		return dest;
	slot_getallattrs(src);

	push_client_encoding(GetDatabaseEncoding());
	PG_TRY();
	{
		natts = dest->tts_tupleDescriptor->natts;
		for(i=0,lc=list_head(convert->io_state);i<natts;++i)
		{
			if((dest->tts_isnull[i] = src->tts_isnull[i]) == false)
			{
				io = lfirst(lc);
				if(io == NULL)
				{
					Form_pg_attribute attr = TupleDescAttr(src->tts_tupleDescriptor, i);
					Assert(attr->atttypid == TupleDescAttr(dest->tts_tupleDescriptor, i)->atttypid);

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
			lc = lnext(lc);
		}
	}PG_CATCH();
	{
		pop_client_encoding();
		PG_RE_THROW();
	}PG_END_TRY();
	pop_client_encoding();

	ExecStoreVirtualTuple(dest);
	return convert_copy_tuple_oid(dest, src, false);
}

TupleTableSlot* do_type_convert_slot_out(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy)
{
	ListCell *lc;
	ConvertIO *io;
	int i,natts;

	AssertArg(list_length(convert->io_state) == dest->tts_tupleDescriptor->natts);
	AssertArg(convert_equal_tuple_desc(src->tts_tupleDescriptor, convert->base_desc));
	AssertArg(convert_equal_tuple_desc(dest->tts_tupleDescriptor, convert->out_desc));
	AssertArg(dest->tts_tupleDescriptor->natts == src->tts_tupleDescriptor->natts);

	ExecClearTuple(dest);
	if(TupIsNull(src))
		return dest;
	slot_getallattrs(src);

	natts = src->tts_tupleDescriptor->natts;
	for(i=0,lc=list_head(convert->io_state);i<natts;++i)
	{
		if ((dest->tts_isnull[i]=src->tts_isnull[i]) == false)
		{
			io = lfirst(lc);
			if(io == NULL)
			{
				Form_pg_attribute attr = TupleDescAttr(src->tts_tupleDescriptor, i);
				Assert(attr->atttypid == TupleDescAttr(dest->tts_tupleDescriptor, i)->atttypid);

				if (need_copy && !attr->attbyval)
					dest->tts_values[i] = datumCopy(src->tts_values[i], false, attr->attlen);
				else
					dest->tts_values[i] = src->tts_values[i];
			}else
			{
				if(io->bin_type)
				{
					bytea *p = SendFunctionCall(&io->out_func, src->tts_values[i]);
					dest->tts_values[i] = PointerGetDatum(p);
				}else
				{
					char *str = OutputFunctionCall(&io->out_func, src->tts_values[i]);
					dest->tts_values[i] = CStringGetDatum(str);
				}
			}
		}
		lc = lnext(lc);
	}

	ExecStoreVirtualTuple(dest);
	return convert_copy_tuple_oid(dest, src, true);
}

void do_type_convert_slot_out_ex(TupleDesc base_desc, void *context_next, void *context_save, int flags,
								 ConvertGetNextRowFunction next, ConvertSaveRowFunction save)
{
	TupleTypeConvert *convert = create_type_convert(base_desc, true, false);
	TupleTableSlot *slot_convert;
	TupleTableSlot *slot;
	StringInfoData buf;

	initStringInfo(&buf);

	if (convert)
	{
		slot_convert = MakeSingleTupleTableSlot(convert->out_desc);
		if ((flags & CONVERT_SAVE_SKIP_CONVERT_HEAD) == 0)
		{
			serialize_slot_convert_head(&buf, convert->out_desc);
			(*save)(context_save, buf.data, buf.len);
			resetStringInfo(&buf);
		}
	}else
	{
		slot_convert = NULL;
	}

	if ((flags & CONVERT_SAVE_SKIP_BASE_HEAD) == 0)
	{
		serialize_slot_head_message(&buf, base_desc);
		(*save)(context_save, buf.data, buf.len);
		resetStringInfo(&buf);
	}

	for(;;)
	{
		slot = (*next)(context_next);
		if (TupIsNull(slot))
			break;
		
		if (convert)
		{
			do_type_convert_slot_out(convert, slot, slot_convert, false);
			serialize_slot_message(&buf, slot_convert, CLUSTER_MSG_CONVERT_TUPLE);
		}else
		{
			serialize_slot_message(&buf, slot, CLUSTER_MSG_TUPLE_DATA);
		}

		(*save)(context_save, buf.data, buf.len);
		resetStringInfo(&buf);
	}

	if ((flags & CONVERT_SAVE_SKIP_RUN_END) == 0)
	{
		static const char run_end[] = {CLUSTER_MSG_EXECUTOR_RUN_END};
		(*save)(context_save, run_end, lengthof(run_end));
	}

	pfree(buf.data);
	if (convert)
	{
		ExecDropSingleTupleTableSlot(slot_convert);
		free_type_convert(convert);
	}
}

Datum do_datum_convert_in(StringInfo buf, Oid typid)
{
	Datum datum;
	ConvertIO io;

	if (setup_convert_io(&io, typid, false, true))
	{
		push_client_encoding(GetDatabaseEncoding());
		PG_TRY();
		{
			datum  = load_stringinfo_datum_io(buf, &io);
		}PG_CATCH();
		{
			pop_client_encoding();
			PG_RE_THROW();
		}PG_END_TRY();
		pop_client_encoding();
		clean_convert_io(&io);
	}else
	{
		int16 typlen;
		bool byval;
		get_typlenbyval(typid, &typlen, &byval);
		datum = load_stringinfo_datum(buf, typlen, byval);
	}

	return datum;
}

void do_datum_convert_out(StringInfo buf, Oid typid, Datum datum)
{
	Datum new_datum;
	ConvertIO io;
	int16 typlen;
	bool byval;

	if (setup_convert_io(&io, typid, true, false))
	{
		byval = false;
		push_client_encoding(GetDatabaseEncoding());
		PG_TRY();
		{
			if(io.bin_type)
			{
				new_datum = PointerGetDatum(SendFunctionCall(&io.out_func, datum));
				typlen = -1;
			}else
			{
				new_datum = PointerGetDatum(OutputFunctionCall(&io.out_func, datum));
				typlen = -2;
			}
		}PG_CATCH();
		{
			pop_client_encoding();
			PG_RE_THROW();
		}PG_END_TRY();
		pop_client_encoding();
		clean_convert_io(&io);
	}else
	{
		new_datum = datum;
		get_typlenbyval(typid, &typlen, &byval);
	}

	append_stringinfo_datum(buf, new_datum, typlen, byval);
}


static TupleDesc create_convert_desc_if_need(TupleDesc indesc)
{
	TupleDesc outdesc;
	Form_pg_attribute attr;
	ConvertIO io;
	int i;
	Oid type;
	bool need_convert;

	for(i=0,need_convert=false;i<indesc->natts;++i)
	{
		attr = TupleDescAttr(indesc, i);

		if (attr->attisdropped == false &&
			setup_convert_io(NULL, attr->atttypid, false, false))
		{
			need_convert = true;
			break;
		}
	}
	if(need_convert == false)
		return NULL;	/* don't need convert */

	outdesc = CreateTemplateTupleDesc(indesc->natts, indesc->tdhasoid);
	for(i=0;i<indesc->natts;++i)
	{
		attr = TupleDescAttr(indesc, i);
		
		if (attr->attisdropped)
		{
			/* like function RemoveAttributeById */
			attr = TupleDescAttr(outdesc, i);
			attr->attisdropped = true;
			attr->atttypid = InvalidOid;
			attr->attnotnull = false;
			attr->attstattarget = 0;
			attr->atthasmissing = false;
			continue;
		}else if (setup_convert_io(&io, attr->atttypid, true, true))
		{
			type = io.bin_type ? BYTEAOID:UNKNOWNOID;
		}else
		{
			type = attr->atttypid;
		}

		TupleDescInitEntry(outdesc,
						   i+1,
						   NameStr(attr->attname),
						   type,
						   -1,
						   0);
	}
	return outdesc;
}

/* return true if need convert */
static bool setup_convert_io(ConvertIO *io, Oid typid, bool need_out, bool need_in)
{
	Size i;
	Oid func;
	Oid io_param;
	static const struct
	{
		Oid			base_func;		/* base input function object ID */
		Oid			recv_oid;		/* convert recv function OID */
		Oid			send_oid;		/* convert send function OID */
		bool		is_binary;		/* is binary type for send and recv function */
		PGFunction	recv_func;		/* convert recv function */
		PGFunction	send_func;		/* convert send function */
		clean_function clean;	/* clean function if need */
	}func_map[] =
	{
		 {F_ENUM_IN, F_ENUM_IN, F_ENUM_OUT, false, NULL, NULL, NULL}
		,{F_REGCLASSIN, F_REGCLASSIN, F_REGCLASSOUT, false, NULL, NULL, NULL}
		,{F_REGPROCIN, F_REGPROCIN, F_REGPROCOUT, false, NULL, NULL, NULL}
		,{F_ARRAY_IN, InvalidOid, InvalidOid, true, convert_array_recv, convert_array_send, (clean_function)free_array_convert}
		,{F_ANYARRAY_IN, InvalidOid, InvalidOid, true, convert_array_recv, convert_array_send, (clean_function)free_array_convert}
		,{F_RECORD_IN, InvalidOid, InvalidOid, true, convert_record_recv, convert_record_send, (clean_function)free_record_convert}
		,{F_RANGE_IN, InvalidOid, InvalidOid, true, convert_range_recv, convert_range_send, (clean_function)free_range_convert}
		,{F_ANYRANGE_IN, InvalidOid, InvalidOid, true, convert_range_recv, convert_range_send, (clean_function)free_range_convert}
		,{F_REGCONFIGIN, InvalidOid, InvalidOid, true, convert_ts_config_recv, convert_ts_config_send, NULL}
	};

	if (get_typtype(typid) == TYPTYPE_DOMAIN)
		getTypeInputInfo(getBaseType(typid), &func, &io_param);
	else
		getTypeInputInfo(typid, &func, &io_param);
	for(i=0;i<lengthof(func_map);++i)
	{
		if(func_map[i].base_func == func)
		{
			if(io)
			{
				MemSet(io, 0, sizeof(*io));
				io->bin_type = func_map[i].is_binary;
				io->should_free = func_map[i].clean ? true:false;
				io->io_param = io_param;
				if(need_in)
				{
					if(OidIsValid(func_map[i].recv_oid))
					{
						fmgr_info(func_map[i].recv_oid, &io->in_func);
					}else
					{
						io->in_func.fn_addr = func_map[i].recv_func;
						io->in_func.fn_nargs = 3;
						fmgr_info_set_expr((fmNodePtr)func_map[i].clean, &io->in_func);
						io->in_func.fn_mcxt = CurrentMemoryContext;
						io->in_func.fn_strict = true;
					}
				}
				if(need_out)
				{
					if(OidIsValid(func_map[i].send_oid))
					{
						fmgr_info(func_map[i].send_oid, &io->out_func);
					}else
					{
						io->out_func.fn_addr = func_map[i].send_func;
						io->out_func.fn_nargs = 1;
						fmgr_info_set_expr((fmNodePtr)func_map[i].clean, &io->out_func);
						io->out_func.fn_mcxt = CurrentMemoryContext;
						io->out_func.fn_strict = true;
					}
				}
			}
			return true;
		}
	}

	if (get_typtype(typid) != TYPTYPE_PSEUDO &&
		(typid < FirstNormalObjectId ||
		 getBaseType(typid) < FirstNormalObjectId))
	{
		return false;
	}

	if(io)
	{
		MemSet(io, 0, sizeof(*io));
		io->io_param = io_param;
		/* called MemSet 0, don't need this code
		io->bin_type = false;
		io->should_free = false; */
		if(need_in)
			fmgr_info(func, &io->in_func);
		if(need_out)
		{
			bool isvarlena;
			getTypeOutputInfo(typid, &func, &isvarlena);
			fmgr_info(func, &io->out_func);
		}
	}
	return false;
}

static void clean_convert_io(ConvertIO *io)
{
	if(io && io->should_free)
	{
		clean_function func = (clean_function)io->in_func.fn_expr;
		if(func)
			(*func)(io->in_func.fn_extra);
		func = ((clean_function)io->out_func.fn_expr);
		if(func)
			(*func)(io->out_func.fn_extra);
	}
}

static void append_stringinfo_datum(StringInfo buf, Datum datum, int16 typlen, bool typbyval)
{
	if (typbyval)
	{
		Assert(typlen > 0);
		enlargeStringInfo(buf, typlen);
		store_att_byval(buf->data+buf->len, datum, typlen);
		buf->len += typlen;
	}else if(typlen == -1)
	{
		/* varlena */
		struct varlena *p;
		if(VARATT_IS_EXTERNAL(DatumGetPointer(datum)))
			p = heap_tuple_fetch_attr((struct varlena *)DatumGetPointer(datum));
		else
			p = (struct varlena *)DatumGetPointer(datum);
		appendBinaryStringInfo(buf, (char*)p, VARSIZE_ANY(p));
	}else if(typlen == -2)
	{
		/* CString */
		char *str = DatumGetCString(datum);
		appendBinaryStringInfo(buf, str, strlen(str)+1);
	}else
	{
		Assert(typlen > 0);
		appendBinaryStringInfo(buf, DatumGetPointer(datum), typlen);
	}

}

static Datum load_stringinfo_datum(StringInfo buf, int16 typlen, bool typbyval)
{
	Datum datum;
	char *ptr = buf->data + buf->cursor;
	if(typbyval || typlen>0)
	{
		Assert(typlen > 0);
		if (buf->cursor + typlen > buf->len)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("insufficient data left in message")));
		}
		if(typbyval)
		{
			datum = fetch_att(ptr, typbyval, typlen);
		}else
		{
			datum = PointerGetDatum(palloc(typlen));
			memcpy(DatumGetPointer(datum), ptr, typlen);
		}
		buf->cursor += typlen;
	}else if(typlen == -2)
	{
		/* CString */
		int len = strlen(ptr) + 1;
		if(buf->cursor + len > buf->len)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("insufficient data left in message")));
		}
		datum = PointerGetDatum(ptr);
		buf->cursor += len;
	}else
	{
		/* varlna */
		int len = VARSIZE_ANY(ptr);
		Assert(typlen == -1);
		if(buf->cursor + len > buf->len)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("insufficient data left in message")));
		}
		datum = PointerGetDatum(palloc(len));
		memcpy(DatumGetPointer(datum), ptr, len);
		buf->cursor += len;
	}
	return datum;
}

static Datum load_stringinfo_datum_io(StringInfo buf, ConvertIO *io)
{
	Datum datum;
	if(io->bin_type)
	{
		StringInfoData tmp;
		tmp.data = buf->data+buf->cursor;
		tmp.cursor = VARDATA_ANY(tmp.data) - tmp.data;
		tmp.len = tmp.maxlen = VARSIZE_ANY(tmp.data);
		if (tmp.len + buf->cursor > buf->len)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("insufficient data left in message")));

		datum = ReceiveFunctionCall(&io->in_func, &tmp, io->io_param, -1);
		buf->cursor += tmp.maxlen;
	}else
	{
		char *ptr = buf->data+buf->cursor;
		int len = strlen(ptr) + 1;
		if (len + buf->cursor > buf->len)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("insufficient data left in message")));

		datum = InputFunctionCall(&io->in_func, ptr, io->io_param, -1);
		buf->cursor += len;
	}
	return datum;
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
				clean_convert_io(io);
				pfree(io);
			}
		}
		list_free(convert->io_state);
		if(convert->out_desc)
			FreeTupleDesc(convert->out_desc);
		ReleaseTupleDesc(convert->base_desc);
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
	}else
	{
		restore_slot_message(buf->data + buf->cursor, buf->len - buf->cursor, my_extra->slot_base);
	}

	PG_RETURN_DATUM(ExecFetchSlotTupleDatum(my_extra->slot_base));
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
		serialize_slot_message(&buf, my_extra->slot_temp, my_extra->send_msg_type);
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
			if(is_send)
				rc->send_msg_type = have_type_convert(rc->convert) ? CLUSTER_MSG_CONVERT_TUPLE : CLUSTER_MSG_TUPLE_DATA;
		}
		MemoryContextSwitchTo(old_context);
	}
	return rc;
}

/* like array_recv, but we convert array item if need */
static Datum convert_array_recv(PG_FUNCTION_ARGS)
{
	ArrayConvert *ac;
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	ArrayType  *retval;
	Datum	   *dataPtr;
	bool	   *nullsPtr;
	Oid			element_type = PG_GETARG_OID(1);		/* type of an array
														 * element */
	/*int32		typmod = PG_GETARG_INT32(2);*/			/* typmod for array elements */
	int			i,
				nitems;
	int			ndim,
				has_null,
				dim[MAXDIM],
				lBound[MAXDIM];
	int32		nbytes;
	int32		dataoffset;

	/* Get the array header information */
	ndim = pq_getmsgint(buf, 4);
	if (ndim < 0)				/* we do allow zero-dimension arrays */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("invalid number of dimensions: %d", ndim)));
	if (ndim > MAXDIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
						ndim, MAXDIM)));

	element_type = load_oid_type(buf);

	for (i = 0; i < ndim; i++)
	{
		dim[i] = pq_getmsgint(buf, 4);
		lBound[i] = pq_getmsgint(buf, 4);

		/*
		 * Check overflow of upper bound. (ArrayNItems() below checks that
		 * dim[i] >= 0)
		 */
		if (dim[i] != 0)
		{
			int			ub = lBound[i] + dim[i] - 1;

			if (lBound[i] > ub)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("integer out of range")));
		}
	}

	/* This checks for overflow of array dimensions */
	nitems = ArrayGetNItems(ndim, dim);

	ac = fcinfo->flinfo->fn_extra
		= set_array_convert(fcinfo->flinfo->fn_extra, element_type, fcinfo->flinfo->fn_mcxt, false);

	if (nitems == 0)
	{
		/* Return empty array ... but not till we've validated element_type */
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(element_type));
	}


	dataPtr = palloc0(nitems * sizeof(Datum));
	nullsPtr = palloc(nitems * sizeof(bool));
	has_null = false;
	nbytes = 0;
	for(i=0;i<nitems;++i)
	{
		bool is_null = pq_getmsgbyte(buf);
		if (is_null)
		{
			nullsPtr[i] = has_null = true;
		}else
		{
			nullsPtr[i] = false;
			if(ac->need_convert)
				dataPtr[i] = load_stringinfo_datum_io(buf, &ac->io);
			else
				dataPtr[i] = load_stringinfo_datum(buf, ac->base_typlen, ac->base_typbyval);
			nbytes = att_addlength_datum(nbytes, ac->base_typlen, dataPtr[i]);
			nbytes = att_align_nominal(nbytes, ac->base_typalign);
		}
	}

	if (has_null)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(ndim, nitems);
		nbytes += dataoffset;
	}else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nbytes += ARR_OVERHEAD_NONULLS(ndim);
	}
	retval = (ArrayType *) palloc0(nbytes);
	SET_VARSIZE(retval, nbytes);
	retval->ndim = ndim;
	retval->dataoffset = dataoffset;
	retval->elemtype = element_type;
	memcpy(ARR_DIMS(retval), dim, ndim * sizeof(int));
	memcpy(ARR_LBOUND(retval), lBound, ndim * sizeof(int));

	CopyArrayEls(retval,
				 dataPtr, nullsPtr, nitems,
				 ac->base_typlen, ac->base_typbyval, ac->base_typalign,
				 ac->need_convert);

	pfree(dataPtr);
	pfree(nullsPtr);

	PG_RETURN_ARRAYTYPE_P(retval);
}

/* like array_send, but we convert array item if need */
static Datum convert_array_send(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	ArrayConvert *ac;
	AnyArrayType *v = PG_GETARG_ANY_ARRAY_P(0);
	Oid			element_type = AARR_ELEMTYPE(v);
	array_iter	iter;
	int			nitems,
				i;
	int			ndim,
			   *dim,
			   *lb;

	ac = fcinfo->flinfo->fn_extra
		= set_array_convert(fcinfo->flinfo->fn_extra, element_type, fcinfo->flinfo->fn_mcxt, true);

	ndim = AARR_NDIM(v);
	dim = AARR_DIMS(v);
	lb = AARR_LBOUND(v);
	nitems = ArrayGetNItems(ndim, dim);

	pq_begintypsend(&buf);

	/* Send the array header information */
	pq_sendint(&buf, ndim, 4);
	save_oid_type(&buf, element_type);
	for (i = 0; i < ndim; i++)
	{
		pq_sendint(&buf, dim[i], 4);
		pq_sendint(&buf, lb[i], 4);
	}

	/* Send the array elements using the element's own sendproc */
	array_iter_setup(&iter, v);

	for (i = 0; i < nitems; i++)
	{
		Datum		itemvalue;
		Datum		new_val;
		bool		isnull;

		/* Get source element, checking for NULL */
		itemvalue = array_iter_next(&iter, &isnull, i,
									ac->base_typlen, ac->base_typbyval, ac->base_typalign);

		if (isnull)
		{
			appendStringInfoCharMacro(&buf, (char)true);
		}else
		{
			appendStringInfoCharMacro(&buf, (char)false);
			if(ac->need_convert)
			{
				if(ac->io.bin_type)
					new_val = PointerGetDatum(SendFunctionCall(&ac->io.out_func, itemvalue));
				else
					new_val = PointerGetDatum(OutputFunctionCall(&ac->io.out_func, itemvalue));
			}else
			{
				new_val = itemvalue;
			}
			append_stringinfo_datum(&buf, new_val, ac->convert_typlen, ac->convert_typbyval);
			if(new_val != itemvalue)
				pfree(DatumGetPointer(new_val));
		}
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static ArrayConvert* set_array_convert(ArrayConvert *ac, Oid element_type, MemoryContext context, bool is_send)
{
	if(ac == NULL)
	{
		ac = MemoryContextAllocZero(context, sizeof(*ac));
		ac->last_type = ~element_type;
	}

	if(ac->last_type != element_type)
	{
		MemoryContext old_context;

		clean_convert_io(&ac->io);
		MemSet(ac, 0, sizeof(*ac));
		ac->last_type = element_type;
		get_typlenbyvalalign(element_type, &ac->base_typlen, &ac->base_typbyval, &ac->base_typalign);

		old_context = MemoryContextSwitchTo(context);
		if (is_send)
			ac->need_convert = setup_convert_io(&ac->io, element_type, true, false);
		else
			ac->need_convert = setup_convert_io(&ac->io, element_type, false, true);
		if(ac->need_convert)
		{
			ac->convert_typbyval = false;
			ac->convert_typlen = ac->io.bin_type ? -1:-2;
		}else
		{
			ac->convert_typbyval = ac->base_typbyval;
			ac->convert_typlen = ac->base_typlen;
		}
		MemoryContextSwitchTo(old_context);
	}

	return ac;
}

static void free_array_convert(ArrayConvert *ac)
{
	if(ac)
	{
		clean_convert_io(&ac->io);
		pfree(ac);
	}
}


/* like range_send, but we convert range item if need */
static Datum convert_range_send(PG_FUNCTION_ARGS)
{
	RangeType  *range = PG_GETARG_RANGE_P(0);
	StringInfo	buf = makeStringInfo();
	RangeBound	lower;
	RangeBound	upper;
	RangeConvert	*ac;
	Oid rngtypoid;
	bool		empty;
	char		flags;

	check_stack_depth();		/* recurses when subtype is a range type */
	rngtypoid = RangeTypeGetOid(range);
	ac = get_convert_range_io_data(fcinfo->flinfo->fn_extra, rngtypoid, fcinfo->flinfo->fn_mcxt, true);

	/* deserialize */
	range_deserialize(ac->typcache, range, &lower, &upper, &empty);
	flags = range_get_flags(range);
	/* construct output */
	pq_begintypsend(buf);
	pq_sendbyte(buf, flags);
	save_oid_type(buf, rngtypoid);

	if (RANGE_HAS_LBOUND(flags))
	{
		Datum bound;

		if(ac->need_convert)
		{
			if(ac->io.bin_type)
				bound = PointerGetDatum(SendFunctionCall(&ac->io.out_func, lower.val));
			else
				bound = PointerGetDatum(OutputFunctionCall(&ac->io.out_func, lower.val));
			append_stringinfo_datum(buf, bound, ac->convert_typlen, ac->convert_typbyval);
			pfree(DatumGetPointer(bound));
		}else
			append_stringinfo_datum(buf, lower.val, ac->base_typlen, ac->base_typbyval);
	}

	if (RANGE_HAS_UBOUND(flags))
	{
		Datum bound;

		if(ac->need_convert)
		{
			if(ac->io.bin_type)
				bound = PointerGetDatum(SendFunctionCall(&ac->io.out_func, upper.val));
			else
				bound = PointerGetDatum(OutputFunctionCall(&ac->io.out_func, upper.val));
			append_stringinfo_datum(buf, bound, ac->convert_typlen, ac->convert_typbyval);
			pfree(DatumGetPointer(bound));
		}else
			append_stringinfo_datum(buf, upper.val, ac->base_typlen, ac->base_typbyval);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(buf));
}

/* like range_recv, but we convert range item if need */
Datum
convert_range_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	RangeType	*range;
	RangeBound	lower;
	RangeBound	upper;
	RangeConvert	*ac;
	Oid		rngtypoid;
	char		flags;

	check_stack_depth();		/* recurses when subtype is a range type */

	/* receive the flags... */
	flags = (unsigned char) pq_getmsgbyte(buf);
	rngtypoid = load_oid_type(buf);
	if (PG_GETARG_OID(1) != ANYRANGEOID && PG_GETARG_OID(1) != rngtypoid)
		elog(ERROR, "input paramete of type %s is different from the type %s which from receive data message", format_type_be(PG_GETARG_OID(1)), format_type_be(rngtypoid));

	ac = get_convert_range_io_data(fcinfo->flinfo->fn_extra, rngtypoid, fcinfo->flinfo->fn_mcxt, false);

	/*
	 * Mask out any unsupported flags, particularly RANGE_xB_NULL which would
	 * confuse following tests.  Note that range_serialize will take care of
	 * cleaning up any inconsistencies in the remaining flags.
	 */
	flags &= (RANGE_EMPTY |
			  RANGE_LB_INC |
			  RANGE_LB_INF |
			  RANGE_UB_INC |
			  RANGE_UB_INF);

	/* receive the bounds ... */
	if (RANGE_HAS_LBOUND(flags))
	{
		if(ac->need_convert)
		{
			lower.val = load_stringinfo_datum_io(buf, &ac->io);
		}
		else
			lower.val = load_stringinfo_datum(buf, ac->base_typlen, ac->base_typbyval);

	}
	else
		lower.val = (Datum) 0;

	if (RANGE_HAS_UBOUND(flags))
	{
		if(ac->need_convert)
		{
			upper.val = load_stringinfo_datum_io(buf, &ac->io);
		}
		else
			upper.val = load_stringinfo_datum(buf, ac->base_typlen, ac->base_typbyval);
	}
	else
		upper.val = (Datum) 0;

	pq_getmsgend(buf);

	/* finish constructing RangeBound representation */
	lower.infinite = (flags & RANGE_LB_INF) != 0;
	lower.inclusive = (flags & RANGE_LB_INC) != 0;
	lower.lower = true;
	upper.infinite = (flags & RANGE_UB_INF) != 0;
	upper.inclusive = (flags & RANGE_UB_INC) != 0;
	upper.lower = false;

	/* serialize and canonicalize */
	range = make_range(ac->typcache, &lower, &upper, flags & RANGE_EMPTY);

	PG_RETURN_RANGE_P(range);
}

/*
 * get_convert_range_io_data: get range convert information needed for range type I/O
 *
 */
static RangeConvert *
get_convert_range_io_data(RangeConvert *ac, Oid rngtypid, MemoryContext context, bool is_send)
{

	if(ac == NULL)
	{
		ac = MemoryContextAllocZero(context, sizeof(*ac));
		ac->last_type = ~rngtypid;
	}

	if (ac->last_type != rngtypid)
	{
		Oid rngelemtypeoid;
		MemoryContext old_context;

		clean_convert_io(&ac->io);
		MemSet(ac, 0, sizeof(*ac));
		ac->typcache = lookup_type_cache(rngtypid, TYPECACHE_RANGE_INFO);
		if (ac->typcache->rngelemtype == NULL)
			elog(ERROR, "type %s is not a range type", format_type_be(rngtypid));
		rngelemtypeoid = ac->typcache->rngelemtype->type_id;

		old_context = MemoryContextSwitchTo(context);

		if (is_send)
			ac->need_convert = setup_convert_io(&ac->io, rngelemtypeoid, true, false);
		else
			ac->need_convert = setup_convert_io(&ac->io, rngelemtypeoid, false, true);

		if(ac->need_convert)
		{
			ac->convert_typbyval = false;
			ac->convert_typlen = ac->io.bin_type ? -1:-2;
		}else
		{
			get_typlenbyvalalign(rngelemtypeoid, &ac->base_typlen, &ac->base_typbyval, &ac->base_typalign);
		}

		MemoryContextSwitchTo(old_context);
	}

	return ac;
}

static void free_range_convert(RangeConvert *ac)
{
	if(ac)
	{
		clean_convert_io(&ac->io);
		pfree(ac);
	}
}

static Datum convert_ts_config_send(PG_FUNCTION_ARGS)
{
	StringInfoData	buf;

	pq_begintypsend(&buf);
	save_oid_ts_config(&buf, PG_GETARG_OID(0));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static Datum convert_ts_config_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	Oid oid = load_oid_ts_config(buf);

	PG_RETURN_OID(oid);
}


#ifdef USE_ASSERT_CHECKING
static bool convert_equal_tuple_desc(TupleDesc desc1, TupleDesc desc2)
{
	int i;
	if (desc1 == desc2)
		return true;

	if (desc1->natts != desc2->natts)
		return false;
	if (desc1->tdhasoid != desc2->tdhasoid)
		return false;

	for (i=0;i<desc1->natts;++i)
	{
		Form_pg_attribute attr1 = TupleDescAttr(desc1,i);
		Form_pg_attribute attr2 = TupleDescAttr(desc2,i);

		if (attr1->attisdropped != attr2->attisdropped)
			return false;
		if (attr1->atttypid != attr2->atttypid)
			return false;
		if (attr1->attlen != attr2->attlen)
			return false;
	}

	return true;
}
#endif /* USE_ASSERT_CHECKING */

static TupleTableSlot* convert_copy_tuple_oid(TupleTableSlot *dest, TupleTableSlot *src, bool use_min)
{
	if(src->tts_tupleDescriptor->tdhasoid)
	{
		HeapTupleHeader header;
		Oid oid;
		Assert(dest->tts_tupleDescriptor->tdhasoid);
		oid = ExecFetchSlotTupleOid(src);
		if (OidIsValid(oid))
		{
			if (use_min)
				header = (HeapTupleHeader) ((char*)ExecFetchSlotMinimalTuple(dest) - MINIMAL_TUPLE_OFFSET);
			else
				header = ExecMaterializeSlot(dest)->t_data;
			HeapTupleHeaderSetOid(header, oid);
		}
	}

	return dest;
}
