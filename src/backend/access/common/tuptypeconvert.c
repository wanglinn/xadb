#include "postgres.h"

#include "access/transam.h"
#include "access/tuptypeconvert.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"

typedef struct ConvertIO
{
	FmgrInfo	in_func;
	FmgrInfo	out_func;
	Oid			io_param;
	bool		bin_type;
}ConvertIO;

static bool type_need_convert(Oid typeoid, bool *binary);
static TupleDesc create_convert_desc_if_need(TupleDesc indesc);

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

TupleTableSlot* do_type_convert_slot_in(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest)
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
			io = lfirst(lc);
			if(io == NULL)
			{
				dest->tts_values[i] = src->tts_values[i];
			}else if(!src->tts_isnull[i])
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

TupleTableSlot* do_type_convert_slot_out(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest)
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
		io = lfirst(lc);
		if(io == NULL)
		{
			dest->tts_values[i] = src->tts_values[i];
			/* dest->tts_isnull[i] = src->tts_isnull[i]; */
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
	char type;

	type = get_typtype(typeoid);
	if (type == TYPTYPE_PSEUDO)
	{
		if(binary)
		{
			if (typeoid == ANYARRAYOID ||
				getBaseType(typeoid) == ANYARRAYOID)
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
	}else if(typeoid < FirstNormalObjectId ||
			 getBaseType(typeoid) < FirstNormalObjectId)
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
			if(lfirst(lc))
				pfree(lfirst(lc));
		}
		list_free(convert->io_state);
		if(convert->out_desc)
			FreeTupleDesc(convert->out_desc);
		pfree(convert);
	}
}
