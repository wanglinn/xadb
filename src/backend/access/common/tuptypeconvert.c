#include "postgres.h"

#include "access/transam.h"
#include "access/tuptypeconvert.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/lsyscache.h"

typedef struct ConvertIO
{
	FmgrInfo	in_func;
	FmgrInfo	out_func;
	Oid			io_param;
}ConvertIO;

#define type_need_convert(oid) (oid >= FirstNormalObjectId)

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
		attr = base_desc->attrs[i];
		if(type_need_convert(attr->atttypid))
		{
			io = palloc0(sizeof(*io));
			if(need_in)
			{
				getTypeBinaryInputInfo(attr->atttypid, &func, &io->io_param);
				fmgr_info(func, &io->in_func);
			}
			if(need_out)
			{
				bool is_varian;
				getTypeBinaryOutputInfo(attr->atttypid, &func, &is_varian);
				fmgr_info(func, &io->out_func);
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
				/* dest->tts_isnull[i] = src->tts_isnull[i]; */
			}else if(!src->tts_isnull[i])
			{
				bytea *p = DatumGetByteaP(src->tts_values[i]);
				buf.data = VARDATA_ANY(p);
				buf.len = buf.maxlen = VARSIZE_ANY_EXHDR(p);
				buf.cursor = 0;
				dest->tts_values[i] = ReceiveFunctionCall(&io->in_func, &buf, io->io_param, -1);
				/* dest->tts_isnull[i] = false; */
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
			bytea *p = SendFunctionCall(&io->out_func, src->tts_values[i]);
			dest->tts_values[i] = PointerGetDatum(p);
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
		if(indesc->attrs[i]->atttypid >= FirstNormalObjectId)
			break;
	}
	if(i>=indesc->natts)
		return NULL;	/* don't need convert */

	outdesc = CreateTemplateTupleDesc(indesc->natts, false);
	for(i=0;i<indesc->natts;++i)
	{
		attr = indesc->attrs[i];
		Assert(attr->attisdropped == false);

		type = attr->atttypid;
		if(type_need_convert(attr->atttypid))
			type = BYTEAOID;
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
