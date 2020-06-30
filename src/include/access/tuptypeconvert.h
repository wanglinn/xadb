
#ifndef TUPTYPECONVERT_H
#define TUPTYPECONVERT_H

#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"

#define CONVERT_SAVE_SKIP_CONVERT_HEAD	1
#define CONVERT_SAVE_SKIP_BASE_HEAD		2
#define CONVERT_SAVE_SKIP_RUN_END		4

typedef struct TupleTypeConvert
{
	TupleDesc		base_desc;
	TupleDesc		out_desc;
	List		   *io_state;	/* list of ConvertIO */
}TupleTypeConvert;

typedef TupleTableSlot* (*ConvertGetNextRowFunction)(void *context);
typedef void (*ConvertSaveRowFunction)(void *context, const char *buf, int len);

extern TupleTypeConvert* create_type_convert(TupleDesc base_desc, bool need_out, bool need_in);

extern TupleTableSlot* do_type_convert_slot_in(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy);
extern TupleTableSlot* do_type_convert_slot_out(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy);
extern void do_type_convert_slot_out_ex(TupleDesc base_desc, void *context_next, void *context_save, int flags,
										ConvertGetNextRowFunction next, ConvertSaveRowFunction save);

extern Datum do_datum_convert_in(StringInfo buf, Oid typid);
extern void do_datum_convert_out(StringInfo buf, Oid typid, Datum datum);

extern void free_type_convert(TupleTypeConvert *convert, bool free_out_desc);

#endif /* TUPTYPECONVERT_H */
