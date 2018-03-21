
#ifndef TUPTYPECONVERT_H
#define TUPTYPECONVERT_H

#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"

typedef struct TupleTypeConvert
{
	TupleDesc		base_desc;
	TupleDesc		out_desc;
	List		   *io_state;	/* list of ConvertIO */
}TupleTypeConvert;

extern TupleTypeConvert* create_type_convert(TupleDesc base_desc, bool need_out, bool need_in);

extern TupleTableSlot* do_type_convert_slot_in(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy);
extern TupleTableSlot* do_type_convert_slot_out(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest, bool need_copy);

extern Datum do_datum_convert_in(StringInfo buf, Oid typid);
extern void do_datum_convert_out(StringInfo buf, Oid typid, Datum datum);

extern void free_type_convert(TupleTypeConvert *convert);

#endif /* TUPTYPECONVERT_H */
