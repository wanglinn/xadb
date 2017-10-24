
#ifndef TUPTYPECONVERT_H
#define TUPTYPECONVERT_H

#include "access/tupdesc.h"
#include "executor/tuptable.h"

typedef struct TupleTypeConvert
{
	TupleDesc		base_desc;
	TupleDesc		out_desc;
	List		   *io_state;	/* list of ConvertIO */
}TupleTypeConvert;

extern TupleTypeConvert* create_type_convert(TupleDesc base_desc, bool need_out, bool need_in);

extern TupleTableSlot* do_type_convert_slot_in(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest);
extern TupleTableSlot* do_type_convert_slot_out(TupleTypeConvert *convert, TupleTableSlot *src, TupleTableSlot *dest);

extern void free_type_convert(TupleTypeConvert *convert);

#endif /* TUPTYPECONVERT_H */
