/*-------------------------------------------------------------------------
 *
 * hashstore.h
 *	  Generalized routines for temporary hash tuple storage.
 *
 * src/include/utils/hashstore.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef HASHSTORE_H
#define HASHSTORE_H

#include "executor/tuptable.h"

#define INVALID_HASHSTORE_READER 0

typedef struct Hashstorestate Hashstorestate;

extern Hashstorestate *hashstore_begin_heap(bool interXact, int nbuckets);

extern void hashstore_put_tupleslot(Hashstorestate *state, TupleTableSlot *slot, uint32 hashvalue);

extern int hashstore_begin_read(Hashstorestate *state, uint32 hashvalue);
extern int hashstore_begin_seq_read(Hashstorestate *state);
extern void hashstore_end_read(Hashstorestate *state, int reader);

extern TupleTableSlot* hashstore_next_slot(Hashstorestate *state, TupleTableSlot *slot, int reader, bool copy);

extern uint32 hashstore_get_reader_hashvalue(Hashstorestate *state, int reader);

extern void hashstore_clear(Hashstorestate *state);

extern void hashstore_end(Hashstorestate *state);

#endif /* HASHSTORE_H */
