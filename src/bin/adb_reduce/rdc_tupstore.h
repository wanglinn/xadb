#ifndef RDC_TUPSTORE_H
#define RDC_TUPSTORE_H

typedef struct RdcBufFile RdcFile;

/*
 * Possible states of a Tuplestore object.  These denote the states that
 * persist between calls of Tuplestore routines.
 */
typedef enum
{
	TSS_INMEM,					/* Tuples still fit in memory */
	TSS_WRITEFILE,				/* Writing to temp file */
	TSS_READFILE				/* Reading from temp file */
} TupStoreStatus;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.  This way we need not update all the read pointers each time
 * we write.
 */
typedef struct
{
	bool		eof_reached;	/* read has reached EOF */
	int			current;		/* next array index to read */
	int			file;			/* temp file# */
	off_t		offset;			/* byte offset in file */
} ReadPointer;

typedef struct ReduceStorestate
{
	TupStoreStatus status;		/* enumerated value as shown above */
	bool		truncated;		/* tuplestore_trim has removed tuples? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	RdcFile    *myfile;			/* underlying file, or NULL if none */
	MemoryContext context;		/* memory context for holding tuples */

	/*
	 * Function to copy a supplied input tuple into palloc'd space. (NB: we
	 * assume that a single pfree() is enough to release the tuple later, so
	 * the representation must be "flat" in one palloc chunk.) state->availMem
	 * must be decreased by the amount of space used.
	 */
	void	   *(*copytup) (struct ReduceStorestate *state, void *tup, uint32 len);

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  After writing the tuple,
	 * pfree() it, and increase state->availMem by the amount of memory space
	 * thereby released.
	 */
	void		(*writetup) (struct ReduceStorestate *state, void *tup);

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  Create and return a
	 * palloc'd copy, and decrease state->availMem by the amount of memory
	 * space consumed.
	 */
	void	   *(*readtup) (struct ReduceStorestate *state, uint32 len);

	/*
	 * This array holds pointers to tuples in memory if we are in state INMEM.
	 * In states WRITEFILE and READFILE it's not used.
	 *
	 * When memtupdeleted > 0, the first memtupdeleted pointers are already
	 * released due to a tuplestore_trim() operation, but we haven't expended
	 * the effort to slide the remaining pointers down.  These unused pointers
	 * are set to NULL to catch any invalid accesses.  Note that memtupcount
	 * includes the deleted pointers.
	 */
	void	  **memtuples;		/* array of pointers to palloc'd tuples */
	int			memtupdeleted;	/* the first N slots are currently unused */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	ReadPointer *readptr;	/* read pointers */

	int			writepos_file;	/* file# (valid if READFILE state) */
	off_t		writepos_offset;	/* offset (valid if READFILE state) */

	char	   *purpose;		/* RSstate used  purpose */
	int			nodeId;			/* node id */
	int 		fileCounter;    /* file counter */
} RSstate;

extern RSstate *rdcstore_begin(int maxKBytes,char* purpose, int nodeId,
							   pid_t pid, pid_t ppid, pg_time_t time);

extern bool rdcstore_ateof(RSstate *state);

extern void rdcstore_gettuple(RSstate *state ,StringInfo buf, bool *hasData);

extern void rdcstore_puttuple(RSstate *state, char *data, int len);

extern void rdcstore_trim(RSstate *state);

extern bool rdcstore_in_memory(RSstate *state);

extern void rdcstore_end(RSstate *state);

extern void rdcstore_flush(RdcFile *file);

#endif	/* RDC_TUPSTORE_H */

