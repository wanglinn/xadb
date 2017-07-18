#include "rdc_globals.h"
#include "rdc_tupstore.h"
#include "pg_config_manual.h"
#include "storage/buffile.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#if 0
#define BLCKSZ 12
#define MAX_PHYSICAL_FILESIZE 12
#endif
/* file size */
#define MAX_PHYSICAL_FILESIZE	0x40000000

#define	FILE_CLOSE				-1
#define FILE_REMOVE				-2
#define	FILE_UNINIT				-3

#define COPYTUP(state,tup,len)	((*(state)->copytup) (state, tup, len))
#define WRITETUP(state,tup) 	((*(state)->writetup) (state, tup))
#define READTUP(state,len)		((*(state)->readtup) (state, len))
#define USEMEM(state,amt)		((state)->availMem -= (amt))
#define FREEMEM(state,amt)		((state)->availMem += (amt))
#define LACKMEM(state)			((state)->availMem < 0)

typedef struct ReduceStoreData
{
	uint32 len;
	char  *data;
} RSdata;

struct RdcBufFile
{
	int			numFiles;		/* number of physical files in set */
	/* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
	File	   *files;			/* palloc'd array with numFiles entries */
	char	   **filelnames;	/* create file name */
	off_t	   *offsets;		/* palloc'd array with numFiles entries */

	/*
	 * offsets[i] is the current seek position of files[i].  We use this to
	 * avoid making redundant FileSeek calls.
	 */
	bool		isTemp;			/* can only add files if this is TRUE */
	bool		dirty;			/* does buffer need to be written? */

	/*
	 * "current pos" is position of start of buffer within the logical file.
	 * Position as seen by user of BufFile is (curFile, curOffset + pos).
	 */
	int			curFile;		/* file index (0..n) part of current pos */
	off_t		curOffset;		/* offset part of current pos */
	int			pos;			/* next read/write position in buffer */
	int			nbytes;			/* total # of valid bytes in buffer */
	char		buffer[BLCKSZ];
};

/* used for extern API */
static RSstate *rdcstore_begin_common(int maxKBytes, char* purpose, int nodeId,
									  pid_t pid, pid_t ppid, pg_time_t time);
static void *rdcstore_gettuple_common(RSstate *state);
static void rdcstore_puttuple_common(RSstate *state, void *tuple);

/* tool function */
static bool rdcGrowMemtuples(RSstate *state);
static void rdcDumpTuples(RSstate *state);
static void rdcCreateDir(char *path);
static RSdata *rdcCopyData(void *tuple, uint32 len);
static uint32 rdcGetTupleLen(RSstate *state);
static void rdcCkeckReadFile(RSstate *state);

/* execute reduce store tuple */
static void *rdcCopyTuple(RSstate *state, void *tup, uint32 len);
static void *rdcReadTuple(RSstate *state, uint32 len);
static void rdcWriteTuple(RSstate *state, void *tup);

/* write or read temp file */
static RdcFile *rdcBufFileCreateTemp(void);
static RdcFile *makeRdcBufFile(File firstfile);
static void rdcBufFileClose(RdcFile *file);
static void rdcRemoveReadCompleteFile(RdcFile *file, int pos);
static void rdcBufFileDumpBuffer(RdcFile *file);
static void rdcExtendBufFile(RdcFile *file);
static void rdcBufFileLoadBuffer(RdcFile *file);
static void rdcBufFileTell(RdcFile *file, int *fileno,
						off_t *offset);
static int rdcBufFileFlush(RdcFile *file);
static int rdcFileWrite(File file, char *buffer,
						int amount);
static int rdcBufFileSeek(RdcFile *file, int fileno,
						off_t offset, int whence);
static int rdcFileRead(File file, char *buffer,
						int amount);
static size_t rdcBufFileWrite(RdcFile *file, void *ptr,
						size_t size);
static size_t rdcBufFileRead(RdcFile *file, void *ptr,
						size_t size);
static File rdcOpenTempFile(char ** filename);

/* RSstate temp file info */
static int  *rdcFileCounter = NULL;
static char *rdcPurpose = NULL;
static int   rdcNodeId = 0;

/* set RSstate temp file info */
#define PUT_FILE_INFO(state) \
{	\
	rdcFileCounter = &(state->fileCounter);	\
	rdcPurpose = state->purpose;	\
	rdcNodeId =  state->nodeId;	\
}	\

/* clean RSstate temp file info */
#define RESET_FILE_INFO() \
{	\
	rdcFileCounter = NULL;	\
	rdcPurpose = NULL;	\
	rdcNodeId = 0;	\
}	\

#define RDC_GET_DATA_MEM(rsData) GetMemoryChunkSpace(rsData)

/*
 *
 * Initialize for a reduce store operation.
 */
static RSstate *
rdcstore_begin_common(int maxKBytes, char *purpose, int nodeId,
					  pid_t pid, pid_t ppid, pg_time_t time)
{
	RSstate *state;

	state = (RSstate *) palloc0(sizeof(RSstate));
	state->status = TSS_INMEM;
	state->allowedMem = maxKBytes * 1024L;
	state->availMem = state->allowedMem;
	state->myfile = NULL;
	state->context = CurrentMemoryContext;

	state->memtupdeleted = 0;
	state->memtupcount = 0;
	/*
	 * Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD;
	 * see comments in rdcGrowMemtuples().
	 */
	state->memtupsize = Max(16384 / sizeof(void *),
							ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);

	state->growmemtuples = true;
	state->memtuples = (void **) palloc(state->memtupsize * sizeof(void *));
	USEMEM(state, GetMemoryChunkSpace(state->memtuples));

	state->readptr = (ReadPointer *) palloc0(sizeof(ReadPointer));
	state->readptr->file = FILE_UNINIT;
	USEMEM(state, GetMemoryChunkSpace(state->readptr));

	state->nodeId = nodeId;
	state->purpose = psprintf("%X.%X.%X.%s",
							  (uint32) pid,
							  (uint32) ppid,
							  (uint32) time,
							  purpose);
	USEMEM(state, GetMemoryChunkSpace(state->purpose));

	return state;
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  If should_free is set, the
 * caller must pfree the returned tuple when done with it.
 *
 * Backward scan is only allowed if randomAccess was set true or
 * EXEC_FLAG_BACKWARD was specified to reduce store_set_eflags().
 */
static void *
rdcstore_gettuple_common(RSstate *state)
{
	ReadPointer *readptr = state->readptr;

	/* Skip state change if we'll just return NULL */
	if (readptr->eof_reached)
		return NULL;

	switch (state->status)
	{
		case TSS_INMEM:
			if (readptr->current < state->memtupcount)
			{
				/* We have another tuple, so return it */
				return state->memtuples[readptr->current++];
			}
			readptr->eof_reached = true;
			return NULL;

			break;
		case TSS_WRITEFILE:
			 /*
			 * Switch from writing to reading.
			 */
			rdcBufFileTell(state->myfile,
						&state->writepos_file, &state->writepos_offset);

			if (rdcBufFileSeek(state->myfile,
							readptr->file, readptr->offset,
							SEEK_SET) != 0)
				elog(ERROR, "rdcstore seek failed");
			state->status = TSS_READFILE;
			/* FALL THRU into READFILE case */

		case TSS_READFILE:
			{
				uint32 len;
				void  *rsData = NULL;

				if ((len = rdcGetTupleLen(state)) != 0)
				{
					rsData = READTUP(state, len);
					return rsData;
				}
				else
				{
					readptr->eof_reached = true;
					/* now file content has been read,record file offset */
					rdcBufFileTell(state->myfile,
								&state->readptr->file,
								&state->readptr->offset);
					return NULL;
				}
			}
			break;
		default:
			elog(ERROR, "invalid reduce store state");
			return NULL;		/* keep compiler quiet */
	}
	return NULL;
}

static void
rdcstore_puttuple_common(RSstate *state, void *tuple)
{
	ReadPointer *readptr = state->readptr;

	switch (state->status)
	{
		case TSS_INMEM:
			/* if TSS_INMEM has read all data, set eof_reached before put data */
			if(readptr->eof_reached)
				readptr->eof_reached = false;

			/*
			 * Grow the array as needed.  Note that we try to grow the array
			 * when there is still one free slot remaining --- if we fail,
			 * there'll still be room to store the incoming tuple, and then
			 * we'll switch to tape-based operation.
			 */
			if (state->memtupcount >= state->memtupsize - 1)
			{
				(void)rdcGrowMemtuples(state);
				/* enlarge state memtuple */
				Assert(state->memtupcount < state->memtupsize);
			}
			/* Stash the tuple in the in-memory array */
			state->memtuples[state->memtupcount++] = tuple;

			/*
			 * Done if we still fit in available memory and have array slots.
			 */
			if (state->memtupcount < state->memtupsize && !LACKMEM(state))
				return;

			/* create temp file */
			 state->myfile = rdcBufFileCreateTemp();

			/*
			 * Freeze the decision about whether trailing length words will be
			 * used.  We can't change this choice once data is on tape, even
			 * though callers might drop the requirement.
			 */
			state->status = TSS_WRITEFILE;
			rdcDumpTuples(state);
			break;

		case TSS_WRITEFILE:
			/* last operation must be write file,  eof_reached must be flase */
			Assert(!readptr->eof_reached);
			WRITETUP(state, tuple);
			break;

		 case TSS_READFILE:
		 	/*
			 * Switch from reading to writing.
			 */
			if (!readptr->eof_reached)  /* whether read eof */
				rdcBufFileTell(state->myfile, &readptr->file,
							&readptr->offset);

			 /* when switch from reading to writing, check read completely file */
			rdcCkeckReadFile(state);

			if (rdcBufFileSeek(state->myfile,
							state->writepos_file, state->writepos_offset,
							SEEK_SET) != 0)
				elog(ERROR, "reduce store seek to EOF failed");
			state->status = TSS_WRITEFILE;

			WRITETUP(state, tuple);

			/* put new data in buffer, if eof_reached is true, set state */
			if (readptr->eof_reached)
				readptr->eof_reached = false;
		 	break;

		default:
			elog(ERROR, "invalid reduce store state");
			break;
	}
}

/*
 * Grow the memtuples[] array, if possible within our memory constraint.
 * Return TRUE if we were able to enlarge the array, FALSE if not.
 *
 * Normally, at each increment we double the size of the array.  When we no
 * longer have enough memory to do that, we attempt one last, smaller increase
 * (and then clear the growmemtuples flag so we don't try any more).  That
 * allows us to use allowedMem as fully as possible; sticking to the pure
 * doubling rule could result in almost half of allowedMem going unused.
 * Because availMem moves around with tuple addition/removal, we need some
 * rule to prevent making repeated small increases in memtupsize, which would
 * just be useless thrashing.  The growmemtuples flag accomplishes that and
 * also prevents useless recalculations in this function.
 */
static bool
rdcGrowMemtuples(RSstate *state)
{
	int			newmemtupsize;
	int			memtupsize = state->memtupsize;
	long		memNowUsed = state->allowedMem - state->availMem;

	/* Forget it if we've already maxed out memtuples, per comment above */
	if (!state->growmemtuples)
		return false;

	/* Select new value of memtupsize */
	if (memNowUsed <= state->availMem)
	{
		/*
		 * It is surely safe to double memtupsize if we've used no more than
		 * half of allowedMem.
		 *
		 * Note: it might seem that we need to worry about memtupsize * 2
		 * overflowing an int, but the MaxAllocSize clamp applied below
		 * ensures the existing memtupsize can't be large enough for that.
		 */
		newmemtupsize = memtupsize * 2;
	}
	else
	{
		/*
		 * This will be the last increment of memtupsize.  Abandon doubling
		 * strategy and instead increase as much as we safely can.
		 *
		 * To stay within allowedMem, we can't increase memtupsize by more
		 * than availMem / sizeof(void *) elements. In practice, we want to
		 * increase it by considerably less, because we need to leave some
		 * space for the tuples to which the new array slots will refer.  We
		 * assume the new tuples will be about the same size as the tuples
		 * we've already seen, and thus we can extrapolate from the space
		 * consumption so far to estimate an appropriate new size for the
		 * memtuples array.  The optimal value might be higher or lower than
		 * this estimate, but it's hard to know that in advance.
		 *
		 * This calculation is safe against enlarging the array so much that
		 * LACKMEM becomes true, because the memory currently used includes
		 * the present array; thus, there would be enough allowedMem for the
		 * new array elements even if no other memory were currently used.
		 *
		 * We do the arithmetic in float8, because otherwise the product of
		 * memtupsize and allowedMem could overflow.  (A little algebra shows
		 * that grow_ratio must be less than 2 here, so we are not risking
		 * integer overflow this way.)	Any inaccuracy in the result should be
		 * insignificant; but even if we computed a completely insane result,
		 * the checks below will prevent anything really bad from happening.
		 */
		double		grow_ratio;

		grow_ratio = (double) state->allowedMem / (double) memNowUsed;
		newmemtupsize = (int) (memtupsize * grow_ratio);

		/* We won't make any further enlargement attempts */
		state->growmemtuples = false;
	}

	/* Must enlarge array by at least one element, else report failure */
	if (newmemtupsize <= memtupsize)
		goto noalloc;

	/*
	 * On a 64-bit machine, allowedMem could be more than MaxAllocSize.  Clamp
	 * to ensure our request won't be rejected by palloc.
	 */
	if ((Size) newmemtupsize >= MaxAllocSize / sizeof(void *))
	{
		newmemtupsize = (int) (MaxAllocSize / sizeof(void *));
		state->growmemtuples = false;	/* can't grow any more */
	}

	/*
	 * We need to be sure that we do not cause LACKMEM to become true, else
	 * the space management algorithm will go nuts.  The code above should
	 * never generate a dangerous request, but to be safe, check explicitly
	 * that the array growth fits within availMem.  (We could still cause
	 * LACKMEM if the memory chunk overhead associated with the memtuples
	 * array were to increase.  That shouldn't happen because we chose the
	 * initial array size large enough to ensure that palloc will be treating
	 * both old and new arrays as separate chunks.  But we'll check LACKMEM
	 * explicitly below just in case.)
	 */
	if (state->availMem < (long) ((newmemtupsize - memtupsize) * sizeof(void *)))
		goto noalloc;

	/* OK, do it */
	FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
	state->memtupsize = newmemtupsize;
	state->memtuples = (void **)
		repalloc(state->memtuples,
				 state->memtupsize * sizeof(void *));
	USEMEM(state, GetMemoryChunkSpace(state->memtuples));
	if (LACKMEM(state))
		elog(ERROR, "unexpected out-of-memory situation in reduce store");
	return true;

noalloc:
	/* If for any reason we didn't realloc, shut off future attempts */
	state->growmemtuples = false;
	return false;
}

/*
 * rdcDumpTuples - remove tuples from memory and write to tape
 */
static void
rdcDumpTuples(RSstate *state)
{
	ReadPointer *readptr = state->readptr;
	int			i;

	for (i = readptr->current;; i++)
	{
		if (i == readptr->current && !readptr->eof_reached)
			rdcBufFileTell(state->myfile,
						&readptr->file, &readptr->offset);

		if (i >= state->memtupcount)
			break;
		WRITETUP(state, state->memtuples[i]);
	}
	state->memtupdeleted = 0;
	state->memtupcount = 0;
}

static RSdata *
rdcCopyData(void *tuple, uint32 len)
{
	RSdata 	   *rsData;

	rsData = (RSdata*)palloc0(sizeof(RSdata) + len);
	rsData->data = (char*)rsData + sizeof(RSdata);
	rsData->len = len;
	memcpy(rsData->data, tuple, len);
	return rsData;
}

/*
 * rdcCopyTuple
 * copy len from tup
 * RETURN RSdata*
 */
static void *
rdcCopyTuple(RSstate *state, void *tup, uint32 len)
{
	RSdata		*rsData = NULL;

	rsData = rdcCopyData(tup, len);

	/* record memory used */
	USEMEM(state, RDC_GET_DATA_MEM(rsData));

	return (void*) rsData;
}

/*
 * rdcWriteTuple write RSstate to file buffer.
 * when buffer full, flush to disk
 */
static void
rdcWriteTuple(RSstate *state, void *tup)
{
	RSdata *rdData = (RSdata*) tup;

	/* record data len */
	if (rdcBufFileWrite(state->myfile, (void *) &(rdData->len),
					 			sizeof(rdData->len)) != sizeof(rdData->len))
			elog(ERROR, "write tuple size failed");

	/* record data */
	if (rdcBufFileWrite(state->myfile, (void *) (rdData->data),
					 			rdData->len) != (size_t) rdData->len)
			elog(ERROR, "write tuple data failed");

	FREEMEM(state, RDC_GET_DATA_MEM(rdData));

	/* free tuple memory */
	pfree(rdData);
}

/*
 * rdcReadTuple
 * read len from state myfile buffer. if buffer is empty or has part data
 * cache data in buffer from temp file first, then read data from buffer.
 * RETURN RSdata *
 */
static void *
rdcReadTuple(RSstate *state, uint32 len)
{
	RSdata *rsData = (RSdata*)palloc0(sizeof(RSdata) + len);
	rsData->len = len;
	rsData->data = (char*)rsData + sizeof(RSdata);

	USEMEM(state, RDC_GET_DATA_MEM(rsData));

	if (rdcBufFileRead(state->myfile, (void *)rsData->data,
				len) != (size_t)len)
		elog(ERROR, "unexpected end of data");

	return rsData;
}

/*
 * rdcCreateDir  create dir in path
 */
static void
rdcCreateDir(char *path)
{
	int i, len;
	char * dir_path = NULL;

	Assert(NULL != path);

	len = strlen(path);
	dir_path = (char*)palloc0(len + 1);
	dir_path[len] = '\0';

	strncpy(dir_path, path, len);

	for(i = 0; i < len; i++)
	{
		if(dir_path[i] == '/' && i > 0)
		{
			dir_path[i] = '\0';
			if (access(dir_path, F_OK) < 0)
			{
				if (mkdir(dir_path, (S_IRUSR | S_IWUSR | S_IXUSR)) < 0)
					elog(ERROR, "create dir error, mkdir=%s:msg=%s",
							dir_path, strerror(errno));
			}
			dir_path[i] = '/';
		}
	}
	if (i == len)
	{
		if (access(dir_path, F_OK) < 0)
		{
			if (mkdir(dir_path, (S_IRUSR | S_IWUSR | S_IXUSR)) < 0)
					elog(ERROR, "create dir error, mkdir=%s:msg=%s",
							dir_path, strerror(errno));
		}
	}
	pfree(dir_path);
}

/*
 * rdcGetTupleLen  read sizeof(uint32) from myfile  buffer
 * if buffer is empty or data not enough. Cache data from
 * temp file first , then get data from buffer.
 * RETURN   next read len
 */
static uint32
rdcGetTupleLen(RSstate *state)
{
	uint32 len;
	size_t		nbytes;
	nbytes = rdcBufFileRead(state->myfile, (void *) &len, sizeof(len));

	if (nbytes == sizeof(len))
		return len;
	if (nbytes != 0)
		elog(ERROR, "unexpected end of tape");
	return 0;
}

/* create temp file to store data */
static RdcFile *
rdcBufFileCreateTemp(void)
{
	RdcFile    *file;
	File		pfile;
	char		*filename = NULL;

	pfile = rdcOpenTempFile(&filename);
	Assert(pfile >= 0);

	file = makeRdcBufFile(pfile);
	file->isTemp = true;
	file->filelnames[0] = filename;
	return file;
}

static RdcFile *
makeRdcBufFile(File firstfile)
{
	RdcFile    *file = (RdcFile *) palloc(sizeof(RdcFile));

	file->numFiles = 1;
	file->files = (File *) palloc(sizeof(File));
	file->files[0] = firstfile;
	file->filelnames = (char**)palloc(sizeof(char *) * file->numFiles);
	file->offsets = (off_t *) palloc(sizeof(off_t));
	file->offsets[0] = 0L;
	file->isTemp = false;
	file->dirty = false;
	file->curFile = 0;
	file->curOffset = 0L;
	file->pos = 0;
	file->nbytes = 0;

	return file;
}

static void
rdcBufFileClose(RdcFile *file)
{
	int			i;

	/* flush any unwritten data */
	rdcBufFileFlush(file);
	/* close the underlying file(s) (with delete if it's a temp file) */
	for (i = 0; i < file->numFiles; i++)
	{
		if (FILE_CLOSE == file->files[i] ||
			FILE_REMOVE == file->files[i])
			continue;

		close(file->files[i]);
		file->files[i] = FILE_CLOSE;

		if (file->isTemp)
		{
			/* delete file */
			if (0 != remove(file->filelnames[i]))
				elog(ERROR,
					 "fail to remove file \"%s\":%m",
					 file->filelnames[i]);
			pfree(file->filelnames[i]);
		}
	}
	/* release the buffer space */
	pfree(file->files);
	pfree(file->offsets);
	pfree(file->filelnames);
	pfree(file);
}

static void
rdcRemoveReadCompleteFile(RdcFile *file, int pos)
{
	Assert(NULL != file && pos >= 0);
	/* close read completely file */
	close(file->files[pos]);
	file->files[pos] = FILE_CLOSE;

	/* delete file */
	if (0 != remove(file->filelnames[pos]))
		elog(ERROR,
			 "fail to remove file \"%s\":%m",
			 file->filelnames[pos]);
	pfree(file->filelnames[pos]);
	file->filelnames[pos] = NULL;
	file->offsets[pos] = 0;
	file->files[pos] = FILE_REMOVE;
}

/*
 *	rdcCkeckReadFile
 *	check read completely file and remove
 */
static void
rdcCkeckReadFile(RSstate *state)
{
	int curRead = state->readptr->file;
	RdcFile *file = state->myfile;
	if (curRead > 0)
	{
		int fd ;
		int i;
		for(i=0; i < curRead; i++)
		{
			fd = file->files[i];
			if (FILE_REMOVE == fd)
				continue;
			else
			{
				if (FILE_CLOSE == fd)
				{
					if (0 != remove(file->filelnames[i]))
						elog(ERROR,
							 "fail to remove file \"%s\":%m",
							 file->filelnames[i]);
					pfree(file->filelnames[i]);
					file->filelnames[i] = NULL;
					file->offsets[i] = 0;
					file->files[i] = FILE_REMOVE;
				}
				else
					rdcRemoveReadCompleteFile(file, i);
			}
		}
	}
}

static int
rdcBufFileFlush(RdcFile *file)
{
	if (file->dirty)
	{
		rdcBufFileDumpBuffer(file);
		if (file->dirty)
			return EOF;
	}
	return 0;
}

static void
rdcBufFileTell(RdcFile *file, int *fileno, off_t *offset)
{
	*fileno = file->curFile;
	*offset = file->curOffset + file->pos;
}

static void
rdcBufFileDumpBuffer(RdcFile *file)
{
	int			wpos = 0;
	int			bytestowrite;
	File		thisfile;

	while (wpos < file->nbytes)
	{
		/*
		 * Advance to next component file if necessary and possible.
		 */
		if (file->curOffset >= MAX_PHYSICAL_FILESIZE && file->isTemp)
		{
			while (file->curFile + 1 >= file->numFiles)
				rdcExtendBufFile(file);
			file->curFile++;
			file->curOffset = 0L;
		}

		/*
		 * Enforce per-file size limit only for temp files, else just try to
		 * write as much as asked...
		 */
		 bytestowrite = file->nbytes - wpos;
		 if (file->isTemp)
		 {
		 	off_t		availbytes = MAX_PHYSICAL_FILESIZE - file->curOffset;

			if ((off_t) bytestowrite > availbytes)
				bytestowrite = (int) availbytes;
		 }

		 /*
		 * May need to reposition physical file.
		 */
		 thisfile = file->files[file->curFile];

		 if (file->curOffset != file->offsets[file->curFile])
		 {
		 	if (lseek(thisfile, file->curOffset, SEEK_SET) < 0)
		 	{
				if (EBADF == errno)
					elog(ERROR, "lseek file error : %s, the file may be not open",
						strerror(errno));

				elog(ERROR, "lseek file error : %s", strerror(errno));
			}
			else
				file->offsets[file->curFile] = file->curOffset;
		 }

		 bytestowrite = rdcFileWrite(thisfile, file->buffer + wpos, bytestowrite);
		 if (bytestowrite <= 0)
			return;				/* failed to write */
		 file->offsets[file->curFile] += bytestowrite;
		 file->curOffset += bytestowrite;
		 wpos += bytestowrite;
	}
	file->dirty = false;

	/*
	 * At this point, curOffset has been advanced to the end of the buffer,
	 * ie, its original value + nbytes.  We need to make it point to the
	 * logical file position, ie, original value + pos, in case that is less
	 * (as could happen due to a small backwards seek in a dirty buffer!)
	 */
	file->curOffset -= (file->nbytes - file->pos);
	if (file->curOffset < 0)	/* handle possible segment crossing */
	{
		file->curFile--;
		Assert(file->curFile >= 0);
		file->curOffset += MAX_PHYSICAL_FILESIZE;
	}

	/*
	 * Now we can set the buffer empty without changing the logical position
	 */
	file->pos = 0;
	file->nbytes = 0;
}

static void
rdcExtendBufFile(RdcFile *file)
{
	File		pfile;
	char 	  *filename;

	Assert(file->isTemp);
	pfile = rdcOpenTempFile(&filename);
	Assert(pfile >= 0);

	file->files = (File *) repalloc(file->files,
									(file->numFiles + 1) * sizeof(File));
	file->offsets = (off_t *) repalloc(file->offsets,
									(file->numFiles + 1) * sizeof(off_t));
	file->filelnames = (char **) repalloc(file->filelnames,
									(file->numFiles + 1) * sizeof(char*));

	file->files[file->numFiles] = pfile;
	file->offsets[file->numFiles] = 0L;
	file->filelnames[file->numFiles] = filename;
	file->numFiles++;
}

static int
rdcFileWrite(File file, char *buffer, int amount)
{
	int			returnCode;
	Assert(file > 0);

retry:
	errno = 0;
	returnCode = write(file, buffer, amount);

	/* if write didn't set errno, assume problem is no disk space */
	if (returnCode != amount && errno == 0)
		errno = ENOSPC;

	if (returnCode < 0)
	{
		/* OK to retry if interrupted */
		if (errno == EINTR)
			goto retry;
		else
			elog(WARNING, "write temp file error , %s",
				 strerror(errno));
	}
	return returnCode;
}

static size_t
rdcBufFileWrite(RdcFile *file, void *ptr, size_t size)
{
	size_t		nwritten = 0;
	size_t		nthistime;

	while (size > 0)
	{
		if (file->pos >= BLCKSZ)
		{
			/* Buffer full, dump it out */
			if (file->dirty)
			{
				rdcBufFileDumpBuffer(file);
				if (file->dirty)
					break;		/* I/O error */
			}
			else
			{
				/* Hmm, went directly from reading to writing? */
				file->curOffset += file->pos;
				file->pos = 0;
				file->nbytes = 0;
			}
		}

		nthistime = BLCKSZ - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(file->buffer + file->pos, ptr, nthistime);

		file->dirty = true;
		file->pos += nthistime;
		if (file->nbytes < file->pos)
			file->nbytes = file->pos;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nwritten += nthistime;
	}

	return nwritten;
}

/*
 * create temp file
 * filename : record new file name
 * return   : return file fd
 */
static File
rdcOpenTempFile(char ** filename)
{
	char	tempdirpath[MAXPGPATH];
	char    tempfilepath[MAXPGPATH];
	File    file;
	int		len;

	/* The default tablespace is {datadir}/base */
	snprintf(tempdirpath, sizeof(tempdirpath), "base/%s", RDC_TEMP_FILES_DIR);

	/*
	 * Generate a tempfile name that should be unique within the current
	 * database instance
	 */
	snprintf(tempfilepath, sizeof(tempfilepath), "%s/%s.%s.%d.%d",
			 tempdirpath, RDC_TEMP_FILE_PREFIX,
			 rdcPurpose, rdcNodeId, (*rdcFileCounter)++);

	/*
	 * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
	 * temp file that can be reused.
	 */
	file = open(tempfilepath, O_RDWR | O_CREAT);

	if (file < 0)
	{
		rdcCreateDir(tempdirpath);
		file = open(tempfilepath, O_RDWR | O_CREAT);

		if (file < 0)
			elog(ERROR, "could not create temporary file \"%s\": %m",
				 tempfilepath);
	}

	len = strlen(tempfilepath);
	*filename = (char *)palloc0(len + 1);
	memcpy(*filename, tempfilepath, len);
	(*filename)[len] = '\0';

	return file;
}

static int
rdcBufFileSeek(RdcFile *file, int fileno,
						off_t offset, int whence)
{
	int			newFile;
	off_t		newOffset;

	Assert(offset >= 0);
	switch (whence)
	{
		case SEEK_SET:
			if (fileno < 0)
				return EOF;
			newFile = fileno;
			newOffset = offset;
			break;
		default:
			elog(ERROR, "invalid whence: %d", whence);
			return EOF;
	}

	if(newFile == file->curFile &&
	   newOffset >= file->curOffset &&
	   newOffset <= file->curOffset + file->nbytes)
	{
		/*
		 * Seek is to a point within existing buffer; we can just adjust
		 * pos-within-buffer, without flushing buffer.  Note this is OK
		 * whether reading or writing, but buffer remains dirty if we were
		 * writing.
		 */

		/* file content had been read, but only part of buffer is used. set pos end of read postion
		 * use the last buffer. now The repeat data in buffer and file , before flush data ,set write
		 * postion at begin of repeate data in file
		 */
		file->pos = (int) (newOffset - file->curOffset);
		return 0;
	}
	/* Otherwise, must reposition buffer, so flush any dirty data */
	if (rdcBufFileFlush(file) != 0)
		return EOF;

	if (file->isTemp)
	{
		/* convert seek to "start of next seg" to "end of last seg" */
		if (newFile == file->numFiles && newOffset == 0)
		{
			newFile--;
			newOffset = MAX_PHYSICAL_FILESIZE;
		}
		while (newOffset > MAX_PHYSICAL_FILESIZE)
		{
			if (++newFile >= file->numFiles)
				return EOF;
			newOffset -= MAX_PHYSICAL_FILESIZE;
		}
	}

	if (newFile >= file->numFiles)
		return EOF;
	/* Seek is OK! */
	file->curFile = newFile;
	file->curOffset = newOffset;
	file->pos = 0;
	file->nbytes = 0;
	return 0;
}

static size_t
rdcBufFileRead(RdcFile *file, void *ptr,
						size_t size)
{
	size_t		nread = 0;
	size_t		nthistime;
	if (file->dirty)
	{
		if (rdcBufFileFlush(file) != 0)
			return 0;			/* could not flush... */
		Assert(!file->dirty);
	}

	while (size > 0)
	{
		/* only first time pos = 0.  then read times, pos = nbytes
		 * read file to buffer again and set current offset
		 */
		if (file->pos >= file->nbytes)
		{
			/* now buffer read over, set file offset */
			file->curOffset += file->pos;
			file->pos = 0;
			file->nbytes = 0;
			rdcBufFileLoadBuffer(file);
			if (file->nbytes <= 0)
				break;			/* no more data available */
		}

		nthistime = file->nbytes - file->pos;
		if (nthistime > size)
			nthistime = size;
		Assert(nthistime > 0);

		memcpy(ptr, file->buffer + file->pos, nthistime);

		file->pos += nthistime;
		ptr = (void *) ((char *) ptr + nthistime);
		size -= nthistime;
		nread += nthistime;
	}
	return nread;
}

static void
rdcBufFileLoadBuffer(RdcFile *file)
{
	File		thisfile;

	/* file has been read completely, now read next file */
	if (file->curOffset >= MAX_PHYSICAL_FILESIZE &&
		file->curFile + 1 < file->numFiles)
	{
		file->curFile++;
		file->curOffset = 0L;
	}

	/*
	 * May need to reposition physical file.
	 */
	thisfile = file->files[file->curFile];

	if (file->curOffset != file->offsets[file->curFile])
	{
		if (lseek(thisfile, file->curOffset, SEEK_SET) < 0)
		{
			if (EBADF == errno)
				elog(ERROR, "lseek file error : %s, the file may be not open",
					strerror(errno));

			elog(ERROR, "lseek file error : %s", strerror(errno));
		}
		else
			file->offsets[file->curFile] = file->curOffset;
	}

	/*
	 * Read whatever we can get, up to a full bufferload.
	 */
	file->nbytes = rdcFileRead(thisfile, file->buffer, sizeof(file->buffer));
	if (file->nbytes < 0)
		file->nbytes = 0;

	file->offsets[file->curFile] += file->nbytes;
}

static int
rdcFileRead(File file, char *buffer, int amount)
{
	int			returnCode;
	/* check file is exist */
retry:
	returnCode = read(file, buffer, amount);

	if (returnCode < 0)
	/* OK to retry if interrupted */
	if (errno == EINTR)
		goto retry;

	return returnCode;
}

/*
 * rdcstore_begin
 *
 * Create a new reducestore
 *
 * maxKBytes: how much data to store in memory (any data beyond this
 * amount is paged to disk).  When in doubt, use work_mem.
 */
RSstate *
rdcstore_begin(int maxKBytes, char* purpose, int nodeId,
			   pid_t pid, pid_t ppid, pg_time_t time)
{
	RSstate *state;
	Assert(maxKBytes > 0 && NULL !=purpose && nodeId >= 0);

	state = rdcstore_begin_common(maxKBytes, purpose, nodeId,
								  pid, ppid, time);

	state->copytup = rdcCopyTuple;
	state->writetup = rdcWriteTuple;
	state->readtup = rdcReadTuple;

	return state;
}

/*
 * rdcstore_ateof
 *
 * Returns the read pointer eof_reached state.
 */
bool
rdcstore_ateof(RSstate *state)
{
	Assert(NULL != state);
	return state->readptr->eof_reached;
}

/*
 * rdcstore_puttuple
 * put data into  memory array. if state availMem not enough,
 * put data into state myfile buffer
 */
void
rdcstore_puttuple(RSstate *state, char *data, int len)
{
	void	*tuple = NULL;

	Assert(NULL != state && NULL != data && len > 0);

	PUT_FILE_INFO(state);
	MemoryContext oldcxt = MemoryContextSwitchTo(state->context);
	/*
	 * Copy the tuple.  (Must do this even in WRITEFILE case.  Note that
	 * COPYTUP includes USEMEM, so we needn't do that here.)
	 */
	tuple = COPYTUP(state, data, len);

	rdcstore_puttuple_common(state, tuple);

	MemoryContextSwitchTo(oldcxt);
	RESET_FILE_INFO();
}

/*
 * Fetch the next tuple in forward direction.
 * hasData false if no more tuples otherwise hasData is true
 * data store in StringInfo
 */
void
rdcstore_gettuple(RSstate *state, StringInfo buf, bool *hasData)
{
	RSdata * rsData = NULL;

	Assert(NULL != state && NULL != buf && NULL != hasData);

	PUT_FILE_INFO(state);
	rsData = (RSdata*) rdcstore_gettuple_common(state);
	RESET_FILE_INFO();

	if(rsData)
	{
		appendBinaryStringInfo(buf, rsData->data, rsData->len);
		FREEMEM(state, RDC_GET_DATA_MEM(rsData));
		*hasData = true;
		pfree(rsData);
		/* trim free data position */
		rdcstore_trim(state);
	}
	else
		*hasData = false;
}

/*
 * rdcstore_trim	- remove all no-longer-needed tuples
 */
void
rdcstore_trim(RSstate *state)
{
	int 		oldest;
	int			nremove;
	int			i;

	Assert(NULL != state);
	/*
	 * We don't bother trimming temp files since it usually would mean more
	 * work than just letting them sit in kernel buffers until they age out.
	 */
	if (state->status != TSS_INMEM)
		return;

	/* Find the oldest read pointer */
	oldest = state->memtupcount;

	if (!state->readptr->eof_reached)
			oldest = Min(oldest, state->readptr->current);

	nremove = oldest - 1;
	if (nremove <= 0)
		return;

	Assert(nremove >= state->memtupdeleted);
	Assert(nremove <= state->memtupcount);

	/* Release no-longer-needed tuples */
	for (i = state->memtupdeleted; i < nremove; i++)
	{
		state->memtuples[i] = NULL;
	}
	state->memtupdeleted = nremove;

	/* mark reduce store as truncated (used for Assert crosschecks only) */
	state->truncated = true;

	/*
	 * If nremove is less than 1/8th memtupcount, just stop here, leaving the
	 * "deleted" slots as NULL.  This prevents us from expending O(N^2) time
	 * repeatedly memmove-ing a large pointer array.  The worst case space
	 * wastage is pretty small, since it's just pointers and not whole tuples.
	 */
	if (nremove < state->memtupcount / 8)
		return;

	/*
	 * Slide the array down and readjust pointers.
	 *
	 * In mergejoin's current usage, it's demonstrable that there will always
	 * be exactly one non-removed tuple; so optimize that case.
	 */
	if (nremove + 1 == state->memtupcount)
		state->memtuples[0] = state->memtuples[nremove];
	else
		memmove(state->memtuples, state->memtuples + nremove,
				(state->memtupcount - nremove) * sizeof(void *));

	state->memtupdeleted = 0;
	state->memtupcount -= nremove;

	state->readptr->current -= nremove;
}

/*
 * rdcstore_in_memory
 *
 * Returns true if the reduce store has not spilled to disk.
 *
 */
bool
rdcstore_in_memory(RSstate *state)
{
	Assert(NULL != state);
	return (state->status == TSS_INMEM);
}

void
rdcstore_end(RSstate *state)
{
	int			i;

	if (!state)
		return ;

	PUT_FILE_INFO(state);

	if (state->myfile)
		rdcBufFileClose(state->myfile);

	if (state->memtuples)
	{
		if (state->status == TSS_INMEM)
		{
			/* tuple had been read and memory free, but rdcstore_trim not
			 * set position pointer to null .SO get max memtupdeleted and
			 * read current to avoid free memory twice
			 */
			int pos = Max(state->memtupdeleted, state->readptr->current);
			for (i = pos; i < state->memtupcount; i++)
				pfree(state->memtuples[i]);
		}
		pfree(state->memtuples);
	}

	RESET_FILE_INFO();
	pfree(state->readptr);
	pfree(state->purpose);
	pfree(state);
}

void
rdcstore_flush(RdcFile *file)
{
	if (NULL == file)
		return;
	rdcBufFileDumpBuffer(file);
}

