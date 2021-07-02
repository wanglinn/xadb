/*-------------------------------------------------------------------------
 *
 * copyfrom.c
 *		COPY <table> FROM file/program/client
 *
 * This file contains routines needed to efficiently load tuples into a
 * table.  That includes looking up the correct partition, firing triggers,
 * calling the table AM function to insert the data, and updating indexes.
 * Reading data from the input file or client and parsing it into Datums
 * is handled in copyfromparse.c.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfrom.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "commands/progress.h"
#include "commands/trigger.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#ifdef ADB
#include "access/tuptypeconvert.h"
#include "commands/defrem.h"
#include "executor/clusterReceiver.h"
#include "executor/execCluster.h"
#include "nodes/makefuncs.h"
#include "optimizer/plancat.h"
#include "optimizer/reduceinfo.h"
#include "parser/parse_relation.h"
#include "pgxc/pgxc.h"
#include "storage/mem_toc.h"
#include "utils/dynamicreduce.h"
#include "utils/lsyscache.h"
#include "libpq-fe.h"
#endif /* ADB */

/*
 * No more than this many tuples per CopyMultiInsertBuffer
 *
 * Caution: Don't make this too big, as we could end up with this many
 * CopyMultiInsertBuffer items stored in CopyMultiInsertInfo's
 * multiInsertBuffers list.  Increasing this can cause quadratic growth in
 * memory requirements during copies into partitioned tables with a large
 * number of partitions.
 */
#define MAX_BUFFERED_TUPLES		1000

/*
 * Flush buffers if there are >= this many bytes, as counted by the input
 * size, of tuples stored.
 */
#define MAX_BUFFERED_BYTES		65535

/* Trim the list of buffers back down to this number after flushing */
#define MAX_PARTITION_BUFFERS	32

/* Stores multi-insert data related to a single relation in CopyFrom. */
typedef struct CopyMultiInsertBuffer
{
	TupleTableSlot *slots[MAX_BUFFERED_TUPLES]; /* Array to store tuples */
	ResultRelInfo *resultRelInfo;	/* ResultRelInfo for 'relid' */
	BulkInsertState bistate;	/* BulkInsertState for this rel */
	int			nused;			/* number of 'slots' containing tuples */
	uint64		linenos[MAX_BUFFERED_TUPLES];	/* Line # of tuple in copy
												 * stream */
} CopyMultiInsertBuffer;

/*
 * Stores one or many CopyMultiInsertBuffers and details about the size and
 * number of tuples which are stored in them.  This allows multiple buffers to
 * exist at once when COPYing into a partitioned table.
 */
typedef struct CopyMultiInsertInfo
{
	List	   *multiInsertBuffers; /* List of tracked CopyMultiInsertBuffers */
	int			bufferedTuples; /* number of tuples buffered over all buffers */
	int			bufferedBytes;	/* number of bytes from all buffered tuples */
	CopyFromState cstate;		/* Copy state for this CopyMultiInsertInfo */
	EState	   *estate;			/* Executor state used for COPY */
	CommandId	mycid;			/* Command Id used for COPY */
	int			ti_options;		/* table insert options */
} CopyMultiInsertInfo;

#ifdef ADB

typedef struct CopyFromReduceState
{
	DynamicReduceIOBuffer	drio;
	CustomNextRowFunction	NextRow;
	void				   *func_data;
	CopyFromState			cstate;
	dsm_segment			   *dsm_seg;
	int						plan_id;
}CopyFromReduceState;

typedef struct TidBufFileScanState
{
	BufFile		   *file;
	ProjectionInfo *project;
	TupleTableSlot *base_slot;
	TupleTableSlot *out_slot;
	Relation		rel;
}TidBufFileScanState;

#endif /* ADB */

/* non-export function prototypes */
static char *limit_printout_length(const char *str);

static void ClosePipeFromProgram(CopyFromState cstate);

#ifdef ADB
static TupleTableSlot* NextLineCallTrigger(CopyFromState cstate, ExprContext *econtext, void *data);
static TupleTableSlot* NextRowFromTuplestore(CopyFromState cstate, ExprContext *econtext, void *data);
static TupleTableSlot* AddNumberNextCopyFrom(CopyFromState cstate, ExprContext *econtext, void *data);
static TupleTableSlot* makeClusterCopySlot(Relation rel);
static CopyStmt* makeClusterCopyFromStmt(Relation rel, bool freeze);

static List* LoadAuxRelCopyInfo(StringInfo mem_toc);

static void ApplyCopyToAuxiliary(CopyFromState parent, List *rnodes);
static TupleTableSlot* NextRowFromReduce(CopyFromState cstate, ExprContext *context, void *data);
static TupleTableSlot* NextRowFromTidBufFile(CopyFromState cstate, ExprContext *context, void *data);
#endif

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyFromState.
 */
void
CopyFromErrorCallback(void *arg)
{
	CopyFromState cstate = (CopyFromState) arg;
	char		curlineno_str[32];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	if (cstate->opts.binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("COPY %s, line %s, column %s",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		else
			errcontext("COPY %s, line %s",
					   cstate->cur_relname, curlineno_str);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char	   *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("COPY %s, line %s, column %s: \"%s\"",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname, attval);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("COPY %s, line %s, column %s: null input",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, print it.
			 */
			if (cstate->line_buf_valid)
			{
				char	   *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("COPY %s, line %s: \"%s\"",
						   cstate->cur_relname, curlineno_str, lineval);
				pfree(lineval);
			}
			else
			{
				errcontext("COPY %s, line %s",
						   cstate->cur_relname, curlineno_str);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * Returns a pstrdup'd copy of the input.
 */
static char *
limit_printout_length(const char *str)
{
#define MAX_COPY_DATA_DISPLAY 100

	int			slen = strlen(str);
	int			len;
	char	   *res;

	/* Fast path if definitely okay */
	if (slen <= MAX_COPY_DATA_DISPLAY)
		return pstrdup(str);

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

	/*
	 * Truncate, and add "..." to show we truncated the input.
	 */
	res = (char *) palloc(len + 4);
	memcpy(res, str, len);
	strcpy(res + len, "...");

	return res;
}

/*
 * Allocate memory and initialize a new CopyMultiInsertBuffer for this
 * ResultRelInfo.
 */
static CopyMultiInsertBuffer *
CopyMultiInsertBufferInit(ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = (CopyMultiInsertBuffer *) palloc(sizeof(CopyMultiInsertBuffer));
	memset(buffer->slots, 0, sizeof(TupleTableSlot *) * MAX_BUFFERED_TUPLES);
	buffer->resultRelInfo = rri;
	buffer->bistate = GetBulkInsertState();
	buffer->nused = 0;

	return buffer;
}

/*
 * Make a new buffer for this ResultRelInfo.
 */
static inline void
CopyMultiInsertInfoSetupBuffer(CopyMultiInsertInfo *miinfo,
							   ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer;

	buffer = CopyMultiInsertBufferInit(rri);

	/* Setup back-link so we can easily find this buffer again */
	rri->ri_CopyMultiInsertBuffer = buffer;
	/* Record that we're tracking this buffer */
	miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
}

/*
 * Initialize an already allocated CopyMultiInsertInfo.
 *
 * If rri is a non-partitioned table then a CopyMultiInsertBuffer is set up
 * for that table.
 */
static void
CopyMultiInsertInfoInit(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						CopyFromState cstate, EState *estate, CommandId mycid,
						int ti_options)
{
	miinfo->multiInsertBuffers = NIL;
	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;
	miinfo->cstate = cstate;
	miinfo->estate = estate;
	miinfo->mycid = mycid;
	miinfo->ti_options = ti_options;

	/*
	 * Only setup the buffer when not dealing with a partitioned table.
	 * Buffers for partitioned tables will just be setup when we need to send
	 * tuples their way for the first time.
	 */
	if (rri->ri_RelationDesc->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		CopyMultiInsertInfoSetupBuffer(miinfo, rri);
}

/*
 * Returns true if the buffers are full
 */
static inline bool
CopyMultiInsertInfoIsFull(CopyMultiInsertInfo *miinfo)
{
	if (miinfo->bufferedTuples >= MAX_BUFFERED_TUPLES ||
		miinfo->bufferedBytes >= MAX_BUFFERED_BYTES)
		return true;
	return false;
}

/*
 * Returns true if we have no buffered tuples
 */
static inline bool
CopyMultiInsertInfoIsEmpty(CopyMultiInsertInfo *miinfo)
{
	return miinfo->bufferedTuples == 0;
}

/*
 * Write the tuples stored in 'buffer' out to the table.
 */
static inline void
CopyMultiInsertBufferFlush(CopyMultiInsertInfo *miinfo,
						   CopyMultiInsertBuffer *buffer)
{
	MemoryContext oldcontext;
	int			i;
	uint64		save_cur_lineno;
	CopyFromState cstate = miinfo->cstate;
	EState	   *estate = miinfo->estate;
	CommandId	mycid = miinfo->mycid;
	int			ti_options = miinfo->ti_options;
	bool		line_buf_valid = cstate->line_buf_valid;
	int			nused = buffer->nused;
	ResultRelInfo *resultRelInfo = buffer->resultRelInfo;
	TupleTableSlot **slots = buffer->slots;

	/*
	 * Print error context information correctly, if one of the operations
	 * below fail.
	 */
	cstate->line_buf_valid = false;
	save_cur_lineno = cstate->cur_lineno;

	/*
	 * table_multi_insert may leak memory, so switch to short-lived memory
	 * context before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	table_multi_insert(resultRelInfo->ri_RelationDesc,
					   slots,
					   nused,
					   mycid,
					   ti_options,
					   buffer->bistate);
	MemoryContextSwitchTo(oldcontext);

#ifdef ADB
	if (cstate->fd_copied_ctid)
	{
		for (i=0;i<nused;++i)
		{
			ItemPointer tid = &buffer->slots[i]->tts_tid;
			Assert(ItemPointerIsValid(tid));
			BufFileWrite(cstate->fd_copied_ctid,
						 tid,
						 sizeof(ItemPointerData));
		}
	}
#endif /* ADB */

	for (i = 0; i < nused; i++)
	{
		/*
		 * If there are any indexes, update them for all the inserted tuples,
		 * and run AFTER ROW INSERT triggers.
		 */
		if (resultRelInfo->ri_NumIndices > 0)
		{
			List	   *recheckIndexes;

			cstate->cur_lineno = buffer->linenos[i];
			recheckIndexes =
				ExecInsertIndexTuples(resultRelInfo,
									  buffer->slots[i], estate, false, false,
									  NULL, NIL);
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], recheckIndexes,
								 cstate->transition_capture);
			list_free(recheckIndexes);
		}

		/*
		 * There's no indexes, but see if we need to run AFTER ROW INSERT
		 * triggers anyway.
		 */
		else if (resultRelInfo->ri_TrigDesc != NULL &&
				 (resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
				  resultRelInfo->ri_TrigDesc->trig_insert_new_table))
		{
			cstate->cur_lineno = buffer->linenos[i];
			ExecARInsertTriggers(estate, resultRelInfo,
								 slots[i], NIL, cstate->transition_capture);
		}

		ExecClearTuple(slots[i]);
	}

	/* Mark that all slots are free */
	buffer->nused = 0;

	/* reset cur_lineno and line_buf_valid to what they were */
	cstate->line_buf_valid = line_buf_valid;
	cstate->cur_lineno = save_cur_lineno;
}

/*
 * Drop used slots and free member for this buffer.
 *
 * The buffer must be flushed before cleanup.
 */
static inline void
CopyMultiInsertBufferCleanup(CopyMultiInsertInfo *miinfo,
							 CopyMultiInsertBuffer *buffer)
{
	int			i;

	/* Ensure buffer was flushed */
	Assert(buffer->nused == 0);

	/* Remove back-link to ourself */
	buffer->resultRelInfo->ri_CopyMultiInsertBuffer = NULL;

	FreeBulkInsertState(buffer->bistate);

	/* Since we only create slots on demand, just drop the non-null ones. */
	for (i = 0; i < MAX_BUFFERED_TUPLES && buffer->slots[i] != NULL; i++)
		ExecDropSingleTupleTableSlot(buffer->slots[i]);

	table_finish_bulk_insert(buffer->resultRelInfo->ri_RelationDesc,
							 miinfo->ti_options);

	pfree(buffer);
}

/*
 * Write out all stored tuples in all buffers out to the tables.
 *
 * Once flushed we also trim the tracked buffers list down to size by removing
 * the buffers created earliest first.
 *
 * Callers should pass 'curr_rri' as the ResultRelInfo that's currently being
 * used.  When cleaning up old buffers we'll never remove the one for
 * 'curr_rri'.
 */
static inline void
CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo, ResultRelInfo *curr_rri)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
	{
		CopyMultiInsertBuffer *buffer = (CopyMultiInsertBuffer *) lfirst(lc);

		CopyMultiInsertBufferFlush(miinfo, buffer);
	}

	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;

	/*
	 * Trim the list of tracked buffers down if it exceeds the limit.  Here we
	 * remove buffers starting with the ones we created first.  It seems less
	 * likely that these older ones will be needed than the ones that were
	 * just created.
	 */
	while (list_length(miinfo->multiInsertBuffers) > MAX_PARTITION_BUFFERS)
	{
		CopyMultiInsertBuffer *buffer;

		buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);

		/*
		 * We never want to remove the buffer that's currently being used, so
		 * if we happen to find that then move it to the end of the list.
		 */
		if (buffer->resultRelInfo == curr_rri)
		{
			miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
			miinfo->multiInsertBuffers = lappend(miinfo->multiInsertBuffers, buffer);
			buffer = (CopyMultiInsertBuffer *) linitial(miinfo->multiInsertBuffers);
		}

		CopyMultiInsertBufferCleanup(miinfo, buffer);
		miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
	}
}

/*
 * Cleanup allocated buffers and free memory
 */
static inline void
CopyMultiInsertInfoCleanup(CopyMultiInsertInfo *miinfo)
{
	ListCell   *lc;

	foreach(lc, miinfo->multiInsertBuffers)
		CopyMultiInsertBufferCleanup(miinfo, lfirst(lc));

	list_free(miinfo->multiInsertBuffers);
}

/*
 * Get the next TupleTableSlot that the next tuple should be stored in.
 *
 * Callers must ensure that the buffer is not full.
 *
 * Note: 'miinfo' is unused but has been included for consistency with the
 * other functions in this area.
 */
static inline TupleTableSlot *
CopyMultiInsertInfoNextFreeSlot(CopyMultiInsertInfo *miinfo,
								ResultRelInfo *rri)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;
	int			nused = buffer->nused;

	Assert(buffer != NULL);
	Assert(nused < MAX_BUFFERED_TUPLES);

	if (buffer->slots[nused] == NULL)
		buffer->slots[nused] = table_slot_create(rri->ri_RelationDesc, NULL);
	return buffer->slots[nused];
}

/*
 * Record the previously reserved TupleTableSlot that was reserved by
 * CopyMultiInsertInfoNextFreeSlot as being consumed.
 */
static inline void
CopyMultiInsertInfoStore(CopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						 TupleTableSlot *slot, int tuplen, uint64 lineno)
{
	CopyMultiInsertBuffer *buffer = rri->ri_CopyMultiInsertBuffer;

	Assert(buffer != NULL);
	Assert(slot == buffer->slots[buffer->nused]);

	/* Store the line number so we can properly report any errors later */
	buffer->linenos[buffer->nused] = lineno;

	/* Record this slot as being used */
	buffer->nused++;

	/* Update how many tuples are stored and their size */
	miinfo->bufferedTuples++;
	miinfo->bufferedBytes += tuplen;
}

/*
 * Copy FROM file to relation.
 */
uint64
CopyFrom(CopyFromState cstate)
{
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *target_resultRelInfo;
	ResultRelInfo *prevResultRelInfo = NULL;
	EState	   *estate = CreateExecutorState(); /* for ExecConstraints() */
	ModifyTableState *mtstate;
	ExprContext *econtext;
	TupleTableSlot *singleslot = NULL;
	MemoryContext oldcontext = CurrentMemoryContext;

	PartitionTupleRouting *proute = NULL;
	ErrorContextCallback errcallback;
	CommandId	mycid = GetCurrentCommandId(true);
	int			ti_options = 0; /* start with default options for insert */
	BulkInsertState bistate = NULL;
	CopyInsertMethod insertMethod;
	CopyMultiInsertInfo multiInsertInfo = {0};	/* pacify compiler */
	int64		processed = 0;
	int64		excluded = 0;
	bool		has_before_insert_row_trig;
	bool		has_instead_insert_row_trig;
	bool		leafpart_use_multi_insert = false;

	Assert(cstate->rel);
	Assert(list_length(cstate->range_table) == 1);

	/*
	 * The target must be a plain, foreign, or partitioned relation, or have
	 * an INSTEAD OF INSERT row trigger.  (Currently, such triggers are only
	 * allowed on views, so we only hint about them in the view case.)
	 */
	if (cstate->rel->rd_rel->relkind != RELKIND_RELATION &&
		cstate->rel->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		!(cstate->rel->trigdesc &&
		  cstate->rel->trigdesc->trig_insert_instead_row))
	{
		if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(cstate->rel)),
					 errhint("To enable copying to a view, provide an INSTEAD OF INSERT trigger.")));
		else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(cstate->rel))));
	}

	/*
	 * If the target file is new-in-transaction, we assume that checking FSM
	 * for free space is a waste of time.  This could possibly be wrong, but
	 * it's unlikely.
	 */
	if (RELKIND_HAS_STORAGE(cstate->rel->rd_rel->relkind) &&
		(cstate->rel->rd_createSubid != InvalidSubTransactionId ||
		 cstate->rel->rd_firstRelfilenodeSubid != InvalidSubTransactionId))
		ti_options |= TABLE_INSERT_SKIP_FSM;

	/*
	 * Optimize if new relfilenode was created in this subxact or one of its
	 * committed children and we won't see those rows later as part of an
	 * earlier scan or command. The subxact test ensures that if this subxact
	 * aborts then the frozen rows won't be visible after xact cleanup.  Note
	 * that the stronger test of exactly which subtransaction created it is
	 * crucial for correctness of this optimization. The test for an earlier
	 * scan or command tolerates false negatives. FREEZE causes other sessions
	 * to see rows they would not see under MVCC, and a false negative merely
	 * spreads that anomaly to the current session.
	 */
	if (cstate->opts.freeze)
	{
		/*
		 * We currently disallow COPY FREEZE on partitioned tables.  The
		 * reason for this is that we've simply not yet opened the partitions
		 * to determine if the optimization can be applied to them.  We could
		 * go and open them all here, but doing so may be quite a costly
		 * overhead for small copies.  In any case, we may just end up routing
		 * tuples to a small number of partitions.  It seems better just to
		 * raise an ERROR for partitioned tables.
		 */
		if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot perform COPY FREEZE on a partitioned table")));
		}

		/*
		 * Tolerate one registration for the benefit of FirstXactSnapshot.
		 * Scan-bearing queries generally create at least two registrations,
		 * though relying on that is fragile, as is ignoring ActiveSnapshot.
		 * Clear CatalogSnapshot to avoid counting its registration.  We'll
		 * still detect ongoing catalog scans, each of which separately
		 * registers the snapshot it uses.
		 */
		InvalidateCatalogSnapshot();
		if (!ThereAreNoPriorRegisteredSnapshots() || !ThereAreNoReadyPortals())
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("cannot perform COPY FREEZE because of prior transaction activity")));

		if (cstate->rel->rd_createSubid != GetCurrentSubTransactionId() &&
			cstate->rel->rd_newRelfilenodeSubid != GetCurrentSubTransactionId())
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction")));

		ti_options |= TABLE_INSERT_FROZEN;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = target_resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);
#ifdef ADB
	if (IsConnFromCoord() &&
		resultRelInfo->ri_TrigDesc)
	{
		/* wen data from coordinator, trig call in coordinator */
		FreeTriggerDesc(resultRelInfo->ri_TrigDesc);
		resultRelInfo->ri_TrigDesc = NULL;
	}
#endif /* ADB */

	/* Verify the named relation is a valid target for INSERT */
	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

	/*
	 * Set up a ModifyTableState so we can let FDW(s) init themselves for
	 * foreign-table result relation(s).
	 */
	mtstate = makeNode(ModifyTableState);
	mtstate->ps.plan = NULL;
	mtstate->ps.state = estate;
	mtstate->operation = CMD_INSERT;
	mtstate->mt_nrels = 1;
	mtstate->resultRelInfo = resultRelInfo;
	mtstate->rootResultRelInfo = resultRelInfo;

	if (resultRelInfo->ri_FdwRoutine != NULL &&
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert != NULL)
		resultRelInfo->ri_FdwRoutine->BeginForeignInsert(mtstate,
														 resultRelInfo);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 *
	 * Because partition tuple routing would like to know about whether
	 * transition capture is active, we also set it in mtstate, which is
	 * passed to ExecFindPartition() below.
	 */
	cstate->transition_capture = mtstate->mt_transition_capture =
		MakeTransitionCaptureState(cstate->rel->trigdesc,
								   RelationGetRelid(cstate->rel),
								   CMD_INSERT);

	/*
	 * If the named relation is a partitioned table, initialize state for
	 * CopyFrom tuple routing.
	 */
	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		proute = ExecSetupPartitionTupleRouting(estate, cstate->rel);

	if (cstate->whereClause)
		cstate->qualexpr = ExecInitQual(castNode(List, cstate->whereClause),
										&mtstate->ps);

	/*
	 * It's generally more efficient to prepare a bunch of tuples for
	 * insertion, and insert them in one table_multi_insert() call, than call
	 * table_tuple_insert() separately for every tuple. However, there are a
	 * number of reasons why we might not be able to do this.  These are
	 * explained below.
	 */
	if (resultRelInfo->ri_TrigDesc != NULL &&
		(resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		 resultRelInfo->ri_TrigDesc->trig_insert_instead_row))
	{
		/*
		 * Can't support multi-inserts when there are any BEFORE/INSTEAD OF
		 * triggers on the table. Such triggers might query the table we're
		 * inserting into and act differently if the tuples that have already
		 * been processed and prepared for insertion are not there.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (proute != NULL && resultRelInfo->ri_TrigDesc != NULL &&
			 resultRelInfo->ri_TrigDesc->trig_insert_new_table)
	{
		/*
		 * For partitioned tables we can't support multi-inserts when there
		 * are any statement level insert triggers. It might be possible to
		 * allow partitioned tables with such triggers in the future, but for
		 * now, CopyMultiInsertInfoFlush expects that any before row insert
		 * and statement level insert triggers are on the same relation.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (resultRelInfo->ri_FdwRoutine != NULL ||
			 cstate->volatile_defexprs)
	{
		/*
		 * Can't support multi-inserts to foreign tables or if there are any
		 * volatile default expressions in the table.  Similarly to the
		 * trigger case above, such expressions may query the table we're
		 * inserting into.
		 *
		 * Note: It does not matter if any partitions have any volatile
		 * default expressions as we use the defaults from the target of the
		 * COPY command.
		 */
		insertMethod = CIM_SINGLE;
	}
	else if (contain_volatile_functions(cstate->whereClause))
	{
		/*
		 * Can't support multi-inserts if there are any volatile function
		 * expressions in WHERE clause.  Similarly to the trigger case above,
		 * such expressions may query the table we're inserting into.
		 */
		insertMethod = CIM_SINGLE;
	}
	else
	{
		/*
		 * For partitioned tables, we may still be able to perform bulk
		 * inserts.  However, the possibility of this depends on which types
		 * of triggers exist on the partition.  We must disable bulk inserts
		 * if the partition is a foreign table or it has any before row insert
		 * or insert instead triggers (same as we checked above for the parent
		 * table).  Since the partition's resultRelInfos are initialized only
		 * when we actually need to insert the first tuple into them, we must
		 * have the intermediate insert method of CIM_MULTI_CONDITIONAL to
		 * flag that we must later determine if we can use bulk-inserts for
		 * the partition being inserted into.
		 */
		if (proute)
			insertMethod = CIM_MULTI_CONDITIONAL;
		else
			insertMethod = CIM_MULTI;

		CopyMultiInsertInfoInit(&multiInsertInfo, resultRelInfo, cstate,
								estate, mycid, ti_options);
	}

	/*
	 * If not using batch mode (which allocates slots as needed) set up a
	 * tuple slot too. When inserting into a partitioned table, we also need
	 * one, even if we might batch insert, to read the tuple in the root
	 * partition's form.
	 */
	if (insertMethod == CIM_SINGLE || insertMethod == CIM_MULTI_CONDITIONAL)
	{
		singleslot = table_slot_create(resultRelInfo->ri_RelationDesc,
									   &estate->es_tupleTable);
		bistate = GetBulkInsertState();
	}

	has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

	has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
								   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	for (;;)
	{
		TupleTableSlot *myslot;
		bool		skip_tuple;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Reset the per-tuple exprcontext. We do this after every tuple, to
		 * clean-up after expression evaluations etc.
		 */
		ResetPerTupleExprContext(estate);

		/* select slot to (initially) load row into */
		if (insertMethod == CIM_SINGLE || proute)
		{
			myslot = singleslot;
			Assert(myslot != NULL);
		}
		else
		{
			Assert(resultRelInfo == target_resultRelInfo);
			Assert(insertMethod == CIM_MULTI);

			myslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
													 resultRelInfo);
		}

		/*
		 * Switch to per-tuple context before calling NextCopyFrom, which does
		 * evaluate default expressions etc. and requires per-tuple context.
		 */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		ExecClearTuple(myslot);

#ifdef ADB
		if (cstate->NextRowFrom)
		{
			TupleTableSlot *slot = (*cstate->NextRowFrom)(cstate, econtext, cstate->func_data);
			if (TupIsNull(slot))
				break;
			if (myslot == singleslot)
			{
				myslot = slot;
			}else
			{
				int natts = myslot->tts_tupleDescriptor->natts;
				Assert(natts <= slot->tts_tupleDescriptor->natts);
				slot_getsomeattrs(slot, natts);
				memcpy(myslot->tts_values, slot->tts_values, sizeof(slot->tts_values[0]) * natts);
				memcpy(myslot->tts_isnull, slot->tts_isnull, sizeof(slot->tts_isnull[0]) * natts);
				ExecStoreVirtualTuple(myslot);
			}
		}else
		{
#endif /* ADB */
		/* Directly store the values/nulls array in the slot */
		if (!NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull))
			break;

		ExecStoreVirtualTuple(myslot);
#ifdef ADB
		}
#endif /* ADB */

		/*
		 * Constraints and where clause might reference the tableoid column,
		 * so (re-)initialize tts_tableOid before evaluating them.
		 */
		myslot->tts_tableOid = RelationGetRelid(target_resultRelInfo->ri_RelationDesc);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		if (cstate->whereClause)
		{
			econtext->ecxt_scantuple = myslot;
			/* Skip items that don't match COPY's WHERE clause */
			if (!ExecQual(cstate->qualexpr, econtext))
			{
				/*
				 * Report that this tuple was filtered out by the WHERE
				 * clause.
				 */
				pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED,
											 ++excluded);
				continue;
			}
		}

		/* Determine the partition to insert the tuple into */
		if (proute)
		{
			TupleConversionMap *map;

			/*
			 * Attempt to find a partition suitable for this tuple.
			 * ExecFindPartition() will raise an error if none can be found or
			 * if the found partition is not suitable for INSERTs.
			 */
			resultRelInfo = ExecFindPartition(mtstate, target_resultRelInfo,
											  proute, myslot, estate);

			if (prevResultRelInfo != resultRelInfo)
			{
#ifdef ADB
				if (IsConnFromCoord() &&
					resultRelInfo->ri_TrigDesc)
				{
					/* wen data from coordinator, trig call in coordinator */
					FreeTriggerDesc(resultRelInfo->ri_TrigDesc);
					resultRelInfo->ri_TrigDesc = NULL;
				}
#endif /* ADB */
				/* Determine which triggers exist on this partition */
				has_before_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											  resultRelInfo->ri_TrigDesc->trig_insert_before_row);

				has_instead_insert_row_trig = (resultRelInfo->ri_TrigDesc &&
											   resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

				/*
				 * Disable multi-inserts when the partition has BEFORE/INSTEAD
				 * OF triggers, or if the partition is a foreign partition.
				 */
				leafpart_use_multi_insert = insertMethod == CIM_MULTI_CONDITIONAL &&
					!has_before_insert_row_trig &&
					!has_instead_insert_row_trig &&
					resultRelInfo->ri_FdwRoutine == NULL;

				/* Set the multi-insert buffer to use for this partition. */
				if (leafpart_use_multi_insert)
				{
					if (resultRelInfo->ri_CopyMultiInsertBuffer == NULL)
						CopyMultiInsertInfoSetupBuffer(&multiInsertInfo,
													   resultRelInfo);
				}
				else if (insertMethod == CIM_MULTI_CONDITIONAL &&
						 !CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
				{
					/*
					 * Flush pending inserts if this partition can't use
					 * batching, so rows are visible to triggers etc.
					 */
					CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}

				if (bistate != NULL)
					ReleaseBulkInsertStatePin(bistate);
				prevResultRelInfo = resultRelInfo;
			}

#ifdef ADB
			if (resultRelInfo->ri_RelationDesc->rd_auxlist != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot route inserted tuples to a has auxiliary table")));
#endif /* ADB */

			/*
			 * If we're capturing transition tuples, we might need to convert
			 * from the partition rowtype to root rowtype. But if there are no
			 * BEFORE triggers on the partition that could change the tuple,
			 * we can just remember the original unconverted tuple to avoid a
			 * needless round trip conversion.
			 */
			if (cstate->transition_capture != NULL)
				cstate->transition_capture->tcs_original_insert_tuple =
					!has_before_insert_row_trig ? myslot : NULL;

			/*
			 * We might need to convert from the root rowtype to the partition
			 * rowtype.
			 */
			map = resultRelInfo->ri_RootToPartitionMap;
			if (insertMethod == CIM_SINGLE || !leafpart_use_multi_insert)
			{
				/* non batch insert */
				if (map != NULL)
				{
					TupleTableSlot *new_slot;

					new_slot = resultRelInfo->ri_PartitionTupleSlot;
					myslot = execute_attr_map_slot(map->attrMap, myslot, new_slot);
				}
			}
			else
			{
				/*
				 * Prepare to queue up tuple for later batch insert into
				 * current partition.
				 */
				TupleTableSlot *batchslot;

				/* no other path available for partitioned table */
				Assert(insertMethod == CIM_MULTI_CONDITIONAL);

				batchslot = CopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
															resultRelInfo);

				if (map != NULL)
					myslot = execute_attr_map_slot(map->attrMap, myslot,
												   batchslot);
				else
				{
					/*
					 * This looks more expensive than it is (Believe me, I
					 * optimized it away. Twice.). The input is in virtual
					 * form, and we'll materialize the slot below - for most
					 * slot types the copy performs the work materialization
					 * would later require anyway.
					 */
					ExecCopySlot(batchslot, myslot);
					myslot = batchslot;
				}
			}

			/* ensure that triggers etc see the right relation  */
			myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		}

		skip_tuple = false;

		/* BEFORE ROW INSERT Triggers */
		if (has_before_insert_row_trig)
		{
			if (!ExecBRInsertTriggers(estate, resultRelInfo, myslot))
				skip_tuple = true;	/* "do nothing" */
		}

		if (!skip_tuple)
		{
			/*
			 * If there is an INSTEAD OF INSERT ROW trigger, let it handle the
			 * tuple.  Otherwise, proceed with inserting the tuple into the
			 * table or foreign table.
			 */
			if (has_instead_insert_row_trig)
			{
				ExecIRInsertTriggers(estate, resultRelInfo, myslot);
			}
			else
			{
				/* Compute stored generated columns */
				if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
					resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored
					ADB_ONLY_CODE(&& cstate->NextRowFrom != NextRowFromCoordinator))
					ExecComputeStoredGenerated(resultRelInfo, estate, myslot,
											   CMD_INSERT);

				/*
				 * If the target is a plain table, check the constraints of
				 * the tuple.
				 */
				if (resultRelInfo->ri_FdwRoutine == NULL &&
					resultRelInfo->ri_RelationDesc->rd_att->constr)
					ExecConstraints(resultRelInfo, myslot, estate);

				/*
				 * Also check the tuple against the partition constraint, if
				 * there is one; except that if we got here via tuple-routing,
				 * we don't need to if there's no BR trigger defined on the
				 * partition.
				 */
				if (resultRelInfo->ri_RelationDesc->rd_rel->relispartition &&
					(proute == NULL || has_before_insert_row_trig))
					ExecPartitionCheck(resultRelInfo, myslot, estate, true);

				/* Store the slot in the multi-insert buffer, when enabled. */
				if (insertMethod == CIM_MULTI || leafpart_use_multi_insert)
				{
					/*
					 * The slot previously might point into the per-tuple
					 * context. For batching it needs to be longer lived.
					 */
					ExecMaterializeSlot(myslot);

					/* Add this tuple to the tuple buffer */
					CopyMultiInsertInfoStore(&multiInsertInfo,
											 resultRelInfo, myslot,
											 cstate->line_buf.len,
											 cstate->cur_lineno);

					/*
					 * If enough inserts have queued up, then flush all
					 * buffers out to their tables.
					 */
					if (CopyMultiInsertInfoIsFull(&multiInsertInfo))
						CopyMultiInsertInfoFlush(&multiInsertInfo, resultRelInfo);
				}
				else
				{
					List	   *recheckIndexes = NIL;

					/* OK, store the tuple */
					if (resultRelInfo->ri_FdwRoutine != NULL)
					{
						myslot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
																				 resultRelInfo,
																				 myslot,
																				 NULL);

						if (myslot == NULL) /* "do nothing" */
							continue;	/* next tuple please */

						/*
						 * AFTER ROW Triggers might reference the tableoid
						 * column, so (re-)initialize tts_tableOid before
						 * evaluating them.
						 */
						myslot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
					}
					else
					{
						/* OK, store the tuple and create index entries for it */
						table_tuple_insert(resultRelInfo->ri_RelationDesc,
										   myslot, mycid, ti_options, bistate);

						if (resultRelInfo->ri_NumIndices > 0)
							recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
																   myslot,
																   estate,
																   false,
																   false,
																   NULL,
																   NIL);
					}

					/* AFTER ROW INSERT Triggers */
					ExecARInsertTriggers(estate, resultRelInfo, myslot,
										 recheckIndexes, cstate->transition_capture);

					list_free(recheckIndexes);
				}
			}

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger
			 * or FDW; this is the same definition used by nodeModifyTable.c
			 * for counting tuples inserted by an INSERT command.  Update
			 * progress of the COPY command as well.
			 */
			pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED,
										 ++processed);
		}
	}

	/* Flush any remaining buffered tuples */
	if (insertMethod != CIM_SINGLE)
	{
		if (!CopyMultiInsertInfoIsEmpty(&multiInsertInfo))
			CopyMultiInsertInfoFlush(&multiInsertInfo, NULL);
	}

	/* Done, clean up */
	error_context_stack = errcallback.previous;

	if (bistate != NULL)
		FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, target_resultRelInfo, cstate->transition_capture);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Allow the FDW to shut down */
	if (target_resultRelInfo->ri_FdwRoutine != NULL &&
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert != NULL)
		target_resultRelInfo->ri_FdwRoutine->EndForeignInsert(estate,
															  target_resultRelInfo);

	/* Tear down the multi-insert buffer data */
	if (insertMethod != CIM_SINGLE)
		CopyMultiInsertInfoCleanup(&multiInsertInfo);

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (proute)
		ExecCleanupTupleRouting(mtstate, proute);

	/* Close the result relations, including any trigger target relations */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);

	FreeExecutorState(estate);

	return processed;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'whereClause': WHERE clause from the COPY FROM command
 * 'filename': Name of server-local file to read, NULL for STDIN
 * 'is_program': true if 'filename' is program to execute
 * 'data_source_cb': callback that provides the input data
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyFromState, to be passed to NextCopyFrom and related functions.
 */
CopyFromState
BeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  Node *whereClause,
			  const char *filename,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  List *attnamelist,
			  List *options)
{
	CopyFromState cstate;
	bool		pipe = (filename == NULL);
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				num_defaults;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	int		   *defmap;
	ExprState **defexprs;
	MemoryContext oldcontext;
	bool		volatile_defexprs;
	const int	progress_cols[] = {
		PROGRESS_COPY_COMMAND,
		PROGRESS_COPY_TYPE,
		PROGRESS_COPY_BYTES_TOTAL
	};
	int64		progress_vals[] = {
		PROGRESS_COPY_COMMAND_FROM,
		0,
		0
	};

	/* Allocate workspace and zero all fields */
	cstate = (CopyFromStateData *) palloc0(sizeof(CopyFromStateData));

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Extract options from the statement node tree */
	ProcessCopyOptions(pstate, &cstate->opts, true /* is_from */ , options);

	/* Process the target relation */
	cstate->rel = rel;

	tupDesc = RelationGetDescr(cstate->rel);

	/* process commmon options or initialization */

	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->opts.force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->opts.force_null_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->opts.force_null)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_null);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->opts.force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->opts.convert_selectively)
	{
		List	   *attnums;
		ListCell   *cur;

		cstate->convert_select_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.convert_select);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg_internal("selected column \"%s\" not referenced by COPY",
										 NameStr(attr->attname))));
			cstate->convert_select_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->opts.file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();
	else
		cstate->file_encoding = cstate->opts.file_encoding;

	/*
	 * Look up encoding conversion function.
	 */
	if (cstate->file_encoding == GetDatabaseEncoding() ||
		cstate->file_encoding == PG_SQL_ASCII ||
		GetDatabaseEncoding() == PG_SQL_ASCII)
	{
		cstate->need_transcoding = false;
	}
	else
	{
		cstate->need_transcoding = true;
		cstate->conversion_proc = FindDefaultConversionProc(cstate->file_encoding,
															GetDatabaseEncoding());
	}

	cstate->copy_src = COPY_FILE;	/* default */

	cstate->whereClause = whereClause;

	MemoryContextSwitchTo(oldcontext);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/*
	 * Allocate buffers for the input pipeline.
	 *
	 * attribute_buf and raw_buf are used in both text and binary modes, but
	 * input_buf and line_buf only in text mode.
	 */
	cstate->raw_buf = palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;
	cstate->raw_reached_eof = false;

	if (!cstate->opts.binary)
	{
		/*
		 * If encoding conversion is needed, we need another buffer to hold
		 * the converted input data.  Otherwise, we can just point input_buf
		 * to the same buffer as raw_buf.
		 */
		if (cstate->need_transcoding)
		{
			cstate->input_buf = (char *) palloc(INPUT_BUF_SIZE + 1);
			cstate->input_buf_index = cstate->input_buf_len = 0;
		}
		else
			cstate->input_buf = cstate->raw_buf;
		cstate->input_reached_eof = false;

		initStringInfo(&cstate->line_buf);
	}

	initStringInfo(&cstate->attribute_buf);

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	num_defaults = 0;
	volatile_defexprs = false;

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info */
		if (cstate->opts.binary)
			getTypeBinaryInputInfo(att->atttypid,
								   &in_func_oid, &typioparams[attnum - 1]);
		else
			getTypeInputInfo(att->atttypid,
							 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum) && !att->attgenerated)
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Expr	   *defexpr = (Expr *) build_column_default(cstate->rel,
																attnum);

			if (defexpr != NULL)
			{
				/* Run the expression through planner */
				defexpr = expression_planner(defexpr);

				/* Initialize executable expression in copycontext */
				defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;

				/*
				 * If a default expression looks at the table being loaded,
				 * then it could give the wrong answer when using
				 * multi-insert. Since database access can be dynamic this is
				 * hard to test for exactly, so we use the much wider test of
				 * whether the default expression is volatile. We allow for
				 * the special case of when the default expression is the
				 * nextval() of a sequence which in this specific case is
				 * known to be safe for use with the multi-insert
				 * optimization. Hence we use this special case function
				 * checker rather than the standard check for
				 * contain_volatile_functions().
				 */
				if (!volatile_defexprs)
					volatile_defexprs = contain_volatile_functions_not_nextval((Node *) defexpr);
			}
		}
	}


	/* initialize progress */
	pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
								  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);
	cstate->bytes_processed = 0;

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->defmap = defmap;
	cstate->defexprs = defexprs;
	cstate->volatile_defexprs = volatile_defexprs;
	cstate->num_defaults = num_defaults;
	cstate->is_program = is_program;

	if (data_source_cb)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK;
		cstate->copy_src = COPY_CALLBACK;
		cstate->data_source_cb = data_source_cb;
	}
	else if (pipe)
	{
		progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;
		Assert(!is_program);	/* the grammar does not allow this */
		if (whereToSendOutput == DestRemote)
			ReceiveCopyBegin(cstate);
		else
			cstate->copy_file = stdin;
	}
	else
	{
		cstate->filename = pstrdup(filename);

		if (cstate->is_program)
		{
			progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
			cstate->copy_file = OpenPipeStream(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not execute command \"%s\": %m",
								cstate->filename)));
		}
		else
		{
			struct stat st;

			progress_vals[1] = PROGRESS_COPY_TYPE_FILE;
			cstate->copy_file = AllocateFile(cstate->filename, PG_BINARY_R);
			if (cstate->copy_file == NULL)
			{
				/* copy errno because ereport subfunctions might change it */
				int			save_errno = errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open file \"%s\" for reading: %m",
								cstate->filename),
						 (save_errno == ENOENT || save_errno == EACCES) ?
						 errhint("COPY FROM instructs the PostgreSQL server process to read a file. "
								 "You may want a client-side facility such as psql's \\copy.") : 0));
			}

			if (fstat(fileno(cstate->copy_file), &st))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m",
								cstate->filename)));

			if (S_ISDIR(st.st_mode))
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("\"%s\" is a directory", cstate->filename)));

			progress_vals[2] = st.st_size;
		}
	}

	pgstat_progress_update_multi_param(3, progress_cols, progress_vals);

	if (cstate->opts.binary)
	{
		/* Read and verify binary header */
		ReceiveCopyBinaryHeader(cstate);
	}

	/* create workspace for CopyReadAttributes results */
	if (!cstate->opts.binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}

#ifdef ADB
	if (RelationGetLocInfoForRemote(rel))
	{
		ReduceInfo *rinfo;

		/* make reduce expr */
		rinfo = MakeReduceInfoFromLocInfo(rel->rd_locator_info,
										  NIL,
										  RelationGetRelid(rel),
										  1 /* only have one relation */);
		cstate->cs_reduce_expr = CreateExprUsingReduceInfo(rinfo);
		cstate->aux_info = MakeAuxRelCopyInfo(rel);
		if (cstate->aux_info)
		{
			List *rnodes;
			cstate->exec_cluster_flag = EXEC_CLUSTER_FLAG_USE_SELF_AND_MEM_REDUCE;
			cstate->fd_copied_ctid = BufFileCreateTemp(false);
			cstate->mem_copy_toc = makeStringInfo();

			begin_mem_toc_insert(cstate->mem_copy_toc, AUX_REL_COPY_INFO);
			SerializeAuxRelCopyInfo(cstate->mem_copy_toc, cstate->aux_info);
			end_mem_toc_insert(cstate->mem_copy_toc, AUX_REL_COPY_INFO);

			begin_mem_toc_insert(cstate->mem_copy_toc, AUX_REL_MAIN_NODES);
			rnodes = list_copy(rel->rd_locator_info->nodeids);
			rnodes = lappend_oid(rnodes, PGXCNodeOid);
			saveNode(cstate->mem_copy_toc, (Node*)rnodes);
			end_mem_toc_insert(cstate->mem_copy_toc, AUX_REL_MAIN_NODES);
			list_free(rnodes);
		}

		/*
		 * when has insert before or after trigger, we call trigger and save tuple to tuplestore,
		 * when all row readed, read tuple from tuplestore and send it to datanode
		 */
		if (rte_has_any_cluster_unsafe_subclass(pstate->p_target_nsitem->p_rte, CMD_INSERT))
		{
			cstate->NextRowFrom = NextLineCallTrigger;
			cstate->cs_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		}else
		{
			CopyStmt *stmt = makeClusterCopyFromStmt(rel, cstate->opts.freeze);
			List *rnodes = adbGetUniqueNodeOids(rel->rd_locator_info->nodeids);
			cstate->NextRowFrom = AddNumberNextCopyFrom;
			cstate->list_connect = ExecStartClusterCopy(rnodes,
														stmt,
														cstate->mem_copy_toc,
														cstate->exec_cluster_flag);
			Assert(list_length(cstate->list_connect) == list_length(rnodes));
			list_free(rnodes);
		}
		cstate->cs_tupleslot = makeClusterCopySlot(cstate->rel);
		cstate->cs_convert = create_type_convert(cstate->cs_tupleslot->tts_tupleDescriptor, true, false);
		if (cstate->cs_convert)
			cstate->cs_tsConvert = MakeSingleTupleTableSlot(cstate->cs_convert->out_desc, &TTSOpsMinimalTuple);
	}
#endif /* ADB */

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
void
EndCopyFrom(CopyFromState cstate)
{
#ifdef ADB
	if (cstate->cs_tuplestore)
		tuplestore_end(cstate->cs_tuplestore);
	if (cstate->cs_tupleslot)
		ExecDropSingleTupleTableSlot(cstate->cs_tupleslot);
	if (cstate->cs_tsConvert)
		ExecDropSingleTupleTableSlot(cstate->cs_tsConvert);
	if (cstate->cs_convert)
		free_type_convert(cstate->cs_convert, true);
	if (cstate->list_connect)
	{
		ListCell *lc;
		PGconn *conn;
		PGresult *res;
		ExecStatusType rst;

		foreach(lc, cstate->list_connect)
		{
			conn = lfirst(lc);
			if (PQisCopyInState(conn))
				PQputCopyEnd(conn, NULL);
		}

		foreach(lc, cstate->list_connect)
		{
			for(;;)
			{
				CHECK_FOR_INTERRUPTS();
				res = PQgetResult(conn);
				if (res == NULL)
					break;
				rst = PQresultStatus(res);
				switch(rst)
				{
				case PGRES_EMPTY_QUERY:
				case PGRES_COMMAND_OK:
					break;
				case PGRES_TUPLES_OK:
				case PGRES_SINGLE_TUPLE:
					PQclear(res);
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("datanode copy command result tuples"),
							 errnode(PQNConnectName(conn))));
					break;
				case PGRES_COPY_OUT:
					{
						const char *msg;
						int len;
						PQclear(res);
						res = NULL;
						len = PQgetCopyDataBuffer(conn, &msg, false);
						if (len > 0)
							clusterRecvTuple(NULL, msg, len, NULL, conn);
					}
					break;
				case PGRES_COPY_IN:
				case PGRES_COPY_BOTH:
					/* copy in should not happen */
					PQputCopyEnd(conn, NULL);
					break;
				case PGRES_NONFATAL_ERROR:
					PQNReportResultError(res, conn, NOTICE, false);
					break;
				case PGRES_BAD_RESPONSE:
				case PGRES_FATAL_ERROR:
					PQNReportResultError(res, conn, ERROR, true);
					break;
				case PGRES_PIPELINE_SYNC:
				case PGRES_PIPELINE_ABORTED:
					PQclear(res);
					ereport(ERROR,
							errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("datanode copy command result pipeline mode"),
							errnode(PQNConnectName(conn)));
					break;
				}
				PQclear(res);
			}
		}
	}
	if (cstate->fd_copied_ctid)
		BufFileClose(cstate->fd_copied_ctid);
#endif /* ADB */

	/* No COPY FROM related resources except memory. */
	if (cstate->is_program)
	{
		ClosePipeFromProgram(cstate);
	}
	else
	{
		if (cstate->filename != NULL && FreeFile(cstate->copy_file))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							cstate->filename)));
	}

	pgstat_progress_end_command();

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Closes the pipe from an external program, checking the pclose() return code.
 */
static void
ClosePipeFromProgram(CopyFromState cstate)
{
	int			pclose_rc;

	Assert(cstate->is_program);

	pclose_rc = ClosePipeStream(cstate->copy_file);
	if (pclose_rc == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
	else if (pclose_rc != 0)
	{
		/*
		 * If we ended a COPY FROM PROGRAM before reaching EOF, then it's
		 * expectable for the called program to fail with SIGPIPE, and we
		 * should not report that as an error.  Otherwise, SIGPIPE indicates a
		 * problem.
		 */
		if (!cstate->raw_reached_eof &&
			wait_result_is_signal(pclose_rc, SIGPIPE))
			return;

		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("program \"%s\" failed",
						cstate->filename),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
	}
}

#ifdef ADB
int
SimpleNextCopyFromNewFE(SimpleCopyDataFunction fun, void *context)
{
	StringInfoData	buf;
	int				mtype;
	int				result = 0;

	initStringInfo(&buf);

readmessage:
	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();
	mtype = pq_getbyte();
	if (mtype == EOF)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	if (pq_getmessage(&buf, PQ_SMALL_MESSAGE_LIMIT))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	RESUME_CANCEL_INTERRUPTS();
	switch (mtype)
	{
		case 'd':	/* CopyData */
			break;
		case 'c':	/* CopyDone */
			/* COPY IN correctly terminated by frontend */
			result = 0;
			goto end_read;
		case 'f':	/* CopyFail */
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("COPY from stdin failed: %s",
							pq_getmsgstring(&buf))));
			break;
		case 'H':	/* Flush */
		case 'S':	/* Sync */

			/*
				* Ignore Flush/Sync for the convenience of client
				* libraries (such as libpq) that may send those
				* without noticing that the command they just
				* sent was COPY.
				*/
			goto readmessage;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected message type 0x%02X during COPY from stdin",
							mtype)));
			break;
	}

	result = (*fun)(context, buf.data+buf.cursor, buf.len - buf.cursor);
	if (result == 0)
		goto readmessage;

end_read:
	pfree(buf.data);
	return result;
}

bool
GetCopyDataFromNewFE(StringInfo buf, bool flush_write, bool copy_done_ok)
{
	int				mtype;
	if (flush_write)
		pq_flush();

readmessage:
	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();
	mtype = pq_getbyte();
	if (mtype == EOF)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	if (pq_getmessage(buf, PQ_LARGE_MESSAGE_LIMIT))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection with an open transaction")));
	RESUME_CANCEL_INTERRUPTS();
	switch (mtype)
	{
		case 'd':	/* CopyData */
			break;
		case 'c':	/* CopyDone */
			/* COPY IN correctly terminated by frontend */
			if (copy_done_ok == false)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection with an open transaction")));
			}
			return false;
		case 'f':	/* CopyFail */
			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("COPY from stdin failed: %s",
							pq_getmsgstring(buf))));
			break;
		case 'H':	/* Flush */
		case 'S':	/* Sync */

			/*
				* Ignore Flush/Sync for the convenience of client
				* libraries (such as libpq) that may send those
				* without noticing that the command they just
				* sent was COPY.
				*/
			goto readmessage;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("unexpected message type 0x%02X during COPY from stdin",
							mtype)));
			break;
	}

	return true;
}

void
DoClusterCopy(CopyStmt *stmt, StringInfo mem_toc)
{
	CopyFromState	cstate;
	Relation		rel;
	MemoryContext	oldcontext;
	ParseState	   *pstate;
	ParseNamespaceItem
				   *nsitem;
	RangeTblEntry  *rte;
	List		   *rnodes = NIL;

	if (stmt->is_from == false)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster copy only support copy from")));
	}

	if (stmt->relation == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("cluster copy no target relation")));
	}
	rel = table_openrv(stmt->relation, RowExclusiveLock);
	/* check read-only transaction and parallel mode */
	if (XactReadOnly && !rel->rd_islocaltemp)
		PreventCommandIfReadOnly("COPY FROM");
	PreventCommandIfParallelMode("COPY FROM");

	pstate = make_parsestate(NULL);

	nsitem = addRangeTableEntryForRelation(pstate, rel, RowExclusiveLock, NULL, false, false);
	rte = nsitem->p_rte;
	rte->requiredPerms = ACL_INSERT;
	/* ADBTODO: rte->insertedCols and ExecCheckRTPerms(pstate->p_rtable, true); */

	cstate = BeginCopyFrom(pstate, rel, NULL, NULL, false, NULL, NULL, stmt->options);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = ACL_INSERT;
	cstate->range_table = pstate->p_rtable;

	/* Initialize state variables */
	cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	cstate->opts.binary = true;
	cstate->copy_src = COPY_FRONTEND;
	cstate->fe_msgbuf = makeStringInfo();

	cstate->cs_tupleslot = makeClusterCopySlot(rel);
	cstate->cs_convert = create_type_convert(cstate->cs_tupleslot->tts_tupleDescriptor,
											 false,
											 true);
	if (cstate->cs_convert)
		cstate->cs_tsConvert = MakeSingleTupleTableSlot(cstate->cs_convert->out_desc, &TTSOpsMinimalTuple);

	cstate->aux_info = LoadAuxRelCopyInfo(mem_toc);
	if (cstate->aux_info)
	{
		StringInfoData buf;
		buf.data = mem_toc_lookup(mem_toc, AUX_REL_MAIN_NODES, &buf.len);
		if (buf.data == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("Can not found relation storage info in cluster message")));
		buf.maxlen = buf.len;
		buf.cursor = 0;
		rnodes = (List*)loadNode(&buf);
		cstate->fd_copied_ctid = BufFileCreateTemp(false);
	}

	/* func_data auto set to target relation TupleTableSlot in CopyFrom if it is null */
	cstate->NextRowFrom = NextRowFromCoordinator;
	CopyFrom(cstate);

	MemoryContextSwitchTo(oldcontext);

	if (cstate->aux_info)
		ApplyCopyToAuxiliary(cstate, rnodes);

	table_close(rel, RowExclusiveLock);
	EndCopyFrom(cstate);
}

uint64
CoordinatorCopyFrom(CopyFromState cstate)
{
	TupleTypeConvert   *type_convert;
	ReduceExprState	   *expr_state;
	TupleTableSlot	   *ts_convert;
	ListCell		   *lc;
	PGconn			   *conn;
	ResultRelInfo	   *resultRelInfo;
	EState			   *estate = CreateExecutorState();
	ExprContext		   *econtext = GetPerTupleExprContext(estate);
	MemoryContext		oldcontext = CurrentMemoryContext;

	ErrorContextCallback errcallback;
	time_t last_time;
	time_t cur_time;
	StringInfoData	buf;
	ExprDoneCond done;
	bool isnull;

	Assert(cstate->rel);
	Assert(list_length(cstate->range_table) == 1);
	/* force refresh currentCommandId */
	GetCurrentCommandId(true);

	if (cstate->rel->rd_rel->relkind != RELKIND_RELATION &&
		cstate->rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		!(cstate->rel->trigdesc &&
		  cstate->rel->trigdesc->trig_insert_instead_row))
	{
		if (cstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to foreign table \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else if (cstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(cstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(cstate->rel))));
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	ExecInitRangeTable(estate, cstate->range_table);
	resultRelInfo = makeNode(ResultRelInfo);
	ExecInitResultRelation(estate, resultRelInfo, 1);

	/* Verify the named relation is a valid target for INSERT */
	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

	cstate->mtstate = makeNode(ModifyTableState);
	cstate->mtstate->ps.state = estate;
	cstate->mtstate->operation = CMD_INSERT;
	cstate->mtstate->mt_nrels = 1;
	cstate->mtstate->resultRelInfo = resultRelInfo;
	cstate->mtstate->rootResultRelInfo = resultRelInfo;

	if (cstate->whereClause)
		cstate->qualexpr = ExecInitQual(castNode(List, cstate->whereClause),
										&cstate->mtstate->ps);

	type_convert = cstate->cs_convert;
	ts_convert = cstate->cs_tsConvert;
	if (cstate->cs_reduce_state == NULL)
	{
		cstate->cs_reduce_state = ExecInitReduceExpr(cstate->cs_reduce_expr);
	}
	expr_state = cstate->cs_reduce_state;
	initStringInfo(&buf);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	for(last_time = time(NULL);;)
	{
		TupleTableSlot *slot;

		CHECK_FOR_INTERRUPTS();

		cur_time = time(NULL);
		if (cur_time != last_time)
		{
			/* check datanode error */
			PQNListExecFinish(cstate->list_connect,
							  NULL,
							  &PQNFalseHookFunctions,
							  false);
			last_time = cur_time;
		}
		ExecClearTuple(cstate->cs_tupleslot);
		if (cstate->cs_tsConvert != NULL)
			ExecClearTuple(cstate->cs_tsConvert);
		ResetPerTupleExprContext(estate);
		/* Switch into its memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		slot = (*cstate->NextRowFrom)(cstate, econtext, cstate->func_data);
		if (TupIsNull(slot))
			break;

		if (type_convert)
			slot = do_type_convert_slot_out(type_convert, slot, ts_convert, false);

		resetStringInfo(&buf);
		econtext->ecxt_scantuple = slot;
		for(;;)
		{
			Datum datum = ExecEvalReduceExpr(expr_state, econtext, &isnull, &done);
			if (done != ExprEndResult)
			{
				/* send to remote */
				if(isnull)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("ReduceExpr return a null value")));
				conn = PQNFindConnUseOid(DatumGetObjectId(datum));
				Assert(conn != NULL);

				if (buf.len == 0)
					serialize_slot_message(&buf,
										   slot,
										   type_convert ? CLUSTER_MSG_CONVERT_TUPLE:CLUSTER_MSG_TUPLE_DATA);
				if (PQputCopyData(conn, buf.data, buf.len) != 1 ||
					PQflush(conn) < 0)
				{
					char *err = PQerrorMessage(conn);
					int len = strlen(err);
					while(len > 0 && err[--len] == '\n')
						err[len] = '\0';
					ereport(ERROR,
							(errmsg("%s", err),
							 errnode(PQNConnectName(conn))));
				}
			}
			if (done != ExprMultipleResult)
				break;
		}
	}

	/* Done, clean up */
	error_context_stack = errcallback.previous;
	foreach(lc, cstate->list_connect)
	{
		if (PQputCopyEnd(lfirst(lc), NULL) < 0)
		{
			ereport(ERROR,
					(errmsg("%s", PQerrorMessage(lfirst(lc))),
					 errnode(PQNConnectName(lfirst(lc)))));
		}
	}
	PQNFlush(cstate->list_connect, true);
	PQNListExecFinish(cstate->list_connect,
					  NULL,
					  &PQNDefaultHookFunctions,
					  true);


	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, resultRelInfo, cstate->transition_capture);

	ExecCloseIndices(resultRelInfo);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	MemoryContextSwitchTo(oldcontext);

	if (cstate->aux_info)
	{
		List *list = list_copy(cstate->rel->rd_locator_info->nodeids);
		list = lappend_oid(list, PGXCNodeOid);
		ApplyCopyToAuxiliary(cstate, list);
		list_free(list);
	}

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Close the result relations, including any trigger target relations */
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);

	FreeExecutorState(estate);

	return cstate->count_tuple;
}

static TupleTableSlot*
NextLineCallTrigger(CopyFromState cstate, ExprContext *econtext, void *data)
{
	EState *estate = econtext->ecxt_estate;
	PartitionTupleRouting *proute = NULL;
	ResultRelInfo  *resultRelInfo;
	ResultRelInfo *target_resultRelInfo;
	TupleTableSlot *slot;
	TupleTableSlot *myslot;
	TupleTableSlot *relslot;
	Tuplestorestate *store;
	TupleDesc desc;
	TupleConversionMap *tcmap;
	MemoryContext query_context = estate->es_query_cxt;
	MemoryContext old_context = MemoryContextSwitchTo(query_context);
	MemoryContext tup_context = GetPerTupleMemoryContext(estate);
	uint64 processed = 0L;

	target_resultRelInfo = resultRelInfo = estate->es_result_relations[0];

	/*
	 * If there are any triggers with transition tables on the named relation,
	 * we need to be prepared to capture transition tuples.
	 */
	cstate->transition_capture =
		MakeTransitionCaptureState(cstate->rel->trigdesc,
								   RelationGetRelid(cstate->rel),
								   CMD_INSERT);

	/*
	 * If the named relation is a partitioned table, initialize state for
	 * CopyFrom tuple routing.
	 */
	if (cstate->rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		proute = ExecSetupPartitionTupleRouting(estate, cstate->rel);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	desc = RelationGetDescr(cstate->rel);
	relslot = ExecAllocTableSlot(&estate->es_tupleTable, desc, &TTSOpsVirtual);

	myslot = cstate->cs_tupleslot;
	store = cstate->cs_tuplestore;

	for(;;)
	{
		MemoryContextReset(tup_context);
		MemoryContextSwitchTo(tup_context);

		ExecClearTuple(relslot);
		if (!NextCopyFrom(cstate, econtext, relslot->tts_values, relslot->tts_isnull))
			break;

		/*
		 * Constraints and where clause might reference the tableoid column,
		 * so (re-)initialize tts_tableOid before evaluating them.
		 */
		relslot->tts_tableOid = RelationGetRelid(target_resultRelInfo->ri_RelationDesc);
		ExecStoreVirtualTuple(relslot);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(query_context);

		if (cstate->whereClause)
		{
			econtext->ecxt_scantuple = relslot;
			/* Skip items that don't match COPY's WHERE clause */
			if (!ExecQual(cstate->qualexpr, econtext))
				continue;
		}

		/* Determine the partition to heap_insert the tuple into */
		if (proute && !TupIsNull(myslot))
		{
			resultRelInfo = ExecFindPartition(cstate->mtstate, target_resultRelInfo,
											  proute, myslot, estate);
			if (resultRelInfo->ri_RelationDesc->rd_auxlist != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot route inserted tuples to a has auxiliary table")));

			/*
			 * If we're capturing transition tuples, we might need to convert
			 * from the partition rowtype to parent rowtype.
			 * And if there are any BEFORE triggers on the partition
			 */
			tcmap = resultRelInfo->ri_RootToPartitionMap;
			if (tcmap &&
				(cstate->transition_capture != NULL ||
				 (resultRelInfo->ri_TrigDesc &&
				  resultRelInfo->ri_TrigDesc->trig_insert_before_row)))
			{
				slot = execute_attr_map_slot(resultRelInfo->ri_RootToPartitionMap->attrMap,
											 relslot,
											 resultRelInfo->ri_PartitionTupleSlot);
			}else
			{
				slot = relslot;
			}

			/* ensure that triggers etc see the right relation  */
			slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
		}else
		{
			slot = relslot;
		}

		/* BEFORE ROW INSERT Triggers */
		if (resultRelInfo->ri_TrigDesc &&
			resultRelInfo->ri_TrigDesc->trig_insert_before_row &&
			!TupIsNull(slot) &&
			ExecBRInsertTriggers(estate, resultRelInfo, slot) == false)
		{
			slot = NULL;
		}

		if (!TupIsNull(slot))
		{
			/* Compute stored generated columns */
			if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
				resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored)
				ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);

			ExecARInsertTriggers(estate, resultRelInfo, slot, NIL /* coordinator no index to check */, cstate->transition_capture);

			/* tstuple is like rel tuple, but we append a line number, now we make a new tuple */
			MemoryContextSwitchTo(tup_context);

			ExecClearTuple(myslot);
			memcpy(myslot->tts_values, relslot->tts_values, sizeof(Datum) * desc->natts);
			memcpy(myslot->tts_isnull, relslot->tts_isnull, sizeof(bool) * desc->natts);
			Assert(TupleDescAttr(myslot->tts_tupleDescriptor, desc->natts)->atttypid == INT4OID);
			myslot->tts_values[desc->natts] = Int32GetDatum(cstate->cur_lineno);
			myslot->tts_isnull[desc->natts] = false;
			ExecStoreVirtualTuple(myslot);

			tuplestore_puttupleslot(store, myslot);
		}
		++processed;
	}

	/* Close all the partitioned tables, leaf partitions, and their indices */
	if (proute)
		ExecCleanupTupleRouting(cstate->mtstate, proute);

	cstate->count_tuple = processed;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	cstate->cur_lineno = 0;

	/* start cluster copy */
	MemoryContextSwitchTo(cstate->copycontext);
	cstate->list_connect = ExecStartClusterCopy(cstate->rel->rd_locator_info->nodeids,
												makeClusterCopyFromStmt(cstate->rel, cstate->opts.freeze),
												cstate->mem_copy_toc,
												cstate->exec_cluster_flag);

	MemoryContextSwitchTo(old_context);
	cstate->NextRowFrom = NextRowFromTuplestore;
	return NextRowFromTuplestore(cstate, econtext, data);
}

static TupleTableSlot*
NextRowFromTuplestore(CopyFromState cstate, ExprContext *econtext, void *data)
{
	TupleTableSlot *slot = cstate->cs_tupleslot;
	if (tuplestore_gettupleslot(cstate->cs_tuplestore, true, true, slot) == false)
		return ExecClearTuple(slot);

	slot_getallattrs(slot);
	Assert(TupleDescAttr(slot->tts_tupleDescriptor, slot->tts_tupleDescriptor->natts - 1)->atttypid == INT4OID);
	cstate->cur_lineno = DatumGetInt32(slot->tts_values[slot->tts_tupleDescriptor->natts-1]);

	return slot;
}

static TupleTableSlot*
AddNumberNextCopyFrom(CopyFromState cstate, ExprContext *econtext, void *data)
{
	TupleTableSlot *slot = cstate->cs_tupleslot;

re_try_:
	if (NextCopyFrom(cstate, econtext, slot->tts_values, slot->tts_isnull) == false)
		return ExecClearTuple(slot);
	ExecStoreVirtualTuple(slot);

	if (cstate->qualexpr)
	{
		econtext->ecxt_scantuple = slot;
		if (ExecQual(cstate->qualexpr, econtext) == false)
		{
			ExecClearTuple(slot);
			goto re_try_;
		}
	}

	++(cstate->count_tuple);

	Assert(TupleDescAttr(slot->tts_tupleDescriptor, slot->tts_tupleDescriptor->natts - 1)->atttypid == INT4OID);
	slot->tts_values[slot->tts_tupleDescriptor->natts-1] = Int32GetDatum(cstate->cur_lineno);
	slot->tts_isnull[slot->tts_tupleDescriptor->natts-1] = false;

	return slot;
}

static TupleTableSlot*
makeClusterCopySlot(Relation rel)
{
	TupleDesc desc = RelationGetDescr(rel);
	TupleDesc new_desc;
	int natts = desc->natts+1;

	/* create and copy TupleDesc */
	new_desc = CreateTemplateTupleDesc(natts);
	for(natts=desc->natts;natts > 0;)
	{
		TupleDescCopyEntry(new_desc, natts, desc, natts);
		--natts;
	}

	/* entry line number */
	natts = desc->natts;
	++natts;
	TupleDescInitEntry(new_desc, (AttrNumber)natts, "lineno", INT4OID, -1, 0);
	TupleDescInitEntryCollation(new_desc, (AttrNumber)natts, InvalidOid);

	return MakeSingleTupleTableSlot(new_desc, &TTSOpsMinimalTuple);
}

static CopyStmt*
makeClusterCopyFromStmt(Relation rel, bool freeze)
{
	List	   *options;
	DefElem	   *def;
	CopyStmt   *stmt = makeNode(CopyStmt);
	stmt->relation = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								  pstrdup(RelationGetRelationName(rel)),
								  -1);
	stmt->is_from = true;

	def = makeDefElem("encoding",
					  (Node*)makeString((char*)GetDatabaseEncodingName()),
					  -1);
	options = list_make1(def);

	if (freeze)
		options = lappend(options, makeDefElem("freeze", (Node*)makeInteger(true), -1));

	stmt->options = options;

	return stmt;
}

void SerializeAuxRelCopyInfo(StringInfo buf, List *list)
{
	AuxiliaryRelCopy *aux;
	ListCell *lc;
	AssertArg(buf && list_length(list) > 0);

	appendBinaryStringInfo(buf, (char*)&list->length, sizeof(list->length));
	foreach(lc, list)
	{
		aux = lfirst(lc);
		appendStringInfoString(buf, aux->schemaname);
		appendStringInfoChar(buf, '\0');

		appendStringInfoString(buf, aux->relname);
		appendStringInfoChar(buf, '\0');

		saveNode(buf, (Node*)aux->targetList);
		saveNode(buf, (Node*)aux->reduce);
		appendBinaryStringInfo(buf, (char*)&aux->id, sizeof(aux->id));
	}
}

static List* LoadAuxRelCopyInfo(StringInfo mem_toc)
{
	StringInfoData buf;
	buf.data = mem_toc_lookup(mem_toc, AUX_REL_COPY_INFO, &buf.maxlen);
	if (buf.data != NULL)
	{
		buf.len = buf.maxlen;
		buf.cursor = 0;
		return RestoreAuxRelCopyInfo(&buf);
	}
	return NIL;
}

List* RestoreAuxRelCopyInfo(StringInfo buf)
{
	List *list;
	AuxiliaryRelCopy *aux;
	int count;

	pq_copymsgbytes(buf, (char*)&count, sizeof(count));
	list = NIL;

	while(--count >= 0)
	{
		aux = palloc(sizeof(*aux));
		aux->schemaname = pstrdup(pq_getmsgrawstring(buf));
		aux->relname = pstrdup(pq_getmsgrawstring(buf));
		aux->targetList = (List*)loadNode(buf);
		aux->reduce = (Expr*)loadNode(buf);
		pq_copymsgbytes(buf, (char*)&aux->id, sizeof(aux->id));

		list = lappend(list, aux);
	}

	return list;
}

AuxiliaryRelCopy *
MakeAuxRelCopyInfoFromMaster(Relation masterrel, Relation auxrel, int auxid)
{
	AuxiliaryRelCopy   *aux_copy;
	ReduceInfo		   *rinfo;

	aux_copy = palloc0(sizeof(*aux_copy));
	aux_copy->schemaname = get_namespace_name(RelationGetNamespace(auxrel));
	aux_copy->relname = pstrdup(RelationGetRelationName(auxrel));
	aux_copy->targetList = MakeMainRelTargetForAux(masterrel, auxrel, 1, true);
	if (auxrel->rd_locator_info)
		rinfo = MakeReduceInfoFromLocInfo(auxrel->rd_locator_info, NIL, RelationGetRelid(auxrel), 1);
	else
		rinfo = MakeCoordinatorReduceInfo();
	aux_copy->reduce = CreateExprUsingReduceInfo(rinfo);

	aux_copy->id = auxid;

	return aux_copy;
}

List*
MakeAuxRelCopyInfo(Relation rel)
{
	List			   *result;
	ListCell		   *lc;
	AuxiliaryRelCopy   *aux_copy;
	Relation			aux_rel;
	int					id = 0;

	if (rel->rd_auxlist == NIL)
		return NULL;

	result = NIL;
	foreach(lc, rel->rd_auxlist)
	{
		aux_rel = table_open(lfirst_oid(lc), NoLock);
		if (RELATION_IS_OTHER_TEMP(aux_rel))
		{
			table_close(aux_rel, NoLock);
			continue;
		}

		aux_copy = MakeAuxRelCopyInfoFromMaster(rel, aux_rel, ++id);

		result = lappend(result, aux_copy);

		table_close(aux_rel, NoLock);
	}

	return result;
}

static void
ApplyCopyToAuxiliary(CopyFromState parent, List *rnodes)
{
	ListCell *lc;
	AuxiliaryRelCopy *aux;
	Relation rel;
	RangeVar range;
	ExprContext *econtext;
	TidBufFileScanState	state;
	TupleDesc	desc;
	MemoryContext aux_context;
	MemoryContext old_context;

	aux_context = AllocSetContextCreate(CurrentMemoryContext,
										"ApplyCopyToAuxiliary",
										ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(aux_context);

	MemSet(&range, 0, sizeof(range));
	NodeSetTag(&range, T_RangeVar);

	MemSet(&state, 0, sizeof(state));
	state.file = parent->fd_copied_ctid;
	state.rel = parent->rel;
	state.base_slot = table_slot_create(parent->rel, NULL);
	state.out_slot = MakeTupleTableSlot(NULL, &TTSOpsVirtual);

	econtext = CreateStandaloneExprContext();

	foreach(lc, parent->aux_info)
	{
		aux = lfirst(lc);
		range.schemaname = aux->schemaname;
		range.relname = aux->relname;
		rel = table_openrv_extended(&range, RowExclusiveLock, true);
		BufFileSeek(parent->fd_copied_ctid, 0, 0, SEEK_SET);

		if (rel)
			desc = RelationGetDescr(rel);
		else
			desc = ExecTypeFromTL(aux->targetList);
		ExecSetSlotDescriptor(state.out_slot, desc);
		state.project = ExecBuildProjectionInfo(aux->targetList,
												econtext,
												state.out_slot,
												NULL,
												RelationGetDescr(parent->rel));

		if (rel)
		{
			ClusterCopyFromReduce(rel, aux->reduce, rnodes, aux->id, parent->opts.freeze, NextRowFromTidBufFile, &state);
			table_close(rel, RowExclusiveLock);
		}else
		{
			ClusterDummyCopyFromReduce(aux->targetList, aux->reduce, rnodes, aux->id, NextRowFromTidBufFile, &state);
		}

		ReScanExprContext(econtext);
		if (rel == NULL)
			FreeTupleDesc(desc);
	}

	FreeExprContext(econtext, true);
	ExecDropSingleTupleTableSlot(state.out_slot);
	ExecDropSingleTupleTableSlot(state.base_slot);

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(aux_context);
}

static TupleTableSlot*
CallNextRow(void *data, ExprContext *econtext)
{
	CopyFromReduceState	   *state = data;
	TupleTableSlot		   *slot;
	slot = (*state->NextRow)(state->cstate, state->drio.econtext, state->func_data);
	econtext->ecxt_scantuple = slot;
	return slot;
}

static TupleTableSlot*
CallNextRowWithDroppedColumn(void *data, ExprContext *econtext)
{
	int i = 0;
	CopyFromReduceState	   *state = data;
	TupleTableSlot		   *slot;
	slot = (*state->NextRow)(state->cstate, state->drio.econtext, state->func_data);

	if (!TupIsNull(slot))
	{
		/* Make sure the tuple is fully deconstructed */
		slot_getallattrs(slot);
		/* Be sure to null out any dropped columns */
		for(i=slot->tts_tupleDescriptor->natts;(--i)>=0;)
		{
			if (TupleDescAttr(slot->tts_tupleDescriptor, i)->attisdropped)
						slot->tts_isnull[i] = true;
		}
	}
	econtext->ecxt_scantuple = slot;
	return slot;
}

static void
InitCopyFromReduce(CopyFromReduceState *state, TupleDesc desc, Expr *reduce,
				   List *rnodes, int id, CustomNextRowFunction fun, void *func_data)
{
	int i = 0;
	bool is_with_dropped_column = false;
	DynamicReduceMQ drmq;
	AssertArg(IsA(rnodes, OidList));

	MemSet(state, 0, sizeof(*state));

	state->dsm_seg = dsm_create(sizeof(*drmq), 0);
	drmq = dsm_segment_address(state->dsm_seg);
	DynamicReduceInitFetch(&state->drio,
						   state->dsm_seg,
						   desc, DR_MQ_INIT_ATTACH_SEND_RECV,
						   drmq->worker_sender_mq, sizeof(drmq->worker_sender_mq),
						   drmq->reduce_sender_mq, sizeof(drmq->reduce_sender_mq));
	state->drio.expr_state = ExecInitReduceExpr(reduce);

	/*check desc has any dropped columns */
	for(i=desc->natts;(--i)>=0;)
	{
		if (TupleDescAttr(desc, i)->attisdropped)
		{
			is_with_dropped_column = true;
			break;
		}
	}

	if (is_with_dropped_column)
		state->drio.FetchLocal = CallNextRowWithDroppedColumn;
	else
		state->drio.FetchLocal = CallNextRow;

	state->drio.user_data = state;
	state->plan_id = id;

	DynamicReduceStartNormalPlan(id,
								 state->dsm_seg,
								 drmq,
								 rnodes,
								 false);

	state->NextRow = fun;
	state->func_data = func_data;
}

static void
CleanCopyFromReduce(CopyFromReduceState *state)
{
	DynamicReduceClearFetch(&state->drio);

	dsm_detach(state->dsm_seg);
}

void ClusterCopyFromReduce(Relation rel, Expr *reduce, List *rnodes, int id, bool freeze, CustomNextRowFunction func, void *data)
{
	MemoryContext		oldcontext;
	CopyFromState		cstate;
	CopyFromReduceState	rstate;
	ParseState		   *pstate;

	/* check read-only transaction and parallel mode */
	if (XactReadOnly && !rel->rd_islocaltemp)
		PreventCommandIfReadOnly("COPY FROM");
	PreventCommandIfParallelMode("COPY FROM");

	pstate = make_parsestate(NULL);
	addRangeTableEntryForRelation(pstate, rel, RowExclusiveLock, NULL, false, false);

	cstate = BeginCopyFrom(pstate, rel, NULL, NULL, false, NULL, NIL, NIL);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	InitCopyFromReduce(&rstate, RelationGetDescr(rel), reduce, rnodes, id, func, data);
	cstate->eol_type = EOL_UNKNOWN;
	cstate->cur_relname = RelationGetRelationName(rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;
	cstate->opts.binary = true;
	cstate->opts.freeze = freeze;
	cstate->NextRowFrom = NextRowFromReduce;
	cstate->func_data = &rstate;

	CopyFrom(cstate);

	CleanCopyFromReduce(&rstate);
	MemoryContextSwitchTo(oldcontext);
	EndCopyFrom(cstate);
}

void
ClusterDummyCopyFromReduce(List *target, Expr *reduce, List *rnodes, int id, CustomNextRowFunction fun, void *data)
{
	MemoryContext copy_context = AllocSetContextCreate(CurrentMemoryContext,
													   "ClusterDummyCopyFromReduce",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext old_context = MemoryContextSwitchTo(copy_context);
	TupleDesc desc = ExecTypeFromTL(target);
	CopyFromReduceState	rstate;
	TupleTableSlot *slot;
	InitCopyFromReduce(&rstate, desc, reduce, rnodes, id, fun, data);

	slot = NextRowFromReduce(NULL, rstate.drio.econtext, &rstate);
	if (!TupIsNull(slot))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("dummy rel got a tuple from reduce")));
	}

	CleanCopyFromReduce(&rstate);
	FreeTupleDesc(desc);
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(copy_context);
}

static TupleTableSlot*
NextRowFromReduce(CopyFromState cstate, ExprContext *econtext, void *data)
{
	CopyFromReduceState	   *state = data;

	state->cstate = cstate;
	state->drio.econtext = econtext;
	return DynamicReduceFetchSlot(&state->drio);
}

static TupleTableSlot*
NextRowFromTidBufFile(CopyFromState cstate, ExprContext *context, void *data)
{
	TidBufFileScanState	   *state = data;
	size_t					nread;
	ExprContext			   *econtext;
	ItemPointerData			tid;

	nread = BufFileRead(state->file, &tid, sizeof(ItemPointerData));
	if (nread == 0)
	{
		return ExecClearTuple(state->out_slot);
	}else if (nread != sizeof(tid))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from copied-ctid temporary file: %m")));
	}
	if (table_tuple_fetch_row_version(state->rel,
									  &tid,
									  SnapshotAny,
									  state->base_slot) == false)
	{
		ereport(ERROR,
				(errmsg("failed to fetch tuple for NextRowFromTidBufFile")));
	}

	econtext = state->project->pi_exprContext;
	econtext->ecxt_scantuple = state->base_slot;
	econtext->ecxt_outertuple = state->out_slot;

	ExecProject(state->project);
	Assert(!TupIsNull(state->out_slot));

	return state->out_slot;
}

typedef struct AuxPaddingState
{
	Relation		aux_currentRelation;
	TableScanDesc	aux_currentScanDesc;
	TupleTableSlot *aux_ScanTupleSlot;
	TupleTableSlot *aux_ResultTupleSlot;
	ExprContext	   *aux_ExprContext;
	ProjectionInfo *aux_ProjInfo;
} AuxPaddingState;

TupleTableSlot *
NextRowForPadding(CopyFromState cstate, ExprContext *context, void *data)
{
	AuxPaddingState*state = (AuxPaddingState *) data;
	TupleTableSlot *scanslot;
	ExprContext	   *econtext;
	ProjectionInfo *projinfo;

	/*
	 * get information from the estate and scan state
	 */
	scanslot = state->aux_ScanTupleSlot;
	projinfo = state->aux_ProjInfo;

	/*
	 * get the next slot from the table
	 */
	if (!table_scan_getnextslot(state->aux_currentScanDesc, ForwardScanDirection, scanslot))
	{
		if (projinfo)
			return ExecClearTuple(projinfo->pi_state.resultslot);
		else
			return ExecClearTuple(scanslot);
	}

	if (projinfo)
	{
		TupleTableSlot *resultslot;

		econtext = state->aux_ExprContext;
		ResetExprContext(econtext);
		econtext->ecxt_scantuple = scanslot;
		resultslot = ExecProject(projinfo);
		Assert(!TupIsNull(resultslot));

		return resultslot;
	} else
	{
		return scanslot;
	}
}

void
DoPaddingDataForAuxRel(Relation master,
					   Relation auxrel,
					   List *rnodes,
					   AuxiliaryRelCopy *auxcopy)
{
	MemoryContext	padding_context;
	MemoryContext	old_context;
	TupleDesc		scan_desc;
	TupleDesc		result_desc;
	AuxPaddingState state;

	Assert(master && auxcopy);
	Assert(list_length(rnodes) > 0);

	PushActiveSnapshot(GetTransactionSnapshot());

	padding_context = AllocSetContextCreate(CurrentMemoryContext,
											"DoPaddingDataForAuxRel",
											ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(padding_context);

	scan_desc = RelationGetDescr(master);
	state.aux_currentRelation = master;
	state.aux_currentScanDesc = table_beginscan(master,
												GetActiveSnapshot(),
												0, NULL);
	state.aux_ScanTupleSlot = table_slot_create(master, NULL);
	if (auxrel)
		result_desc = RelationGetDescr(auxrel);
	else
		result_desc = ExecTypeFromTL(auxcopy->targetList);
	state.aux_ResultTupleSlot = MakeSingleTupleTableSlot(result_desc, &TTSOpsVirtual);
	state.aux_ExprContext = CreateStandaloneExprContext();
	state.aux_ProjInfo = ExecBuildProjectionInfo(auxcopy->targetList,
												 state.aux_ExprContext,
												 state.aux_ResultTupleSlot,
												 NULL,
												 scan_desc);

	if (auxrel)
	{
		ClusterCopyFromReduce(auxrel,
							  auxcopy->reduce,
							  rnodes,
							  auxcopy->id,
							  false,
							  NextRowForPadding,
							  &state);
	} else
	{
		ClusterDummyCopyFromReduce(auxcopy->targetList,
								   auxcopy->reduce,
								   rnodes,
								   auxcopy->id,
								   NextRowForPadding,
								   &state);
	}

	ReScanExprContext(state.aux_ExprContext);
	if (auxrel == NULL)
		FreeTupleDesc(result_desc);

	FreeExprContext(state.aux_ExprContext, true);
	ExecDropSingleTupleTableSlot(state.aux_ScanTupleSlot);
	ExecDropSingleTupleTableSlot(state.aux_ResultTupleSlot);
	if (state.aux_currentScanDesc != NULL)
		table_endscan(state.aux_currentScanDesc);

	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(padding_context);

	PopActiveSnapshot();
}
#endif /* ADB */
