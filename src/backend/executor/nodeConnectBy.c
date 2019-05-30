#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "executor/execExpr.h"
#include "executor/hashjoin.h"
#include "executor/nodeConnectBy.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tuplestore.h"

typedef enum CBMethod
{
	CB_NEST = 1,
	CB_HASH,
	CB_TUPLESORT,
	CB_HASHSORT
}CBMethod;
#define CONNECT_BY_METHOD(state_) (*((CBMethod*)(state_)->private_state))

/* for no order connect by */
typedef struct NestConnectByState
{
	CBMethod		method;
	Tuplestorestate *scan_ts;
	Tuplestorestate *save_ts;
	int				hash_reader;
	bool			inner_ateof;
}NestConnectByState;

typedef struct HashConnectByState
{
	CBMethod		method;
	BufFile		  **outer_save;
	List		   *outer_HashKeys;
	List		   *inner_HashKeys;
	List		   *hj_hashOperators;
	ExprState	   *hash_clauses;
	HashJoinTuple	cur_tuple;
	int				cur_skewno;
	int				cur_bucketno;
	uint32			cur_hashvalue;
	bool			inner_ateof;
}HashConnectByState;

typedef struct TuplestoreConnectByLeaf
{
	slist_node		snode;
	MinimalTuple	outer_tup;
	Tuplesortstate *scan_ts;
}TuplestoreConnectByLeaf;

typedef struct TuplesortConnectByState
{
	CBMethod		method;
	void 		  (*ProcessRoot)();
	TuplestoreConnectByLeaf *
				  (*GetNextLeaf)();
	slist_head		slist_level;
	slist_head		slist_idle;
	ProjectionInfo *sort_project;
	TupleTableSlot *sort_slot;
}TuplesortConnectByState;

typedef struct HashsortConnectByLeaf
{
	TuplestoreConnectByLeaf
					base;
}HashsortConnectByLeaf;

typedef struct HashsortConnectByState
{
	TuplesortConnectByState
					base;
	List		   *outer_HashKeys;
	List		   *inner_HashKeys;
	List		   *hj_hashOperators;
	ExprState	   *hash_clause;
}HashsortConnectByState;

typedef struct InsertRootHashSortContext
{
	ExprContext	   *econtext;
	ExprState	   *start_with;
	ProjectionInfo *sort_project;
	Tuplesortstate *tss;
}InsertRootHashSortContext;

static TupleTableSlot *ExecNestConnectBy(PlanState *pstate);
static TupleTableSlot *ExecHashConnectBy(PlanState *pstate);
static TupleTableSlot *ExecSortConnectBy(PlanState *pstate);

static TupleTableSlot* ExecNestConnectByStartWith(ConnectByState *ps);
static TuplestoreConnectByLeaf* GetConnectBySortLeaf(ConnectByState *ps);
static HashsortConnectByLeaf* GetConnectByHashSortLeaf(ConnectByState *ps);
static TuplestoreConnectByLeaf *GetNextTuplesortLeaf(ConnectByState *cbstate, TupleTableSlot *parent_slot);
static TuplestoreConnectByLeaf *GetNextHashsortLeaf(ConnectByState *cbstate, TupleTableSlot *parent_slot);
static TupleTableSlot *InsertRootHashValue(ConnectByState *cbstate, TupleTableSlot *slot);
static TupleTableSlot *InsertRootHashSortValue(InsertRootHashSortContext *context, TupleTableSlot *slot);
static bool ExecHashNewBatch(HashJoinTable hashtable, HashConnectByState *state, TupleTableSlot *slot);
static void ProcessTuplesortRoot(ConnectByState *cbstate, TuplestoreConnectByLeaf *leaf);
static void ProcessHashsortRoot(ConnectByState *cbstate, HashsortConnectByLeaf *leaf);
static void RestartBufFile(BufFile *file);

ConnectByState* ExecInitConnectBy(ConnectByPlan *node, EState *estate, int eflags)
{
	ConnectByState *cbstate = makeNode(ConnectByState);
	TupleDesc input_desc;
	TupleDesc save_desc;

	cbstate->ps.plan = (Plan*)node;
	cbstate->ps.state = estate;

	if (node->hash_quals != NIL)
		eflags &= ~(EXEC_FLAG_REWIND|EXEC_FLAG_MARK);
	outerPlanState(cbstate) = ExecInitNode(outerPlan(node), estate, 0);
	input_desc = ExecGetResultType(outerPlanState(cbstate));

	ExecAssignExprContext(estate, &cbstate->ps);
	ExecInitResultTupleSlotTL(estate, &cbstate->ps);
	ExecAssignProjectionInfo(&cbstate->ps, input_desc);

	save_desc = ExecTypeFromTL(node->save_targetlist, false);
	cbstate->outer_slot = ExecInitExtraTupleSlot(estate, save_desc);
	cbstate->pj_save_targetlist = ExecBuildProjectionInfo(node->save_targetlist,
														  cbstate->ps.ps_ExprContext,
														  ExecInitExtraTupleSlot(estate, save_desc),
														  &cbstate->ps,
														  input_desc);
	cbstate->inner_slot = ExecInitExtraTupleSlot(estate, input_desc);

	cbstate->start_with = ExecInitQual(node->start_with, &cbstate->ps);
	cbstate->ps.qual = ExecInitQual(node->plan.qual, &cbstate->ps);
	cbstate->joinclause = ExecInitQual(node->join_quals, &cbstate->ps);
	if (node->hash_quals != NIL)
	{
		List	   *rhclause = NIL;
		List	   *outer_HashKeys = NIL;
		List	   *inner_HashKeys = NIL;
		List	   *hj_hashOperators = NIL;
		ListCell   *lc;
		OpExpr	   *op;
		Oid			left_hash;
		Oid			right_hash;

		foreach (lc, node->hash_quals)
		{
			/* make hash ExprState(s) */
			op = lfirst_node(OpExpr, lc);
			if (get_op_hash_functions(op->opno, &left_hash, &right_hash) == false)
			{
				ereport(ERROR,
						(errmsg("could not find hash function for hash operator %u", op->opno)));
			}
			outer_HashKeys = lappend(outer_HashKeys,
									 ExecInitExpr(linitial(op->args), &cbstate->ps));

			inner_HashKeys = lappend(inner_HashKeys,
									 ExecInitExpr(llast(op->args), &cbstate->ps));

			rhclause = lappend(rhclause, ExecInitExpr(llast(op->args), outerPlanState(cbstate)));

			hj_hashOperators = lappend_oid(hj_hashOperators, op->opno);
		}
		castNode(HashState, outerPlanState(cbstate))->hashkeys = rhclause;

		if (node->numCols == 0)
		{
			HashConnectByState *state = palloc0(sizeof(HashConnectByState));
			state->method = CB_HASH;
			state->outer_HashKeys = outer_HashKeys;
			state->inner_HashKeys = inner_HashKeys;
			state->hj_hashOperators = hj_hashOperators;
			cbstate->private_state = state;
			cbstate->ps.ExecProcNode = ExecHashConnectBy;
			state->hash_clauses = ExecInitQual(node->hash_quals, &cbstate->ps);
		}else
		{
			HashsortConnectByLeaf *leaf;
			HashsortConnectByState *state = palloc0(sizeof(HashsortConnectByState));
			state->base.method = CB_HASHSORT;
			state->outer_HashKeys = outer_HashKeys;
			state->inner_HashKeys = inner_HashKeys;
			state->hj_hashOperators = hj_hashOperators;
			state->base.ProcessRoot = ProcessHashsortRoot;
			state->base.GetNextLeaf = GetNextHashsortLeaf;
			cbstate->ps.ExecProcNode = ExecSortConnectBy;
			cbstate->private_state = state;
			slist_init(&state->base.slist_level);
			slist_init(&state->base.slist_idle);
			state->base.sort_slot = ExecInitExtraTupleSlot(estate,
														   ExecTypeFromTL(node->sort_targetlist, false));
			state->base.sort_project = ExecBuildProjectionInfo(node->sort_targetlist,
															   cbstate->ps.ps_ExprContext,
															   state->base.sort_slot,
															   &cbstate->ps,
															   input_desc);
			state->hash_clause = ExecInitQual(node->hash_quals, &cbstate->ps);
			leaf = GetConnectByHashSortLeaf(cbstate);
			slist_push_head(&state->base.slist_level, &leaf->base.snode);
		}
	}else
	{
		cbstate->ts = tuplestore_begin_heap(false, false, work_mem);
		tuplestore_set_eflags(cbstate->ts, EXEC_FLAG_REWIND);

		if (node->numCols == 0)
		{
			NestConnectByState *state = palloc0(sizeof(NestConnectByState));
			state->method = CB_NEST;
			cbstate->ps.ExecProcNode = ExecNestConnectBy;
			cbstate->private_state = state;
			state->inner_ateof = true;
			state->scan_ts = tuplestore_begin_heap(false, false, work_mem/2);
			state->save_ts = tuplestore_begin_heap(false, false, work_mem/2);
		}else
		{
			TuplestoreConnectByLeaf *leaf;
			TuplesortConnectByState *state = palloc0(sizeof(TuplesortConnectByState));
			state->method = CB_TUPLESORT;
			state->ProcessRoot = ProcessTuplesortRoot;
			state->GetNextLeaf = GetNextTuplesortLeaf;
			cbstate->ps.ExecProcNode = ExecSortConnectBy;
			cbstate->private_state = state;
			slist_init(&state->slist_level);
			slist_init(&state->slist_idle);
			state->sort_slot = ExecInitExtraTupleSlot(estate,
													  ExecTypeFromTL(node->sort_targetlist, false));
			state->sort_project = ExecBuildProjectionInfo(node->sort_targetlist,
														  cbstate->ps.ps_ExprContext,
														  state->sort_slot,
														  &cbstate->ps,
														  input_desc);

			leaf = GetConnectBySortLeaf(cbstate);
			slist_push_head(&state->slist_level, &leaf->snode);
		}
	}

	cbstate->level = 1L;
	cbstate->rescan_reader = -1;
	cbstate->processing_root = true;

	return cbstate;
}

static TupleTableSlot *ExecNestConnectBy(PlanState *pstate)
{
	ConnectByState *cbstate = castNode(ConnectByState, pstate);
	NestConnectByState *state = cbstate->private_state;
	TupleTableSlot *outer_slot;
	TupleTableSlot *inner_slot;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;

	if (cbstate->processing_root)
	{
reget_start_with_:
		inner_slot = ExecNestConnectByStartWith(cbstate);
		if (!TupIsNull(inner_slot))
		{
			CHECK_FOR_INTERRUPTS();
			econtext->ecxt_innertuple = inner_slot;
			econtext->ecxt_outertuple = ExecClearTuple(cbstate->outer_slot);
			tuplestore_puttupleslot(state->save_ts,
									ExecProject(cbstate->pj_save_targetlist));
			if (pstate->qual == NULL ||
				ExecQual(pstate->qual, econtext))
			{
				return ExecProject(pstate->ps_ProjInfo);
			}
			InstrCountFiltered1(pstate, 1);
			goto reget_start_with_;
		}

		state->inner_ateof = true;
		cbstate->processing_root = false;
	}

	outer_slot = cbstate->outer_slot;
	inner_slot = cbstate->inner_slot;
	econtext = cbstate->ps.ps_ExprContext;

re_get_tuplestore_connect_by_:
	if (state->inner_ateof)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(pstate));
		tuplestore_gettupleslot(state->scan_ts, true, true, outer_slot);
		MemoryContextSwitchTo(oldcontext);
		if (TupIsNull(outer_slot))
		{
			/* switch work tuplestore */
			Tuplestorestate *ts = state->save_ts;
			state->save_ts = state->scan_ts;
			state->scan_ts = ts;
			tuplestore_clear(state->save_ts);

			/* read new data from last saved tuplestore */
			oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(pstate));
			tuplestore_gettupleslot(state->scan_ts, true, true, outer_slot);
			MemoryContextSwitchTo(oldcontext);

			if (TupIsNull(outer_slot))	/* no more data, end plan */
				return ExecClearTuple(pstate->ps_ProjInfo->pi_state.resultslot);
			++(cbstate->level);
		}

		tuplestore_rescan(cbstate->ts);
		state->inner_ateof = false;
	}

	for(;;)
	{
		CHECK_FOR_INTERRUPTS();
		tuplestore_gettupleslot(cbstate->ts, true, false, inner_slot);
		if (TupIsNull(inner_slot))
			break;

		econtext->ecxt_innertuple = inner_slot;
		econtext->ecxt_outertuple = outer_slot;
		if (ExecQualAndReset(cbstate->joinclause, econtext))
		{
			tuplestore_puttupleslot(state->save_ts,
									ExecProject(cbstate->pj_save_targetlist));
			if (pstate->qual == NULL ||
				ExecQual(pstate->qual, econtext))
			{
				return ExecProject(pstate->ps_ProjInfo);
			}
			InstrCountFiltered1(pstate, 1);
		}
	}

	state->inner_ateof = true;
	goto re_get_tuplestore_connect_by_;
}

static TupleTableSlot *ExecHashConnectBy(PlanState *pstate)
{
	ConnectByState *cbstate = castNode(ConnectByState, pstate);
	HashConnectByState *state = cbstate->private_state;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;
	HashJoinTable hjt = cbstate->hjt;
	TupleTableSlot *inner_slot = cbstate->inner_slot;
	TupleTableSlot *outer_slot = cbstate->outer_slot;
	BufFile *file;
	uint32 hashvalue;

	if (cbstate->processing_root)
	{
		if (hjt == NULL)
		{
			/* initialize */
			HashState *hash = castNode(HashState, outerPlanState(pstate));
			int i;

			hjt = ExecHashTableCreate(hash,
									  state->hj_hashOperators,
									  false);	/* inner join not need keep nulls */
			cbstate->hjt = hjt;
			hash->hashtable = hjt;
			if (hjt->outerBatchFile == NULL)
			{
				Assert(hjt->nbatch == 1);
				hjt->outerBatchFile = MemoryContextAllocZero(hjt->hashCxt, sizeof(BufFile**));
				hjt->innerBatchFile = MemoryContextAllocZero(hjt->hashCxt, sizeof(BufFile**));
			}
			/* I am not sure about the impact of "grow", so disable it */
			hjt->growEnabled = false;
			MultiExecHashEx(hash, InsertRootHashValue, cbstate);

			/* prepare for save joind tuple */
			state->outer_save = MemoryContextAllocZero(GetMemoryChunkContext(hjt->outerBatchFile),
													   sizeof(BufFile*) * hjt->nbatch);

			/* save batch 0 to BufFile, we need rescan */
			for (i=hjt->nbuckets;--i>=0;)
			{
				HashJoinTuple hashTuple = hjt->buckets.unshared[i];
				while (hashTuple != NULL)
				{
					ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(hashTuple),
										  hashTuple->hashvalue,
										  &hjt->innerBatchFile[0]);
					hashTuple = hashTuple->next.unshared;
				}
			}
			/* seek batch file to start */
			for(i=0;i<hjt->nbatch;++i)
				RestartBufFile(hjt->outerBatchFile[i]);
		}

reget_start_with_:
		CHECK_FOR_INTERRUPTS();
		if (hjt->curbatch >= hjt->nbatch)
		{
			cbstate->processing_root = false;
			hjt->curbatch = 0;
			RestartBufFile(hjt->outerBatchFile[hjt->curbatch]);
			ExecClearTuple(outer_slot);
			++(cbstate->level);
			goto reget_hash_connect_by_;
		}
		file = hjt->outerBatchFile[hjt->curbatch];
		if (file == NULL)
		{
			++hjt->curbatch;
			goto reget_start_with_;
		}
		for(;;)
		{
			ExecHashJoinReadTuple(file, &hashvalue, inner_slot);
			if (TupIsNull(inner_slot))
			{
				/* end of current batch */
				++hjt->curbatch;
				goto reget_start_with_;
			}
			econtext->ecxt_innertuple = inner_slot;
			if (pstate->qual == NULL ||
				ExecQual(pstate->qual, econtext))
			{
				return ExecProject(pstate->ps_ProjInfo);
			}
			InstrCountFiltered1(pstate, 1);
		}
	}

reget_hash_connect_by_:
	CHECK_FOR_INTERRUPTS();
	if (TupIsNull(outer_slot))
	{
		file = hjt->outerBatchFile[hjt->curbatch];
		if (file == NULL)
			ExecClearTuple(outer_slot);
		else
			ExecHashJoinReadTuple(file, &hashvalue, outer_slot);
		if (TupIsNull(outer_slot))
		{
			if (ExecHashNewBatch(hjt, state, inner_slot) == false)
			{
				/* check prior is empty */
				int i;
				int count = hjt->nbatch;
				bool not_empty = false;
				for (i=0;i<count;++i)
				{
					if (state->outer_save[i] != NULL)
					{
						RestartBufFile(state->outer_save[i]);
						not_empty = true;
					}
				}
				if (not_empty == false)
					return NULL;	/* no more level */

				/* switch outer BufFile */
				{
					BufFile **tmp = hjt->outerBatchFile;
					hjt->outerBatchFile = state->outer_save;
					state->outer_save = tmp;
				}
				/* and reset HashJoinTable */
				if (hjt->curbatch != 0)
				{
					hjt->curbatch = -1;
					not_empty = ExecHashNewBatch(hjt, state, inner_slot);
					Assert(not_empty);
				}
				++(cbstate->level);
			}
			goto reget_hash_connect_by_;
		}

		state->cur_hashvalue = hashvalue;
		state->cur_skewno = ExecHashGetSkewBucket(hjt, hashvalue);
		if (state->cur_skewno == INVALID_SKEW_BUCKET_NO)
		{
			int batch_no;
			ExecHashGetBucketAndBatch(hjt, hashvalue, &state->cur_bucketno, &batch_no);
			Assert(batch_no == hjt->curbatch);
		}
		state->cur_tuple = NULL;
	}

	econtext->ecxt_innertuple = inner_slot;
	econtext->ecxt_outertuple = outer_slot;
	if (ExecScanHashBucketExt(econtext,
							  state->hash_clauses,
							  &state->cur_tuple,
							  state->cur_hashvalue,
							  state->cur_skewno,
							  state->cur_bucketno,
							  hjt,
							  inner_slot) == false)
	{
		ExecClearTuple(outer_slot);
		goto reget_hash_connect_by_;
	}

	if (cbstate->joinclause &&
		ExecQualAndReset(cbstate->joinclause, econtext) == false)
	{
		goto reget_hash_connect_by_;
	}

	/* inner tuple is outer tuple at next level */
	econtext->ecxt_outertuple = inner_slot;
	if (ExecHashGetHashValue(hjt,
							 econtext,
							 state->outer_HashKeys,
							 true,
							 false,
							 &hashvalue))
	{
		TupleTableSlot *save_slot;
		int batch_no;
		int bucket_no;

		econtext->ecxt_outertuple = outer_slot;
		ExecHashGetBucketAndBatch(hjt, hashvalue, &bucket_no, &batch_no);
		if (hjt->innerBatchFile[batch_no] != NULL)
		{
			/* don't need save it when inner batch is empty */
			save_slot = ExecProject(cbstate->pj_save_targetlist);
			ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(save_slot),
								hashvalue,
								&state->outer_save[batch_no]);
		}
	}
	econtext->ecxt_outertuple = outer_slot;

	if (pstate->qual == NULL ||
		ExecQualAndReset(pstate->qual, econtext))
	{
		return ExecProject(pstate->ps_ProjInfo);
	}

	InstrCountFiltered1(pstate, 1);
	goto reget_hash_connect_by_;
}

static TupleTableSlot *ExecSortConnectBy(PlanState *pstate)
{
	ConnectByState *cbstate = castNode(ConnectByState, pstate);
	TuplesortConnectByState *state = cbstate->private_state;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;
	TupleTableSlot *outer_slot;
	TupleTableSlot *inner_slot;
	TupleTableSlot *sort_slot;
	TupleTableSlot *save_slot;
	TupleTableSlot *result_slot;
	TuplestoreConnectByLeaf *leaf;

	if (cbstate->processing_root)
	{
		leaf = slist_head_element(TuplestoreConnectByLeaf, snode, &state->slist_level);
		Assert(leaf->snode.next == NULL);
		(*state->ProcessRoot)(cbstate, leaf);
		tuplesort_performsort(leaf->scan_ts);
		leaf->outer_tup = NULL;
		cbstate->processing_root = false;
	}

	outer_slot = cbstate->outer_slot;
	inner_slot = cbstate->inner_slot;
	sort_slot = state->sort_slot;

re_get_tuplesort_connect_by_:
	CHECK_FOR_INTERRUPTS();
	if (slist_is_empty(&state->slist_level))
	{
		Assert(cbstate->level == 0L);
		return ExecClearTuple(pstate->ps_ResultTupleSlot);
	}

	leaf = slist_head_element(TuplestoreConnectByLeaf, snode, &state->slist_level);
	if (tuplesort_gettupleslot(leaf->scan_ts, true, false, sort_slot, NULL) == false)
	{
		/* end of current leaf */
		slist_pop_head_node(&state->slist_level);
		slist_push_head(&state->slist_idle, &leaf->snode);
		--(cbstate->level);
		tuplesort_end(leaf->scan_ts);
		leaf->scan_ts = NULL;
		if (leaf->outer_tup)
		{
			pfree(leaf->outer_tup);
			leaf->outer_tup = NULL;
		}
		goto re_get_tuplesort_connect_by_;
	}

	if (leaf->outer_tup)
	{
		Assert(cbstate->level > 1L);
		ExecStoreMinimalTuple(leaf->outer_tup, outer_slot, false);
	}else
	{
		Assert(cbstate->level == 1L);
		ExecClearTuple(outer_slot);
	}
	econtext->ecxt_outertuple = outer_slot;
	econtext->ecxt_innertuple = sort_slot;
	if (pstate->qual == NULL ||
		ExecQual(pstate->qual, econtext))
	{
		result_slot = ExecProject(pstate->ps_ProjInfo);
		/* function GetNextTuplesortLeaf well free Datum, so we need materialize result */
		ExecMaterializeSlot(pstate->ps_ResultTupleSlot);
	}else
	{
		InstrCountFiltered1(pstate, 1);
		result_slot = ExecClearTuple(pstate->ps_ResultTupleSlot);
	}

	save_slot = ExecProject(cbstate->pj_save_targetlist);
	++(cbstate->level);
	ExecMaterializeSlot(save_slot);		/* GetNextLeaf will reset memory context */
	leaf = (*state->GetNextLeaf)(cbstate, save_slot);
	if (leaf)
	{
		leaf->outer_tup = ExecCopySlotMinimalTuple(save_slot);
		tuplesort_performsort(leaf->scan_ts);
		slist_push_head(&state->slist_level, &leaf->snode);
	}else
	{
		--(cbstate->level);
	}

	if (TupIsNull(result_slot))
		goto re_get_tuplesort_connect_by_;	/* removed by qual */

	return pstate->ps_ResultTupleSlot;
}

static void ProcessTuplesortRoot(ConnectByState *cbstate, TuplestoreConnectByLeaf *leaf)
{
	TupleTableSlot *inner_slot;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;
	TuplesortConnectByState *state = cbstate->private_state;
	Assert(state->method == CB_TUPLESORT);

	for(;;)
	{
		CHECK_FOR_INTERRUPTS();
		inner_slot = ExecNestConnectByStartWith(cbstate);
		if (TupIsNull(inner_slot))
			break;

		econtext->ecxt_innertuple = inner_slot;
		econtext->ecxt_outertuple = NULL;
		tuplesort_puttupleslot(leaf->scan_ts,
							   ExecProject(state->sort_project));
	}
}

static void ProcessHashsortRoot(ConnectByState *cbstate, HashsortConnectByLeaf *leaf)
{
	HashsortConnectByState *state = cbstate->private_state;
	HashJoinTable hjt = cbstate->hjt;
	InsertRootHashSortContext context;

	context.econtext = cbstate->ps.ps_ExprContext;
	context.start_with = cbstate->start_with;
	context.sort_project = state->base.sort_project;
	context.tss = leaf->base.scan_ts;

	if (hjt == NULL)
	{
		HashState *hash = castNode(HashState, outerPlanState(cbstate));
		HashJoinTuple hashTuple;
		int bucket;

		hjt = ExecHashTableCreate(hash, state->hj_hashOperators, false);
		cbstate->hjt = hjt;
		hash->hashtable = hjt;
		MultiExecHashEx(hash, InsertRootHashSortValue, &context);
		hjt->growEnabled = false;
		if (hjt->outerBatchFile == NULL)
		{
			Assert(hjt->nbatch == 1);
			hjt->outerBatchFile = MemoryContextAllocZero(hjt->hashCxt, sizeof(BufFile**));
		}

		/* save batch 0 to BufFile, we need reload */
		for (bucket=hjt->nbuckets;--bucket>=0;)
		{
			hashTuple = hjt->buckets.unshared[bucket];
			while(hashTuple != NULL)
			{
				ExecHashJoinSaveTuple(HJTUPLE_MINTUPLE(hashTuple),
									  hashTuple->hashvalue,
									  &hjt->outerBatchFile[0]);
				hashTuple = hashTuple->next.unshared;
			}
		}

	}else
	{
		TupleTableSlot *inner_slot = cbstate->inner_slot;
		uint32 hashvalue;
		int i;

		for (i = hjt->nbatch;--i>=0;)
		{
			BufFile *file = hjt->innerBatchFile[i];
			if (file)
			{
				RestartBufFile(file);
				for(;;)
				{
					CHECK_FOR_INTERRUPTS();
					ExecHashJoinReadTuple(file, &hashvalue, inner_slot);
					if (TupIsNull(inner_slot))
						break;
					InsertRootHashSortValue(&context, inner_slot);
				}
			}
		}

		for (i = hjt->nSkewBuckets;--i>=0;)
		{
			HashJoinTuple hashTuple;
			if (hjt->skewBucket[i])
			{
				hashTuple = hjt->skewBucket[i]->tuples;
				while (hashTuple != NULL)
				{
					CHECK_FOR_INTERRUPTS();
					ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
										  inner_slot,
										  false);
					InsertRootHashSortValue(&context, inner_slot);
					hashTuple = hashTuple->next.unshared;
				}
			}
		}
	}
}

void ExecEndConnectBy(ConnectByState *node)
{
	ExecEndNode(outerPlanState(node));

	if (node->ts)
	{
		tuplestore_end(node->ts);
		node->ts = NULL;
	}

	switch(CONNECT_BY_METHOD(node))
	{
	case CB_NEST:
		{
			NestConnectByState *state = node->private_state;
			tuplestore_end(state->scan_ts);
			tuplestore_end(state->save_ts);
		}
		break;
	case CB_HASH:
		{
			HashConnectByState *state = node->private_state;
			if (state->outer_save)
			{
				int i = node->hjt->nbatch;
				while (--i>=0)
				{
					if (state->outer_save[i])
						BufFileClose(state->outer_save[i]);
				}
				pfree(state->outer_save);
				state->outer_save = NULL;
			}
			if (node->hjt)
			{
				HashJoinTable hjt = node->hjt;

				/* ExecHashTableDestroy not close batch 0 file */
				if (hjt->innerBatchFile &&
					hjt->innerBatchFile[0])
					BufFileClose(hjt->innerBatchFile[0]);
				if (hjt->outerBatchFile &&
					hjt->outerBatchFile[0])
					BufFileClose(hjt->outerBatchFile[0]);

				ExecHashTableDestroy(hjt);
				node->hjt = NULL;
			}
		}
		break;
	case CB_TUPLESORT:
	case CB_HASHSORT:
		{
			TuplesortConnectByState *state = node->private_state;
			TuplestoreConnectByLeaf *leaf;
			slist_node *node;
			while (slist_is_empty(&state->slist_idle) == false)
			{
				node = slist_pop_head_node(&state->slist_idle);
				leaf = slist_container(TuplestoreConnectByLeaf, snode, node);
				Assert (leaf->scan_ts == NULL);
				pfree(leaf);
			}
			while (slist_is_empty(&state->slist_level) == false)
			{
				node = slist_pop_head_node(&state->slist_level);
				leaf = slist_container(TuplestoreConnectByLeaf, snode, node);
				tuplesort_end(leaf->scan_ts);
				if (leaf->outer_tup)
					pfree(leaf->outer_tup);
				pfree(leaf);
			}
		}
		if (node->hjt)
		{
			HashJoinTable hjt = node->hjt;

			/* ExecHashTableDestroy not close batch 0 file */
			if (hjt->outerBatchFile &&
				hjt->outerBatchFile[0])
				BufFileClose(hjt->outerBatchFile[0]);
			ExecHashTableDestroy(hjt);
			node->hjt = NULL;
		}
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown connect by method %u", CONNECT_BY_METHOD(node))));
	}
	ExecFreeExprContext(&node->ps);
}

static void ExecReScanNestConnectBy(ConnectByState *cbstate, NestConnectByState *state)
{
	tuplestore_clear(state->save_ts);
	tuplestore_clear(state->scan_ts);
	if (outerPlanState(cbstate)->chgParam != NULL)
	{
		if (cbstate->ts)
			tuplestore_clear(cbstate->ts);
		cbstate->is_rescan = false;
		cbstate->eof_underlying = false;
	}
}

static void ExecReScanHashConnectBy(ConnectByState *cbstate, HashConnectByState *state)
{
	HashJoinTable hjt = cbstate->hjt;
	int i;

	if (hjt == NULL)
		return;	/* not initialized */

	/* clear saved */
	for (i=hjt->nbatch;--i>=0;)
	{
		if (state->outer_save[i])
		{
			BufFileClose(state->outer_save[i]);
			state->outer_save[i] = NULL;
		}
	}

	if (outerPlanState(cbstate)->chgParam == NULL)
	{
		if (cbstate->processing_root == false)
		{
			TupleTableSlot *slot = cbstate->inner_slot;
			/* clear outer */
			for (i=hjt->nbatch;--i>=0;)
			{
				if (hjt->outerBatchFile[i])
				{
					BufFileClose(hjt->outerBatchFile[i]);
					hjt->outerBatchFile[i] = NULL;
				}
			}
			/* make start with */
			for(i=hjt->nbatch;--i>=0;)
			{
				BufFile *file = hjt->innerBatchFile[i];
				uint32 hashvalue;

				if (file == NULL)
					continue;

				RestartBufFile(file);
				for(;;)
				{
					ExecHashJoinReadTuple(file, &hashvalue, slot);
					if (TupIsNull(slot))
						break;
					InsertRootHashValue(cbstate, slot);
				}
			}
			for (i=hjt->nSkewBuckets;--i>=0;)
			{
				HashJoinTuple hashTuple;
				if (hjt->skewBucket[i])
				{
					hashTuple = hjt->skewBucket[i]->tuples;
					while (hashTuple)
					{
						ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple),
											  slot,
											  false);
						InsertRootHashValue(cbstate, slot);
						hashTuple = hashTuple->next.unshared;
					}
				}
			}
		}
		if (hjt->curbatch == 0)
		{
			RestartBufFile(hjt->outerBatchFile[0]);
		}else
		{
			hjt->curbatch = -1;
			ExecHashNewBatch(hjt, state, cbstate->inner_slot);
		}
	}else
	{
		if (hjt->outerBatchFile &&
			hjt->outerBatchFile[0])
			BufFileClose(hjt->outerBatchFile[0]);
		ExecHashTableDestroy(hjt);
		cbstate->hjt = NULL;
		/* state->outer_save pfreed by ExecHashTableDestroy() */
		state->outer_save = NULL;
		castNode(HashState, outerPlanState(cbstate))->hashtable = NULL;
	}
}

static void ExecReScanTuplesortConnectBy(ConnectByState *cbstate, TuplesortConnectByState *state)
{
	slist_node *slistnode;
	TuplestoreConnectByLeaf *leaf;

	while (slist_is_empty(&state->slist_level) == false)
	{
		slistnode = slist_pop_head_node(&state->slist_level);
		slist_push_head(&state->slist_idle, slistnode);
		leaf = slist_container(TuplestoreConnectByLeaf, snode, slistnode);
		tuplesort_end(leaf->scan_ts);
		leaf->scan_ts = NULL;
		if (leaf->outer_tup)
		{
			pfree(leaf->outer_tup);
			leaf->outer_tup = NULL;
		}
	}
}

static void ExecReScanHashsortConnectBy(ConnectByState *cbstate, HashsortConnectByState *state)
{
	HashJoinTable hjt = cbstate->hjt;
	ExecReScanTuplesortConnectBy(cbstate, &state->base);

	if (hjt == NULL)
		return;

	if (outerPlanState(cbstate)->chgParam != NULL)
	{
		if (hjt->outerBatchFile &&
			hjt->outerBatchFile[0])
			BufFileClose(hjt->outerBatchFile[0]);
		ExecHashTableDestroy(hjt);
		cbstate->hjt = NULL;
		castNode(HashState, outerPlanState(cbstate))->hashtable = NULL;
	}
}

void ExecReScanConnectBy(ConnectByState *node)
{
	switch(*((CBMethod*)node->private_state))
	{
	case CB_NEST:
		ExecReScanNestConnectBy(node, node->private_state);
		break;
	case CB_HASH:
		ExecReScanHashConnectBy(node, node->private_state);
		break;
	case CB_TUPLESORT:
		ExecReScanTuplesortConnectBy(node, node->private_state);
		break;
	case CB_HASHSORT:
		ExecReScanHashsortConnectBy(node, node->private_state);
		break;
	default:
		ereport(ERROR,
				(errmsg("unknown connect by method %u", CONNECT_BY_METHOD(node))));
	}

	if (outerPlanState(node)->chgParam != NULL)
		ExecReScan(outerPlanState(node));

	ExecClearTuple(node->outer_slot);
	ExecClearTuple(node->inner_slot);
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	node->processing_root = true;
	node->level = 1L;
}

static TupleTableSlot* ExecNestConnectByStartWith(ConnectByState *ps)
{
	Tuplestorestate *outer_ts = ps->ts;
	PlanState	   *outer_ps = outerPlanState(ps);
	ExprContext	   *econtext = ps->ps.ps_ExprContext;
	TupleTableSlot *slot;
	uint64			removed = 0;

#ifdef USE_ASSERT_CHECKING
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_outertuple = NULL;
	econtext->ecxt_innertuple = NULL;
#endif
	for(;;)
	{
		if (ps->is_rescan)
		{
			slot = ps->inner_slot;
			tuplestore_gettupleslot(outer_ts, true, false, slot);
			if (TupIsNull(slot))
			{
				ps->is_rescan = false;
				continue;	/* try is is eof underlying? */
			}
		}else if (ps->eof_underlying == false)
		{
			ResetExprContext(econtext);
			slot = ExecProcNode(outer_ps);
			if (TupIsNull(slot))
			{
				ps->eof_underlying = true;
				break;
			}

			tuplestore_puttupleslot(outer_ts, slot);
		}else
		{
			/* not in rescan and eof underlying */
			break;
		}

		econtext->ecxt_outertuple = slot;
		if (ps->start_with == NULL ||
			ExecQual(ps->start_with, econtext))
		{
			InstrCountFiltered2(ps, removed);
			return slot;
		}
		++removed;
	}

	InstrCountFiltered2(ps, removed);
	return NULL;
}

static TuplestoreConnectByLeaf* GetConnectBySortLeaf(ConnectByState *ps)
{
	TuplestoreConnectByLeaf *leaf;
	MemoryContext oldcontext;
	TuplesortConnectByState *state = ps->private_state;
	ConnectByPlan *node = castNode(ConnectByPlan, ps->ps.plan);

	if (slist_is_empty(&state->slist_idle))
	{
		leaf = MemoryContextAllocZero(GetMemoryChunkContext(ps), sizeof(*leaf));
	}else
	{
		slist_node *node = slist_pop_head_node(&state->slist_idle);
		leaf = slist_container(TuplestoreConnectByLeaf, snode, node);
	}

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(ps));
	leaf->scan_ts = tuplesort_begin_heap(state->sort_slot->tts_tupleDescriptor,
										 node->numCols,
										 node->sortColIdx,
										 node->sortOperators,
										 node->collations,
										 node->nullsFirst,
										 0,
										 NULL,
										 false);
	MemoryContextSwitchTo(oldcontext);

	return leaf;
}

static HashsortConnectByLeaf* GetConnectByHashSortLeaf(ConnectByState *ps)
{
	HashsortConnectByLeaf *leaf;
	MemoryContext oldcontext;
	HashsortConnectByState *state = ps->private_state;
	ConnectByPlan *node = castNode(ConnectByPlan, ps->ps.plan);

	if (slist_is_empty(&state->base.slist_idle))
	{
		leaf = MemoryContextAllocZero(GetMemoryChunkContext(ps), sizeof(*leaf));
	}else
	{
		slist_node *node = slist_pop_head_node(&state->base.slist_idle);
		leaf = slist_container(HashsortConnectByLeaf, base.snode, node);
	}

	oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(ps));
	leaf->base.scan_ts = tuplesort_begin_heap(state->base.sort_slot->tts_tupleDescriptor,
											  node->numCols,
											  node->sortColIdx,
											  node->sortOperators,
											  node->collations,
											  node->nullsFirst,
											  0,
											  NULL,
											  false);
	MemoryContextSwitchTo(oldcontext);

	return leaf;
}

static TuplestoreConnectByLeaf *GetNextTuplesortLeaf(ConnectByState *cbstate, TupleTableSlot *outer_slot)
{
	TuplesortConnectByState *state = cbstate->private_state;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;
	TupleTableSlot *inner_slot = cbstate->inner_slot;
	TuplestoreConnectByLeaf *leaf;
	Assert(!TupIsNull(outer_slot));

	ExecMaterializeSlot(outer_slot);
	econtext->ecxt_outertuple = outer_slot;
	econtext->ecxt_innertuple = NULL;
	tuplestore_rescan(cbstate->ts);

	leaf = NULL;
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		tuplestore_gettupleslot(cbstate->ts, true, false, inner_slot);
		if (TupIsNull(inner_slot))
			break;
		econtext->ecxt_innertuple = inner_slot;
		if (ExecQualAndReset(cbstate->joinclause, econtext))
		{
			if (leaf == NULL)
				leaf = GetConnectBySortLeaf(cbstate);
			tuplesort_puttupleslot(leaf->scan_ts,
								   ExecProject(state->sort_project));
		}
	}

	return leaf;
}

static TuplestoreConnectByLeaf *GetNextHashsortLeaf(ConnectByState *cbstate, TupleTableSlot *outer_slot)
{
	HashsortConnectByState *state = cbstate->private_state;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;
	HashJoinTable hjt = cbstate->hjt;
	TupleTableSlot *inner_slot = cbstate->inner_slot;
	HashsortConnectByLeaf *leaf;
	HashJoinTuple hashTuple;
	uint32 hashvalue;
	int skewno;
	int bucketno;

	econtext->ecxt_outertuple = outer_slot;
	if (ExecHashGetHashValue(hjt,
							 econtext,
							 state->outer_HashKeys,
							 true,
							 false,&hashvalue) == false)
		return NULL;

	skewno = ExecHashGetSkewBucket(hjt, hashvalue);
	if (skewno != INVALID_SKEW_BUCKET_NO)
	{
		bucketno = -1;
	}else
	{
		int batchno;
		ExecHashGetBucketAndBatch(hjt, hashvalue, &bucketno, &batchno);
		if (batchno != hjt->curbatch ||
			hjt->buckets.unshared[bucketno] == NULL)
		{
			/* load batch and bucketno */
			BufFile *file = hjt->outerBatchFile[batchno];
			uint32 hashval;
			int batch,bucket;
			if (file == NULL)
				return NULL;	/* not match */

			RestartBufFile(file);
			if (hjt->curbatch != batchno)
			{
				hjt->curbatch = batchno;
				ExecHashTableReset(hjt);
			}

			for(;;)
			{
				ExecHashJoinReadTuple(file, &hashval, inner_slot);
				if (TupIsNull(inner_slot))
					break;

				ExecHashGetBucketAndBatch(hjt, hashval, &bucket, &batch);
				Assert(batch == batchno);
				if (bucket == bucketno)
				{
					/* only insert same bucket */
					ExecHashTableInsert(hjt, inner_slot, hashval);
				}
			}
		}
	}

	hashTuple = NULL;
	leaf = NULL;
	while(ExecScanHashBucketExt(econtext,
								state->hash_clause,
								&hashTuple,
								hashvalue,
								skewno,
								bucketno,
								hjt,
								inner_slot))
	{
		if (cbstate->joinclause &&
			ExecQualAndReset(cbstate->joinclause, econtext) == false)
			continue;
		if (leaf == NULL)
			leaf = GetConnectByHashSortLeaf(cbstate);
		tuplesort_puttupleslot(leaf->base.scan_ts,
							   ExecProject(state->base.sort_project));
	}

	return (TuplestoreConnectByLeaf*)leaf;
}

static TupleTableSlot *InsertRootHashValue(ConnectByState *cbstate, TupleTableSlot *slot)
{
	ExprContext	   *econtext;
	HashJoinTable	hjt;
	uint32			hashvalue;
	int				bucket_no;
	int				batch_no;
	bool			saved;

	if (TupIsNull(slot))
		return slot;

	saved = false;
	econtext = cbstate->ps.ps_ExprContext;
	ResetExprContext(econtext);
	econtext->ecxt_outertuple = slot;
	if (cbstate->start_with == NULL ||
		ExecQual(cbstate->start_with, econtext))
	{
		HashConnectByState *state = cbstate->private_state;
		hjt = cbstate->hjt;
		if (ExecHashGetHashValue(hjt,
								 econtext,
								 state->outer_HashKeys,
								 true,
								 false,
								 &hashvalue))
		{
			econtext->ecxt_innertuple = slot;
			econtext->ecxt_outertuple = ExecClearTuple(cbstate->outer_slot);
			ExecHashGetBucketAndBatch(hjt, hashvalue, &bucket_no, &batch_no);
			ExecHashJoinSaveTuple(ExecFetchSlotMinimalTuple(ExecProject(cbstate->pj_save_targetlist)),
								  hashvalue,
								  &hjt->outerBatchFile[batch_no]);
			saved = true;
		}
	}

	if (!saved)
		InstrCountFiltered2(cbstate, 1);
	return slot;
}

static TupleTableSlot *InsertRootHashSortValue(InsertRootHashSortContext *context, TupleTableSlot *slot)
{
	ExprContext	   *econtext;

	if (TupIsNull(slot))
		return slot;

	econtext = context->econtext;
	ResetExprContext(econtext);
	econtext->ecxt_outertuple = slot;
	econtext->ecxt_innertuple = NULL;
	if (context->start_with == NULL ||
		ExecQual(context->start_with, econtext))
	{
		econtext->ecxt_outertuple = NULL;
		econtext->ecxt_innertuple = slot;
		tuplesort_puttupleslot(context->tss,
							   ExecProject(context->sort_project));
	}

	return slot;
}

/* like ExecHashJoinNewBatch */
static bool ExecHashNewBatch(HashJoinTable hashtable, HashConnectByState *state, TupleTableSlot *slot)
{
	BufFile	   *innerFile;
	int			nbatch = hashtable->nbatch;
	int			curbatch = hashtable->curbatch;
	uint32		hashvalue;

	if (curbatch >= 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.
		 */
		if (hashtable->outerBatchFile[curbatch])
		{
			BufFileClose(hashtable->outerBatchFile[curbatch]);
			hashtable->outerBatchFile[curbatch] = NULL;
		}
	}

	++curbatch;
	while (curbatch < nbatch &&
		   (hashtable->outerBatchFile[curbatch] == NULL ||
			hashtable->innerBatchFile[curbatch] == NULL))
	{
		/* Release associated temp files right away. */
		if (hashtable->outerBatchFile[curbatch])
		{
			BufFileClose(hashtable->outerBatchFile[curbatch]);
			hashtable->outerBatchFile[curbatch] = NULL;
		}
		++curbatch;
	}

	if (curbatch >= nbatch)
		return false;			/* no more batches */

	hashtable->curbatch = curbatch;

	/*
	 * Reload the hash table with the new inner batch (which could be empty)
	 */
	ExecHashTableReset(hashtable);

	innerFile = hashtable->innerBatchFile[curbatch];

	if (innerFile != NULL)
	{
		RestartBufFile(innerFile);

		while ((slot = ExecHashJoinReadTuple(innerFile, &hashvalue, slot)))
		{
			/*
			 * NOTE: some tuples may be sent to future batches.  Also, it is
			 * possible for hashtable->nbatch to be increased here!
			 */
			ExecHashTableInsert(hashtable, slot, hashvalue);
		}
	}

	RestartBufFile(hashtable->outerBatchFile[curbatch]);

	return true;
}

static void RestartBufFile(BufFile *file)
{
	if (file && BufFileSeek(file, 0, 0L, SEEK_SET))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rewind connect-by temporary file: %m")));
	}
}

void ExecEvalSysConnectByPathExpr(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ConnectByState *cbstate = castNode(ConnectByState, state->parent);
	StringInfoData buf;
	short narg;
	bool isnull = true;

	initStringInfo(&buf);
	if (cbstate->level != 1L)
	{
		TupleTableSlot *slot = econtext->ecxt_outertuple;
		char *prior_str;
		AttrNumber attnum = op->d.scbp.attnum;

		slot_getsomeattrs(slot, attnum+1);
		if (slot->tts_isnull[attnum] == false)
		{
			prior_str = TextDatumGetCString(slot->tts_values[attnum]);
			appendStringInfoString(&buf, prior_str);
			pfree(prior_str);
			isnull = false;
		}
	}

	narg = op->d.scbp.narg;
	while (narg > 0)
	{
		--narg;
		if (op->d.scbp.argnull[narg] == false)
		{
			appendStringInfoString(&buf, DatumGetCString(op->d.scbp.arg[narg]));
			isnull = false;
		}
	}

	*op->resnull = isnull;
	if (isnull == false)
	{
		text *result = cstring_to_text_with_len(buf.data, buf.len);
		*op->resvalue = PointerGetDatum(result);
	}
}
