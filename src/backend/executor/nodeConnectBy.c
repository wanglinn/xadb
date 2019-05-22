#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "executor/execExpr.h"
#include "executor/nodeConnectBy.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/hashstore.h"
#include "utils/lsyscache.h"
#include "utils/tuplestore.h"

/* for no order connect by */
typedef struct TuplestoreConnectByState
{
	Tuplestorestate *scan_ts;
	Tuplestorestate *save_ts;
	int				hash_reader;
	bool			inner_ateof;
}TuplestoreConnectByState;

typedef struct TuplestoreConnectByLeaf
{
	slist_node		snode;
	MinimalTuple	outer_tup;
	Tuplesortstate *scan_ts;
}TuplestoreConnectByLeaf;

typedef struct TuplesortConnectByState
{
	slist_head		slist_level;
	slist_head		slist_idle;
	ProjectionInfo *sort_project;
	TupleTableSlot *sort_slot;
}TuplesortConnectByState;

static TupleTableSlot *ExecTuplestoreConnectBy(PlanState *pstate);
static TupleTableSlot *ExecTuplesortConnectBy(PlanState *pstate);
static ExprState *makeHashExprState(Expr *expr, Oid hash_oid, PlanState *ps);
static uint32 getHashValue(List *hashlist, ExprContext *econtext);
static TupleTableSlot* ExecConnectByStartWith(ConnectByState *ps);
static TuplestoreConnectByLeaf* GetconnectBySortLeaf(ConnectByState *ps);
static TuplestoreConnectByLeaf *GetNextTuplesortLeaf(ConnectByState *cbstate, TupleTableSlot *parent_slot);

ConnectByState* ExecInitConnectBy(ConnectByPlan *node, EState *estate, int eflags)
{
	ConnectByState *cbstate = makeNode(ConnectByState);
	TupleDesc input_desc;
	TupleDesc save_desc;

	cbstate->ps.plan = (Plan*)node;
	cbstate->ps.state = estate;

	if (bms_is_empty(node->hash_quals) == false)
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
	if (bms_is_empty(node->hash_quals) == false)
	{
		ListCell   *lc;
		OpExpr	   *op;
		Oid			left_hash;
		Oid			right_hash;
		int			i;

		for (i=0,lc=list_head(node->join_quals);lc!=NULL;lc=lnext(lc),++i)
		{
			if (bms_is_member(i, node->hash_quals) == false)
				continue;

			/* make hash ExprState(s) */
			op = lfirst_node(OpExpr, lc);
			if (get_op_hash_functions(op->opno, &left_hash, &right_hash) == false)
			{
				ereport(ERROR,
						(errmsg("could not find hash function for hash operator %u", op->opno)));
			}
			cbstate->left_hashfuncs = lappend(cbstate->left_hashfuncs,
											  makeHashExprState(linitial(op->args), left_hash, &cbstate->ps));

			cbstate->right_hashfuncs = lappend(cbstate->right_hashfuncs,
											   makeHashExprState(llast(op->args), right_hash, &cbstate->ps));

		}
		Assert(cbstate->left_hashfuncs != NULL);
		Assert(cbstate->right_hashfuncs != NULL);
		cbstate->hs = hashstore_begin_heap(false, node->num_buckets);
	}else
	{
		cbstate->ts = tuplestore_begin_heap(false, false, work_mem);
		tuplestore_set_eflags(cbstate->ts, EXEC_FLAG_REWIND);
	}

	if (node->numCols == 0)
	{
		TuplestoreConnectByState *state = palloc0(sizeof(TuplestoreConnectByState));
		cbstate->ps.ExecProcNode = ExecTuplestoreConnectBy;
		cbstate->private_state = state;
		state->hash_reader = INVALID_HASHSTORE_READER;
		state->inner_ateof = true;
		state->scan_ts = tuplestore_begin_heap(false, false, work_mem/2);
		state->save_ts = tuplestore_begin_heap(false, false, work_mem/2);
	}else
	{
		TuplestoreConnectByLeaf *leaf;
		TuplesortConnectByState *state = palloc0(sizeof(TuplesortConnectByState));
		cbstate->ps.ExecProcNode = ExecTuplesortConnectBy;
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

		leaf = GetconnectBySortLeaf(cbstate);
		slist_push_head(&state->slist_level, &leaf->snode);
	}
	cbstate->level = 1L;
	cbstate->rescan_reader = -1;
	cbstate->processing_root = true;

	return cbstate;
}

static TupleTableSlot *ExecTuplestoreConnectBy(PlanState *pstate)
{
	ConnectByState *cbstate = castNode(ConnectByState, pstate);
	TuplestoreConnectByState *state = cbstate->private_state;
	TupleTableSlot *outer_slot;
	TupleTableSlot *inner_slot;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;

	if (cbstate->processing_root)
	{
reget_start_with_:
		inner_slot = ExecConnectByStartWith(cbstate);
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

		if (cbstate->hs)
		{
			econtext->ecxt_innertuple = NULL;
			econtext->ecxt_outertuple = outer_slot;
			state->hash_reader = hashstore_begin_read(cbstate->hs,
													  getHashValue(cbstate->left_hashfuncs, econtext));
		}else
		{
			tuplestore_rescan(cbstate->ts);
		}
		state->inner_ateof = false;
	}

	for(;;)
	{
		CHECK_FOR_INTERRUPTS();
		if (cbstate->hs)
			hashstore_next_slot(cbstate->hs, inner_slot, state->hash_reader, false);
		else
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

	if (cbstate->hs)
	{
		hashstore_end_read(cbstate->hs, state->hash_reader);
		state->hash_reader = INVALID_HASHSTORE_READER;
	}
	state->inner_ateof = true;
	goto re_get_tuplestore_connect_by_;
}

static TupleTableSlot *ExecTuplesortConnectBy(PlanState *pstate)
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
		for(;;)
		{
			CHECK_FOR_INTERRUPTS();
			inner_slot = ExecConnectByStartWith(cbstate);
			if (TupIsNull(inner_slot))
				break;

			econtext->ecxt_innertuple = inner_slot;
			econtext->ecxt_outertuple = NULL;
			tuplesort_puttupleslot(leaf->scan_ts,
								   ExecProject(state->sort_project));
		}
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
	leaf = GetNextTuplesortLeaf(cbstate, save_slot);
	if (leaf)
		slist_push_head(&state->slist_level, &leaf->snode);
	else
		--(cbstate->level);

	if (TupIsNull(result_slot))
		goto re_get_tuplesort_connect_by_;	/* removed by qual */

	return pstate->ps_ResultTupleSlot;
}

void ExecEndConnectBy(ConnectByState *node)
{
	ExecEndNode(outerPlanState(node));
	if (node->hs)
		hashstore_end(node->hs);
	if (node->ts)
		tuplestore_end(node->ts);
	if (castNode(ConnectByPlan, node->ps.plan)->numCols)
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
	}else
	{
		TuplestoreConnectByState *state = node->private_state;
		tuplestore_end(state->scan_ts);
		tuplestore_end(state->save_ts);
	}
	ExecFreeExprContext(&node->ps);
}

void ExecReScanConnectBy(ConnectByState *node)
{
	PlanState *outer_ps = outerPlanState(node);

	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	node->processing_root = true;
	node->level = 1L;
	if (castNode(ConnectByPlan, node->ps.plan)->numCols == 0)
	{
		TuplestoreConnectByState *state = node->private_state;
		tuplestore_clear(state->save_ts);
		tuplestore_clear(state->scan_ts);
	}else
	{
		slist_node *slistnode;
		TuplestoreConnectByLeaf *leaf;
		TuplesortConnectByState *state = node->private_state;
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
	if (outer_ps->chgParam != NULL)
	{
		if (node->hs)
			hashstore_clear(node->hs);
		else
			tuplestore_clear(node->ts);
		node->is_rescan = false;
		node->eof_underlying = false;
		ExecReScan(outer_ps);
	}else
	{
		if (node->hs)
			node->rescan_reader = hashstore_begin_seq_read(node->hs);
		else
			tuplestore_rescan(node->ts);
		node->is_rescan = true;
	}
}

static ExprState *makeHashExprState(Expr *expr, Oid hash_oid, PlanState *ps)
{
	FuncExpr *func;

	func = makeFuncExpr(hash_oid,
						INT4OID,
						list_make1(expr),
						InvalidOid,
						exprCollation((Node*)expr),
						COERCE_EXPLICIT_CALL);
	return ExecInitExpr((Expr*)func, ps);
}

static uint32 getHashValue(List *hashlist, ExprContext *econtext)
{
	ListCell   *lc;
	Datum		datum;
	uint32		hash_value = 0;
	bool		isnull;

	foreach (lc, hashlist)
	{
		/* rotate hashkey left 1 bit at each step */
		hash_value = (hash_value << 1) | ((hash_value & 0x80000000) ? 1 : 0);

		ResetExprContext(econtext);
		datum = ExecEvalExprSwitchContext(lfirst(lc), econtext, &isnull);
		if (isnull == false)
		{
			hash_value ^= DatumGetUInt32(datum);
		}
	}

	return hash_value;
}

static TupleTableSlot* ExecConnectByStartWith(ConnectByState *ps)
{
	Hashstorestate *hs = ps->hs;
	Tuplestorestate *outer_ts = ps->ts;
	PlanState	   *outer_ps = outerPlanState(ps);
	ExprContext	   *econtext = ps->ps.ps_ExprContext;
	TupleTableSlot *slot;
	uint64			removed = 0;
	uint32			hashvalue;

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
			if (hs)
				hashstore_next_slot(hs, slot, ps->rescan_reader, false);
			else
				tuplestore_gettupleslot(outer_ts, true, false, slot);
			if (TupIsNull(slot))
			{
				if (hs)
				{
					hashstore_end_read(hs, ps->rescan_reader);
					ps->rescan_reader = INVALID_HASHSTORE_READER;
				}
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

			if (hs)
			{
				econtext->ecxt_innertuple = slot;
				hashvalue = getHashValue(ps->right_hashfuncs, econtext);
				hashstore_put_tupleslot(hs, slot, hashvalue);
				econtext->ecxt_innertuple = NULL;
			}else
			{
				tuplestore_puttupleslot(outer_ts, slot);
			}
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

static TuplestoreConnectByLeaf* GetconnectBySortLeaf(ConnectByState *ps)
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

static TuplestoreConnectByLeaf *GetNextTuplesortLeaf(ConnectByState *cbstate, TupleTableSlot *outer_slot)
{
	TuplesortConnectByState *state = cbstate->private_state;
	ExprContext *econtext = cbstate->ps.ps_ExprContext;
	TupleTableSlot *inner_slot = cbstate->inner_slot;
	TuplestoreConnectByLeaf *leaf;
	int reader;
	Assert(!TupIsNull(outer_slot));

	ExecMaterializeSlot(outer_slot);
	econtext->ecxt_outertuple = outer_slot;
	econtext->ecxt_innertuple = NULL;
	if (cbstate->hs)
	{
		reader = hashstore_begin_read(cbstate->hs,
									  getHashValue(cbstate->left_hashfuncs, econtext));
	}else
	{
		tuplestore_rescan(cbstate->ts);
	}

	leaf = NULL;
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		if (cbstate->hs)
			hashstore_next_slot(cbstate->hs, inner_slot, reader, false);
		else
			tuplestore_gettupleslot(cbstate->ts, true, false, inner_slot);
		if (TupIsNull(inner_slot))
			break;
		econtext->ecxt_innertuple = inner_slot;
		if (ExecQualAndReset(cbstate->joinclause, econtext))
		{
			if (leaf == NULL)
				leaf = GetconnectBySortLeaf(cbstate);
			tuplesort_puttupleslot(leaf->scan_ts,
								   ExecProject(state->sort_project));
		}
	}

	if (cbstate->hs)
		hashstore_end_read(cbstate->hs, reader);
	if (leaf)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(state));
		leaf->outer_tup = ExecCopySlotMinimalTuple(outer_slot);
		MemoryContextSwitchTo(oldcontext);
		tuplesort_performsort(leaf->scan_ts);
	}
	return leaf;
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
