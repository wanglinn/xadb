#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "executor/execExpr.h"
#include "executor/nodeConnectBy.h"
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
	uint64			level;
	int				hash_reader;
	bool			inner_ateof;
}TuplestoreConnectByState;

static TupleTableSlot *ExecTuplestoreConnectBy(PlanState *pstate);
static ExprState *makeHashExprState(Expr *expr, Oid hash_oid, PlanState *ps);
static uint32 getHashValue(List *hashlist, ExprContext *econtext);
static TupleTableSlot* ExecConnectByTuplestoreStartWith(ConnectByState *ps, Tuplestorestate *ts);

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
	cbstate->joinclause = ExecInitQual(node->plan.qual, &cbstate->ps);
	if (bms_is_empty(node->hash_quals) == false)
	{
		ListCell   *lc;
		OpExpr	   *op;
		Oid			left_hash;
		Oid			right_hash;
		int			i;

		for (i=0,lc=list_head(node->plan.qual);lc!=NULL;lc=lnext(lc),++i)
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
	cbstate->ps.ExecProcNode = ExecTuplestoreConnectBy;
	{
		TuplestoreConnectByState *state = palloc0(sizeof(TuplestoreConnectByState));
		cbstate->private_state = state;
		state->hash_reader = INVALID_HASHSTORE_READER;
		state->inner_ateof = true;
		state->scan_ts = tuplestore_begin_heap(false, false, work_mem/2);
		state->save_ts = tuplestore_begin_heap(false, false, work_mem/2);
		state->level = 1L;
	}
	cbstate->processing_root = true;

	return cbstate;
}

static TupleTableSlot *ExecTuplestoreConnectBy(PlanState *pstate)
{
	ConnectByState *cbstate = castNode(ConnectByState, pstate);
	TuplestoreConnectByState *state = cbstate->private_state;
	TupleTableSlot *outer_slot;
	TupleTableSlot *inner_slot;
	ExprContext *econtext;

	if (cbstate->processing_root)
	{
		outer_slot = ExecConnectByTuplestoreStartWith(cbstate, state->save_ts);
		if (!TupIsNull(outer_slot))
			return outer_slot;

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
			++(state->level);
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
			return ExecProject(pstate->ps_ProjInfo);
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

void ExecEndConnectBy(ConnectByState *node)
{
	TuplestoreConnectByState *state = node->private_state;
	ExecEndNode(outerPlanState(node));
	tuplestore_end(state->scan_ts);
	tuplestore_end(state->save_ts);
	if (node->hs)
		hashstore_end(node->hs);
	if (node->ts)
		tuplestore_end(node->ts);
	ExecFreeExprContext(&node->ps);
}

void ExecReScanConnectBy(ConnectByState *node)
{
	elog(ERROR, "not support ExecReScanConnectBy yet!");
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

static TupleTableSlot* ExecConnectByTuplestoreStartWith(ConnectByState *ps, Tuplestorestate *start_ts)
{
	Hashstorestate *hs = ps->hs;
	Tuplestorestate *outer_ts = ps->ts;
	PlanState	   *outer_ps = outerPlanState(ps);
	ExprContext	   *econtext = ps->ps.ps_ExprContext;
	TupleTableSlot *slot;
	TupleTableSlot *save_slot;
	uint32			hashvalue;

#ifdef USE_ASSERT_CHECKING
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_outertuple = NULL;
	econtext->ecxt_innertuple = NULL;
#endif
	for(;;)
	{
		ResetExprContext(econtext);
		slot = ExecProcNode(outer_ps);
		if (TupIsNull(slot))
			break;

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

		econtext->ecxt_outertuple = slot;
		if (ps->start_with == NULL ||
			ExecQual(ps->start_with, econtext))
		{
			econtext->ecxt_innertuple = slot;
			econtext->ecxt_outertuple = NULL;
			save_slot = ExecProject(ps->pj_save_targetlist);
			tuplestore_puttupleslot(start_ts, save_slot);
			return ExecProject(ps->ps.ps_ProjInfo);
		}
	}

	return NULL;
}

void ExecEvalLevelExpr(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ConnectByState *cbstate = castNode(ConnectByState, state->parent);
	TuplestoreConnectByState *tstate = cbstate->private_state;

	*op->resvalue = Int64GetDatum(tstate->level);
	*op->resnull = false;
}

void ExecEvalSysConnectByPathExpr(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
	ConnectByState *cbstate = castNode(ConnectByState, state->parent);
	StringInfoData buf;
	short narg;
	bool isnull = true;

	initStringInfo(&buf);
	if (cbstate->processing_root == false)
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