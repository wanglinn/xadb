/*--------------------------------------------------------------------------
 * 
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "tcop/tcopprot.h"
#include "pgxc/pgxc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/ps_status.h"
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(explain_rtable_of_query);
PG_FUNCTION_INFO_V1(explain_rtable_plan_of_query);

typedef struct RelationPlan
{
	char *schemaname;
	char *relname;
	char *planname;
	char *attname;
	Oid relid;
	Index varattno;
	Index scanrelid;
} RelationPlan;

static bool relationPlanWalker(Plan *plan, PlannedStmt *stmt, List **relationPlans);

Datum explain_rtable_of_query(PG_FUNCTION_ARGS)
{
	char *query_string = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	List *parsetree_list;
	ListCell *parsetree_item;
#if defined(ADB_MULTI_GRAM)
	ParseGrammar grammar = PARSE_GRAM_POSTGRES;
#endif

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

#ifdef ADB_MULTI_GRAM
	parsetree_list = parse_query_auto_gram(query_string, &grammar);
#else
	parsetree_list = pg_parse_query(query_string);
#endif

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach (parsetree_item, parsetree_list)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
		List *querytree_list,
			*plantree_list;
		ListCell *cell;

		set_ps_display("explain query", false);

#ifdef ADB_MULTI_GRAM
		querytree_list = pg_analyze_and_rewrite_for_gram(parsetree,
														 query_string,
														 NULL, 0, NULL,
														 grammar);
		current_grammar = grammar;
#else
		querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
												NULL, 0, NULL);
#endif

#ifdef ADB
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_CLUSTER_PLAN_SAFE,
										NULL);
#else
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK, NULL);
#endif
		/* Explain every plan */
		foreach (cell, plantree_list)
		{
			ListCell *rteCell;
			PlannedStmt *plannedStmt = (PlannedStmt *)lfirst(cell);

			if (!IsA(plannedStmt, PlannedStmt))
				continue;
			foreach (rteCell, plannedStmt->rtable)
			{
				Datum values[2];
				bool nulls[2];
				int i = 0;
				RangeTblEntry *rte = (RangeTblEntry *)lfirst(rteCell);
				Relation relation;

				if (!IsA(rte, RangeTblEntry))
					continue;
				if (rte->rtekind != RTE_RELATION)
					continue;
				if (rte->relkind != RELKIND_RELATION &&
					rte->relkind != RELKIND_MATVIEW)
					continue;

				memset(values, 0, sizeof(values));
				memset(nulls, 0, sizeof(nulls));

				relation = heap_open(rte->relid, AccessShareLock);
				values[i++] = CStringGetDatum(get_namespace_name(relation->rd_rel->relnamespace));
				values[i++] = CStringGetDatum(pstrdup(NameStr(relation->rd_rel->relname)));
				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				heap_close(relation, AccessShareLock);
			}
		}
	}

	PG_RETURN_VOID();
}

Datum explain_rtable_plan_of_query(PG_FUNCTION_ARGS)
{
	char *query_string = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	List *parsetree_list;
	ListCell *parsetree_item;
#if defined(ADB_MULTI_GRAM)
	ParseGrammar grammar = PARSE_GRAM_POSTGRES;
#endif

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not "
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

#ifdef ADB_MULTI_GRAM
	parsetree_list = parse_query_auto_gram(query_string, &grammar);
#else
	parsetree_list = pg_parse_query(query_string);
#endif

	/*
	 * Run through the raw parsetree(s) and process each one.
	 */
	foreach (parsetree_item, parsetree_list)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
		List *querytree_list,
			*plantree_list;
		ListCell *cell;

		set_ps_display("explain query", false);

#ifdef ADB_MULTI_GRAM
		querytree_list = pg_analyze_and_rewrite_for_gram(parsetree,
														 query_string,
														 NULL, 0, NULL,
														 grammar);
		current_grammar = grammar;
#else
		querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
												NULL, 0, NULL);
#endif

#ifdef ADB
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_CLUSTER_PLAN_SAFE,
										NULL);
#else
		plantree_list = pg_plan_queries(querytree_list,
										CURSOR_OPT_PARALLEL_OK, NULL);
#endif
		/* Explain every plan */
		foreach (cell, plantree_list)
		{
			List *relationPlans = NIL;
			PlannedStmt *plannedStmt = (PlannedStmt *)lfirst(cell);

			if (!IsA(plannedStmt, PlannedStmt))
				continue;

			relationPlanWalker(plannedStmt->planTree, plannedStmt, &relationPlans);
			if (relationPlans != NIL)
			{
				ListCell *relationPlanCell;

				foreach (relationPlanCell, relationPlans)
				{
					Datum values[4];
					bool nulls[4];
					int i = 0;
					RelationPlan *relationPlan = (RelationPlan *)lfirst(relationPlanCell);

					memset(values, 0, sizeof(values));
					memset(nulls, 0, sizeof(nulls));

					values[i++] = CStringGetDatum(relationPlan->schemaname);
					values[i++] = CStringGetDatum(relationPlan->relname);
					if (relationPlan->attname)
						values[i++] = CStringGetDatum(relationPlan->attname);
					else
						nulls[i++] = true;
					values[i++] = CStringGetTextDatum(relationPlan->planname);

					tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				}
			}
		}
	}

	PG_RETURN_VOID();
}

static bool relationExpressionWalker(Node *node, List **relationPlans)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;
		List *plans = *relationPlans;
		ListCell *cell;

		foreach (cell, plans)
		{
			RelationPlan *relationPlan = (RelationPlan *)lfirst(cell);
			if (relationPlan->scanrelid == var->varno)
			{
				TupleDesc tupleDesc;
				Relation relation;

				relationPlan->varattno = var->varattno;
				relation = heap_open(relationPlan->relid, AccessShareLock);
				tupleDesc = RelationGetDescr(relation);
				if (var->varattno > tupleDesc->natts)
					ereport(ERROR,
							(errmsg("relation:%u does not have attnum:%d",
									RelationGetRelid(relation), var->varattno)));

				relationPlan->attname = pstrdup(NameStr(TupleDescAttr(tupleDesc, var->varattno - 1)->attname));
				heap_close(relation, AccessShareLock);
			}
		}
	}
	return expression_tree_walker((Node *)node, relationExpressionWalker, relationPlans);
}

static bool relationPlanWalker(Plan *plan, PlannedStmt *stmt, List **relationPlans)
{
	if (plan == NULL)
		return false;

	if (IsA(plan, SeqScan))
	{
		ListCell *rteCell;
		int i = 0;
		SeqScan *seqScan;

		seqScan = (SeqScan *)plan;
		foreach (rteCell, stmt->rtable)
		{
			i++;
			if (seqScan->scanrelid == i)
			{
				RangeTblEntry *rte = (RangeTblEntry *)lfirst(rteCell);
				Relation relation;
				RelationPlan *relationPlan;

				if (!IsA(rte, RangeTblEntry))
					break;
				if (rte->rtekind != RTE_RELATION)
					break;
				if (rte->relkind != RELKIND_RELATION &&
					rte->relkind != RELKIND_MATVIEW)
					break;

				relationPlan = palloc0(sizeof(RelationPlan));
				relation = heap_open(rte->relid, AccessShareLock);
				relationPlan->relid = rte->relid;
				relationPlan->schemaname = get_namespace_name(relation->rd_rel->relnamespace);
				relationPlan->relname = pstrdup(NameStr(relation->rd_rel->relname));
				relationPlan->planname = pstrdup("Seq Scan");
				relationPlan->scanrelid = seqScan->scanrelid;
				heap_close(relation, AccessShareLock);
				*relationPlans = lappend(*relationPlans, relationPlan);

				expression_tree_walker((Node *)plan->qual, relationExpressionWalker, relationPlans);
			}
		}
	}

	return plan_tree_walker(plan, (Node *)stmt, relationPlanWalker, relationPlans);
}