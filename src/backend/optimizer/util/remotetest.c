/*-------------------------------------------------------------------------
 *
 * remotetest.c
 *	  Routines to attempt to prove logical implications between predicate
 *	  expressions.
 *
 * Portions Copyright (c) 2018, AntDB Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/remotetest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "access/tuptypeconvert.h"
#include "catalog/heap.h"
#include "catalog/pg_aux_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/clusterHeapScan.h"
#include "executor/clusterReceiver.h"
#include "libpq-fe.h"
#include "libpq/libpq-node.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/plancat.h"
#include "optimizer/reduceinfo.h"
#include "optimizer/restrictinfo.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "partitioning/partdesc.h"
#include "partitioning/partprune.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "pgxc/slot.h"

#define AUX_SCAN_INFO_SIZE_STEP		8

#define AUX_REL_LOC_INFO_INVALID	0
#define AUX_REL_LOC_INFO_REP		1	/* replicated */
#define AUX_REL_LOC_INFO_REDUCE		2	/* distribute by value */

typedef struct ModifyContext
{
	RelationLocInfo	   *loc_info;
	Expr			   *partition_expr;				/* like hash(column) % datanode_count */
	Expr			   *right_expr;					/* like hash(value) % datanode_count */
	Param			   *param_expr;					/* right_expr's param expr */
	ParamExecData	   *param_data;					/* right_expr's value */
	Var				   *var_expr;					/* right_expr's type info */
	ExprState		   *right_state;				/* init by right_expr */
	ExprContext		   *expr_context;
	Index				relid;
	AttrNumber			varattno;
	int16				var_typlen;
	bool				var_typbyval;
	bool				hint;
}ModifyContext;
#define MODIFY_CONTEXT_PARAM_COUNT		2
#define MODIFY_CONTEXT_PARAM_RIGHT		0
#define MODIFY_CONTEXT_PARAM_CONVERT	1

typedef struct CoerceInfo
{
	ParamExecData  *param_data;
	Param		   *param_expr;
	Expr		   *expr;
	ExprState	   *exprState;
	Oid				type;
	bool			valid;
}CoerceInfo;

typedef struct GatherAuxColumnInfo
{
	Form_pg_attribute	attr;			/* main relation attribute for auxiliary column */
	List			   *list_coerce;	/* list of CoerceInfo */
	ExprContext		   *econtext;		/* for execute convert and reduce expr state */
	ParamExecData	   *param_data;		/* for reduce expr input data */
	Param			   *param_expr;		/* for reduce expr input param expr */
	Expr			   *reduce_expr;	/* reduce expr */
	ExprState		   *reduce_state;	/* reduce expr state */
	Relation			rel;			/* auxiliary relation */
	uint32				max_size;		/* alloc count of datum */
	uint32				cur_size;		/* saved datum count */
	int					aux_loc;		/* AUX_REL_LOC_INFO_XXX */
	AttrNumber			attno;			/* auxiliary table for main's column */
	bool				has_null;		/* has null value equal */
	List			   *list_exec;		/* auxiliary relation execute on */
	Datum			   *datum;			/* datum buffer */
}GatherAuxColumnInfo;
#define GACI_PARAM_COUNT		2
#define GACI_PARAM_REDUCE		0
#define GACI_PARAM_CONVERT		1

typedef struct GatherAuxInfoContext
{
	ExprContext	   *econtext;
	Relation		rel;
	List		   *list_info;
	Bitmapset	   *bms_attnos;
	GatherAuxColumnInfo *cheapest;
	Index			relid;
	bool			deep;
	bool			hint;
}GatherAuxInfoContext;

typedef struct GatherMainRelExecOn
{
	PQNHookFunctions		funcs;
	TupleTableSlot		   *slot;
	List				   *list_nodeid;
	AttrNumber				index_nodeid;
	AttrNumber				index_tid;
	uint32					max_tid_size;
	uint32					cur_tid_size;
	ItemPointer				tids;
}GatherMainRelExecOn;
#define HOOK_GET_GATHER(hook_) ((GatherMainRelExecOn*)hook_)

int use_aux_type = USE_AUX_CTID;
int use_aux_max_times = 1;

static Expr* makeInt4EQ(Expr *l, Expr *r);
static Expr* makeOidEQ(Expr *l, Expr *r);
static Expr* makeInt4ArrayIn(Expr *l, Datum *values, int count);
static Expr* makeOidArrayIn(Expr *l, Datum *values, int count);
static Expr* makeInt4Const(int32 val);
static Expr* makeOidConst(Oid val);
static Expr* makeNotNullTest(Expr *expr, bool isrow);
static Expr* makePartitionExpr(RelationLocInfo *loc_info, Node *node);
static List* make_new_qual_list(ModifyContext *context, Node *quals, bool need_eval_const);
static Node* mutator_equal_expr(Node *node, ModifyContext *context);
static void init_context_expr_if_need(ModifyContext *context);
static Const* get_var_equal_const(List *args, Oid opno, Index relid, AttrNumber attno, Var **var);
static Const* get_mvar_equal_const(List *args, Oid opno, Index relid, Bitmapset *attnos, Var **var);
static Const* is_const_able_expr(Expr *expr);
static List* get_auxiary_quals(List *baserestrictinfo, Index relid, const Bitmapset *attnos);

static bool gather_aux_info(Node *node, GatherAuxInfoContext *context);
static GatherAuxColumnInfo* get_gather_aux_col_info(GatherAuxInfoContext *context, AttrNumber attno);
static void free_aux_col_info(GatherAuxColumnInfo *info);
static CoerceInfo* get_coerce_info(GatherAuxColumnInfo *info, Oid typeoid);
static bool push_const_to_aux_col_info(GatherAuxColumnInfo *info, Const *c, bool isarray);
static void push_aux_col_info_to_gather(GatherAuxInfoContext *context, GatherAuxColumnInfo *info);
static void init_auxiliary_info_if_need(GatherAuxInfoContext *context);
static void set_cheapest_auxiliary(GatherAuxInfoContext *context);
static List* get_aux_table_execute_on(GatherAuxColumnInfo *info);
static GatherMainRelExecOn* get_main_table_execute_on(GatherAuxInfoContext *context, GatherAuxColumnInfo *info);
static bool process_remote_aux_tuple(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);
static void push_tid_to_exec_on(GatherMainRelExecOn *context, Datum datum);
static Expr* make_ctid_in_expr(Index relid, ItemPointer tids, uint32 count);

/* return remote oid list */
List *relation_remote_by_constraints(PlannerInfo *root, RelOptInfo *rel, bool modify_info_when_aux)
{
	List *result;
	Relation relation;
	RangeTblEntry *rte;
	List *new_quals;
	ListCell *lc;
	MemoryContext old_mctx;
	MemoryContext main_mctx;
	GatherAuxInfoContext gather;

	result = relation_remote_by_constraints_base(root,
												(Node*)rel->baserestrictinfo,
												rel->loc_info,
												rel->relid);
	if (use_aux_type == USE_AUX_OFF
		|| (use_aux_max_times >= 0 && root->glob->usedRemoteAux >= use_aux_max_times)
		|| rel->baserestrictinfo == NIL
		|| list_length(result) <= 1
		|| list_length(result) < list_length(rel->loc_info->nodeids)
		|| IsLocatorReplicated(rel->loc_info->locatorType)
		|| rel->loc_info->locatorType == LOCATOR_TYPE_RANDOM)
		return result;

	/* have auxiliary table ? */
	rte = root->simple_rte_array[rel->relid];
	relation = table_open(rte->relid, AccessShareLock);
	if (relation->rd_auxatt == NULL ||
		/* quals have auxiliary var ? */
		(new_quals = get_auxiary_quals(rel->baserestrictinfo, rel->relid, relation->rd_auxatt)) == NULL)
	{
		table_close(relation, AccessShareLock);
		return result;
	}

	main_mctx = AllocSetContextCreate(CurrentMemoryContext,
									  "RRBC",
									  ALLOCSET_DEFAULT_SIZES);
	old_mctx = MemoryContextSwitchTo(main_mctx);

	gather.econtext = CreateStandaloneExprContext();
	gather.econtext->ecxt_param_exec_vals = palloc0(sizeof(ParamExecData) * GACI_PARAM_COUNT);
	gather.rel = relation;
	gather.list_info = NIL;
	gather.bms_attnos = relation->rd_auxatt;
	gather.relid = rel->relid;
	gather.cheapest = NULL;

	/* only try top quals */
	gather.deep = false;
	foreach(lc, new_quals)
		gather_aux_info(lfirst(lc), &gather);

	init_auxiliary_info_if_need(&gather);
	set_cheapest_auxiliary(&gather);
	if (gather.cheapest != NULL)
	{
		ListCell *lc;
		GatherMainRelExecOn *exec_on = get_main_table_execute_on(&gather, gather.cheapest);
		list_free(result);
		result = NIL;
		MemoryContextSwitchTo(old_mctx);
		if (exec_on->list_nodeid != NIL)
		{
			foreach (lc, rel->loc_info->nodeids)
			{
				if (list_member_int(exec_on->list_nodeid, get_pgxc_node_id(lfirst_oid(lc))))
					result = list_append_unique_oid(result, lfirst_oid(lc));
			}
		}
		if (modify_info_when_aux && exec_on->cur_tid_size > 0)
		{
			Expr *expr = make_ctid_in_expr(rel->relid,
										   exec_on->tids,
										   exec_on->cur_tid_size);
			RestrictInfo *ri = make_restrictinfo(expr,
												 true,
												 false,
												 false,
												 0,
												 bms_make_singleton(rel->relid),
												 NULL,
												 NULL);
			rel->baserestrictinfo = lappend(rel->baserestrictinfo, ri);
			rel->rows = exec_on->cur_tid_size ;
		}
		root->glob->transientPlan = true;
		++(root->glob->usedRemoteAux);
	}

	foreach(lc, gather.list_info)
		free_aux_col_info(lfirst(lc));
	FreeExprContext(gather.econtext, true);
	table_close(relation, AccessShareLock);
	MemoryContextSwitchTo(old_mctx);
	result = list_copy(result);
	MemoryContextDelete(main_mctx);

	return result;
}

List *relation_remote_by_constraints_base(PlannerInfo *root, Node *quals, RelationLocInfo *loc_info, Index varno)
{
	MemoryContext main_mctx;
	MemoryContext temp_mctx;
	MemoryContext old_mctx;
	ModifyContext context;
	List		   *result;
	List		   *constraint_pred;
	List		   *safe_constraints;
	List		   *temp_constraints;
	List		   *new_clauses;
	List		   *null_test_list;
	ListCell	   *lc;
	int				i;

	MemSet(&context, 0, sizeof(context));
	AssertArg(loc_info != NULL);
	context.loc_info = loc_info;

	if(quals == NULL)
		return list_copy(loc_info->nodeids);

	/* create memory context */
	main_mctx = AllocSetContextCreate(CurrentMemoryContext,
									  "RRBC main",
									  ALLOCSET_DEFAULT_SIZES);
	temp_mctx = AllocSetContextCreate(main_mctx,
									  "RRBC temp",
									  ALLOCSET_DEFAULT_SIZES);
	old_mctx = MemoryContextSwitchTo(main_mctx);

	context.relid = varno;
	null_test_list = NIL;
	if (loc_info->locatorType == LOCATOR_TYPE_LIST ||
		loc_info->locatorType == LOCATOR_TYPE_RANGE)
	{
		Relation	rel;
		ListCell   *lc;
		PartitionKey part_key;
		PartitionDesc part_desc;
		PartitionScheme part_scheme;
		List	   *clauses;
		Bitmapset  *bms;

		rel = table_open(loc_info->relid, NoLock);
		part_key = RelationGenerateDistributeKeyFromLocInfo(rel, loc_info);
		part_scheme = build_partschema_from_partkey(part_key);
		part_desc = DistributeRelationGenerateDesc(part_key, loc_info->nodeids, loc_info->values);

		if (IsA(quals, List))
			clauses = (List*)quals;
		else
			clauses = list_make1(quals);
		bms = prune_distribute_rel(varno, clauses, part_scheme, part_desc, part_key);
		table_close(rel, NoLock);

		result = NIL;
		if (bms_is_empty(bms) == false)
		{
			i = 0;
			MemoryContextSwitchTo(old_mctx);
			foreach(lc, loc_info->nodeids)
			{
				if (bms_is_member(i, bms))
					result = lappend_oid(result, lfirst_oid(lc));
				++i;
			}
		}
		goto end_test_;
	}
	if(IsLocatorDistributedByValue(loc_info->locatorType))
	{
		context.varattno = GetFirstLocAttNumIfOnlyOne(loc_info);
		context.var_expr = makeVarByRel(context.varattno, loc_info->relid, varno);
		context.partition_expr = makePartitionExpr(loc_info, (Node*)context.var_expr);
		null_test_list = list_make1(makeNotNullTest((Expr*)context.var_expr, false));
	}

	constraint_pred = get_relation_constraints_base(root, loc_info->relid, varno, true, true, false);
	safe_constraints = NIL;
	foreach(lc, constraint_pred)
	{
		Node	   *pred = (Node *) lfirst(lc);

		if (!contain_mutable_functions(pred))
			safe_constraints = lappend(safe_constraints, pred);
	}
	/* append TABLE.XC_NODE_ID is not null */
	safe_constraints = lappend(safe_constraints,
							   makeNotNullTest((Expr*)makeVarByRel(XC_NodeIdAttributeNumber, loc_info->relid, varno), false));

	new_clauses = make_new_qual_list(&context, quals, root == NULL);

	i=0;
	result = NIL;
	foreach(lc, loc_info->nodeids)
	{
		Expr *expr;
		Oid node_oid = lfirst_oid(lc);
		MemoryContextSwitchTo(temp_mctx);
		MemoryContextResetAndDeleteChildren(temp_mctx);
		temp_constraints = list_copy(safe_constraints);

		/* make TABLE.XC_NODE_ID=id */
		expr = makeInt4EQ((Expr*)makeVarByRel(XC_NodeIdAttributeNumber, loc_info->relid, varno),
						  makeInt4Const(get_pgxc_node_id(node_oid)));
		temp_constraints = lappend(temp_constraints, expr);

		/* when not first remote node, partition key is not null */
		if (i != 0 && null_test_list)
		{
			ListCell *lc2;
			foreach(lc2, null_test_list)
				temp_constraints = lappend(temp_constraints, lfirst(lc2));
		}

		if(context.partition_expr)
		{
			if(LOCATOR_TYPE_HASHMAP==loc_info->locatorType)
				expr = makeOidEQ(context.partition_expr, makeOidConst(node_oid));
			else
				expr = makeInt4EQ(context.partition_expr, makeInt4Const(i));
			temp_constraints = lappend(temp_constraints, expr);
		}

		if (predicate_refuted_by(temp_constraints, new_clauses, false) == false)
		{
			MemoryContextSwitchTo(old_mctx);
			result = list_append_unique_oid(result, node_oid);
			/* MemoryContextSwitchTo(...) */
		}
		++i;
	}

end_test_:
	MemoryContextSwitchTo(old_mctx);
	MemoryContextDelete(main_mctx);
	return result;
}

static Expr* makeInt4EQ(Expr *l, Expr *r)
{
	OpExpr *op;
	AssertArg(exprType((Node*)l) == INT4OID);
	AssertArg(exprType((Node*)r) == INT4OID);

	op = makeNode(OpExpr);
	op->opno = Int4EqualOperator;
	op->opfuncid = F_INT4EQ;
	op->opresulttype = BOOLOID;
	op->opretset = false;
	op->opcollid = InvalidOid;
	op->inputcollid = InvalidOid;
	op->args = list_make2(l, r);
	op->location = -1;

	return (Expr*)op;
}

static Expr* makeOidEQ(Expr *l, Expr *r)
{
	OpExpr *op;
	AssertArg(exprType((Node*)l) == OIDOID);
	AssertArg(exprType((Node*)r) == OIDOID);

	op = makeNode(OpExpr);
	op->opno = OidEqualOperator;
	op->opfuncid = F_OIDEQ;
	op->opresulttype = BOOLOID;
	op->opretset = false;
	op->opcollid = InvalidOid;
	op->inputcollid = InvalidOid;
	op->args = list_make2(l, r);
	op->location = -1;

	return (Expr*)op;
}

static Expr* makeInt4ArrayIn(Expr *l, Datum *values, int count)
{
	ArrayType *arr = construct_array(values, count, INT4OID, sizeof(int32), true, 'i');
	Const *c = makeConst(INT4ARRAYOID,
						 -1,
						 InvalidOid,
						 -1,
						 PointerGetDatum(arr),
						 false,
						 false);
	ScalarArrayOpExpr *sao = makeNode(ScalarArrayOpExpr);
	sao->opno = Int4EqualOperator;
	sao->opfuncid = F_INT4EQ;
	sao->useOr = true;
	sao->inputcollid = InvalidOid;
	sao->args = list_make2(l, c);
	sao->location = -1;

	return (Expr*)sao;
}

static Expr* makeOidArrayIn(Expr *l, Datum *values, int count)
{
	ArrayType *arr = construct_array(values, count, OIDOID, sizeof(int32), true, 'i');
	Const *c = makeConst(OIDARRAYOID,
						 -1,
						 InvalidOid,
						 -1,
						 PointerGetDatum(arr),
						 false,
						 false);
	ScalarArrayOpExpr *sao = makeNode(ScalarArrayOpExpr);
	sao->opno = OidEqualOperator;
	sao->opfuncid = F_OIDEQ;
	sao->useOr = true;
	sao->inputcollid = InvalidOid;
	sao->args = list_make2(l, c);
	sao->location = -1;

	return (Expr*)sao;
}

static Expr* makeInt4Const(int32 val)
{
	return (Expr*)makeConst(INT4OID,
							-1,
							InvalidOid,
							sizeof(int32),
							Int32GetDatum(val),
							false,
							true);
}

static Expr* makeOidConst(Oid val)
{
	return (Expr*)makeConst(OIDOID,
							-1,
							InvalidOid,
							sizeof(Oid),
							ObjectIdGetDatum(val),
							false,
							true);
}

static Expr* makeNotNullTest(Expr *expr, bool isrow)
{
	NullTest *null_test = makeNode(NullTest);

	null_test->arg = expr;
	null_test->nulltesttype = IS_NOT_NULL;
	null_test->argisrow = isrow;
	null_test->location = -1;

	return (Expr*)null_test;
}

static Expr* makePartitionExpr(RelationLocInfo *loc_info, Node *node)
{
	CoalesceExpr  *coalesce;
	Expr		   *expr;
	Const		   *count;
	LocatorKeyInfo *key;
	uint32			n;

	if (list_length(loc_info->keys) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("not distribute by only one expression not support yet")));
	key = FirstLocKeyInfo(loc_info);

	if (loc_info->locatorType == LOCATOR_TYPE_HASHMAP)
		n = HASHMAP_SLOTSIZE;
	else
		n = list_length(loc_info->nodeids);

	count = makeConst(INT4OID,
					  -1,
					  InvalidOid,
					  sizeof(n),
					  UInt32GetDatum(n),
					  false,
					  true);

	switch(loc_info->locatorType)
	{
	case LOCATOR_TYPE_HASH:
	case LOCATOR_TYPE_HASHMAP:
		expr = makeHashExprFamily((Expr*)node, key->opfamily, get_opclass_input_type(key->opclass));
		break;
	case LOCATOR_TYPE_MODULO:
		expr = (Expr*)coerce_to_target_type(NULL,
											(Node*)node,
											exprType((Node*)node),
											INT4OID,
											-1,
											COERCION_EXPLICIT,
											COERCE_IMPLICIT_CAST,
											-1);
		break;
	default:
		return NULL;
	}

	expr = (Expr*)makeFuncExpr(F_HASH_COMBIN_MOD,
							   INT4OID,
							   list_make2(count, expr),
							   InvalidOid,
							   InvalidOid,
							   COERCE_EXPLICIT_CALL);

	if(LOCATOR_TYPE_HASHMAP==loc_info->locatorType)
	{
		expr = CreateNodeOidFromSlotIndexExpr(expr);
	}else
	{
		coalesce = makeNode(CoalesceExpr);
		coalesce->coalescetype = INT4OID,
		coalesce->coalescecollid = InvalidOid;
		coalesce->args = list_make2(expr, makeInt4Const(0)); /* when null, first node */
		expr = (Expr*)coalesce;
	}

	return expr;
}

static List* make_new_qual_list(ModifyContext *context, Node *quals, bool need_eval_const)
{
	List *result;
	ListCell *lc;
	Node *clause;

	result = NIL;
	if (IsA(quals, List))
	{
		foreach(lc, (List*)quals)
		{
			clause = lfirst(lc);
			if (IsA(clause, RestrictInfo))
				clause = (Node*)((RestrictInfo*)clause)->clause;
			if (need_eval_const)
				clause = eval_const_expressions(NULL, clause);
			result = lappend(result, clause);

			if(context->partition_expr)
			{
				context->hint = false;
				clause = mutator_equal_expr(clause, context);
				if(context->hint)
					result = lappend(result, clause);
			}
		}
	}else
	{
		clause = IsA(quals, RestrictInfo) ? (Node*)((RestrictInfo*)quals)->clause : quals;
		if (need_eval_const)
			clause = eval_const_expressions(NULL, clause);
		result = list_make1(clause);

		if(context->partition_expr)
		{
			context->hint = false;
			clause = mutator_equal_expr(clause, context);
			if(context->hint)
				result = lappend(result, clause);
		}
	}

	return result;
}
/*
 * let column=val to (like) hash(column)%remote_count = hash(value)%remote_count to hash(column)%remote_count = new_value
 *   column [not] in (v1,v2) to hash(column)%remote [not] in (new_v1,new_v2)
 */
static Node* mutator_equal_expr(Node *node, ModifyContext *context)
{
	Var *var;
	Const *c;
	Expr *convert;

	if (node == NULL)
		return NULL;

	if (IsA(node, OpExpr) &&
		list_length(((OpExpr*)node)->args) == 2)
	{
		OpExpr *op = (OpExpr*)node;
		c = get_var_equal_const(op->args, op->opno, context->relid, context->varattno, &var);
		if (c == NULL)
			goto next_mutator_equal_expr_;
		convert = (Expr*) coerce_to_target_type(NULL,
												(Node*)c,
												exprType((Node*)c),
												var->vartype,
												var->vartypmod,
												COERCION_EXPLICIT,
												COERCE_IMPLICIT_CAST,
												-1);

		if (convert != NULL)
		{
			Const *c2;
			ParamExecData *param;
			init_context_expr_if_need(context);
			param = &context->param_data[MODIFY_CONTEXT_PARAM_RIGHT];

			if ((void*)convert == (void*)c)
			{
				param->value = c->constvalue;
				param->isnull = c->constisnull;
			}else
			{
				MemoryContext old_context = MemoryContextSwitchTo(context->expr_context->ecxt_per_tuple_memory);
				ExprState *expr_state = ExecInitExpr(convert, NULL);
				param->value = ExecEvalExpr(expr_state,
											context->expr_context,
											&param->isnull);
				MemoryContextSwitchTo(old_context);

				if (param->isnull == false && context->var_typbyval == false)
					param->value = datumCopy(param->value, context->var_typbyval, context->var_typlen);
			}
			if (context->loc_info->locatorType == LOCATOR_TYPE_HASHMAP)
				c2 = (Const*)makeOidConst(InvalidOid);
			else
				c2 = (Const*)makeInt4Const(0);
			MemoryContextReset(context->expr_context->ecxt_per_tuple_memory);
			c2->constvalue = ExecEvalExprSwitchContext(context->right_state,
													   context->expr_context,
													   &c2->constisnull);
			context->hint = true;
			if (context->loc_info->locatorType == LOCATOR_TYPE_HASHMAP)
				return (Node*)makeOidEQ(context->partition_expr, (Expr*)c2);
			else
				return (Node*)makeInt4EQ(context->partition_expr, (Expr*)c2);
		}
	}else if (IsA(node, ScalarArrayOpExpr) &&
		((ScalarArrayOpExpr*)node)->useOr &&
		list_length(((ScalarArrayOpExpr*)node)->args) == 2)
	{
		ArrayType *arrayval;
		ScalarArrayOpExpr *sao = (ScalarArrayOpExpr*)node;
		c = get_var_equal_const(sao->args, sao->opno, context->relid, context->varattno, &var);
		if (c && c->constisnull == false &&
			type_is_array(c->consttype))
		{
			convert = NULL;
			arrayval = DatumGetArrayTypeP(c->constvalue);
			if (ARR_ELEMTYPE(arrayval) != var->vartype &&
				can_coerce_type(1, &ARR_ELEMTYPE(arrayval), &var->vartype, COERCION_EXPLICIT))
			{
				Param *param = makeNode(Param);
				param->paramkind = PARAM_EXEC;
				param->paramid = MODIFY_CONTEXT_PARAM_CONVERT;
				param->paramtype = ARR_ELEMTYPE(arrayval);
				param->paramtypmod = -1;
				param->paramcollid = InvalidOid;
				param->location = -1;

				convert = (Expr*) coerce_to_target_type(NULL,
														(Node*)param,
														ARR_ELEMTYPE(arrayval),
														var->vartype,
														var->vartypmod,
														COERCION_EXPLICIT,
														COERCE_IMPLICIT_CAST,
														-1);
			}

			if (ARR_ELEMTYPE(arrayval) == var->vartype ||
				convert != NULL)
			{
				ExprState *convert_state = NULL;
				ParamExecData *param;
				Datum	   *values;
				Datum	   *new_values;
				bool	   *nulls;
				int			num_elems;
				int			i;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
									 &elmlen,
									 &elmbyval,
									 &elmalign);
				deconstruct_array(arrayval,
								  ARR_ELEMTYPE(arrayval),
								  elmlen, elmbyval, elmalign,
								  &values,
								  &nulls,
								  &num_elems);

				init_context_expr_if_need(context);
				param = context->param_data;
				new_values = palloc(sizeof(Datum)*num_elems);
				if (convert)
					convert_state = ExecInitExpr(convert, NULL);
				for (i=0;i<num_elems;++i)
				{
					MemoryContextReset(context->expr_context->ecxt_per_tuple_memory);
					if (convert_state)
					{
						param[MODIFY_CONTEXT_PARAM_CONVERT].value = values[i];
						param[MODIFY_CONTEXT_PARAM_CONVERT].isnull = nulls[i];
						param[MODIFY_CONTEXT_PARAM_RIGHT].value = ExecEvalExprSwitchContext(convert_state,
																							context->expr_context,
																							&param[MODIFY_CONTEXT_PARAM_RIGHT].isnull);
					}else
					{
						param[MODIFY_CONTEXT_PARAM_RIGHT].value = values[i];
						param[MODIFY_CONTEXT_PARAM_RIGHT].isnull = nulls[i];
					}
					new_values[i] = ExecEvalExprSwitchContext(context->right_state,
															  context->expr_context,
															  &elmbyval	/* Interim use */);
					/* right_expr not return NULL value, even input is NULL */
					Assert(elmbyval == false);
				}
				if (context->loc_info->locatorType == LOCATOR_TYPE_HASHMAP)
					node = (Node*)makeOidArrayIn(context->partition_expr, new_values, num_elems);
				else
					node = (Node*)makeInt4ArrayIn(context->partition_expr, new_values, num_elems);
				context->hint = true;
				pfree(new_values);
				if (convert_state)
				{
					pfree(convert_state);
					pfree(convert);
				}
				return node;
			}
		}
	}

next_mutator_equal_expr_:
	return expression_tree_mutator(node, mutator_equal_expr, context);
}

static Const* get_var_equal_const(List *args, Oid opno, Index relid, AttrNumber attno, Var **var)
{
	Expr *l;
	Expr *r;
	Const *c;
	Oid type_oid;

	if (list_length(args)!=2)
		return NULL;

	l = linitial(args);
	r = llast(args);
	type_oid = exprType((Node*)l);

	if (op_hashjoinable(opno, type_oid) ||
		op_mergejoinable(opno, type_oid))
	{
		while (IsA(l, RelabelType))
			l = ((RelabelType*)l)->arg;
		while (IsA(r, RelabelType))
			r = ((RelabelType*)r)->arg;

		if (IsA(l, Var) &&
			((Var*)l)->varno == relid &&
			((Var*)l)->varattno == attno &&
			(c=is_const_able_expr(r)) != NULL)
		{
			*var = (Var*)l;
			return c;
		}else if (IsA(r, Var) &&
			((Var*)r)->varno == relid &&
			((Var*)r)->varattno == attno &&
			(c=is_const_able_expr(l)) != NULL)
		{
			*var = (Var*)r;
			return c;
		}
	}

	return NULL;
}

static Const* get_mvar_equal_const(List *args, Oid opno, Index relid, Bitmapset *attnos, Var **var)
{
	Expr *l;
	Expr *r;
	Const *c;
	Oid type_oid;

	if (list_length(args)!=2)
		return NULL;

	l = linitial(args);
	r = llast(args);
	type_oid = exprType((Node*)l);

	if (op_hashjoinable(opno, type_oid) ||
		op_mergejoinable(opno, type_oid))
	{
		while (IsA(l, RelabelType))
			l = ((RelabelType*)l)->arg;
		while (IsA(r, RelabelType))
			r = ((RelabelType*)r)->arg;

		if (IsA(l, Var) &&
			((Var*)l)->varno == relid &&
			bms_is_member(((Var*)l)->varattno, attnos)&&
			(c=is_const_able_expr(r)) != NULL)
		{
			*var = (Var*)l;
			return c;
		}else if (IsA(r, Var) &&
			((Var*)r)->varno == relid &&
			bms_is_member(((Var*)r)->varattno, attnos) &&
			(c=is_const_able_expr(l)) != NULL)
		{
			*var = (Var*)r;
			return c;
		}
	}

	return NULL;
}

static Const* is_const_able_expr(Expr *expr)
{
	for(;;)
	{
		if (IsA(expr, RelabelType))
			expr = ((RelabelType*)expr)->arg;
		else if (IsA(expr, CollateExpr))
			expr = ((CollateExpr*)expr)->arg;
		else
			break;
	}

	return IsA(expr, Const) ? (Const*)expr:NULL;
}

static void init_context_expr_if_need(ModifyContext *context)
{
	if (context->right_state == NULL)
	{
		Param *param;
		context->expr_context = CreateStandaloneExprContext();
		context->param_data = palloc0(sizeof(ParamExecData) * MODIFY_CONTEXT_PARAM_COUNT);
		context->expr_context->ecxt_param_exec_vals = context->param_data;

		param = context->param_expr = makeNode(Param);
		param->paramkind = PARAM_EXEC;
		param->paramid = MODIFY_CONTEXT_PARAM_RIGHT;
		param->paramtype = context->var_expr->vartype;
		param->paramtypmod = context->var_expr->vartypmod;
		param->paramcollid = context->var_expr->varcollid;
		param->location = -1;

		get_typlenbyval(param->paramtype, &context->var_typlen, &context->var_typbyval);

		context->right_expr = makePartitionExpr(context->loc_info, (Node*)param);
		context->right_state = ExecInitExpr(context->right_expr, NULL);
	}
}

static List* get_auxiary_quals(List *baserestrictinfo, Index relid, const Bitmapset *attnos)
{
	ListCell *lc;
	List *new_quals = NIL;
	Bitmapset *auxattnos = NULL;
	Bitmapset *varattnos;
	int x = -1;
	while((x=bms_next_member(attnos, x)) >= 0)
		auxattnos = bms_add_member(auxattnos, x - FirstLowInvalidHeapAttributeNumber);

	/* quals have auxiliary var ? */
	new_quals = NIL;
	foreach(lc, baserestrictinfo)
	{
		Expr *qual = ((RestrictInfo*)lfirst(lc))->clause;
		if (contain_mutable_functions((Node*)qual))
			continue;

		varattnos = NULL;
		pull_varattnos((Node*)qual, relid, &varattnos);
		if (bms_overlap(varattnos, auxattnos))
			new_quals = lappend(new_quals, qual);
		bms_free(varattnos);
	}

	bms_free(auxattnos);
	return new_quals;
}

static bool push_const_to_aux_col_info(GatherAuxColumnInfo *info, Const *c, bool isarray)
{
	CoerceInfo *coerce;
	Datum	   *datums;
	Datum		value;
	bool	   *nulls;
	Oid			typid;
	int			i,count;

	if (c->constisnull)
	{
		/* safely we only not use null values */
		return false;
	}

	if (isarray == false)
	{
		typid = c->consttype;
		datums = &c->constvalue;
		nulls = &c->constisnull;
		count = 1;
	}else
	{
		ArrayType		   *arrayval;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		arrayval = DatumGetArrayTypeP(c->constvalue);
		typid = ARR_ELEMTYPE(arrayval);
		get_typlenbyvalalign(typid, &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  typid,
						  elmlen,
						  elmbyval,
						  elmalign,
						  &datums,
						  &nulls,
						  &count);
	}

	coerce = get_coerce_info(info, typid);
	if (coerce)
	{
		if (coerce->valid == false)
			return false;	/* can not coerce */
	}

	for(i=0;i<count;++i)
	{
		if (nulls[i])
		{
			info->has_null = true;
			continue;
		}

		if (coerce)
		{
			bool isnull;
			coerce->param_data->value = datums[i];
			coerce->param_data->isnull = nulls[i];
			MemoryContextReset(info->econtext->ecxt_per_tuple_memory);
			value = ExecEvalExprSwitchContext(coerce->exprState,
											  info->econtext,
											  &isnull);
			if (isnull)
			{
				info->has_null = true;
				continue;
			}
			if (info->attr->attbyval == false)
				value = datumCopy(value, false, info->attr->attlen);
		}else
		{
			value = datums[i];
		}

		Assert(info->cur_size <= info->max_size);
		if (info->cur_size == info->max_size)
		{
			uint32 new_size = info->max_size + AUX_SCAN_INFO_SIZE_STEP;
			info->datum = repalloc(info->datum, new_size);
			info->max_size = new_size;
		}
		Assert(info->cur_size < info->max_size);
		info->datum[info->cur_size++] = value;
	}
	return true;
}

static void push_aux_col_info_to_gather(GatherAuxInfoContext *context, GatherAuxColumnInfo *info)
{
	GatherAuxColumnInfo *old_info = get_gather_aux_col_info(context, info->attno);
	if (info->has_null)
		old_info->has_null = true;
	if (old_info->max_size < old_info->cur_size+info->cur_size)
	{
		uint32 new_size = old_info->max_size + info->max_size;
		old_info->datum = repalloc(old_info->datum, new_size);
		old_info->max_size = new_size;
	}
	Assert(old_info->cur_size+info->cur_size <= old_info->max_size);
	memcpy(&old_info->datum[old_info->cur_size],
		   info->datum,
		   info->cur_size * sizeof(Datum));
	old_info->cur_size += info->cur_size;
}

static GatherAuxColumnInfo* get_gather_aux_col_info(GatherAuxInfoContext *context, AttrNumber attno)
{
	ListCell			*lc;
	GatherAuxColumnInfo *info;

	foreach(lc, context->list_info)
	{
		info = lfirst(lc);
		if (info->attno == attno)
			return info;
	}

	info = palloc0(sizeof(*info));
	info->datum = palloc0(sizeof(Datum) * AUX_SCAN_INFO_SIZE_STEP);
	info->max_size = AUX_SCAN_INFO_SIZE_STEP;
	info->attno = attno;
	info->attr = TupleDescAttr(RelationGetDescr(context->rel), attno-1);
	info->econtext = context->econtext;

	context->list_info = lappend(context->list_info, info);

	return info;
}

static void free_aux_col_info(GatherAuxColumnInfo *info)
{
	list_free_deep(info->list_coerce);
	if (info->param_expr)
		pfree(info->param_expr);
	if (info->reduce_expr)
		pfree(info->reduce_expr);
	if (info->rel)
		table_close(info->rel, AccessShareLock);
	if (info->datum)
		pfree(info->datum);
	list_free(info->list_exec);
	pfree(info);
}

static CoerceInfo* get_coerce_info(GatherAuxColumnInfo *info, Oid typeoid)
{
	ListCell   *lc;
	CoerceInfo *coerce;
	Expr	   *expr;
	Param	   *param;

	Form_pg_attribute attr = info->attr;
	if (typeoid == attr->atttypid)
		return NULL;

	foreach(lc, info->list_coerce)
	{
		coerce = lfirst(lc);
		if (coerce->type == typeoid)
			return coerce->expr ? coerce:NULL;
	}

	coerce = palloc0(sizeof(*coerce));
	info->list_coerce = lappend(info->list_coerce, coerce);
	coerce->type = typeoid;
	param = coerce->param_expr = makeNode(Param);
	param->paramkind = PARAM_EXEC;
	param->paramid = MODIFY_CONTEXT_PARAM_CONVERT;
	param->paramtype = typeoid;
	param->paramtypmod = -1;
	param->paramcollid = InvalidOid;
	param->location = -1;
	coerce->expr = (Expr*)coerce_to_target_type(NULL,
												(Node*)param,
												typeoid,
												attr->atttypid,
												attr->atttypmod,
												COERCION_EXPLICIT,
												COERCE_IMPLICIT_CAST,
												-1);
	expr = coerce->expr;
	if (expr == NULL)
	{
		/* can not coerce */
		pfree(coerce->param_expr);
		coerce->param_expr = NULL;
		coerce->valid = false;
		return coerce;
	}

	coerce->valid = true;
	while(IsA(expr, RelabelType))
		expr = ((RelabelType*)expr)->arg;

	if ((void*)expr == (void*)(coerce->param_expr))
	{
		/* don't need coerce */
		pfree(coerce->param_expr);
		coerce->param_expr = NULL;
		coerce->expr = NULL;
		return NULL;
	}

	coerce->exprState = ExecInitExpr(coerce->expr, NULL);
	coerce->param_data = &info->econtext->ecxt_param_exec_vals[MODIFY_CONTEXT_PARAM_CONVERT];

	return coerce;
}

static bool gather_aux_info(Node *node, GatherAuxInfoContext *context)
{
	Var *var;
	Const *c;
	GatherAuxColumnInfo *info;

	if (node == NULL ||
		is_notclause(node))
		return false;

	/* check_stack_depth(); */

	if (IsA(node, OpExpr) &&
		list_length(((OpExpr*)node)->args) == 2)
	{
		OpExpr *op = (OpExpr*)node;
		c = get_mvar_equal_const(op->args,
								 op->opno,
								 context->relid,
								 context->bms_attnos,
								 &var);
		if (c != NULL)
		{
			info = get_gather_aux_col_info(context, var->varattno);
			if (push_const_to_aux_col_info(info, c, false))
				context->hint = true;
			return false;
		}
	}else if (IsA(node, ScalarArrayOpExpr) &&
		((ScalarArrayOpExpr*)node)->useOr &&
		list_length(((ScalarArrayOpExpr*)node)->args) == 2)
	{
		ScalarArrayOpExpr *sao = (ScalarArrayOpExpr*)node;
		c = get_mvar_equal_const(sao->args,
								 sao->opno,
								 context->relid,
								 context->bms_attnos,
								 &var);
		if (c && c->constisnull == false &&
			type_is_array(c->consttype))
		{
			info = get_gather_aux_col_info(context, var->varattno);
			if (push_const_to_aux_col_info(info, c, true))
				context->hint = true;
			return false;
		}
	}else if (is_orclause(node))
	{
		BoolExpr *bexpr = (BoolExpr*)node;
		ListCell *lc;
		GatherAuxInfoContext tmp;
		memcpy(&tmp, context, sizeof(tmp));
		tmp.list_info = NIL;
		tmp.deep = false;
		foreach(lc, bexpr->args)
		{
			tmp.hint = false;
			gather_aux_info(lfirst(lc), &tmp);
			if (tmp.hint == false ||				/* not got */
				list_length(tmp.list_info) != 1)	/* too many var */
				break;
		}
		if (lc == NULL)
		{
			/* all args using same auxiliary column */
			GatherAuxColumnInfo *info;
			Assert(list_length(tmp.list_info) == 1);
			info = linitial(tmp.list_info);
			push_aux_col_info_to_gather(context, info);
			free_aux_col_info(info);
			context->hint = true;
			return false;
		}else
		{
			foreach(lc, tmp.list_info)
				free_aux_col_info(lfirst(lc));
		}
	}else if (IsA(node, NullTest) &&
		((NullTest*)node)->nulltesttype == IS_NULL)
	{
		Expr *expr = ((NullTest*)node)->arg;
		while (IsA(expr, RelabelType))
			expr = ((RelabelType*)expr)->arg;
		if (IsA(expr, Var) &&
			bms_is_member(((Var*)expr)->varattno, context->bms_attnos))
		{
			info = get_gather_aux_col_info(context, ((Var*)expr)->varattno);
			info->has_null = true;
			context->hint = true;
			return false;
		}
	}

	if (context->deep == false)
		return false;
	return expression_tree_walker(node, gather_aux_info, context);
}

static void init_auxiliary_info_if_need(GatherAuxInfoContext *context)
{
	ListCell			*lc;
	GatherAuxColumnInfo *info;
	Oid					reloid;
	foreach(lc, context->list_info)
	{
		info = lfirst(lc);
		if (info->rel == NULL)
		{
			Form_pg_attribute a1;
			Form_pg_attribute a2;

			reloid = LookupAuxRelation(RelationGetRelid(context->rel),
									   info->attno);
			info->rel = table_open(reloid, AccessShareLock);
			if (RELATION_IS_OTHER_TEMP(info->rel))
			{
				table_close(info->rel, AccessShareLock);
				info->rel = NULL;
				continue;
			}

			/* compare attribute */
			a1 = TupleDescAttr(RelationGetDescr(context->rel), info->attno-1);
			a2 = TupleDescAttr(RelationGetDescr(info->rel), Anum_aux_table_key-1);
			if (a1->atttypid != a2->atttypid ||
				a1->atttypmod != a2->atttypmod ||
				a1->attlen != a2->attlen ||
				a1->attcollation != a2->attcollation ||
				a1->attbyval != a2->attbyval ||
				a1->attndims != a2->attndims ||
				a1->attalign != a2->attalign)
			{
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("auxiliary table \"%s\" key column \"%s\" not equal main table",
						 		RelationGetRelationName(info->rel),
								NameStr(a2->attname)),
						 err_generic_string(PG_DIAG_SCHEMA_NAME, get_namespace_name(RelationGetNamespace(info->rel))),
						 err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(info->rel)),
						 err_generic_string(PG_DIAG_COLUMN_NAME, NameStr(a2->attname))));
			}
		}
		if (info->aux_loc == AUX_REL_LOC_INFO_INVALID)
		{
			if (info->rel->rd_locator_info)
			{
				RelationLocInfo *loc = info->rel->rd_locator_info;
				if (IsLocatorReplicated(loc->locatorType))
				{
					info->aux_loc = AUX_REL_LOC_INFO_REP;
				}else if (loc->locatorType == LOCATOR_TYPE_RANDOM)
				{
					ereport(ERROR, (errmsg("auxiliary table not support distribte by roundrobin")));
				}else
				{
					/* make reduce expr and status */
					Param *param = info->param_expr = makeNode(Param);
					param->paramkind = PARAM_EXEC;
					param->paramid = GACI_PARAM_REDUCE;
					param->paramtype = info->attr->atttypid;
					param->paramtypmod = info->attr->atttypmod;
					param->paramcollid = info->attr->attcollation;
					param->location = -1;
					info->reduce_expr = makePartitionExpr(loc, (Node*)param);
					info->reduce_state = ExecInitExpr(info->reduce_expr, NULL);
					info->aux_loc = AUX_REL_LOC_INFO_REDUCE;
					info->param_data = &info->econtext->ecxt_param_exec_vals[GACI_PARAM_REDUCE];
				}
			}else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Auxiliary table \"%s\" is not a remote table", RelationGetRelationName(info->rel))));
			}
		}
	}
}

static void set_cheapest_auxiliary(GatherAuxInfoContext *context)
{
	ListCell			*lc;
	GatherAuxColumnInfo *info;
	GatherAuxColumnInfo *cheapest = NULL;

	foreach(lc, context->list_info)
	{
		info = lfirst(lc);
		info->list_exec = get_aux_table_execute_on(info);

		/* ignore empty */
		if (info->cur_size == 0 &&
			info->has_null == false)
			continue;

		if (cheapest == NULL)
		{
			cheapest = info;
		}else if(list_length(info->list_exec) < list_length(cheapest->list_exec))
		{
			cheapest = info;
		}
	}

	context->cheapest = cheapest;
}

static List* get_aux_table_execute_on(GatherAuxColumnInfo *info)
{
	Datum value;
	List *exec_on = NIL;

	if (info->aux_loc == AUX_REL_LOC_INFO_REP)
	{
		exec_on = GetPreferredRepNodeIds(info->rel->rd_locator_info->nodeids);
	}else if (info->aux_loc == AUX_REL_LOC_INFO_REDUCE)
	{
		ExprContext *econtext = info->econtext;
		ParamExecData *param = info->param_data;
		Bitmapset *bms = NULL;
		ListCell *lc;
		uint32 i;
		bool isnull;

		param->isnull = false;
		for (i=0;i<info->cur_size;++i)
		{
			MemoryContextReset(econtext->ecxt_per_tuple_memory);
			param->value = info->datum[i];
			value = ExecEvalExprSwitchContext(info->reduce_state,
											  econtext,
											  &isnull);
			Assert(isnull == false);
			bms = bms_add_member(bms, DatumGetInt32(value));
		}
		if (info->has_null)
		{
			MemoryContextReset(econtext->ecxt_per_tuple_memory);
			param->isnull = true;
			param->value = (Datum)0;
			value = ExecEvalExprSwitchContext(info->reduce_state,
											  econtext,
											  &isnull);
			Assert(isnull == false);
			bms = bms_add_member(bms, DatumGetInt32(value));
		}

		i = 0;
		foreach(lc, info->rel->rd_locator_info->nodeids)
		{
			if (bms_is_member(i++, bms))
				exec_on = lappend_oid(exec_on, lfirst_oid(lc));
		}
		bms_free(bms);
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("Unknown locator %d for set cheapest auxiliary",
						info->aux_loc)));
	}

	return exec_on;
}

static GatherMainRelExecOn* get_main_table_execute_on(GatherAuxInfoContext *context, GatherAuxColumnInfo *info)
{
	Bitmapset		   *bms_attnos;
	List			   *list_conn;
	TupleDesc		 	result_desc;
	Form_pg_attribute	attrs[2];
	GatherMainRelExecOn	*exec_on;

	bms_attnos = bms_make_singleton(Anum_aux_table_auxnodeid - FirstLowInvalidHeapAttributeNumber);
	bms_attnos = bms_add_member(bms_attnos, Anum_aux_table_auxctid - FirstLowInvalidHeapAttributeNumber);
	exec_on = palloc0(sizeof(*exec_on));

	list_conn = ExecClusterHeapScan(info->list_exec,
									info->rel,
									bms_attnos,
									Anum_aux_table_key,
									info->datum,
									info->cur_size,
									info->has_null);
	result_desc = CreateTemplateTupleDesc(2);
#if (Anum_aux_table_auxnodeid < Anum_aux_table_auxctid)
	attrs[0] = TupleDescAttr(RelationGetDescr(info->rel), Anum_aux_table_auxnodeid-1);
	attrs[1] = TupleDescAttr(RelationGetDescr(info->rel), Anum_aux_table_auxctid-1);
	exec_on->index_nodeid = 0;
	exec_on->index_tid = 1;
#else
#error change result_desc
#endif
	result_desc = CreateTupleDesc(lengthof(attrs), attrs);
	if (create_type_convert(result_desc, false, true) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("integer and tid should not need convert")));
	exec_on->slot = MakeSingleTupleTableSlot(result_desc, &TTSOpsMinimalTuple);
	exec_on->tids = palloc(sizeof(ItemPointerData)*AUX_SCAN_INFO_SIZE_STEP);
	exec_on->max_tid_size = AUX_SCAN_INFO_SIZE_STEP;
	exec_on->cur_tid_size = 0;
	exec_on->funcs = PQNDefaultHookFunctions;
	exec_on->funcs.HookCopyOut = process_remote_aux_tuple;
	PQNListExecFinish(list_conn, NULL, &exec_on->funcs, true);

	ExecDropSingleTupleTableSlot(exec_on->slot);
	exec_on->slot = NULL;
	FreeTupleDesc(result_desc);

	return exec_on;
}

static bool process_remote_aux_tuple(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len)
{
	GatherMainRelExecOn *context = HOOK_GET_GATHER(pub);
	TupleTableSlot *slot = context->slot;

	if (clusterRecvTuple(context->slot, buf, len, NULL, conn))
	{
		slot_getallattrs(slot);
		context->list_nodeid = list_append_unique_int(context->list_nodeid,
													  DatumGetInt32(slot->tts_values[context->index_nodeid]));
		push_tid_to_exec_on(context, slot->tts_values[context->index_tid]);
	}

	return false;
}

static void push_tid_to_exec_on(GatherMainRelExecOn *context, Datum datum)
{
	uint32	i,count = context->cur_tid_size;
	ItemPointer itemPtr = (ItemPointer)DatumGetPointer(datum);

	for (i=0;i<count;++i)
	{
		if (ItemPointerEquals(itemPtr, &context->tids[i]))
			return;
	}

	if (context->cur_tid_size == context->max_tid_size)
	{
		uint32 new_size = context->max_tid_size + AUX_SCAN_INFO_SIZE_STEP;
		context->tids = repalloc(context->tids, new_size * sizeof(ItemPointerData));
		context->max_tid_size = new_size;
	}
	Assert(context->cur_tid_size < context->max_tid_size);
	context->tids[context->cur_tid_size++] = *itemPtr;
}

static Expr* make_ctid_in_expr(Index relid, ItemPointer tids, uint32 count)
{
	Const *c;
	const FormData_pg_attribute *attr PG_USED_FOR_ASSERTS_ONLY;
	Expr *expr;
	Var *var = makeVar(relid,
					   SelfItemPointerAttributeNumber,
					   TIDOID,
					   -1,
					   InvalidOid,
					   0);
	Assert(count > 0);
	Assert((attr = SystemAttributeDefinition(SelfItemPointerAttributeNumber)) != NULL &&
		   attr->atttypid == TIDOID &&
		   attr->attbyval == false &&
		   attr->attlen == sizeof(ItemPointerData));
	if (count == 1)
	{
		c = makeConst(TIDOID,
					  -1,
					  InvalidOid,
					  sizeof(ItemPointerData),
					  datumCopy(PointerGetDatum(tids), false, sizeof(ItemPointerData)),
					  false,
					  false);
		expr = make_op(NULL, SystemFuncName("="), (Node*)var, (Node*)c, NULL, -1);
	}else
	{
		Datum *ctids = palloc(sizeof(Datum) * count);
		ArrayType *array;
		uint32 i;

		//qsort(ctids, count, sizeof(Datum), (int(*)(const void*, const void*))ItemPointerCompare);
		qsort(tids, count, sizeof(ItemPointerData), (int(*)(const void*, const void*))ItemPointerCompare);
		for (i=0;i<count;++i)
			ctids[i] = PointerGetDatum(&tids[i]);

		array = construct_array(ctids,
								count,
								TIDOID,
								sizeof(ItemPointerData),
								false,
								's');
		pfree(ctids);

		c = makeConst(get_array_type(TIDOID),
					  -1,
					  InvalidOid,
					  -1,
					  PointerGetDatum(array),
					  false,
					  false);
		expr = make_scalar_array_op(NULL,
									SystemFuncName("="),
									true,
									(Node*)var,
									(Node*)c,
									-1);
	}

	return expr;
}
