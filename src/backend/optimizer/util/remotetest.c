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
#include "catalog/pg_aux_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/plancat.h"
#include "optimizer/predtest.h"
#include "optimizer/reduceinfo.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "pgxc/locator.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"

typedef struct ModifyContext
{
	RelationLocInfo *loc_info;
	Expr *partition_expr;				/* like hash(column) % datanode_count */
	Expr *right_expr;					/* like hash(value) % datanode_count */
	Const *const_expr;					/* right_expr's value */
	ExprState *right_state;				/* init by right_expr */
	ExprContext *expr_context;
	Index relid;
	AttrNumber varattno;
	bool hint;
}ModifyContext;

typedef struct ModifyAuxVarContext
{
	char			   *query_sql;			/* select ... from aux_table where key = ... */
	char			   *aux_rel_name;		/* table name for auxiliary table */
	char			   *aux_var_name;		/* auxiliary table column name(distribute key), same to main table's auxiliary column */
	List			   *list_remote;		/* remote node's Oid execute on */
	RelationLocInfo	   *loc_info;			/* auxiliary table locator info */
	Expr			   *reduce_expr;		/* auxiliary table reduce expr */
	Const			   *const_expr;			/* auxiliary distribute key in reduce_expr */
	ExprState		   *reduce_expr_state;	/* is ExecInitExpr(reduce_expr) */
	ExprContext		   *expr_context;		/* execute context */
	StringInfoData		buf;				/* temp buffer */
	Oid					aux_rel_oid;		/* auxiliary table's OID */
	AttrNumber			varattno;			/* var attribute number for main table(not auxilary) */
}ModifyAuxVarContext;

typedef struct ModifyToAuxContext
{
	Relation main_rel;	/* main relation */
	List *list_auxvar;	/* list of ModifyAuxVarContext */
	Index relid;
	bool hint;
	bool in_not_expr;
}ModifyToAuxContext;

int use_aux_type = USE_AUX_NODE;
int use_aux_arg = 100;

static Expr* makeInt4EQ(Expr *l, Expr *r);
static Expr* makeInt4ArrayIn(Expr *l, Datum *values, int count);
static Expr* makeInt4Const(int32 val);
static Expr* makeNotNullTest(Expr *expr, bool isrow);
static Expr* makePartitionExpr(RelationLocInfo *loc_info, Node *node);
static List* make_new_qual_list(ModifyContext *context, Node *quals, bool need_eval_const);
static Node* mutator_equal_expr(Node *node, ModifyContext *context);
static void init_context_expr_if_need(ModifyContext *context);
static Const* get_var_equal_const(List *args, Oid opno, Index relid, AttrNumber attno, Var **var);
static Const* get_mvar_equal_const(List *args, Oid opno, Index relid, Bitmapset *attnos, Var **var);
static Const* is_const_able_expr(Expr *expr);
static List* get_auxiary_quals(List *baserestrictinfo, Index relid, const Bitmapset *attnos);
static Node* mutator_aux_equal_expr(Node *node, ModifyToAuxContext *context);
static void reset_aux_var_context(ModifyAuxVarContext *context);
static ModifyAuxVarContext* get_aux_var_context(ModifyToAuxContext *context, AttrNumber attno);
static void make_aux_table_query_sql(ModifyAuxVarContext *mavc, Const *c, bool isarray);
static List* get_aux_table_execute_on(ModifyAuxVarContext *mavc, Var *var, Const *c, bool isarray);
static List* get_main_table_execute_on(ModifyAuxVarContext *mavc);
static List* oid_list_copy_bms(List *oid_list, Bitmapset *bms_index);
static Node* make_xc_node_id_in_expr(Index relid, List *list);

/* return remote oid list */
List *relation_remote_by_constraints(PlannerInfo *root, RelOptInfo *rel)
{
	List *result;
	Relation relation;
	RangeTblEntry *rte;
	List *new_quals;
	List *tmp_list;;
	MemoryContext old_mctx;
	MemoryContext main_mctx;
	ModifyToAuxContext context;

	result = relation_remote_by_constraints_base(root,
												(Node*)rel->baserestrictinfo,
												rel->loc_info,
												rel->relid);
	if (use_aux_type == USE_AUX_OFF
		|| rel->baserestrictinfo == NIL
		|| list_length(result) <= 1
		|| list_length(result) < list_length(rel->loc_info->nodeids)
		|| IsLocatorReplicated(rel->loc_info->locatorType)
		|| rel->loc_info->locatorType == LOCATOR_TYPE_RROBIN)
		return result;

	/* have auxiliary table ? */
	rte = root->simple_rte_array[rel->relid];
	relation = relation_open(rte->relid, AccessShareLock);
	if (relation->rd_auxatt == NULL ||
		/* quals have auxiliary var ? */
		(new_quals = get_auxiary_quals(rel->baserestrictinfo, rel->relid, relation->rd_auxatt)) == NULL)
	{
		relation_close(relation, AccessShareLock);
		return result;
	}

	main_mctx = AllocSetContextCreate(CurrentMemoryContext,
									  "RRBC",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	old_mctx = MemoryContextSwitchTo(main_mctx);

	context.main_rel = relation;
	context.list_auxvar = NIL;
	context.relid = rel->relid;
	context.hint = false;
	tmp_list = (List*)mutator_aux_equal_expr((Node*)new_quals, &context);
	list_free(new_quals);
	list_free(result);
	new_quals = tmp_list;
	result = relation_remote_by_constraints_base(root, (Node*)new_quals, rel->loc_info, rel->relid);

	relation_close(relation, AccessShareLock);
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
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	temp_mctx = AllocSetContextCreate(main_mctx,
									  "RRBC temp",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	old_mctx = MemoryContextSwitchTo(main_mctx);

	context.relid = varno;
	null_test_list = NIL;
	if (loc_info->locatorType == LOCATOR_TYPE_USER_DEFINED)
	{
		if(list_length(loc_info->funcAttrNums) == 1)
		{
			context.varattno = linitial_int(loc_info->funcAttrNums);
			context.partition_expr = makePartitionExpr(loc_info, (Node*)makeVarByRel(context.varattno, loc_info->relid, varno));
		}
		if (func_strict(loc_info->funcid))
		{
			foreach(lc, loc_info->funcAttrNums)
			{
				null_test_list = lappend(null_test_list,
										 makeNotNullTest((Expr*)makeVarByRel(lfirst_int(lc), loc_info->relid, varno), false));
			}
		}
	}else if(IsLocatorDistributedByValue(loc_info->locatorType))
	{
		context.varattno = loc_info->partAttrNum;
		context.partition_expr = makePartitionExpr(loc_info, (Node*)makeVarByRel(loc_info->partAttrNum, loc_info->relid, varno));
		null_test_list = list_make1(makeNotNullTest((Expr*)makeVarByRel(context.varattno, loc_info->relid, varno), false));
	}

	constraint_pred = get_relation_constraints_base(root, loc_info->relid, varno, true);
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
			expr = makeInt4EQ(context.partition_expr, makeInt4Const(i));
			temp_constraints = lappend(temp_constraints, expr);
		}

		if (predicate_refuted_by(temp_constraints, new_clauses) == false)
		{
			MemoryContextSwitchTo(old_mctx);
			result = lappend_oid(result, node_oid);
			/* MemoryContextSwitchTo(...) */
		}
		++i;
	}

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
	CoalesceExpr *coalesce;
	Expr *expr;

	switch(loc_info->locatorType)
	{
	case LOCATOR_TYPE_HASH:
		expr = makeHashExpr((Expr*)node);
		break;
	case LOCATOR_TYPE_MODULO:
		expr = (Expr*)node;
		break;
	case LOCATOR_TYPE_USER_DEFINED:
		if(list_length(loc_info->funcAttrNums) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("only support one param")));
		}else
		{
			List *args;
			Oid *argTypes;
			int narg;
			int i;

			get_func_signature(loc_info->funcid, &argTypes, &narg);
			if(narg == 0)
				elog(ERROR, "too many argument for user hash distribute table");

			expr = (Expr*)coerce_to_target_type(NULL,
												node,
												exprType(node),
												argTypes[0],
												-1,
												COERCION_EXPLICIT,
												COERCE_IMPLICIT_CAST,
												-1);
			args = list_make1(expr);

			for(i=1;i<narg;++i)
				args = lappend(args, makeNullConst(argTypes[i], -1, InvalidOid));
			expr = (Expr*) makeFuncExpr(loc_info->funcid,
										get_func_rettype(loc_info->funcid),
										args,
										InvalidOid,
										InvalidOid,
										COERCE_EXPLICIT_CALL);
		}
		break;
	default:
		return NULL;
	}

	expr = makeModuloExpr(expr, list_length(loc_info->nodeids));
	expr = (Expr*)coerce_to_target_type(NULL,
										(Node*)expr,
										exprType((Node*)expr),
										INT4OID,
										-1,
										COERCION_EXPLICIT,
										COERCE_IMPLICIT_CAST,
										-1);
	expr = (Expr*)makeFuncExpr(F_INT4ABS,
								INT4OID,
								list_make1(expr),
								InvalidOid,
								InvalidOid,
								COERCE_EXPLICIT_CALL);

	coalesce = makeNode(CoalesceExpr);
	coalesce->coalescetype = INT4OID,
	coalesce->coalescecollid = InvalidOid;
	coalesce->args = list_make2(expr, makeInt4Const(0)); /* when null, first node */

	return (Expr*)coalesce;
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
			init_context_expr_if_need(context);

			if ((void*)convert == (void*)c)
			{
				context->const_expr->constvalue = c->constvalue;
				context->const_expr->constisnull = c->constisnull;
			}else
			{
				MemoryContext old_context = MemoryContextSwitchTo(context->expr_context->ecxt_per_tuple_memory);
				ExprState *expr_state = ExecInitExpr(convert, NULL);
				c2 = context->const_expr;
				c2->constvalue = ExecEvalExpr(expr_state,
											  context->expr_context,
											  &c2->constisnull,
											  NULL);
				MemoryContextSwitchTo(old_context);

				if (c2->constisnull == false && c2->constbyval == false)
					c2->constvalue = datumCopy(c2->constvalue, c2->constbyval, c2->constlen);
			}
			c2 = (Const*)makeInt4Const(0);
			MemoryContextReset(context->expr_context->ecxt_per_tuple_memory);
			c2->constvalue = ExecEvalExprSwitchContext(context->right_state,
													   context->expr_context,
													   &c2->constisnull,
													   NULL);
			context->hint = true;
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
				c = makeNullConst(ARR_ELEMTYPE(arrayval), -1, InvalidOid);
				convert = (Expr*) coerce_to_target_type(NULL,
														(Node*)c,
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
				new_values = palloc(sizeof(Datum)*num_elems);
				if (convert)
					convert_state = ExecInitExpr(convert, NULL);
				for (i=0;i<num_elems;++i)
				{
					MemoryContextReset(context->expr_context->ecxt_per_tuple_memory);
					if (convert_state)
					{
						/* c is new Const, not ScalarArrayOpExpr's arg */
						c->constvalue = values[i];
						c->constisnull = nulls[i];
						context->const_expr->constvalue = ExecEvalExprSwitchContext(convert_state,
																					context->expr_context,
																					&context->const_expr->constisnull,
																					NULL);
					}else
					{
						context->const_expr->constvalue = values[i];
						context->const_expr->constisnull = nulls[i];
					}
					new_values[i] = ExecEvalExprSwitchContext(context->right_state,
															  context->expr_context,
															  &elmbyval,	/* Interim use */
															  NULL);
					/* right_expr not return NULL value, even input is NULL */
					Assert(elmbyval == false);
				}
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

/*
 * let aux_col=val to xc_node_id = node
 *   aux_col in (v1,v2) to xc_node_id in (node1,node2)
 */
static Node* mutator_aux_equal_expr(Node *node, ModifyToAuxContext *context)
{
	Var *var;
	Const *c;
	ModifyAuxVarContext *avc;
	List *execute_on;

	if (node == NULL)
		return NULL;

	if (context->in_not_expr == false)
	{
		if (IsA(node, OpExpr) &&
			list_length(((OpExpr*)node)->args) == 2)
		{
			OpExpr *op = (OpExpr*)node;
			c = get_mvar_equal_const(op->args,
									 op->opno,
									 context->relid,
									 context->main_rel->rd_auxatt,
									 &var);
			if (c == NULL)
				goto next_mutator_aux_equal_expr_;

			avc = get_aux_var_context(context, var->varattno);
			reset_aux_var_context(avc);
			make_aux_table_query_sql(avc, c, false);
			list_free(avc->list_remote);
			avc->list_remote = get_aux_table_execute_on(avc, var, c, false);
			execute_on = get_main_table_execute_on(avc);
			node = make_xc_node_id_in_expr(var->varno, execute_on);
			list_free(execute_on);
			context->hint = true;

			return node;
		}else if (IsA(node, ScalarArrayOpExpr) &&
			((ScalarArrayOpExpr*)node)->useOr &&
			list_length(((ScalarArrayOpExpr*)node)->args) == 2)
		{
			ScalarArrayOpExpr *sao = (ScalarArrayOpExpr*)node;
			c = get_mvar_equal_const(sao->args,
									 sao->opno,
									 context->relid,
									 context->main_rel->rd_auxatt,
									 &var);
			if (c && c->constisnull == false &&
				type_is_array(c->consttype))
			{
				avc = get_aux_var_context(context, var->varattno);
				reset_aux_var_context(avc);
				make_aux_table_query_sql(avc, c, true);
				list_free(avc->list_remote);
				avc->list_remote = get_aux_table_execute_on(avc, var, c, true);
				execute_on = get_main_table_execute_on(avc);
				node = make_xc_node_id_in_expr(var->varno, execute_on);
				list_free(execute_on);
				context->hint = true;

				return node;
			}
		}
	}else if(not_clause(node))
	{
		context->in_not_expr = true;
		node = expression_tree_mutator(node, mutator_aux_equal_expr, context);
		context->in_not_expr = false;
		return node;
	}

next_mutator_aux_equal_expr_:
	return expression_tree_mutator(node, mutator_aux_equal_expr, context);
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
	if (context->const_expr == NULL)
	{
		Const *c = context->const_expr = makeNode(Const);
		int16 typlen;
		get_atttypetypmodcoll(context->loc_info->relid,
								context->varattno,
								&c->consttype,
								&c->consttypmod,
								&c->constcollid);
		get_typlenbyval(c->consttype, &typlen, &c->constbyval);
		c->constlen = typlen;
		context->const_expr->location = -1;

		context->right_expr = makePartitionExpr(context->loc_info, (Node*)context->const_expr);

		context->right_state = ExecInitExpr(context->right_expr, NULL);

		context->expr_context = CreateStandaloneExprContext();
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

static void reset_aux_var_context(ModifyAuxVarContext *context)
{
	AssertArg(context);
	resetStringInfo(&context->buf);
	list_free(context->list_remote);
	context->list_remote = NIL;
}

static ModifyAuxVarContext* get_aux_var_context(ModifyToAuxContext *context, AttrNumber attno)
{
	ListCell *lc;
	ModifyAuxVarContext *auxvar;
	TupleDesc tupdesc;
	Form_pg_attribute aux_key_attr;
	Relation aux_rel;

	foreach(lc, context->list_auxvar)
	{
		auxvar = lfirst(lc);
		if (auxvar->varattno == attno)
			return auxvar;
	}

	/* not found, make new */
	tupdesc = RelationGetDescr(context->main_rel);
	if (!AttributeNumberIsValid(attno) ||
		attno > tupdesc->natts)
		ereport(ERROR, (errmsg("Invalid attribute number")));
	if (attno < 0)
		ereport(ERROR, (errmsg("system columns is not supported for auxiliary table")));

	auxvar = palloc0(sizeof(*auxvar));
	auxvar->aux_rel_oid = LookupAuxRelation(RelationGetRelid(context->main_rel), attno);
	aux_key_attr = tupdesc->attrs[attno-1];
	if (!OidIsValid(auxvar->aux_rel_oid))
	{
		ereport(ERROR,
				(errmsg("not found auxilary table for \"%s\"(\"%s\")",
					RelationGetRelationName(context->main_rel),
					NameStr(aux_key_attr->attname))));
	}
	aux_rel = relation_open(auxvar->aux_rel_oid, AccessShareLock);
	auxvar->aux_rel_name = pstrdup(RelationGetRelationName(aux_rel));
	auxvar->loc_info = aux_rel->rd_locator_info;
	relation_close(aux_rel, AccessShareLock);
	auxvar->aux_var_name = pstrdup(NameStr(aux_key_attr->attname));
	auxvar->varattno = attno;
	context->list_auxvar = lappend(context->list_auxvar, auxvar);
	initStringInfo(&auxvar->buf);

	return auxvar;
}

static void make_aux_table_query_sql(ModifyAuxVarContext *mavc, Const *c, bool isarray)
{
	char *value;
	resetStringInfo(&mavc->buf);
	appendStringInfo(&mavc->buf,
					 "select distinct auxnodeid from \"%s\" where \"%s\" = ",
					 mavc->aux_rel_name,
					 mavc->aux_var_name);
	value = deparse_expression((Node*)c, NIL, false, false);
	if (isarray)
		appendStringInfo(&mavc->buf, "ANY(%s)", value);
	else
		appendStringInfoString(&mavc->buf, value);
	pfree(value);
	if(mavc->query_sql)
		pfree(mavc->query_sql);
	mavc->query_sql = pstrdup(mavc->buf.data);
	resetStringInfo(&mavc->buf);
}

static List* get_aux_table_execute_on(ModifyAuxVarContext *mavc, Var *var, Const *c, bool isarray)
{
	List		   *result;
	Const		   *input;
	Const		   *target;
	Expr		   *convert;
	ExprState	   *convert_state;
	ExprContext	   *expr_context;
	MemoryContext	old_context;
	ArrayType	   *arrayval;
	Bitmapset	   *bms_nodes;
	Datum		   *values;
	bool		   *nulls;
	int				count;
	int				i;

	if (IsLocatorReplicated(mavc->loc_info->locatorType))
	{
		return GetPreferredRepNodeIds(mavc->loc_info->nodeids);
	}else if (mavc->loc_info->locatorType == LOCATOR_TYPE_RROBIN)
	{
		ereport(ERROR, (errmsg("auxiliary table not support distribte by roundrobin")));
	}else if (IsRelationDistributedByUserDefined(mavc->loc_info) &&
			  list_length(mavc->loc_info->funcAttrNums) > 1)
	{
		ereport(ERROR, (errmsg("auxiliary table not support distribute by multi-column")));
	}

	/* get expr execute context */
	if (mavc->expr_context == NULL)
	{
		/* init expr... */
		target = mavc->const_expr = makeNode(Const);
		int16 typlen;
		get_atttypetypmodcoll(mavc->aux_rel_oid,
							  Anum_aux_table_key,
							  &target->consttype,
							  &target->consttypmod,
							  &target->constcollid);
		get_typlenbyval(target->consttype, &typlen, &target->constbyval);
		target->constlen = typlen;

		mavc->reduce_expr = makePartitionExpr(mavc->loc_info, (Node*)target);
		mavc->reduce_expr_state = ExecInitExpr(mavc->reduce_expr, NULL);
		mavc->expr_context = expr_context = CreateStandaloneExprContext();
	}else
	{
		target = mavc->const_expr;
		expr_context = mavc->expr_context;
		MemoryContextReset(expr_context->ecxt_per_tuple_memory);
	}

	/* get const values */
	old_context = MemoryContextSwitchTo(expr_context->ecxt_per_tuple_memory);
	if (isarray)
	{
		int16 typlen;
		char align;
		Assert(type_is_array(c->consttype));
		arrayval = DatumGetArrayTypeP(c->constvalue);
		input = makeNode(Const);
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &typlen,
							 &input->constbyval,
							 &align);
		input->consttype = ARR_ELEMTYPE(arrayval);
		input->constlen = typlen;
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  typlen,
						  input->constbyval,
						  align,
						  &values,
						  &nulls,
						  &count);
	}else
	{
		values = &c->constvalue;
		nulls = &c->constisnull;
		count = 1;
		input = palloc(sizeof(*c));
		memcpy(input, c, sizeof(*c));
	}

	/* make sure input const type same to auxiliary table's distribute column */
	convert = (Expr*)coerce_to_target_type(NULL,
										   (Node*)input,
										   input->consttype,
										   target->consttype,
										   target->consttypmod,
										   COERCION_EXPLICIT,
										   COERCE_IMPLICIT_CAST,
										   -1);
	if ((void*)convert == (void*)input)
		convert = NULL;
	convert_state = convert ? ExecInitExpr(convert, NULL) : NULL;
	MemoryContextSwitchTo(old_context);

	/* compute execute on nodes */
	bms_nodes = NULL;
	for (i=0;i<count;++i)
	{
		Datum datum;
		bool isnull;
		if (convert_state)
		{
			input->constvalue = values[i];
			input->constisnull = nulls[i];
			target->constvalue = ExecEvalExprSwitchContext(convert_state,
														   expr_context,
														   &target->constisnull,
														   NULL);
		}else
		{
			target->constvalue = values[i];
			target->constisnull = nulls[i];
		}
		datum = ExecEvalExprSwitchContext(mavc->reduce_expr_state,
										  expr_context,
										  &isnull,
										  NULL);
		Assert(isnull == false);
		bms_nodes = bms_add_member(bms_nodes, DatumGetInt32(datum));
	}

	result = oid_list_copy_bms(mavc->loc_info->nodeids, bms_nodes);
	bms_free(bms_nodes);

	return result;
}

/* return xc_node_id list */
static List* get_main_table_execute_on(ModifyAuxVarContext *mavc)
{
	List		   *result;
	EState		   *estate;
	RemoteQuery	   *step;
	PlanState	   *ps;
	TupleTableSlot *slot;
	MemoryContext	old_context;
	Datum			datum;
	int				xc_node_id;
	bool			isnull;

	estate = CreateExecutorState();
	estate->es_snapshot = GetActiveSnapshot();
	old_context = MemoryContextSwitchTo(estate->es_query_cxt);
	step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_NONE;
	step->exec_nodes = MakeExecNodesByOids(mavc->loc_info,
										   mavc->list_remote,
										   RELATION_ACCESS_READ);
	step->sql_statement = mavc->query_sql;
	step->force_autocommit = false;
	step->exec_type = EXEC_ON_DATANODES;
	makeTargetEntry((Expr*)makeVar(1, 1, INT4OID, -1, InvalidOid, 0),
					1, NULL, false);
	/* we only have on column result */
	step->scan.plan.targetlist = list_make1(makeTargetEntry((Expr*)makeVar(1, 1, INT4OID, -1, InvalidOid, 0),
															1,
															NULL,
															false));

	ps = ExecInitNode((Plan*)step, estate, 0);
	result = NIL;
	for(;;)
	{
		slot = ExecProcNode(ps);
		if (TupIsNull(slot))
			break;

		datum = slot_getattr(slot, 1, &isnull);
		Assert(isnull == false);
		xc_node_id = DatumGetInt32(datum);
		MemoryContextSwitchTo(old_context);
		if (list_member_int(result, xc_node_id) == false)
			result = lappend_int(result, xc_node_id);
	}
	ExecEndNode(ps);
	MemoryContextSwitchTo(old_context);
	FreeExecutorState(estate);

	return result;
}

static List* oid_list_copy_bms(List *oid_list, Bitmapset *bms_index)
{
	List *list = NIL;
	ListCell *lc;
	int i = 0;

	foreach(lc, oid_list)
	{
		if (bms_is_member(i, bms_index))
			list = lappend_oid(list, lfirst_oid(lc));
		++i;
	}

	return list;
}

static Node* make_xc_node_id_in_expr(Index relid, List *list)
{
	oidvector *ids;
	ListCell *lc;
	int32 *values;
	int i,count,size;

	count = list_length(list);
	size = offsetof(oidvector, values) + count * sizeof(int32);
	ids = palloc0(size);
	ids->ndim = 1;
	ids->dataoffset = 0;
	ids->elemtype = INT4OID;
	ids->dim1 = count;
	ids->lbound1 = 0;
	values = (int32*)(ids->values);
	SET_VARSIZE(ids, size);

	i=0;
	foreach(lc, list)
		values[i++] = lfirst_int(lc);

	return (Node*)make_scalar_array_op(NULL,
									   SystemFuncName("="),
									   true,
									   (Node*)makeVar(relid, XC_NodeIdAttributeNumber, INT4OID, -1, InvalidOid, 0),
									   (Node*)makeConst(INT4ARRAYOID, -1, InvalidOid, -1, PointerGetDatum(ids), false, false),
									   -1);
}
