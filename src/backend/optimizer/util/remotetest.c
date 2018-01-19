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
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/plancat.h"
#include "optimizer/predtest.h"
#include "optimizer/reduceinfo.h"
#include "parser/parse_coerce.h"
#include "pgxc/locator.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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

static Expr* makeInt4EQ(Expr *l, Expr *r);
static Expr* makeInt4ArrayIn(Expr *l, Datum *values, int count);
static Expr* makeInt4Const(int32 val);
static Expr* makeNotNullTest(Expr *expr, bool isrow);
static Expr* makePartitionExpr(RelationLocInfo *loc_info, Node *node);
static Node* mutator_equal_expr(Node *node, ModifyContext *context);
static void init_context_expr_if_need(ModifyContext *context, Const *c);
static Const* get_var_equal_const(List *args, Oid opno, Index relid, AttrNumber attno, Var **var);

/* return remote oid list */
List *relation_remote_by_constraints(PlannerInfo *root, RelOptInfo *rel)
{
	MemoryContext main_mctx;
	MemoryContext temp_mctx;
	MemoryContext old_mctx;
	ModifyContext context;
	RelationLocInfo *loc_info;
	List		   *result;
	List		   *constraint_pred;
	List		   *safe_constraints;
	List		   *temp_constraints;
	List		   *new_clauses;
	List		   *null_test_list;
	ListCell	   *lc;
	int				i;

	MemSet(&context, 0, sizeof(context));
	AssertArg(rel->loc_info != NULL);
	loc_info = context.loc_info = rel->loc_info;

	if(rel->baserestrictinfo == NIL)
	{
		if(loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
			return list_make1_oid(linitial_oid(loc_info->nodeids));
		else
			return list_copy(loc_info->nodeids);
	}

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

	context.relid = rel->relid;
	null_test_list = NIL;
	if (loc_info->locatorType == LOCATOR_TYPE_USER_DEFINED)
	{
		if(list_length(loc_info->funcAttrNums) == 1)
		{
			context.varattno = linitial_int(loc_info->funcAttrNums);
			context.partition_expr = makePartitionExpr(loc_info, (Node*)makeVarByRel(context.varattno, loc_info->relid, rel->relid));
		}
		if (func_strict(loc_info->funcid))
		{
			foreach(lc, loc_info->funcAttrNums)
			{
				null_test_list = lappend(null_test_list,
										 makeNotNullTest((Expr*)makeVarByRel(lfirst_int(lc), loc_info->relid, rel->relid), false));
			}
		}
	}else if(IsLocatorDistributedByValue(loc_info->locatorType))
	{
		context.varattno = loc_info->partAttrNum;
		context.partition_expr = makePartitionExpr(loc_info, (Node*)makeVarByRel(loc_info->partAttrNum, loc_info->relid, rel->relid));
		null_test_list = list_make1(makeNotNullTest((Expr*)makeVarByRel(context.varattno, loc_info->relid, rel->relid), false));
	}

	constraint_pred = get_relation_constraints(root, loc_info->relid, rel, true);
	safe_constraints = NIL;
	foreach(lc, constraint_pred)
	{
		Node	   *pred = (Node *) lfirst(lc);

		if (!contain_mutable_functions(pred))
			safe_constraints = lappend(safe_constraints, pred);
	}

	new_clauses = NIL;
	if (context.partition_expr)
	{
		foreach(lc, rel->baserestrictinfo)
		{
			Node *clause;
			RestrictInfo *r = lfirst(lc);

			new_clauses = lappend(new_clauses, r->clause);

			context.hint = false;
			clause = mutator_equal_expr((Node*)r->clause, &context);
			if(context.hint)
				new_clauses = lappend(new_clauses, clause);
		}
	}else
	{
		new_clauses = rel->baserestrictinfo;
	}

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
		expr = makeInt4EQ((Expr*)makeVarByRel(XC_NodeIdAttributeNumber, loc_info->relid, rel->relid),
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

	if (loc_info->locatorType == LOCATOR_TYPE_REPLICATED)
	{
		lc = list_head(result);
		while(lnext(lc))
			list_delete_cell(result, lnext(lc), lc);
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

/*
 * let column=val to (like) hash(column)%remote_count = hash(value)%remote_count to hash(column)%remote_count = new_value
 *   column [not] in (v1,v2) to hash(column)%remote [not] in (new_v1,new_v2)
 */
static Node* mutator_equal_expr(Node *node, ModifyContext *context)
{
	Var *var;
	Const *c;

	if (node == NULL)
		return NULL;

	if (IsA(node, OpExpr) &&
		list_length(((OpExpr*)node)->args) == 2)
	{
		OpExpr *op = (OpExpr*)node;
		c = get_var_equal_const(op->args, op->opno, context->relid, context->varattno, &var);
		if (c && c->consttype == var->vartype)
		{
			Const *c2;
			init_context_expr_if_need(context, c);

			context->const_expr->constvalue = c->constvalue;
			context->const_expr->constisnull = c->constisnull;
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
		list_length(((ScalarArrayOpExpr*)node)->args) == 2)
	{
		ArrayType *arrayval;
		ScalarArrayOpExpr *sao = (ScalarArrayOpExpr*)node;
		c = get_var_equal_const(sao->args, sao->opno, context->relid, context->varattno, &var);
		if (c && c->constisnull == false &&
			type_is_array(c->consttype))
		{
			arrayval = DatumGetArrayTypeP(c->constvalue);
			if(ARR_ELEMTYPE(arrayval) == var->vartype)
			{
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

				init_context_expr_if_need(context, NULL);
				new_values = palloc(sizeof(Datum)*num_elems);
				for (i=0;i<num_elems;++i)
				{
					MemoryContextReset(context->expr_context->ecxt_per_tuple_memory);
					context->const_expr->constvalue = values[i];
					context->const_expr->constisnull = nulls[i];
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
				return node;
			}
		}
	}

	return expression_tree_mutator(node, mutator_equal_expr, context);
}

static Const* get_var_equal_const(List *args, Oid opno, Index relid, AttrNumber attno, Var **var)
{
	Expr *l;
	Expr *r;
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
			IsA(r, Const))
		{
			*var = (Var*)l;
			return (Const*)r;
		}else if (IsA(r, Var) &&
			((Var*)r)->varno == relid &&
			((Var*)r)->varattno == attno &&
			IsA(l, Const))
		{
			*var = (Var*)r;
			return (Const*)l;
		}
	}

	return NULL;
}

static void init_context_expr_if_need(ModifyContext *context, Const *c)
{
	if (context->const_expr == NULL)
	{
		if(c != NULL)
		{
			context->const_expr = palloc(sizeof(Const));
			memcpy(context->const_expr, c, sizeof(Const));
		}else
		{
			c = context->const_expr = makeNode(Const);
			get_atttypetypmodcoll(context->loc_info->relid,
								  context->varattno,
								  &c->consttype,
								  &c->consttypmod,
								  &c->constcollid);
		}
		context->const_expr->location = -1;

		context->right_expr = makePartitionExpr(context->loc_info, (Node*)context->const_expr);

		context->right_state = ExecInitExpr(context->right_expr, NULL);

		context->expr_context = CreateStandaloneExprContext();
	}
}