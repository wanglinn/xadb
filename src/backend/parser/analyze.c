/*-------------------------------------------------------------------------
 *
 * analyze.c
 *	  transform the raw parse tree into a query tree
 *
 * For optimizable statements, we are careful to obtain a suitable lock on
 * each referenced table, and other modules of the backend preserve or
 * re-obtain these locks before depending on the results.  It is therefore
 * okay to do significant semantic analysis of these statements.  For
 * utility commands, no locks are obtained here (and if they were, we could
 * not be sure we'd still have them at execution).  Hence the general rule
 * for utility commands is to just dump them into a Query node untransformed.
 * DECLARE CURSOR, EXPLAIN, and CREATE TABLE AS are exceptions because they
 * contain optimizable statements, which we should transform.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/parser/analyze.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cte.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_param.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/queryjumble.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#ifdef ADB
#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/adb_proc.h"
#include "catalog/pg_inherits.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/fmgroids.h"
#include "catalog/pg_aux_class.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "utils/lsyscache.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/subselect.h"
#include "tcop/tcopprot.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_node.h"
#include "pgxc/xc_maintenance_mode.h"
#include "catalog/pg_operator.h"
#include "parser/parser.h"
#include "utils/syscache.h"
#endif
#ifdef ADB_GRAM_ORA
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/ora_convert_d.h"
#include "catalog/pg_operator.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#endif
#if defined(ADB) || defined(ADB_GRAM_ORA)
#include "access/xact.h"
#endif


/* Hook for plugins to get control at end of parse analysis */
post_parse_analyze_hook_type post_parse_analyze_hook = NULL;
#ifdef ADB
extern bool enable_readsql_on_slave;
#endif

static Query *transformOptionalSelectInto(ParseState *pstate, Node *parseTree);
static Query *transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt);
static Query *transformInsertStmt(ParseState *pstate, InsertStmt *stmt);
static List *transformInsertRow(ParseState *pstate, List *exprlist,
								List *stmtcols, List *icolumns, List *attrnos,
								bool strip_indirection);
static OnConflictExpr *transformOnConflictClause(ParseState *pstate,
												 OnConflictClause *onConflictClause);
static int	count_rowexpr_columns(ParseState *pstate, Node *expr);
static Query *transformSelectStmt(ParseState *pstate, SelectStmt *stmt);
static Query *transformValuesClause(ParseState *pstate, SelectStmt *stmt);
static Query *transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt);
static Node *transformSetOperationTree(ParseState *pstate, SelectStmt *stmt,
									   bool isTopLevel, List **targetlist);
static void determineRecursiveColTypes(ParseState *pstate,
									   Node *larg, List *nrtargetlist);
static Query *transformReturnStmt(ParseState *pstate, ReturnStmt *stmt);
static Query *transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt);
static List *transformReturningList(ParseState *pstate, List *returningList);
static List *transformUpdateTargetList(ParseState *pstate,
									   List *targetList);
static Query *transformPLAssignStmt(ParseState *pstate,
									PLAssignStmt *stmt);
static Query *transformDeclareCursorStmt(ParseState *pstate,
										 DeclareCursorStmt *stmt);
static Query *transformExplainStmt(ParseState *pstate,
								   ExplainStmt *stmt);
static Query *transformCreateTableAsStmt(ParseState *pstate,
										 CreateTableAsStmt *stmt);
static Query *transformCallStmt(ParseState *pstate,
								CallStmt *stmt);
static void transformLockingClause(ParseState *pstate, Query *qry,
								   LockingClause *lc, bool pushedDown);
#ifdef RAW_EXPRESSION_COVERAGE_TEST
static bool test_raw_expression_coverage(Node *node, void *context);
#endif

#ifdef ADB
static Query *transformExecDirectStmt(ParseState *pstate, ExecDirectStmt *stmt);
static bool IsExecDirectUtilityStmt(Node *node);
static bool is_relation_child(RangeTblEntry *child_rte, List *rtable);
static bool is_rel_child_of_rel(RangeTblEntry *child_rte, RangeTblEntry *parent_rte);
#endif /* ADB */
#ifdef ADB_GRAM_ORA
static Node* transformFromAndWhere(ParseState *pstate, Node *quals);
static void rewrite_rownum_query(Query *query);
static bool const_get_int64(const Expr *expr, int64 *val);
static Expr* make_int8_const(Datum value);
static Oid get_operator_for_function(Oid funcid);
static Query *transformCreateFunctionStmt(ParseState *pstate, CreateFunctionStmt *cf);
#endif /* ADB */


/*
 * parse_analyze
 *		Analyze a raw parse tree and transform it to Query form.
 *
 * Optionally, information about $n parameter types can be supplied.
 * References to $n indexes not defined by paramTypes[] are disallowed.
 *
 * The result is a Query node.  Optimizable statements require considerable
 * transformation, while utility-type statements are simply hung off
 * a dummy CMD_UTILITY Query node.
 */
Query *
parse_analyze(RawStmt *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams,
			  QueryEnvironment *queryEnv)
{
#ifdef ADB_MULTI_GRAM
	return parse_analyze_for_gram(parseTree, sourceText,
								  paramTypes, numParams,
								  queryEnv, PARSE_GRAM_POSTGRES);
}

Query *
parse_analyze_for_gram(RawStmt *parseTree, const char *sourceText,
					   Oid *paramTypes, int numParams,
					   QueryEnvironment *queryEnv, ParseGrammar grammar)
{
	ParseState *pstate = make_parsestate(NULL);
	Query	   *query;
	volatile bool push_search_path = false;
	pstate->p_grammar = grammar;
	if (IsTransactionState())
	{
		PushOverrideSearchPathForGrammar(grammar);
		push_search_path = true;
	}
	PG_TRY();
	{
#else /* ADB_MULTI_GRAM */
	ParseState *pstate = make_parsestate(NULL);
	Query	   *query;
#endif /* ADB_MULTI_GRAM */
	JumbleState *jstate = NULL;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;

	if (numParams > 0)
		parse_fixed_parameters(pstate, paramTypes, numParams);

	pstate->p_queryEnv = queryEnv;

	query = transformTopLevelStmt(pstate, parseTree);
#ifdef ADB_GRAM_ORA
	check_joinon_column_join((Node*)(query->jointree), pstate);
	rewrite_rownum_query_enum((Node*)query, NULL);
#endif /* ADB_GRAM_ORA */

	if (IsQueryIdEnabled())
		jstate = JumbleQuery(query, sourceText);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

	free_parsestate(pstate);
#ifdef ADB_MULTI_GRAM
	}PG_CATCH();
	{
		if (push_search_path)
			PopOverrideSearchPath();
		PG_RE_THROW();
	}PG_END_TRY();
	if (push_search_path)
		PopOverrideSearchPath();
#endif

	pgstat_report_query_id(query->queryId, false);

	return query;
}

/*
 * parse_analyze_varparams
 *
 * This variant is used when it's okay to deduce information about $n
 * symbol datatypes from context.  The passed-in paramTypes[] array can
 * be modified or enlarged (via repalloc).
 */
Query *
parse_analyze_varparams(RawStmt *parseTree, const char *sourceText,
						Oid **paramTypes, int *numParams)
{
#ifdef ADB_MULTI_GRAM
	return parse_analyze_varparams_for_gram(parseTree, sourceText,
											paramTypes, numParams,
											PARSE_GRAM_POSTGRES);
}

Query *
parse_analyze_varparams_for_gram(RawStmt *parseTree, const char *sourceText,
								 Oid **paramTypes, int *numParams,
								 ParseGrammar grammar)
{
	ParseState *pstate = make_parsestate(NULL);
	Query	   *query;
	pstate->p_grammar = grammar;
	PushOverrideSearchPathForGrammar(grammar);
	PG_TRY();
	{
#else /* ADB_MULTI_GRAM */
	ParseState *pstate = make_parsestate(NULL);
	Query	   *query;
#endif /* ADB_MULTI_GRAM */
	JumbleState *jstate = NULL;

	Assert(sourceText != NULL); /* required as of 8.4 */

	pstate->p_sourcetext = sourceText;

	parse_variable_parameters(pstate, paramTypes, numParams);

	query = transformTopLevelStmt(pstate, parseTree);

	/* make sure all is well with parameter types */
	check_variable_parameters(pstate, query);
#ifdef ADB_GRAM_ORA
	check_joinon_column_join((Node*)(query->jointree), pstate);
	rewrite_rownum_query_enum((Node*)query, NULL);
#endif /* ADB_GRAM_ORA */

	if (IsQueryIdEnabled())
		jstate = JumbleQuery(query, sourceText);

	if (post_parse_analyze_hook)
		(*post_parse_analyze_hook) (pstate, query, jstate);

	free_parsestate(pstate);

#ifdef ADB_MULTI_GRAM
	}PG_CATCH();
	{
		PopOverrideSearchPath();
		PG_RE_THROW();
	}PG_END_TRY();
	PopOverrideSearchPath();
#endif /* ADB_MULTI_GRAM */
	pgstat_report_query_id(query->queryId, false);

	return query;
}

/*
 * parse_sub_analyze
 *		Entry point for recursively analyzing a sub-statement.
 */
Query *
parse_sub_analyze(Node *parseTree, ParseState *parentParseState,
				  CommonTableExpr *parentCTE,
				  bool locked_from_parent,
				  bool resolve_unknowns)
{
	ParseState *pstate = make_parsestate(parentParseState);
	Query	   *query;

	pstate->p_parent_cte = parentCTE;
	pstate->p_locked_from_parent = locked_from_parent;
	pstate->p_resolve_unknowns = resolve_unknowns;

	query = transformStmt(pstate, parseTree);

	free_parsestate(pstate);

	return query;
}

/*
 * transformTopLevelStmt -
 *	  transform a Parse tree into a Query tree.
 *
 * This function is just responsible for transferring statement location data
 * from the RawStmt into the finished Query.
 */
Query *
transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree)
{
	Query	   *result;

	/* We're at top level, so allow SELECT INTO */
	result = transformOptionalSelectInto(pstate, parseTree->stmt);

	result->stmt_location = parseTree->stmt_location;
	result->stmt_len = parseTree->stmt_len;
#ifdef ADB
	if (enable_readsql_on_slave == true)
	{	
		Query *query = result;
		/* skip explain */
		if (query->commandType == CMD_UTILITY &&
			query->utilityStmt &&
			IsA(query->utilityStmt, ExplainStmt))
		{
			ExplainStmt *explain = (ExplainStmt *)query->utilityStmt;
			query = (Query *)explain->query;
		}

		if (!GetTopTransactionIdIfAny() &&
			sql_readonly != SQLTYPE_WRITE && 
			!check_query_not_readonly_walker((Node *)query, NULL))
			sql_readonly = SQLTYPE_READ;
		else
			sql_readonly = SQLTYPE_WRITE;
	}
	else
		sql_readonly = SQLTYPE_WRITE;
#endif

	return result;
}

/*
 * transformOptionalSelectInto -
 *	  If SELECT has INTO, convert it to CREATE TABLE AS.
 *
 * The only thing we do here that we don't do in transformStmt() is to
 * convert SELECT ... INTO into CREATE TABLE AS.  Since utility statements
 * aren't allowed within larger statements, this is only allowed at the top
 * of the parse tree, and so we only try it before entering the recursive
 * transformStmt() processing.
 */
static Query *
transformOptionalSelectInto(ParseState *pstate, Node *parseTree)
{
	if (IsA(parseTree, SelectStmt))
	{
		SelectStmt *stmt = (SelectStmt *) parseTree;

		/* If it's a set-operation tree, drill down to leftmost SelectStmt */
		while (stmt && stmt->op != SETOP_NONE)
			stmt = stmt->larg;
		Assert(stmt && IsA(stmt, SelectStmt) && stmt->larg == NULL);

		if (stmt->intoClause)
		{
			CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);

			ctas->query = parseTree;
			ctas->into = stmt->intoClause;
			ctas->objtype = OBJECT_TABLE;
			ctas->is_select_into = true;

			/*
			 * Remove the intoClause from the SelectStmt.  This makes it safe
			 * for transformSelectStmt to complain if it finds intoClause set
			 * (implying that the INTO appeared in a disallowed place).
			 */
			stmt->intoClause = NULL;

			parseTree = (Node *) ctas;
		}
	}

	return transformStmt(pstate, parseTree);
}

/*
 * transformStmt -
 *	  recursively transform a Parse tree into a Query tree.
 */
Query *
transformStmt(ParseState *pstate, Node *parseTree)
{
	Query	   *result;

	/*
	 * We apply RAW_EXPRESSION_COVERAGE_TEST testing to basic DML statements;
	 * we can't just run it on everything because raw_expression_tree_walker()
	 * doesn't claim to handle utility statements.
	 */
#ifdef RAW_EXPRESSION_COVERAGE_TEST
	switch (nodeTag(parseTree))
	{
		case T_SelectStmt:
		case T_InsertStmt:
		case T_UpdateStmt:
		case T_DeleteStmt:
			(void) test_raw_expression_coverage(parseTree, NULL);
			break;
		default:
			break;
	}
#endif							/* RAW_EXPRESSION_COVERAGE_TEST */

	switch (nodeTag(parseTree))
	{
			/*
			 * Optimizable statements
			 */
		case T_InsertStmt:
			result = transformInsertStmt(pstate, (InsertStmt *) parseTree);
			break;

		case T_DeleteStmt:
			result = transformDeleteStmt(pstate, (DeleteStmt *) parseTree);
			break;

		case T_UpdateStmt:
			result = transformUpdateStmt(pstate, (UpdateStmt *) parseTree);
			break;

		case T_SelectStmt:
			{
				SelectStmt *n = (SelectStmt *) parseTree;

				if (n->valuesLists)
					result = transformValuesClause(pstate, n);
				else if (n->op == SETOP_NONE)
					result = transformSelectStmt(pstate, n);
				else
					result = transformSetOperationStmt(pstate, n);
			}
			break;

		case T_ReturnStmt:
			result = transformReturnStmt(pstate, (ReturnStmt *) parseTree);
			break;

		case T_PLAssignStmt:
			result = transformPLAssignStmt(pstate,
										   (PLAssignStmt *) parseTree);
			break;

			/*
			 * Special cases
			 */
		case T_DeclareCursorStmt:
			result = transformDeclareCursorStmt(pstate,
												(DeclareCursorStmt *) parseTree);
			break;

		case T_ExplainStmt:
			result = transformExplainStmt(pstate,
										  (ExplainStmt *) parseTree);
			break;
#ifdef ADB
		case T_ExecDirectStmt:
			result = transformExecDirectStmt(pstate,
											 (ExecDirectStmt *) parseTree);
			break;
#endif
		case T_CreateTableAsStmt:
			result = transformCreateTableAsStmt(pstate,
												(CreateTableAsStmt *) parseTree);
			break;

		case T_CallStmt:
			result = transformCallStmt(pstate,
									   (CallStmt *) parseTree);
			break;

#ifdef ADB_GRAM_ORA
		case T_CreateFunctionStmt:
			result = transformCreateFunctionStmt(pstate,
												 (CreateFunctionStmt*) parseTree);
			break;
#endif /* ADB_GRAM_ORA */

		default:
#ifdef ADB
			if (IsA(parseTree, DropStmt))
			{
				DropStmt *stmt = (DropStmt *) parseTree;

				if (stmt->removeType == OBJECT_AUX_TABLE)
				{
					stmt->removeType = OBJECT_TABLE;
					stmt->auxiliary = true;
				}
			}
#endif
			/*
			 * other statements don't require any transformation; just return
			 * the original parsetree with a Query node plastered on top.
			 */
			result = makeNode(Query);
			result->commandType = CMD_UTILITY;
			result->utilityStmt = (Node *) parseTree;
			break;
	}

	/* Mark as original query until we learn differently */
	result->querySource = QSRC_ORIGINAL;
	result->canSetTag = true;

	return result;
}

/*
 * analyze_requires_snapshot
 *		Returns true if a snapshot must be set before doing parse analysis
 *		on the given raw parse tree.
 *
 * Classification here should match transformStmt().
 */
bool
analyze_requires_snapshot(RawStmt *parseTree)
{
	bool		result;

	switch (nodeTag(parseTree->stmt))
	{
			/*
			 * Optimizable statements
			 */
		case T_InsertStmt:
		case T_DeleteStmt:
		case T_UpdateStmt:
		case T_SelectStmt:
		case T_PLAssignStmt:
			result = true;
			break;

			/*
			 * Special cases
			 */
		case T_DeclareCursorStmt:
		case T_ExplainStmt:
		case T_CreateTableAsStmt:
			/* yes, because we must analyze the contained statement */
			result = true;
			break;
#ifdef ADB
		case T_CreateAuxStmt:
		case T_ExecDirectStmt:

			/*
			 * We will parse/analyze/plan inner query, which probably will
			 * need a snapshot. Ensure it is set.
			 */
			result = true;
			break;
#endif
		default:
			/* other utility statements don't have any real parse analysis */
			result = false;
			break;
	}

	return result;
}

/*
 * transformDeleteStmt -
 *	  transforms a Delete Statement
 */
static Query *
transformDeleteStmt(ParseState *pstate, DeleteStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	ParseNamespaceItem *nsitem;
	Node	   *qual;
#ifdef ADB
	ListCell   *tl;
#endif
	qry->commandType = CMD_DELETE;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
#ifdef ADB
		/*
		 * For a WITH query that deletes from a parent table in the
		 * main query & inserts a row in the child table in the WITH query
		 * we need to use command ID communication to remote nodes in order
		 * to maintain global data visibility.
		 * For example
		 * CREATE TEMP TABLE parent ( id int, val text ) DISTRIBUTE BY REPLICATION;
		 * CREATE TEMP TABLE child ( ) INHERITS ( parent ) DISTRIBUTE BY REPLICATION;
		 * INSERT INTO parent VALUES ( 42, 'old' );
		 * INSERT INTO child VALUES ( 42, 'older' );
		 * WITH wcte AS ( INSERT INTO child VALUES ( 42, 'new' ) RETURNING id AS newid )
		 * DELETE FROM parent USING wcte WHERE id = newid;
		 * The last query gets translated into the following multi-statement
		 * transaction on the primary datanode
		 * (a) SELECT id, ctid FROM ONLY parent WHERE true
		 * (b) START TRANSACTION ISOLATION LEVEL read committed READ WRITE
		 * (c) INSERT INTO child (id, val) VALUES ($1, $2) RETURNING id -- (42, 'new')
		 * (d) DELETE FROM ONLY parent parent WHERE (parent.ctid = $1)
		 * (e) SELECT id, ctid FROM ONLY child parent WHERE true
		 * (f) DELETE FROM ONLY child parent WHERE (parent.ctid = $1)
		 * (g) COMMIT TRANSACTION
		 * The command id of the select in step (e), should be such that
		 * it does not see the insert of step (c)
		 */
		if (IsCnMaster())
		{
			foreach(tl, stmt->withClause->ctes)
			{
				CommonTableExpr *cte = (CommonTableExpr *) lfirst(tl);
				if (IsA(cte->ctequery, InsertStmt))
				{
					qry->has_to_save_cmd_id = true;
					SetSendCommandId(true);
					break;
				}
			}
		}
#endif
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* set up range table with just the result rel */
	qry->resultRelation = setTargetTable(pstate, stmt->relation,
										 stmt->relation->inh,
										 true,
										 ACL_DELETE);
	nsitem = pstate->p_target_nsitem;

	/* there's no DISTINCT in DELETE */
	qry->distinctClause = NIL;

	/* subqueries in USING cannot access the result relation */
	nsitem->p_lateral_only = true;
	nsitem->p_lateral_ok = false;

	/*
	 * The USING clause is non-standard SQL syntax, and is equivalent in
	 * functionality to the FROM list that can be specified for UPDATE. The
	 * USING keyword is used rather than FROM because FROM is already a
	 * keyword in the DELETE syntax.
	 */
	transformFromClause(pstate, stmt->usingClause);

	/* remaining clauses can reference the result relation normally */
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");

	qry->returningList = transformReturningList(pstate, stmt->returningList);

	/* done building the range table and jointree */
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs)
		parseCheckAggregates(pstate, qry);

	return qry;
}

/*
 * transformInsertStmt -
 *	  transform an Insert Statement
 */
static Query *
transformInsertStmt(ParseState *pstate, InsertStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	SelectStmt *selectStmt = (SelectStmt *) stmt->selectStmt;
	List	   *exprList = NIL;
	bool		isGeneralSelect;
	List	   *sub_rtable;
	List	   *sub_namespace;
	List	   *icolumns;
	List	   *attrnos;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *rte;
	ListCell   *icols;
	ListCell   *attnos;
	ListCell   *lc;
	bool		isOnConflictUpdate;
	AclMode		targetPerms;

	/* There can't be any outer WITH to worry about */
	Assert(pstate->p_ctenamespace == NIL);

	qry->commandType = CMD_INSERT;
	pstate->p_is_insert = true;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	qry->override = stmt->override;

	isOnConflictUpdate = (stmt->onConflictClause &&
						  stmt->onConflictClause->action == ONCONFLICT_UPDATE);

	/*
	 * We have three cases to deal with: DEFAULT VALUES (selectStmt == NULL),
	 * VALUES list, or general SELECT input.  We special-case VALUES, both for
	 * efficiency and so we can handle DEFAULT specifications.
	 *
	 * The grammar allows attaching ORDER BY, LIMIT, FOR UPDATE, or WITH to a
	 * VALUES clause.  If we have any of those, treat it as a general SELECT;
	 * so it will work, but you can't use DEFAULT items together with those.
	 */
	isGeneralSelect = (selectStmt && (selectStmt->valuesLists == NIL ||
									  selectStmt->sortClause != NIL ||
									  selectStmt->limitOffset != NULL ||
									  selectStmt->limitCount != NULL ||
									  selectStmt->lockingClause != NIL ||
									  selectStmt->withClause != NULL));

	/*
	 * If a non-nil rangetable/namespace was passed in, and we are doing
	 * INSERT/SELECT, arrange to pass the rangetable/namespace down to the
	 * SELECT.  This can only happen if we are inside a CREATE RULE, and in
	 * that case we want the rule's OLD and NEW rtable entries to appear as
	 * part of the SELECT's rtable, not as outer references for it.  (Kluge!)
	 * The SELECT's joinlist is not affected however.  We must do this before
	 * adding the target table to the INSERT's rtable.
	 */
	if (isGeneralSelect)
	{
		sub_rtable = pstate->p_rtable;
		pstate->p_rtable = NIL;
		sub_namespace = pstate->p_namespace;
		pstate->p_namespace = NIL;
	}
	else
	{
		sub_rtable = NIL;		/* not used, but keep compiler quiet */
		sub_namespace = NIL;
	}

	/*
	 * Must get write lock on INSERT target table before scanning SELECT, else
	 * we will grab the wrong kind of initial lock if the target table is also
	 * mentioned in the SELECT part.  Note that the target table is not added
	 * to the joinlist or namespace.
	 */
	targetPerms = ACL_INSERT;
	if (isOnConflictUpdate)
		targetPerms |= ACL_UPDATE;
	qry->resultRelation = setTargetTable(pstate, stmt->relation,
										 false, false, targetPerms);

#ifdef ADB_MULTI_GRAM
	if(pstate->p_grammar == PARSE_GRAM_ORACLE)
		addNSItemToQuery(pstate, pstate->p_target_nsitem, false, true, true);
#endif /* ADB */

	/* Validate stmt->cols list, or build default list if no list given */
	icolumns = checkInsertTargets(pstate, stmt->cols, &attrnos);
	Assert(list_length(icolumns) == list_length(attrnos));

	/*
	 * Determine which variant of INSERT we have.
	 */
	if (selectStmt == NULL)
	{
		/*
		 * We have INSERT ... DEFAULT VALUES.  We can handle this case by
		 * emitting an empty targetlist --- all columns will be defaulted when
		 * the planner expands the targetlist.
		 */
		exprList = NIL;
	}
	else if (isGeneralSelect)
	{
		/*
		 * We make the sub-pstate a child of the outer pstate so that it can
		 * see any Param definitions supplied from above.  Since the outer
		 * pstate's rtable and namespace are presently empty, there are no
		 * side-effects of exposing names the sub-SELECT shouldn't be able to
		 * see.
		 */
		ParseState *sub_pstate = make_parsestate(pstate);
		Query	   *selectQuery;
#ifdef ADB
		RangeTblEntry	*target_rte;
#endif
		/*
		 * Process the source SELECT.
		 *
		 * It is important that this be handled just like a standalone SELECT;
		 * otherwise the behavior of SELECT within INSERT might be different
		 * from a stand-alone SELECT. (Indeed, Postgres up through 6.5 had
		 * bugs of just that nature...)
		 *
		 * The sole exception is that we prevent resolving unknown-type
		 * outputs as TEXT.  This does not change the semantics since if the
		 * column type matters semantically, it would have been resolved to
		 * something else anyway.  Doing this lets us resolve such outputs as
		 * the target column's type, which we handle below.
		 */
		sub_pstate->p_rtable = sub_rtable;
		sub_pstate->p_joinexprs = NIL;	/* sub_rtable has no joins */
		sub_pstate->p_namespace = sub_namespace;
		sub_pstate->p_resolve_unknowns = false;

		selectQuery = transformStmt(sub_pstate, stmt->selectStmt);

		free_parsestate(sub_pstate);

		/* The grammar should have produced a SELECT */
		if (!IsA(selectQuery, Query) ||
			selectQuery->commandType != CMD_SELECT)
			elog(ERROR, "unexpected non-SELECT command in INSERT ... SELECT");

		/*
		 * Make the source be a subquery in the INSERT's rangetable, and add
		 * it to the INSERT's joinlist (but not the namespace).
		 */
		nsitem = addRangeTableEntryForSubquery(pstate,
											   selectQuery,
											   makeAlias("*SELECT*", NIL),
											   false,
											   false);
#ifdef ADB
		/*
		 * For an INSERT SELECT involving INSERT on a child after scanning
		 * the parent, set flag to send command ID communication to remote
		 * nodes in order to maintain global data visibility.
		 */
		if (IsCnMaster())
		{
			target_rte = rt_fetch(qry->resultRelation, pstate->p_rtable);
			if (is_relation_child(target_rte, selectQuery->rtable))
			{
				qry->has_to_save_cmd_id = true;
				SetSendCommandId(true);
			}
		}
#endif
		addNSItemToQuery(pstate, nsitem, true, false, false);

		/*----------
		 * Generate an expression list for the INSERT that selects all the
		 * non-resjunk columns from the subquery.  (INSERT's tlist must be
		 * separate from the subquery's tlist because we may add columns,
		 * insert datatype coercions, etc.)
		 *
		 * HACK: unknown-type constants and params in the SELECT's targetlist
		 * are copied up as-is rather than being referenced as subquery
		 * outputs.  This is to ensure that when we try to coerce them to
		 * the target column's datatype, the right things happen (see
		 * special cases in coerce_type).  Otherwise, this fails:
		 *		INSERT INTO foo SELECT 'bar', ... FROM baz
		 *----------
		 */
		exprList = NIL;
		foreach(lc, selectQuery->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Expr	   *expr;

			if (tle->resjunk)
				continue;
			if (tle->expr &&
				(IsA(tle->expr, Const) || IsA(tle->expr, Param)) &&
				exprType((Node *) tle->expr) == UNKNOWNOID)
				expr = tle->expr;
			else
			{
				Var		   *var = makeVarFromTargetEntry(nsitem->p_rtindex, tle);

				var->location = exprLocation((Node *) tle->expr);
				expr = (Expr *) var;
			}
			exprList = lappend(exprList, expr);
		}

		/* Prepare row for assignment to target table */
		exprList = transformInsertRow(pstate, exprList,
									  stmt->cols,
									  icolumns, attrnos,
									  false);
	}
	else if (list_length(selectStmt->valuesLists) > 1)
	{
		/*
		 * Process INSERT ... VALUES with multiple VALUES sublists. We
		 * generate a VALUES RTE holding the transformed expression lists, and
		 * build up a targetlist containing Vars that reference the VALUES
		 * RTE.
		 */
		List	   *exprsLists = NIL;
		List	   *coltypes = NIL;
		List	   *coltypmods = NIL;
		List	   *colcollations = NIL;
		int			sublist_length = -1;
		bool		lateral = false;

		Assert(selectStmt->intoClause == NULL);

		foreach(lc, selectStmt->valuesLists)
		{
			List	   *sublist = (List *) lfirst(lc);

			/*
			 * Do basic expression transformation (same as a ROW() expr, but
			 * allow SetToDefault at top level)
			 */
			sublist = transformExpressionList(pstate, sublist,
											  EXPR_KIND_VALUES, true);

			/*
			 * All the sublists must be the same length, *after*
			 * transformation (which might expand '*' into multiple items).
			 * The VALUES RTE can't handle anything different.
			 */
			if (sublist_length < 0)
			{
				/* Remember post-transformation length of first sublist */
				sublist_length = list_length(sublist);
			}
			else if (sublist_length != list_length(sublist))
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("VALUES lists must all be the same length"),
						 parser_errposition(pstate,
											exprLocation((Node *) sublist))));
			}

			/*
			 * Prepare row for assignment to target table.  We process any
			 * indirection on the target column specs normally but then strip
			 * off the resulting field/array assignment nodes, since we don't
			 * want the parsed statement to contain copies of those in each
			 * VALUES row.  (It's annoying to have to transform the
			 * indirection specs over and over like this, but avoiding it
			 * would take some really messy refactoring of
			 * transformAssignmentIndirection.)
			 */
			sublist = transformInsertRow(pstate, sublist,
										 stmt->cols,
										 icolumns, attrnos,
										 true);

			/*
			 * We must assign collations now because assign_query_collations
			 * doesn't process rangetable entries.  We just assign all the
			 * collations independently in each row, and don't worry about
			 * whether they are consistent vertically.  The outer INSERT query
			 * isn't going to care about the collations of the VALUES columns,
			 * so it's not worth the effort to identify a common collation for
			 * each one here.  (But note this does have one user-visible
			 * consequence: INSERT ... VALUES won't complain about conflicting
			 * explicit COLLATEs in a column, whereas the same VALUES
			 * construct in another context would complain.)
			 */
			assign_list_collations(pstate, sublist);

			exprsLists = lappend(exprsLists, sublist);
		}

		/*
		 * Construct column type/typmod/collation lists for the VALUES RTE.
		 * Every expression in each column has been coerced to the type/typmod
		 * of the corresponding target column or subfield, so it's sufficient
		 * to look at the exprType/exprTypmod of the first row.  We don't care
		 * about the collation labeling, so just fill in InvalidOid for that.
		 */
		foreach(lc, (List *) linitial(exprsLists))
		{
			Node	   *val = (Node *) lfirst(lc);

			coltypes = lappend_oid(coltypes, exprType(val));
			coltypmods = lappend_int(coltypmods, exprTypmod(val));
			colcollations = lappend_oid(colcollations, InvalidOid);
		}

		/*
		 * Ordinarily there can't be any current-level Vars in the expression
		 * lists, because the namespace was empty ... but if we're inside
		 * CREATE RULE, then NEW/OLD references might appear.  In that case we
		 * have to mark the VALUES RTE as LATERAL.
		 */
		if (list_length(pstate->p_rtable) != 1 &&
			contain_vars_of_level((Node *) exprsLists, 0))
			lateral = true;

		/*
		 * Generate the VALUES RTE
		 */
		nsitem = addRangeTableEntryForValues(pstate, exprsLists,
											 coltypes, coltypmods, colcollations,
											 NULL, lateral, true);
		addNSItemToQuery(pstate, nsitem, true, false, false);

		/*
		 * Generate list of Vars referencing the RTE
		 */
		exprList = expandNSItemVars(nsitem, 0, -1, NULL);

		/*
		 * Re-apply any indirection on the target column specs to the Vars
		 */
		exprList = transformInsertRow(pstate, exprList,
									  stmt->cols,
									  icolumns, attrnos,
									  false);
	}
	else
	{
		/*
		 * Process INSERT ... VALUES with a single VALUES sublist.  We treat
		 * this case separately for efficiency.  The sublist is just computed
		 * directly as the Query's targetlist, with no VALUES RTE.  So it
		 * works just like a SELECT without any FROM.
		 */
		List	   *valuesLists = selectStmt->valuesLists;

		Assert(list_length(valuesLists) == 1);
		Assert(selectStmt->intoClause == NULL);

		/*
		 * Do basic expression transformation (same as a ROW() expr, but allow
		 * SetToDefault at top level)
		 */
		exprList = transformExpressionList(pstate,
										   (List *) linitial(valuesLists),
										   EXPR_KIND_VALUES_SINGLE,
										   true);

		/* Prepare row for assignment to target table */
		exprList = transformInsertRow(pstate, exprList,
									  stmt->cols,
									  icolumns, attrnos,
									  false);
	}

	/*
	 * Generate query's target list using the computed list of expressions.
	 * Also, mark all the target columns as needing insert permissions.
	 */
	rte = pstate->p_target_nsitem->p_rte;
	qry->targetList = NIL;
	Assert(list_length(exprList) <= list_length(icolumns));
	forthree(lc, exprList, icols, icolumns, attnos, attrnos)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		ResTarget  *col = lfirst_node(ResTarget, icols);
		AttrNumber	attr_num = (AttrNumber) lfirst_int(attnos);
		TargetEntry *tle;

		tle = makeTargetEntry(expr,
							  attr_num,
							  col->name,
							  false);
		qry->targetList = lappend(qry->targetList, tle);

		rte->insertedCols = bms_add_member(rte->insertedCols,
										   attr_num - FirstLowInvalidHeapAttributeNumber);
#ifdef ADB
		if (IsA(expr, SetToDefault))
			rte->defaultCols = bms_add_member(rte->defaultCols,
											  attr_num - FirstLowInvalidHeapAttributeNumber);
#endif /* ADB */
	}

	/*
	 * If we have any clauses yet to process, set the query namespace to
	 * contain only the target relation, removing any entries added in a
	 * sub-SELECT or VALUES list.
	 */
	if (stmt->onConflictClause || stmt->returningList)
	{
		pstate->p_namespace = NIL;
		addNSItemToQuery(pstate, pstate->p_target_nsitem,
						 false, true, true);
	}

	/* Process ON CONFLICT, if any. */
	if (stmt->onConflictClause)
		qry->onConflict = transformOnConflictClause(pstate,
													stmt->onConflictClause);

	/* Process RETURNING, if any. */
	if (stmt->returningList)
		qry->returningList = transformReturningList(pstate,
													stmt->returningList);

	/* done building the range table and jointree */
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * Prepare an INSERT row for assignment to the target table.
 *
 * exprlist: transformed expressions for source values; these might come from
 * a VALUES row, or be Vars referencing a sub-SELECT or VALUES RTE output.
 * stmtcols: original target-columns spec for INSERT (we just test for NIL)
 * icolumns: effective target-columns spec (list of ResTarget)
 * attrnos: integer column numbers (must be same length as icolumns)
 * strip_indirection: if true, remove any field/array assignment nodes
 */
static List *
transformInsertRow(ParseState *pstate, List *exprlist,
				   List *stmtcols, List *icolumns, List *attrnos,
				   bool strip_indirection)
{
	List	   *result;
	ListCell   *lc;
	ListCell   *icols;
	ListCell   *attnos;

	/*
	 * Check length of expr list.  It must not have more expressions than
	 * there are target columns.  We allow fewer, but only if no explicit
	 * columns list was given (the remaining columns are implicitly
	 * defaulted).  Note we must check this *after* transformation because
	 * that could expand '*' into multiple items.
	 */
	if (list_length(exprlist) > list_length(icolumns))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("INSERT has more expressions than target columns"),
				 parser_errposition(pstate,
									exprLocation(list_nth(exprlist,
														  list_length(icolumns))))));
	if (stmtcols != NIL &&
		list_length(exprlist) < list_length(icolumns))
	{
		/*
		 * We can get here for cases like INSERT ... SELECT (a,b,c) FROM ...
		 * where the user accidentally created a RowExpr instead of separate
		 * columns.  Add a suitable hint if that seems to be the problem,
		 * because the main error message is quite misleading for this case.
		 * (If there's no stmtcols, you'll get something about data type
		 * mismatch, which is less misleading so we don't worry about giving a
		 * hint in that case.)
		 */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("INSERT has more target columns than expressions"),
				 ((list_length(exprlist) == 1 &&
				   count_rowexpr_columns(pstate, linitial(exprlist)) ==
				   list_length(icolumns)) ?
				  errhint("The insertion source is a row expression containing the same number of columns expected by the INSERT. Did you accidentally use extra parentheses?") : 0),
				 parser_errposition(pstate,
									exprLocation(list_nth(icolumns,
														  list_length(exprlist))))));
	}

	/*
	 * Prepare columns for assignment to target table.
	 */
	result = NIL;
	forthree(lc, exprlist, icols, icolumns, attnos, attrnos)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		ResTarget  *col = lfirst_node(ResTarget, icols);
		int			attno = lfirst_int(attnos);

		expr = transformAssignedExpr(pstate, expr,
									 EXPR_KIND_INSERT_TARGET,
									 col->name,
									 attno,
									 col->indirection,
									 col->location);

		if (strip_indirection)
		{
			while (expr)
			{
				if (IsA(expr, FieldStore))
				{
					FieldStore *fstore = (FieldStore *) expr;

					expr = (Expr *) linitial(fstore->newvals);
				}
				else if (IsA(expr, SubscriptingRef))
				{
					SubscriptingRef *sbsref = (SubscriptingRef *) expr;

					if (sbsref->refassgnexpr == NULL)
						break;

					expr = sbsref->refassgnexpr;
				}
				else
					break;
			}
		}

		result = lappend(result, expr);
	}

	return result;
}

/*
 * transformOnConflictClause -
 *	  transforms an OnConflictClause in an INSERT
 */
static OnConflictExpr *
transformOnConflictClause(ParseState *pstate,
						  OnConflictClause *onConflictClause)
{
	ParseNamespaceItem *exclNSItem = NULL;
	List	   *arbiterElems;
	Node	   *arbiterWhere;
	Oid			arbiterConstraint;
	List	   *onConflictSet = NIL;
	Node	   *onConflictWhere = NULL;
	int			exclRelIndex = 0;
	List	   *exclRelTlist = NIL;
	OnConflictExpr *result;

	/*
	 * If this is ON CONFLICT ... UPDATE, first create the range table entry
	 * for the EXCLUDED pseudo relation, so that that will be present while
	 * processing arbiter expressions.  (You can't actually reference it from
	 * there, but this provides a useful error message if you try.)
	 */
	if (onConflictClause->action == ONCONFLICT_UPDATE)
	{
		Relation	targetrel = pstate->p_target_relation;
		RangeTblEntry *exclRte;

		exclNSItem = addRangeTableEntryForRelation(pstate,
												   targetrel,
												   RowExclusiveLock,
												   makeAlias("excluded", NIL),
												   false, false);
		exclRte = exclNSItem->p_rte;
		exclRelIndex = exclNSItem->p_rtindex;

		/*
		 * relkind is set to composite to signal that we're not dealing with
		 * an actual relation, and no permission checks are required on it.
		 * (We'll check the actual target relation, instead.)
		 */
		exclRte->relkind = RELKIND_COMPOSITE_TYPE;
		exclRte->requiredPerms = 0;
		/* other permissions fields in exclRte are already empty */

		/* Create EXCLUDED rel's targetlist for use by EXPLAIN */
		exclRelTlist = BuildOnConflictExcludedTargetlist(targetrel,
														 exclRelIndex);
	}

	/* Process the arbiter clause, ON CONFLICT ON (...) */
	transformOnConflictArbiter(pstate, onConflictClause, &arbiterElems,
							   &arbiterWhere, &arbiterConstraint);

	/* Process DO UPDATE */
	if (onConflictClause->action == ONCONFLICT_UPDATE)
	{
		/*
		 * Expressions in the UPDATE targetlist need to be handled like UPDATE
		 * not INSERT.  We don't need to save/restore this because all INSERT
		 * expressions have been parsed already.
		 */
		pstate->p_is_insert = false;

		/*
		 * Add the EXCLUDED pseudo relation to the query namespace, making it
		 * available in the UPDATE subexpressions.
		 */
		addNSItemToQuery(pstate, exclNSItem, false, true, true);

		/*
		 * Now transform the UPDATE subexpressions.
		 */
		onConflictSet =
			transformUpdateTargetList(pstate, onConflictClause->targetList);

		onConflictWhere = transformWhereClause(pstate,
											   onConflictClause->whereClause,
											   EXPR_KIND_WHERE, "WHERE");

		/*
		 * Remove the EXCLUDED pseudo relation from the query namespace, since
		 * it's not supposed to be available in RETURNING.  (Maybe someday we
		 * could allow that, and drop this step.)
		 */
		Assert((ParseNamespaceItem *) llast(pstate->p_namespace) == exclNSItem);
		pstate->p_namespace = list_delete_last(pstate->p_namespace);
	}

	/* Finally, build ON CONFLICT DO [NOTHING | UPDATE] expression */
	result = makeNode(OnConflictExpr);

	result->action = onConflictClause->action;
	result->arbiterElems = arbiterElems;
	result->arbiterWhere = arbiterWhere;
	result->constraint = arbiterConstraint;
	result->onConflictSet = onConflictSet;
	result->onConflictWhere = onConflictWhere;
	result->exclRelIndex = exclRelIndex;
	result->exclRelTlist = exclRelTlist;

	return result;
}


/*
 * BuildOnConflictExcludedTargetlist
 *		Create target list for the EXCLUDED pseudo-relation of ON CONFLICT,
 *		representing the columns of targetrel with varno exclRelIndex.
 *
 * Note: Exported for use in the rewriter.
 */
List *
BuildOnConflictExcludedTargetlist(Relation targetrel,
								  Index exclRelIndex)
{
	List	   *result = NIL;
	int			attno;
	Var		   *var;
	TargetEntry *te;

	/*
	 * Note that resnos of the tlist must correspond to attnos of the
	 * underlying relation, hence we need entries for dropped columns too.
	 */
	for (attno = 0; attno < RelationGetNumberOfAttributes(targetrel); attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(targetrel->rd_att, attno);
		char	   *name;

		if (attr->attisdropped)
		{
			/*
			 * can't use atttypid here, but it doesn't really matter what type
			 * the Const claims to be.
			 */
			var = (Var *) makeNullConst(INT4OID, -1, InvalidOid);
			name = NULL;
		}
		else
		{
			var = makeVar(exclRelIndex, attno + 1,
						  attr->atttypid, attr->atttypmod,
						  attr->attcollation,
						  0);
			name = pstrdup(NameStr(attr->attname));
		}

		te = makeTargetEntry((Expr *) var,
							 attno + 1,
							 name,
							 false);

		result = lappend(result, te);
	}

	/*
	 * Add a whole-row-Var entry to support references to "EXCLUDED.*".  Like
	 * the other entries in the EXCLUDED tlist, its resno must match the Var's
	 * varattno, else the wrong things happen while resolving references in
	 * setrefs.c.  This is against normal conventions for targetlists, but
	 * it's okay since we don't use this as a real tlist.
	 */
	var = makeVar(exclRelIndex, InvalidAttrNumber,
				  targetrel->rd_rel->reltype,
				  -1, InvalidOid, 0);
	te = makeTargetEntry((Expr *) var, InvalidAttrNumber, NULL, true);
	result = lappend(result, te);

	return result;
}


/*
 * count_rowexpr_columns -
 *	  get number of columns contained in a ROW() expression;
 *	  return -1 if expression isn't a RowExpr or a Var referencing one.
 *
 * This is currently used only for hint purposes, so we aren't terribly
 * tense about recognizing all possible cases.  The Var case is interesting
 * because that's what we'll get in the INSERT ... SELECT (...) case.
 */
static int
count_rowexpr_columns(ParseState *pstate, Node *expr)
{
	if (expr == NULL)
		return -1;
	if (IsA(expr, RowExpr))
		return list_length(((RowExpr *) expr)->args);
	if (IsA(expr, Var))
	{
		Var		   *var = (Var *) expr;
		AttrNumber	attnum = var->varattno;

		if (attnum > 0 && var->vartype == RECORDOID)
		{
			RangeTblEntry *rte;

			rte = GetRTEByRangeTablePosn(pstate, var->varno, var->varlevelsup);
			if (rte->rtekind == RTE_SUBQUERY)
			{
				/* Subselect-in-FROM: examine sub-select's output expr */
				TargetEntry *ste = get_tle_by_resno(rte->subquery->targetList,
													attnum);

				if (ste == NULL || ste->resjunk)
					return -1;
				expr = (Node *) ste->expr;
				if (IsA(expr, RowExpr))
					return list_length(((RowExpr *) expr)->args);
			}
		}
	}
	return -1;
}


/*
 * transformSelectStmt -
 *	  transforms a Select Statement
 *
 * Note: this covers only cases with no set operations and no VALUES lists;
 * see below for the other cases.
 */
static Query *
transformSelectStmt(ParseState *pstate, SelectStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	Node	   *qual;
	ListCell   *l;

	qry->commandType = CMD_SELECT;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/* Complain if we get called from someplace where INTO is not allowed */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT ... INTO is not allowed here"),
				 parser_errposition(pstate,
									exprLocation((Node *) stmt->intoClause))));

	/* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
	pstate->p_locking_clause = stmt->lockingClause;

	/* make WINDOW info available for window functions, too */
	pstate->p_windowdefs = stmt->windowClause;

	/* process the FROM clause */
	transformFromClause(pstate, stmt->fromClause);
#ifdef ADB_GRAM_ORA
	if (pstate->p_grammar == PARSE_GRAM_ORACLE)
	{
		/* transform WHERE */
		qual = transformWhereClause(pstate, stmt->whereClause,
									EXPR_KIND_WHERE, "WHERE");

		qual = transformFromAndWhere(pstate, qual);

		if (stmt->ora_connect_by)
		{
			OracleConnectBy *new_ = makeNode(OracleConnectBy);
			OracleConnectBy *old = stmt->ora_connect_by;
			int save_next_resno = pstate->p_next_resno;
			new_->no_cycle = old->no_cycle;
			if (old->start_with)
			{
				new_->start_with = transformExpr(pstate, old->start_with, EXPR_KIND_START_WITH);
				new_->start_with = coerce_to_boolean(pstate, new_->start_with, "START WITH");
			}
			new_->connect_by = transformExpr(pstate, old->connect_by, EXPR_KIND_CONNECT_BY);
			new_->connect_by = coerce_to_boolean(pstate, new_->connect_by, "CONNECT BY");
			new_->sortClause = transformSortClause(pstate,
												   old->sortClause,
												   &new_->sort_tlist,
												   EXPR_KIND_ORDER_SIBLINGS_BY,
												   true);
			qry->connect_by = new_;
			pstate->p_next_resno = save_next_resno;
		}
	}
#endif /* ADB_GRAM_ORA */
	/* transform targetlist */
	qry->targetList = transformTargetList(pstate, stmt->targetList,
										  EXPR_KIND_SELECT_TARGET);

	/* mark column origins */
	markTargetListOrigins(pstate, qry->targetList);

	/* transform WHERE */
#ifdef ADB_GRAM_ORA
	if (pstate->p_grammar != PARSE_GRAM_ORACLE)
#endif /* ADB_GRAM_ORA */
	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = transformWhereClause(pstate, stmt->havingClause,
										   EXPR_KIND_HAVING, "HAVING");

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
#ifdef ADB_GRAM_ORA
	if (pstate->p_grammar == PARSE_GRAM_ORACLE &&
		pstate->p_hasAggs &&
		stmt->groupClause == NIL)
	{
		/* when has aggregate, but not has group, we can ignore sort clause */
		qry->sortClause = NIL;
	}else
#endif /* ADB_GRAM_ORA */
	qry->sortClause = transformSortClause(pstate,
										  stmt->sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	qry->groupClause = transformGroupClause(pstate,
											stmt->groupClause,
											&qry->groupingSets,
											&qry->targetList,
											qry->sortClause,
											EXPR_KIND_GROUP_BY,
											false /* allow SQL92 rules */ );
	qry->groupDistinct = stmt->groupDistinct;

	if (stmt->distinctClause == NIL)
	{
		qry->distinctClause = NIL;
		qry->hasDistinctOn = false;
	}
	else if (linitial(stmt->distinctClause) == NULL)
	{
		/* We had SELECT DISTINCT */
		qry->distinctClause = transformDistinctClause(pstate,
													  &qry->targetList,
													  qry->sortClause,
													  false);
		qry->hasDistinctOn = false;
	}
	else
	{
		/* We had SELECT DISTINCT ON */
		qry->distinctClause = transformDistinctOnClause(pstate,
														stmt->distinctClause,
														&qry->targetList,
														qry->sortClause);
		qry->hasDistinctOn = true;
	}

	/* transform LIMIT */
	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											stmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   stmt->limitOption);
	qry->limitOption = stmt->limitOption;

	/* transform window clauses after we have seen all window functions */
	qry->windowClause = transformWindowDefinitions(pstate,
												   pstate->p_windowdefs,
												   &qry->targetList);

	/* resolve any still-unresolved output columns as being type text */
	if (pstate->p_resolve_unknowns)
		resolveTargetListUnknowns(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;

	foreach(l, stmt->lockingClause)
	{
		transformLockingClause(pstate, qry,
							   (LockingClause *) lfirst(l), false);
	}

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	return qry;
}

/*
 * transformValuesClause -
 *	  transforms a VALUES clause that's being used as a standalone SELECT
 *
 * We build a Query containing a VALUES RTE, rather as if one had written
 *			SELECT * FROM (VALUES ...) AS "*VALUES*"
 */
static Query *
transformValuesClause(ParseState *pstate, SelectStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	List	   *exprsLists;
	List	   *coltypes = NIL;
	List	   *coltypmods = NIL;
	List	   *colcollations = NIL;
	List	  **colexprs = NULL;
	int			sublist_length = -1;
	bool		lateral = false;
	ParseNamespaceItem *nsitem;
	ListCell   *lc;
	ListCell   *lc2;
	int			i;

	qry->commandType = CMD_SELECT;

	/* Most SELECT stuff doesn't apply in a VALUES clause */
	Assert(stmt->distinctClause == NIL);
	Assert(stmt->intoClause == NULL);
	Assert(stmt->targetList == NIL);
	Assert(stmt->fromClause == NIL);
	Assert(stmt->whereClause == NULL);
	Assert(stmt->groupClause == NIL);
	Assert(stmt->havingClause == NULL);
	Assert(stmt->windowClause == NIL);
	Assert(stmt->op == SETOP_NONE);

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * For each row of VALUES, transform the raw expressions.
	 *
	 * Note that the intermediate representation we build is column-organized
	 * not row-organized.  That simplifies the type and collation processing
	 * below.
	 */
	foreach(lc, stmt->valuesLists)
	{
		List	   *sublist = (List *) lfirst(lc);

		/*
		 * Do basic expression transformation (same as a ROW() expr, but here
		 * we disallow SetToDefault)
		 */
		sublist = transformExpressionList(pstate, sublist,
										  EXPR_KIND_VALUES, false);

		/*
		 * All the sublists must be the same length, *after* transformation
		 * (which might expand '*' into multiple items).  The VALUES RTE can't
		 * handle anything different.
		 */
		if (sublist_length < 0)
		{
			/* Remember post-transformation length of first sublist */
			sublist_length = list_length(sublist);
			/* and allocate array for per-column lists */
			colexprs = (List **) palloc0(sublist_length * sizeof(List *));
		}
		else if (sublist_length != list_length(sublist))
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("VALUES lists must all be the same length"),
					 parser_errposition(pstate,
										exprLocation((Node *) sublist))));
		}

		/* Build per-column expression lists */
		i = 0;
		foreach(lc2, sublist)
		{
			Node	   *col = (Node *) lfirst(lc2);

			colexprs[i] = lappend(colexprs[i], col);
			i++;
		}

		/* Release sub-list's cells to save memory */
		list_free(sublist);
	}

	/*
	 * Now resolve the common types of the columns, and coerce everything to
	 * those types.  Then identify the common typmod and common collation, if
	 * any, of each column.
	 *
	 * We must do collation processing now because (1) assign_query_collations
	 * doesn't process rangetable entries, and (2) we need to label the VALUES
	 * RTE with column collations for use in the outer query.  We don't
	 * consider conflict of implicit collations to be an error here; instead
	 * the column will just show InvalidOid as its collation, and you'll get a
	 * failure later if that results in failure to resolve a collation.
	 *
	 * Note we modify the per-column expression lists in-place.
	 */
	for (i = 0; i < sublist_length; i++)
	{
		Oid			coltype;
		int32		coltypmod;
		Oid			colcoll;

		coltype = select_common_type(pstate, colexprs[i], "VALUES", NULL);

		foreach(lc, colexprs[i])
		{
			Node	   *col = (Node *) lfirst(lc);

			col = coerce_to_common_type(pstate, col, coltype, "VALUES");
			lfirst(lc) = (void *) col;
		}

		coltypmod = select_common_typmod(pstate, colexprs[i], coltype);
		colcoll = select_common_collation(pstate, colexprs[i], true);

		coltypes = lappend_oid(coltypes, coltype);
		coltypmods = lappend_int(coltypmods, coltypmod);
		colcollations = lappend_oid(colcollations, colcoll);
	}
#ifdef ADB
	/* fix: Array access (from variable 'colexprs') results in a null pointer
	 * dereference
	 */
	AssertArg(colexprs);
#endif
	/*
	 * Finally, rearrange the coerced expressions into row-organized lists.
	 */
	exprsLists = NIL;
	foreach(lc, colexprs[0])
	{
		Node	   *col = (Node *) lfirst(lc);
		List	   *sublist;

		sublist = list_make1(col);
		exprsLists = lappend(exprsLists, sublist);
	}
	list_free(colexprs[0]);
	for (i = 1; i < sublist_length; i++)
	{
		forboth(lc, colexprs[i], lc2, exprsLists)
		{
			Node	   *col = (Node *) lfirst(lc);
			List	   *sublist = lfirst(lc2);

			sublist = lappend(sublist, col);
		}
		list_free(colexprs[i]);
	}

	/*
	 * Ordinarily there can't be any current-level Vars in the expression
	 * lists, because the namespace was empty ... but if we're inside CREATE
	 * RULE, then NEW/OLD references might appear.  In that case we have to
	 * mark the VALUES RTE as LATERAL.
	 */
	if (pstate->p_rtable != NIL &&
		contain_vars_of_level((Node *) exprsLists, 0))
		lateral = true;

	/*
	 * Generate the VALUES RTE
	 */
	nsitem = addRangeTableEntryForValues(pstate, exprsLists,
										 coltypes, coltypmods, colcollations,
										 NULL, lateral, true);
	addNSItemToQuery(pstate, nsitem, true, true, true);

	/*
	 * Generate a targetlist as though expanding "*"
	 */
	Assert(pstate->p_next_resno == 1);
	qry->targetList = expandNSItemAttrs(pstate, nsitem, 0, -1);

	/*
	 * The grammar allows attaching ORDER BY, LIMIT, and FOR UPDATE to a
	 * VALUES, so cope.
	 */
	qry->sortClause = transformSortClause(pstate,
										  stmt->sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	qry->limitOffset = transformLimitClause(pstate, stmt->limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											stmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, stmt->limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   stmt->limitOption);
	qry->limitOption = stmt->limitOption;

	if (stmt->lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s cannot be applied to VALUES",
						LCS_asString(((LockingClause *)
									  linitial(stmt->lockingClause))->strength))));

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformSetOperationStmt -
 *	  transforms a set-operations tree
 *
 * A set-operation tree is just a SELECT, but with UNION/INTERSECT/EXCEPT
 * structure to it.  We must transform each leaf SELECT and build up a top-
 * level Query that contains the leaf SELECTs as subqueries in its rangetable.
 * The tree of set operations is converted into the setOperations field of
 * the top-level Query.
 */
static Query *
transformSetOperationStmt(ParseState *pstate, SelectStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	SelectStmt *leftmostSelect;
	int			leftmostRTI;
	Query	   *leftmostQuery;
	SetOperationStmt *sostmt;
	List	   *sortClause;
	Node	   *limitOffset;
	Node	   *limitCount;
	List	   *lockingClause;
	WithClause *withClause;
	Node	   *node;
	ListCell   *left_tlist,
			   *lct,
			   *lcm,
			   *lcc,
			   *l;
	List	   *targetvars,
			   *targetnames,
			   *sv_namespace;
	int			sv_rtable_length;
	ParseNamespaceItem *jnsitem;
	ParseNamespaceColumn *sortnscolumns;
	int			sortcolindex;
	int			tllen;

	qry->commandType = CMD_SELECT;

	/*
	 * Find leftmost leaf SelectStmt.  We currently only need to do this in
	 * order to deliver a suitable error message if there's an INTO clause
	 * there, implying the set-op tree is in a context that doesn't allow
	 * INTO.  (transformSetOperationTree would throw error anyway, but it
	 * seems worth the trouble to throw a different error for non-leftmost
	 * INTO, so we produce that error in transformSetOperationTree.)
	 */
	leftmostSelect = stmt->larg;
	while (leftmostSelect && leftmostSelect->op != SETOP_NONE)
		leftmostSelect = leftmostSelect->larg;
	Assert(leftmostSelect && IsA(leftmostSelect, SelectStmt) &&
		   leftmostSelect->larg == NULL);
	if (leftmostSelect->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("SELECT ... INTO is not allowed here"),
				 parser_errposition(pstate,
									exprLocation((Node *) leftmostSelect->intoClause))));

	/*
	 * We need to extract ORDER BY and other top-level clauses here and not
	 * let transformSetOperationTree() see them --- else it'll just recurse
	 * right back here!
	 */
	sortClause = stmt->sortClause;
	limitOffset = stmt->limitOffset;
	limitCount = stmt->limitCount;
	lockingClause = stmt->lockingClause;
	withClause = stmt->withClause;

	stmt->sortClause = NIL;
	stmt->limitOffset = NULL;
	stmt->limitCount = NULL;
	stmt->lockingClause = NIL;
	stmt->withClause = NULL;

	/* We don't support FOR UPDATE/SHARE with set ops at the moment. */
	if (lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT",
						LCS_asString(((LockingClause *)
									  linitial(lockingClause))->strength))));

	/* Process the WITH clause independently of all else */
	if (withClause)
	{
		qry->hasRecursive = withClause->recursive;
		qry->cteList = transformWithClause(pstate, withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	/*
	 * Recursively transform the components of the tree.
	 */
	sostmt = castNode(SetOperationStmt,
					  transformSetOperationTree(pstate, stmt, true, NULL));
	Assert(sostmt);
	qry->setOperations = (Node *) sostmt;

	/*
	 * Re-find leftmost SELECT (now it's a sub-query in rangetable)
	 */
	node = sostmt->larg;
	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));
	leftmostRTI = ((RangeTblRef *) node)->rtindex;
	leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;
	Assert(leftmostQuery != NULL);

	/*
	 * Generate dummy targetlist for outer query using column names of
	 * leftmost select and common datatypes/collations of topmost set
	 * operation.  Also make lists of the dummy vars and their names for use
	 * in parsing ORDER BY.
	 *
	 * Note: we use leftmostRTI as the varno of the dummy variables. It
	 * shouldn't matter too much which RT index they have, as long as they
	 * have one that corresponds to a real RT entry; else funny things may
	 * happen when the tree is mashed by rule rewriting.
	 */
	qry->targetList = NIL;
	targetvars = NIL;
	targetnames = NIL;
	sortnscolumns = (ParseNamespaceColumn *)
		palloc0(list_length(sostmt->colTypes) * sizeof(ParseNamespaceColumn));
	sortcolindex = 0;

	forfour(lct, sostmt->colTypes,
			lcm, sostmt->colTypmods,
			lcc, sostmt->colCollations,
			left_tlist, leftmostQuery->targetList)
	{
		Oid			colType = lfirst_oid(lct);
		int32		colTypmod = lfirst_int(lcm);
		Oid			colCollation = lfirst_oid(lcc);
		TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
		char	   *colName;
		TargetEntry *tle;
		Var		   *var;

		Assert(!lefttle->resjunk);
		colName = pstrdup(lefttle->resname);
		var = makeVar(leftmostRTI,
					  lefttle->resno,
					  colType,
					  colTypmod,
					  colCollation,
					  0);
		var->location = exprLocation((Node *) lefttle->expr);
		tle = makeTargetEntry((Expr *) var,
							  (AttrNumber) pstate->p_next_resno++,
							  colName,
							  false);
		qry->targetList = lappend(qry->targetList, tle);
		targetvars = lappend(targetvars, var);
		targetnames = lappend(targetnames, makeString(colName));
		sortnscolumns[sortcolindex].p_varno = leftmostRTI;
		sortnscolumns[sortcolindex].p_varattno = lefttle->resno;
		sortnscolumns[sortcolindex].p_vartype = colType;
		sortnscolumns[sortcolindex].p_vartypmod = colTypmod;
		sortnscolumns[sortcolindex].p_varcollid = colCollation;
		sortnscolumns[sortcolindex].p_varnosyn = leftmostRTI;
		sortnscolumns[sortcolindex].p_varattnosyn = lefttle->resno;
		sortcolindex++;
	}

	/*
	 * As a first step towards supporting sort clauses that are expressions
	 * using the output columns, generate a namespace entry that makes the
	 * output columns visible.  A Join RTE node is handy for this, since we
	 * can easily control the Vars generated upon matches.
	 *
	 * Note: we don't yet do anything useful with such cases, but at least
	 * "ORDER BY upper(foo)" will draw the right error message rather than
	 * "foo not found".
	 */
	sv_rtable_length = list_length(pstate->p_rtable);

	jnsitem = addRangeTableEntryForJoin(pstate,
										targetnames,
										sortnscolumns,
										JOIN_INNER,
										0,
										targetvars,
										NIL,
										NIL,
										NULL,
										NULL,
										false);

	sv_namespace = pstate->p_namespace;
	pstate->p_namespace = NIL;

	/* add jnsitem to column namespace only */
	addNSItemToQuery(pstate, jnsitem, false, false, true);

	/*
	 * For now, we don't support resjunk sort clauses on the output of a
	 * setOperation tree --- you can only use the SQL92-spec options of
	 * selecting an output column by name or number.  Enforce by checking that
	 * transformSortClause doesn't add any items to tlist.
	 */
	tllen = list_length(qry->targetList);

	qry->sortClause = transformSortClause(pstate,
										  sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	/* restore namespace, remove join RTE from rtable */
	pstate->p_namespace = sv_namespace;
	pstate->p_rtable = list_truncate(pstate->p_rtable, sv_rtable_length);

	if (tllen != list_length(qry->targetList))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid UNION/INTERSECT/EXCEPT ORDER BY clause"),
				 errdetail("Only result column names can be used, not expressions or functions."),
				 errhint("Add the expression/function to every SELECT, or move the UNION into a FROM clause."),
				 parser_errposition(pstate,
									exprLocation(list_nth(qry->targetList, tllen)))));

	qry->limitOffset = transformLimitClause(pstate, limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											stmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   stmt->limitOption);
	qry->limitOption = stmt->limitOption;

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;

	foreach(l, lockingClause)
	{
		transformLockingClause(pstate, qry,
							   (LockingClause *) lfirst(l), false);
	}

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	return qry;
}

/*
 * Make a SortGroupClause node for a SetOperationStmt's groupClauses
 */
SortGroupClause *
makeSortGroupClauseForSetOp(Oid rescoltype)
{
	SortGroupClause *grpcl = makeNode(SortGroupClause);
	Oid			sortop;
	Oid			eqop;
	bool		hashable;

	/* determine the eqop and optional sortop */
	get_sort_group_operators(rescoltype,
							 false, true, false,
							 &sortop, &eqop, NULL,
							 &hashable);

	/* we don't have a tlist yet, so can't assign sortgrouprefs */
	grpcl->tleSortGroupRef = 0;
	grpcl->eqop = eqop;
	grpcl->sortop = sortop;
	grpcl->nulls_first = false; /* OK with or without sortop */
	grpcl->hashable = hashable;

	return grpcl;
}

/*
 * transformSetOperationTree
 *		Recursively transform leaves and internal nodes of a set-op tree
 *
 * In addition to returning the transformed node, if targetlist isn't NULL
 * then we return a list of its non-resjunk TargetEntry nodes.  For a leaf
 * set-op node these are the actual targetlist entries; otherwise they are
 * dummy entries created to carry the type, typmod, collation, and location
 * (for error messages) of each output column of the set-op node.  This info
 * is needed only during the internal recursion of this function, so outside
 * callers pass NULL for targetlist.  Note: the reason for passing the
 * actual targetlist entries of a leaf node is so that upper levels can
 * replace UNKNOWN Consts with properly-coerced constants.
 */
static Node *
transformSetOperationTree(ParseState *pstate, SelectStmt *stmt,
						  bool isTopLevel, List **targetlist)
{
	bool		isLeaf;

	Assert(stmt && IsA(stmt, SelectStmt));

	/* Guard against stack overflow due to overly complex set-expressions */
	check_stack_depth();

	/*
	 * Validity-check both leaf and internal SELECTs for disallowed ops.
	 */
	if (stmt->intoClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("INTO is only allowed on first SELECT of UNION/INTERSECT/EXCEPT"),
				 parser_errposition(pstate,
									exprLocation((Node *) stmt->intoClause))));

	/* We don't support FOR UPDATE/SHARE with set ops at the moment. */
	if (stmt->lockingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT",
						LCS_asString(((LockingClause *)
									  linitial(stmt->lockingClause))->strength))));

	/*
	 * If an internal node of a set-op tree has ORDER BY, LIMIT, FOR UPDATE,
	 * or WITH clauses attached, we need to treat it like a leaf node to
	 * generate an independent sub-Query tree.  Otherwise, it can be
	 * represented by a SetOperationStmt node underneath the parent Query.
	 */
	if (stmt->op == SETOP_NONE)
	{
		Assert(stmt->larg == NULL && stmt->rarg == NULL);
		isLeaf = true;
	}
	else
	{
		Assert(stmt->larg != NULL && stmt->rarg != NULL);
		if (stmt->sortClause || stmt->limitOffset || stmt->limitCount ||
			stmt->lockingClause || stmt->withClause)
			isLeaf = true;
		else
			isLeaf = false;
	}

	if (isLeaf)
	{
		/* Process leaf SELECT */
		Query	   *selectQuery;
		char		selectName[32];
		ParseNamespaceItem *nsitem;
		RangeTblRef *rtr;
		ListCell   *tl;

		/*
		 * Transform SelectStmt into a Query.
		 *
		 * This works the same as SELECT transformation normally would, except
		 * that we prevent resolving unknown-type outputs as TEXT.  This does
		 * not change the subquery's semantics since if the column type
		 * matters semantically, it would have been resolved to something else
		 * anyway.  Doing this lets us resolve such outputs using
		 * select_common_type(), below.
		 *
		 * Note: previously transformed sub-queries don't affect the parsing
		 * of this sub-query, because they are not in the toplevel pstate's
		 * namespace list.
		 */
		selectQuery = parse_sub_analyze((Node *) stmt, pstate,
										NULL, false, false);

		/*
		 * Check for bogus references to Vars on the current query level (but
		 * upper-level references are okay). Normally this can't happen
		 * because the namespace will be empty, but it could happen if we are
		 * inside a rule.
		 */
		if (pstate->p_namespace)
		{
			if (contain_vars_of_level((Node *) selectQuery, 1))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("UNION/INTERSECT/EXCEPT member statement cannot refer to other relations of same query level"),
						 parser_errposition(pstate,
											locate_var_of_level((Node *) selectQuery, 1))));
		}

		/*
		 * Extract a list of the non-junk TLEs for upper-level processing.
		 */
		if (targetlist)
		{
			*targetlist = NIL;
			foreach(tl, selectQuery->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(tl);

				if (!tle->resjunk)
					*targetlist = lappend(*targetlist, tle);
			}
		}

		/*
		 * Make the leaf query be a subquery in the top-level rangetable.
		 */
		snprintf(selectName, sizeof(selectName), "*SELECT* %d",
				 list_length(pstate->p_rtable) + 1);
		nsitem = addRangeTableEntryForSubquery(pstate,
											   selectQuery,
											   makeAlias(selectName, NIL),
											   false,
											   false);

		/*
		 * Return a RangeTblRef to replace the SelectStmt in the set-op tree.
		 */
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = nsitem->p_rtindex;
		return (Node *) rtr;
	}
	else
	{
		/* Process an internal node (set operation node) */
		SetOperationStmt *op = makeNode(SetOperationStmt);
		List	   *ltargetlist;
		List	   *rtargetlist;
		ListCell   *ltl;
		ListCell   *rtl;
		const char *context;

		context = (stmt->op == SETOP_UNION ? "UNION" :
				   (stmt->op == SETOP_INTERSECT ? "INTERSECT" :
					"EXCEPT"));

		op->op = stmt->op;
		op->all = stmt->all;

		/*
		 * Recursively transform the left child node.
		 */
		op->larg = transformSetOperationTree(pstate, stmt->larg,
											 false,
											 &ltargetlist);

		/*
		 * If we are processing a recursive union query, now is the time to
		 * examine the non-recursive term's output columns and mark the
		 * containing CTE as having those result columns.  We should do this
		 * only at the topmost setop of the CTE, of course.
		 */
		if (isTopLevel &&
			pstate->p_parent_cte &&
			pstate->p_parent_cte->cterecursive)
			determineRecursiveColTypes(pstate, op->larg, ltargetlist);

		/*
		 * Recursively transform the right child node.
		 */
		op->rarg = transformSetOperationTree(pstate, stmt->rarg,
											 false,
											 &rtargetlist);

		/*
		 * Verify that the two children have the same number of non-junk
		 * columns, and determine the types of the merged output columns.
		 */
		if (list_length(ltargetlist) != list_length(rtargetlist))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("each %s query must have the same number of columns",
							context),
					 parser_errposition(pstate,
										exprLocation((Node *) rtargetlist))));

		if (targetlist)
			*targetlist = NIL;
		op->colTypes = NIL;
		op->colTypmods = NIL;
		op->colCollations = NIL;
		op->groupClauses = NIL;
		forboth(ltl, ltargetlist, rtl, rtargetlist)
		{
			TargetEntry *ltle = (TargetEntry *) lfirst(ltl);
			TargetEntry *rtle = (TargetEntry *) lfirst(rtl);
			Node	   *lcolnode = (Node *) ltle->expr;
			Node	   *rcolnode = (Node *) rtle->expr;
			Oid			lcoltype = exprType(lcolnode);
			Oid			rcoltype = exprType(rcolnode);
			Node	   *bestexpr;
			int			bestlocation;
			Oid			rescoltype;
			int32		rescoltypmod;
			Oid			rescolcoll;

			/* select common type, same as CASE et al */
#ifdef ADB_GRAM_ORA
			if (IsOracleParseGram(pstate))
				rescoltype=select_oracle_type(pstate,
											   list_make2(lcolnode, rcolnode),
											   context,
											   &bestexpr,
											   ORA_CONVERT_KIND_COMMON,
											   "");
			else
#endif /* ADB_GRAM_ORA */
			rescoltype = select_common_type(pstate,
											list_make2(lcolnode, rcolnode),
											context,
											&bestexpr);
			bestlocation = exprLocation(bestexpr);

			/*
			 * Verify the coercions are actually possible.  If not, we'd fail
			 * later anyway, but we want to fail now while we have sufficient
			 * context to produce an error cursor position.
			 *
			 * For all non-UNKNOWN-type cases, we verify coercibility but we
			 * don't modify the child's expression, for fear of changing the
			 * child query's semantics.
			 *
			 * If a child expression is an UNKNOWN-type Const or Param, we
			 * want to replace it with the coerced expression.  This can only
			 * happen when the child is a leaf set-op node.  It's safe to
			 * replace the expression because if the child query's semantics
			 * depended on the type of this output column, it'd have already
			 * coerced the UNKNOWN to something else.  We want to do this
			 * because (a) we want to verify that a Const is valid for the
			 * target type, or resolve the actual type of an UNKNOWN Param,
			 * and (b) we want to avoid unnecessary discrepancies between the
			 * output type of the child query and the resolved target type.
			 * Such a discrepancy would disable optimization in the planner.
			 *
			 * If it's some other UNKNOWN-type node, eg a Var, we do nothing
			 * (knowing that coerce_to_common_type would fail).  The planner
			 * is sometimes able to fold an UNKNOWN Var to a constant before
			 * it has to coerce the type, so failing now would just break
			 * cases that might work.
			 */
			if (lcoltype != UNKNOWNOID)
#ifdef ADB_GRAM_ORA
				if (IsOracleParseGram(pstate))
					lcolnode = coerce_to_common_type_extend(pstate, lcolnode, rescoltype,
															context, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST);
				else
#endif
				lcolnode = coerce_to_common_type(pstate, lcolnode,
												 rescoltype, context);
			else if (IsA(lcolnode, Const) ||
					 IsA(lcolnode, Param))
			{
#ifdef ADB_GRAM_ORA
				if (IsOracleParseGram(pstate))
					lcolnode = coerce_to_common_type_extend(pstate, lcolnode, rescoltype,
															context, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST);
				else
#endif
				lcolnode = coerce_to_common_type(pstate, lcolnode,
												 rescoltype, context);
				ltle->expr = (Expr *) lcolnode;
			}

			if (rcoltype != UNKNOWNOID)
#ifdef ADB_GRAM_ORA
				if (IsOracleParseGram(pstate))
					rcolnode = coerce_to_common_type_extend(pstate, rcolnode, rescoltype,
															context, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST);
				else
#endif
				rcolnode = coerce_to_common_type(pstate, rcolnode,
												 rescoltype, context);
			else if (IsA(rcolnode, Const) ||
					 IsA(rcolnode, Param))
			{
#ifdef ADB_GRAM_ORA
				if (IsOracleParseGram(pstate))
					rcolnode = coerce_to_common_type_extend(pstate, rcolnode, rescoltype,
															context, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST);
				else
#endif
				rcolnode = coerce_to_common_type(pstate, rcolnode,
												 rescoltype, context);
				rtle->expr = (Expr *) rcolnode;
			}

			rescoltypmod = select_common_typmod(pstate,
												list_make2(lcolnode, rcolnode),
												rescoltype);

			/*
			 * Select common collation.  A common collation is required for
			 * all set operators except UNION ALL; see SQL:2008 7.13 <query
			 * expression> Syntax Rule 15c.  (If we fail to identify a common
			 * collation for a UNION ALL column, the colCollations element
			 * will be set to InvalidOid, which may result in a runtime error
			 * if something at a higher query level wants to use the column's
			 * collation.)
			 */
			rescolcoll = select_common_collation(pstate,
												 list_make2(lcolnode, rcolnode),
												 (op->op == SETOP_UNION && op->all));

			/* emit results */
			op->colTypes = lappend_oid(op->colTypes, rescoltype);
			op->colTypmods = lappend_int(op->colTypmods, rescoltypmod);
			op->colCollations = lappend_oid(op->colCollations, rescolcoll);

			/*
			 * For all cases except UNION ALL, identify the grouping operators
			 * (and, if available, sorting operators) that will be used to
			 * eliminate duplicates.
			 */
			if (op->op != SETOP_UNION || !op->all)
			{
				ParseCallbackState pcbstate;

				setup_parser_errposition_callback(&pcbstate, pstate,
												  bestlocation);

				op->groupClauses = lappend(op->groupClauses,
										   makeSortGroupClauseForSetOp(rescoltype));

				cancel_parser_errposition_callback(&pcbstate);
			}

			/*
			 * Construct a dummy tlist entry to return.  We use a SetToDefault
			 * node for the expression, since it carries exactly the fields
			 * needed, but any other expression node type would do as well.
			 */
			if (targetlist)
			{
				SetToDefault *rescolnode = makeNode(SetToDefault);
				TargetEntry *restle;

				rescolnode->typeId = rescoltype;
				rescolnode->typeMod = rescoltypmod;
				rescolnode->collation = rescolcoll;
				rescolnode->location = bestlocation;
				restle = makeTargetEntry((Expr *) rescolnode,
										 0, /* no need to set resno */
										 NULL,
										 false);
				*targetlist = lappend(*targetlist, restle);
			}
		}

		return (Node *) op;
	}
}

/*
 * Process the outputs of the non-recursive term of a recursive union
 * to set up the parent CTE's columns
 */
static void
determineRecursiveColTypes(ParseState *pstate, Node *larg, List *nrtargetlist)
{
	Node	   *node;
	int			leftmostRTI;
	Query	   *leftmostQuery;
	List	   *targetList;
	ListCell   *left_tlist;
	ListCell   *nrtl;
	int			next_resno;

	/*
	 * Find leftmost leaf SELECT
	 */
	node = larg;
	while (node && IsA(node, SetOperationStmt))
		node = ((SetOperationStmt *) node)->larg;
	Assert(node && IsA(node, RangeTblRef));
	leftmostRTI = ((RangeTblRef *) node)->rtindex;
	leftmostQuery = rt_fetch(leftmostRTI, pstate->p_rtable)->subquery;
	Assert(leftmostQuery != NULL);

	/*
	 * Generate dummy targetlist using column names of leftmost select and
	 * dummy result expressions of the non-recursive term.
	 */
	targetList = NIL;
	next_resno = 1;

	forboth(nrtl, nrtargetlist, left_tlist, leftmostQuery->targetList)
	{
		TargetEntry *nrtle = (TargetEntry *) lfirst(nrtl);
		TargetEntry *lefttle = (TargetEntry *) lfirst(left_tlist);
		char	   *colName;
		TargetEntry *tle;

		Assert(!lefttle->resjunk);
		colName = pstrdup(lefttle->resname);
		tle = makeTargetEntry(nrtle->expr,
							  next_resno++,
							  colName,
							  false);
		targetList = lappend(targetList, tle);
	}

	/* Now build CTE's output column info using dummy targetlist */
	analyzeCTETargetList(pstate, pstate->p_parent_cte, targetList);
}


/*
 * transformReturnStmt -
 *	  transforms a return statement
 */
static Query *
transformReturnStmt(ParseState *pstate, ReturnStmt *stmt)
{
	Query	   *qry = makeNode(Query);

	qry->commandType = CMD_SELECT;
	qry->isReturn = true;

	qry->targetList = list_make1(makeTargetEntry((Expr *) transformExpr(pstate, stmt->returnval, EXPR_KIND_SELECT_TARGET),
												 1, NULL, false));

	if (pstate->p_resolve_unknowns)
		resolveTargetListUnknowns(pstate, qry->targetList);
	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;

	assign_query_collations(pstate, qry);

	return qry;
}


/*
 * transformUpdateStmt -
 *	  transforms an update statement
 */
static Query *
transformUpdateStmt(ParseState *pstate, UpdateStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	ParseNamespaceItem *nsitem;
	Node	   *qual;
#ifdef ADB
	ListCell   *tl;
#endif
	qry->commandType = CMD_UPDATE;
	pstate->p_is_insert = false;

	/* process the WITH clause independently of all else */
	if (stmt->withClause)
	{
#ifdef ADB
		/*
		 * For a WITH query that updates a table in the main query and
		 * inserts a row in the same table in the WITH query set flag
		 * to send command ID communication to remote nodes in order to
		 * maintain global data visibility.
		 * For example
		 * CREATE TEMP TABLE tab (id int,val text) DISTRIBUTE BY REPLICATION;
		 * INSERT INTO tab VALUES (1,'p1');
		 * WITH wcte AS (INSERT INTO tab VALUES(42,'new') RETURNING id AS newid)
		 * UPDATE tab SET id = id + newid FROM wcte;
		 * The last query gets translated into the following multi-statement
		 * transaction on the primary datanode
		 * (a) START TRANSACTION ISOLATION LEVEL read committed READ WRITE
		 * (b) INSERT INTO tab (id, val) VALUES ($1, $2) RETURNING id -- (42,'new)'
		 * (c) SELECT id, val, ctid FROM ONLY tab WHERE true
		 * (d) UPDATE ONLY tab tab SET id = $1 WHERE (tab.ctid = $3) -- (43,(0,1)]
		 * (e) COMMIT TRANSACTION
		 * The command id of the select in step (c), should be such that
		 * it does not see the insert of step (b)
		 */
		if (IsCnMaster())
		{
			foreach(tl, stmt->withClause->ctes)
			{
				CommonTableExpr *cte = (CommonTableExpr *) lfirst(tl);
				if (IsA(cte->ctequery, InsertStmt))
				{
					qry->has_to_save_cmd_id = true;
					SetSendCommandId(true);
					break;
				}
			}
		}
#endif
		qry->hasRecursive = stmt->withClause->recursive;
		qry->cteList = transformWithClause(pstate, stmt->withClause);
		qry->hasModifyingCTE = pstate->p_hasModifyingCTE;
	}

	qry->resultRelation = setTargetTable(pstate, stmt->relation,
										 stmt->relation->inh,
										 true,
										 ACL_UPDATE);
	nsitem = pstate->p_target_nsitem;

	/* subqueries in FROM cannot access the result relation */
	nsitem->p_lateral_only = true;
	nsitem->p_lateral_ok = false;

	/*
	 * the FROM clause is non-standard SQL syntax. We used to be able to do
	 * this with REPLACE in POSTQUEL so we keep the feature.
	 */
	transformFromClause(pstate, stmt->fromClause);

	/* remaining clauses can reference the result relation normally */
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	qual = transformWhereClause(pstate, stmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");

	qry->returningList = transformReturningList(pstate, stmt->returningList);

	/*
	 * Now we are done with SELECT-like processing, and can get on with
	 * transforming the target list to match the UPDATE target columns.
	 */
	qry->targetList = transformUpdateTargetList(pstate, stmt->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformUpdateTargetList -
 *	handle SET clause in UPDATE/INSERT ... ON CONFLICT UPDATE
 */
static List *
transformUpdateTargetList(ParseState *pstate, List *origTlist)
{
	List	   *tlist = NIL;
	RangeTblEntry *target_rte;
	ListCell   *orig_tl;
	ListCell   *tl;

	tlist = transformTargetList(pstate, origTlist,
								EXPR_KIND_UPDATE_SOURCE);

	/* Prepare to assign non-conflicting resnos to resjunk attributes */
	if (pstate->p_next_resno <= RelationGetNumberOfAttributes(pstate->p_target_relation))
		pstate->p_next_resno = RelationGetNumberOfAttributes(pstate->p_target_relation) + 1;

	/* Prepare non-junk columns for assignment to target table */
	target_rte = pstate->p_target_nsitem->p_rte;
	orig_tl = list_head(origTlist);

	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		ResTarget  *origTarget;
		int			attrno;

		if (tle->resjunk)
		{
			/*
			 * Resjunk nodes need no additional processing, but be sure they
			 * have resnos that do not match any target columns; else rewriter
			 * or planner might get confused.  They don't need a resname
			 * either.
			 */
			tle->resno = (AttrNumber) pstate->p_next_resno++;
			tle->resname = NULL;
			continue;
		}
		if (orig_tl == NULL)
			elog(ERROR, "UPDATE target count mismatch --- internal error");
		origTarget = lfirst_node(ResTarget, orig_tl);

		attrno = attnameAttNum(pstate->p_target_relation,
							   origTarget->name, true);
		if (attrno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							origTarget->name,
							RelationGetRelationName(pstate->p_target_relation)),
					 parser_errposition(pstate, origTarget->location)));

		updateTargetListEntry(pstate, tle, origTarget->name,
							  attrno,
							  origTarget->indirection,
							  origTarget->location);

		/* Mark the target column as requiring update permissions */
		target_rte->updatedCols = bms_add_member(target_rte->updatedCols,
												 attrno - FirstLowInvalidHeapAttributeNumber);

#ifdef ADB
		if (IsA(tle->expr, SetToDefault))
			target_rte->defaultCols = bms_add_member(target_rte->defaultCols,
													 attrno - FirstLowInvalidHeapAttributeNumber);
#endif /* ADB */

		orig_tl = lnext(origTlist, orig_tl);
	}
	if (orig_tl != NULL)
		elog(ERROR, "UPDATE target count mismatch --- internal error");

	return tlist;
}

/*
 * transformReturningList -
 *	handle a RETURNING clause in INSERT/UPDATE/DELETE
 */
static List *
transformReturningList(ParseState *pstate, List *returningList)
{
	List	   *rlist;
	int			save_next_resno;

	if (returningList == NIL)
		return NIL;				/* nothing to do */

	/*
	 * We need to assign resnos starting at one in the RETURNING list. Save
	 * and restore the main tlist's value of p_next_resno, just in case
	 * someone looks at it later (probably won't happen).
	 */
	save_next_resno = pstate->p_next_resno;
	pstate->p_next_resno = 1;

	/* transform RETURNING identically to a SELECT targetlist */
	rlist = transformTargetList(pstate, returningList, EXPR_KIND_RETURNING);

	/*
	 * Complain if the nonempty tlist expanded to nothing (which is possible
	 * if it contains only a star-expansion of a zero-column table).  If we
	 * allow this, the parsed Query will look like it didn't have RETURNING,
	 * with results that would probably surprise the user.
	 */
	if (rlist == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RETURNING must have at least one column"),
				 parser_errposition(pstate,
									exprLocation(linitial(returningList)))));

	/* mark column origins */
	markTargetListOrigins(pstate, rlist);

	/* resolve any still-unresolved output columns as being type text */
	if (pstate->p_resolve_unknowns)
		resolveTargetListUnknowns(pstate, rlist);

	/* restore state */
	pstate->p_next_resno = save_next_resno;

	return rlist;
}


/*
 * transformPLAssignStmt -
 *	  transform a PL/pgSQL assignment statement
 *
 * If there is no opt_indirection, the transformed statement looks like
 * "SELECT a_expr ...", except the expression has been cast to the type of
 * the target.  With indirection, it's still a SELECT, but the expression will
 * incorporate FieldStore and/or assignment SubscriptingRef nodes to compute a
 * new value for a container-type variable represented by the target.  The
 * expression references the target as the container source.
 */
static Query *
transformPLAssignStmt(ParseState *pstate, PLAssignStmt *stmt)
{
	Query	   *qry = makeNode(Query);
	ColumnRef  *cref = makeNode(ColumnRef);
	List	   *indirection = stmt->indirection;
	int			nnames = stmt->nnames;
	SelectStmt *sstmt = stmt->val;
	Node	   *target;
	Oid			targettype;
	int32		targettypmod;
	Oid			targetcollation;
	List	   *tlist;
	TargetEntry *tle;
	Oid			type_id;
	Node	   *qual;
	ListCell   *l;

	/*
	 * First, construct a ColumnRef for the target variable.  If the target
	 * has more than one dotted name, we have to pull the extra names out of
	 * the indirection list.
	 */
	cref->fields = list_make1(makeString(stmt->name));
	cref->location = stmt->location;
	if (nnames > 1)
	{
		/* avoid munging the raw parsetree */
		indirection = list_copy(indirection);
		while (--nnames > 0 && indirection != NIL)
		{
			Node	   *ind = (Node *) linitial(indirection);

			if (!IsA(ind, String))
				elog(ERROR, "invalid name count in PLAssignStmt");
			cref->fields = lappend(cref->fields, ind);
			indirection = list_delete_first(indirection);
		}
	}

	/*
	 * Transform the target reference.  Typically we will get back a Param
	 * node, but there's no reason to be too picky about its type.
	 */
	target = transformExpr(pstate, (Node *) cref,
						   EXPR_KIND_UPDATE_TARGET);
	targettype = exprType(target);
	targettypmod = exprTypmod(target);
	targetcollation = exprCollation(target);

	/*
	 * The rest mostly matches transformSelectStmt, except that we needn't
	 * consider WITH or INTO, and we build a targetlist our own way.
	 */
	qry->commandType = CMD_SELECT;
	pstate->p_is_insert = false;

	/* make FOR UPDATE/FOR SHARE info available to addRangeTableEntry */
	pstate->p_locking_clause = sstmt->lockingClause;

	/* make WINDOW info available for window functions, too */
	pstate->p_windowdefs = sstmt->windowClause;

	/* process the FROM clause */
	transformFromClause(pstate, sstmt->fromClause);

	/* initially transform the targetlist as if in SELECT */
	tlist = transformTargetList(pstate, sstmt->targetList,
								EXPR_KIND_SELECT_TARGET);

	/* we should have exactly one targetlist item */
	if (list_length(tlist) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg_plural("assignment source returned %d column",
							   "assignment source returned %d columns",
							   list_length(tlist),
							   list_length(tlist))));

	tle = linitial_node(TargetEntry, tlist);

	/*
	 * This next bit is similar to transformAssignedExpr; the key difference
	 * is we use COERCION_PLPGSQL not COERCION_ASSIGNMENT.
	 */
	type_id = exprType((Node *) tle->expr);

	pstate->p_expr_kind = EXPR_KIND_UPDATE_TARGET;

	if (indirection)
	{
		tle->expr = (Expr *)
			transformAssignmentIndirection(pstate,
										   target,
										   stmt->name,
										   false,
										   targettype,
										   targettypmod,
										   targetcollation,
										   indirection,
										   list_head(indirection),
										   (Node *) tle->expr,
										   COERCION_PLPGSQL,
										   exprLocation(target));
	}
	else if (targettype != type_id &&
			 (targettype == RECORDOID || ISCOMPLEX(targettype)) &&
			 (type_id == RECORDOID || ISCOMPLEX(type_id)))
	{
		/*
		 * Hack: do not let coerce_to_target_type() deal with inconsistent
		 * composite types.  Just pass the expression result through as-is,
		 * and let the PL/pgSQL executor do the conversion its way.  This is
		 * rather bogus, but it's needed for backwards compatibility.
		 */
	}
	else
	{
		/*
		 * For normal non-qualified target column, do type checking and
		 * coercion.
		 */
		Node	   *orig_expr = (Node *) tle->expr;

		tle->expr = (Expr *)
			coerce_to_target_type(pstate,
								  orig_expr, type_id,
								  targettype, targettypmod,
								  COERCION_PLPGSQL,
								  COERCE_IMPLICIT_CAST,
								  -1);
		/* With COERCION_PLPGSQL, this error is probably unreachable */
		if (tle->expr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("variable \"%s\" is of type %s"
							" but expression is of type %s",
							stmt->name,
							format_type_be(targettype),
							format_type_be(type_id)),
					 errhint("You will need to rewrite or cast the expression."),
					 parser_errposition(pstate, exprLocation(orig_expr))));
	}

	pstate->p_expr_kind = EXPR_KIND_NONE;

	qry->targetList = list_make1(tle);

	/* transform WHERE */
	qual = transformWhereClause(pstate, sstmt->whereClause,
								EXPR_KIND_WHERE, "WHERE");

	/* initial processing of HAVING clause is much like WHERE clause */
	qry->havingQual = transformWhereClause(pstate, sstmt->havingClause,
										   EXPR_KIND_HAVING, "HAVING");

	/*
	 * Transform sorting/grouping stuff.  Do ORDER BY first because both
	 * transformGroupClause and transformDistinctClause need the results. Note
	 * that these functions can also change the targetList, so it's passed to
	 * them by reference.
	 */
	qry->sortClause = transformSortClause(pstate,
										  sstmt->sortClause,
										  &qry->targetList,
										  EXPR_KIND_ORDER_BY,
										  false /* allow SQL92 rules */ );

	qry->groupClause = transformGroupClause(pstate,
											sstmt->groupClause,
											&qry->groupingSets,
											&qry->targetList,
											qry->sortClause,
											EXPR_KIND_GROUP_BY,
											false /* allow SQL92 rules */ );

	if (sstmt->distinctClause == NIL)
	{
		qry->distinctClause = NIL;
		qry->hasDistinctOn = false;
	}
	else if (linitial(sstmt->distinctClause) == NULL)
	{
		/* We had SELECT DISTINCT */
		qry->distinctClause = transformDistinctClause(pstate,
													  &qry->targetList,
													  qry->sortClause,
													  false);
		qry->hasDistinctOn = false;
	}
	else
	{
		/* We had SELECT DISTINCT ON */
		qry->distinctClause = transformDistinctOnClause(pstate,
														sstmt->distinctClause,
														&qry->targetList,
														qry->sortClause);
		qry->hasDistinctOn = true;
	}

	/* transform LIMIT */
	qry->limitOffset = transformLimitClause(pstate, sstmt->limitOffset,
											EXPR_KIND_OFFSET, "OFFSET",
											sstmt->limitOption);
	qry->limitCount = transformLimitClause(pstate, sstmt->limitCount,
										   EXPR_KIND_LIMIT, "LIMIT",
										   sstmt->limitOption);
	qry->limitOption = sstmt->limitOption;

	/* transform window clauses after we have seen all window functions */
	qry->windowClause = transformWindowDefinitions(pstate,
												   pstate->p_windowdefs,
												   &qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;

	foreach(l, sstmt->lockingClause)
	{
		transformLockingClause(pstate, qry,
							   (LockingClause *) lfirst(l), false);
	}

	assign_query_collations(pstate, qry);

	/* this must be done after collations, for reliable comparison of exprs */
	if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
		parseCheckAggregates(pstate, qry);

	return qry;
}


/*
 * transformDeclareCursorStmt -
 *	transform a DECLARE CURSOR Statement
 *
 * DECLARE CURSOR is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.  We used to postpone that until execution, but it's really necessary
 * to do it during the normal parse analysis phase to ensure that side effects
 * of parser hooks happen at the expected time.
 */
static Query *
transformDeclareCursorStmt(ParseState *pstate, DeclareCursorStmt *stmt)
{
	Query	   *result;
	Query	   *query;

	if ((stmt->options & CURSOR_OPT_SCROLL) &&
		(stmt->options & CURSOR_OPT_NO_SCROLL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
		/* translator: %s is a SQL keyword */
				 errmsg("cannot specify both %s and %s",
						"SCROLL", "NO SCROLL")));

	if ((stmt->options & CURSOR_OPT_ASENSITIVE) &&
		(stmt->options & CURSOR_OPT_INSENSITIVE))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
		/* translator: %s is a SQL keyword */
				 errmsg("cannot specify both %s and %s",
						"ASENSITIVE", "INSENSITIVE")));

	/* Transform contained query, not allowing SELECT INTO */
	query = transformStmt(pstate, stmt->query);
	stmt->query = (Node *) query;

	/* Grammar should not have allowed anything but SELECT */
	if (!IsA(query, Query) ||
		query->commandType != CMD_SELECT)
		elog(ERROR, "unexpected non-SELECT command in DECLARE CURSOR");

	/*
	 * We also disallow data-modifying WITH in a cursor.  (This could be
	 * allowed, but the semantics of when the updates occur might be
	 * surprising.)
	 */
	if (query->hasModifyingCTE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("DECLARE CURSOR must not contain data-modifying statements in WITH")));

	/* FOR UPDATE and WITH HOLD are not compatible */
	if (query->rowMarks != NIL && (stmt->options & CURSOR_OPT_HOLD))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("DECLARE CURSOR WITH HOLD ... %s is not supported",
						LCS_asString(((RowMarkClause *)
									  linitial(query->rowMarks))->strength)),
				 errdetail("Holdable cursors must be READ ONLY.")));

	/* FOR UPDATE and SCROLL are not compatible */
	if (query->rowMarks != NIL && (stmt->options & CURSOR_OPT_SCROLL))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("DECLARE SCROLL CURSOR ... %s is not supported",
						LCS_asString(((RowMarkClause *)
									  linitial(query->rowMarks))->strength)),
				 errdetail("Scrollable cursors must be READ ONLY.")));

	/* FOR UPDATE and INSENSITIVE are not compatible */
	if (query->rowMarks != NIL && (stmt->options & CURSOR_OPT_INSENSITIVE))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("DECLARE INSENSITIVE CURSOR ... %s is not valid",
						LCS_asString(((RowMarkClause *)
									  linitial(query->rowMarks))->strength)),
				 errdetail("Insensitive cursors must be READ ONLY.")));

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

/*
 * transformExplainStmt -
 *	transform an EXPLAIN Statement
 *
 * EXPLAIN is like other utility statements in that we emit it as a
 * CMD_UTILITY Query node; however, we must first transform the contained
 * query.  We used to postpone that until execution, but it's really necessary
 * to do it during the normal parse analysis phase to ensure that side effects
 * of parser hooks happen at the expected time.
 */
static Query *
transformExplainStmt(ParseState *pstate, ExplainStmt *stmt)
{
	Query	   *result;

	/* transform contained query, allowing SELECT INTO */
	stmt->query = (Node *) transformOptionalSelectInto(pstate, stmt->query);
#ifdef ADB
	((Query *)(stmt->query))->in_explain = true;
#endif

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}


/*
 * transformCreateTableAsStmt -
 *	transform a CREATE TABLE AS, SELECT ... INTO, or CREATE MATERIALIZED VIEW
 *	Statement
 *
 * As with DECLARE CURSOR and EXPLAIN, transform the contained statement now.
 */
static Query *
transformCreateTableAsStmt(ParseState *pstate, CreateTableAsStmt *stmt)
{
	Query	   *result;
	Query	   *query;

	/* transform contained query, not allowing SELECT INTO */
	query = transformStmt(pstate, stmt->query);
	stmt->query = (Node *) query;

	/* additional work needed for CREATE MATERIALIZED VIEW */
	if (stmt->objtype == OBJECT_MATVIEW)
	{
		/*
		 * Prohibit a data-modifying CTE in the query used to create a
		 * materialized view. It's not sufficiently clear what the user would
		 * want to happen if the MV is refreshed or incrementally maintained.
		 */
		if (query->hasModifyingCTE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views must not use data-modifying statements in WITH")));

		/*
		 * Check whether any temporary database objects are used in the
		 * creation query. It would be hard to refresh data or incrementally
		 * maintain it if a source disappeared.
		 */
		if (isQueryUsingTempRelation(query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views must not use temporary tables or views")));

		/*
		 * A materialized view would either need to save parameters for use in
		 * maintaining/loading the data or prohibit them entirely.  The latter
		 * seems safer and more sane.
		 */
		if (query_contains_extern_params(query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views may not be defined using bound parameters")));

		/*
		 * For now, we disallow unlogged materialized views, because it seems
		 * like a bad idea for them to just go to empty after a crash. (If we
		 * could mark them as unpopulated, that would be better, but that
		 * requires catalog changes which crash recovery can't presently
		 * handle.)
		 */
		if (stmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("materialized views cannot be unlogged")));

		/*
		 * At runtime, we'll need a copy of the parsed-but-not-rewritten Query
		 * for purposes of creating the view's ON SELECT rule.  We stash that
		 * in the IntoClause because that's where intorel_startup() can
		 * conveniently get it from.
		 */
		stmt->into->viewQuery = (Node *) copyObject(query);
	}

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

#ifdef ADB
/*
 * transformExecDirectStmt -
 *	transform an EXECUTE DIRECT Statement
 *
 * Handling is depends if we should execute on nodes or on Coordinator.
 * To execute on nodes we return CMD_UTILITY query having one T_RemoteQuery node
 * with the inner statement as a sql_command.
 * If statement is to run on Coordinator we should parse inner statement and
 * analyze resulting query tree.
 */
static Query *
transformExecDirectStmt(ParseState *pstate, ExecDirectStmt *stmt)
{
	Query		*result = makeNode(Query);
	char		*query = stmt->query;
	List		*nodelist = stmt->node_names;
	RemoteQuery	*step = makeNode(RemoteQuery);
	bool		is_local = false;
	List		*raw_parsetree_list;
	char		*nodename;
	Oid			nodeoid;
	char		nodetype;

	/* Support not available on Datanodes */
	if (IS_PGXC_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot be executed on a Datanode")));

	if (list_length(nodelist) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Support for EXECUTE DIRECT on multiple nodes is not available yet")));

	Assert(list_length(nodelist) == 1);
	Assert(IS_PGXC_COORDINATOR);

	/* There is a single element here */
	nodename = strVal(linitial(nodelist));
	nodeoid = get_pgxc_nodeoid(nodename);

	if (!OidIsValid(nodeoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						nodename)));

	/* Get node type and index */
	nodetype = get_pgxc_nodetype(nodeoid);

	/* Check if node is requested is the self-node or not */
	if (nodetype == PGXC_NODE_COORDINATOR && nodeoid == PGXCNodeOid)
		is_local = true;

	/* Transform the query into a raw parse list */
	raw_parsetree_list = pg_parse_query(query);

	/* EXECUTE DIRECT can just be executed with a single query */
	if (list_length(raw_parsetree_list) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot execute multiple queries")));

	/*
	 * Analyze the Raw parse tree
	 * EXECUTE DIRECT is restricted to one-step usage
	 */
	result = parse_analyze(linitial(raw_parsetree_list), query, NULL, 0, pstate->p_queryEnv);

	/* Needed by planner */
	result->sql_statement = pstrdup(query);

	/* Default list of parameters to set */
	step->sql_statement = NULL;
	step->exec_nodes = makeNode(ExecNodes);
	step->combine_type = COMBINE_TYPE_NONE;
	step->read_only = true;
	step->force_autocommit = false;
	step->cursor = NULL;

	/* This is needed by executor */
	step->sql_statement = pstrdup(query);
	if (nodetype == PGXC_NODE_COORDINATOR)
		step->exec_type = EXEC_ON_COORDS;
	else
		step->exec_type = EXEC_ON_DATANODES;

	step->base_tlist = NIL;

	/* Change the list of nodes that will be executed for the query and others */
	step->force_autocommit = false;
	step->combine_type = COMBINE_TYPE_SAME;
	step->read_only = true;
	step->exec_direct_type = EXEC_DIRECT_NONE;

	/* Set up EXECUTE DIRECT flag */
	if (is_local)
	{
		if (result->commandType == CMD_UTILITY)
			step->exec_direct_type = EXEC_DIRECT_LOCAL_UTILITY;
		else
			step->exec_direct_type = EXEC_DIRECT_LOCAL;
	}
	else
	{
		switch(result->commandType)
		{
			case CMD_UTILITY:
				step->exec_direct_type = EXEC_DIRECT_UTILITY;
				break;
			case CMD_SELECT:
				step->exec_direct_type = EXEC_DIRECT_SELECT;
				break;
			case CMD_INSERT:
				step->exec_direct_type = EXEC_DIRECT_INSERT;
				break;
			case CMD_UPDATE:
				step->exec_direct_type = EXEC_DIRECT_UPDATE;
				break;
			case CMD_DELETE:
				step->exec_direct_type = EXEC_DIRECT_DELETE;
				break;
			default:
				Assert(0);
		}
	}

	/*
	 * Features not yet supported
	 * DML can be launched without errors but this could compromise data
	 * consistency, so block it.
	 */
	if (!xc_maintenance_mode && (step->exec_direct_type == EXEC_DIRECT_DELETE
								 || step->exec_direct_type == EXEC_DIRECT_UPDATE
								 || step->exec_direct_type == EXEC_DIRECT_INSERT))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot execute DML queries")));
	else if (step->exec_direct_type == EXEC_DIRECT_UTILITY &&
			 !IsExecDirectUtilityStmt(result->utilityStmt) && !xc_maintenance_mode)
	{
		/* In case this statement is an utility, check if it is authorized */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot execute this utility query")));
	}
	else if (step->exec_direct_type == EXEC_DIRECT_LOCAL_UTILITY && !xc_maintenance_mode)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("EXECUTE DIRECT cannot execute locally this utility query")));
	}

	/* Build Execute Node list, there is a unique node for the time being */
	step->exec_nodes->nodeids = lappend_oid(step->exec_nodes->nodeids, nodeoid);

	/* Associate newly-created RemoteQuery node to the returned Query result */
	result->is_local = is_local;
	if (!is_local)
		result->utilityStmt = (Node *) step;

	return result;
}

/*
 * Check if given node is authorized to go through EXECUTE DURECT
 */
static bool
IsExecDirectUtilityStmt(Node *node)
{
	bool res = true;

	if (!node)
		return res;

	switch(nodeTag(node))
	{
		/*
		 * CREATE/DROP TABLESPACE are authorized to control
		 * tablespace at single node level.
		 */
		case T_CreateTableSpaceStmt:
		case T_DropTableSpaceStmt:
			res = true;
			break;
		default:
			res = false;
			break;
	}

	return res;
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * contain any relation who is the parent of
 * the passed relation
 */
static bool
is_relation_child(RangeTblEntry *child_rte, List *rtable)
{
	ListCell *item;

	if (child_rte == NULL || rtable == NULL)
		return false;

	if (child_rte->rtekind != RTE_RELATION)
		return false;

	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (is_rel_child_of_rel(child_rte, rte))
				return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			return is_relation_child(child_rte, rte->subquery->rtable);
		}
	}
	return false;
}

/*
 * Returns whether the passed RTEs have a parent child relationship
 */
static bool
is_rel_child_of_rel(RangeTblEntry *child_rte, RangeTblEntry *parent_rte)
{
	Oid		parentOID;
	bool		res;
	Relation	relation;
	SysScanDesc	scan;
	ScanKeyData	key[1];
	HeapTuple	inheritsTuple;
	Oid		inhrelid;

	/* Does parent RT entry allow inheritance? */
	if (!parent_rte->inh)
		return false;

	/* Ignore any already-expanded UNION ALL nodes */
	if (parent_rte->rtekind != RTE_RELATION)
		return false;

	/* Fast path for common case of childless table */
	parentOID = parent_rte->relid;
	if (!has_subclass(parentOID))
		return false;

	/* Assume we did not find any match */
	res = false;

	/* Scan pg_inherits and get all the subclass OIDs one by one. */
	relation = table_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&key[0], Anum_pg_inherits_inhparent, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parentOID));
	scan = systable_beginscan(relation, InheritsParentIndexId, true, NULL, 1, key);

	while ((inheritsTuple = systable_getnext(scan)) != NULL)
	{
		inhrelid = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;

		/* Did we find the Oid of the passed RTE in one of the children? */
		if (child_rte->relid == inhrelid)
		{
			res = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(relation, AccessShareLock);
	return res;
}

#endif
/*
 * transform a CallStmt
 */
static Query *
transformCallStmt(ParseState *pstate, CallStmt *stmt)
{
	List	   *targs;
	ListCell   *lc;
	Node	   *node;
	FuncExpr   *fexpr;
	HeapTuple	proctup;
	Datum		proargmodes;
	bool		isNull;
	List	   *outargs = NIL;
	Query	   *result;

	/*
	 * First, do standard parse analysis on the procedure call and its
	 * arguments, allowing us to identify the called procedure.
	 */
	targs = NIL;
	foreach(lc, stmt->funccall->args)
	{
		targs = lappend(targs, transformExpr(pstate,
											 (Node *) lfirst(lc),
											 EXPR_KIND_CALL_ARGUMENT));
	}

	node = ParseFuncOrColumn(pstate,
							 stmt->funccall->funcname,
							 targs,
							 pstate->p_last_srf,
							 stmt->funccall,
							 true,
							 stmt->funccall->location);

	assign_expr_collations(pstate, node);

	fexpr = castNode(FuncExpr, node);

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", fexpr->funcid);

	/*
	 * Expand the argument list to deal with named-argument notation and
	 * default arguments.  For ordinary FuncExprs this'd be done during
	 * planning, but a CallStmt doesn't go through planning, and there seems
	 * no good reason not to do it here.
	 */
	fexpr->args = expand_function_arguments(fexpr->args,
											true,
											fexpr->funcresulttype,
											proctup);

	/* Fetch proargmodes; if it's null, there are no output args */
	proargmodes = SysCacheGetAttr(PROCOID, proctup,
								  Anum_pg_proc_proargmodes,
								  &isNull);
	if (!isNull)
	{
		/*
		 * Split the list into input arguments in fexpr->args and output
		 * arguments in stmt->outargs.  INOUT arguments appear in both lists.
		 */
		ArrayType  *arr;
		int			numargs;
		char	   *argmodes;
		List	   *inargs;
		int			i;

		arr = DatumGetArrayTypeP(proargmodes);	/* ensure not toasted */
		numargs = list_length(fexpr->args);
		if (ARR_NDIM(arr) != 1 ||
			ARR_DIMS(arr)[0] != numargs ||
			ARR_HASNULL(arr) ||
			ARR_ELEMTYPE(arr) != CHAROID)
			elog(ERROR, "proargmodes is not a 1-D char array of length %d or it contains nulls",
				 numargs);
		argmodes = (char *) ARR_DATA_PTR(arr);

		inargs = NIL;
		i = 0;
		foreach(lc, fexpr->args)
		{
			Node	   *n = lfirst(lc);

			switch (argmodes[i])
			{
				case PROARGMODE_IN:
				case PROARGMODE_VARIADIC:
					inargs = lappend(inargs, n);
					break;
				case PROARGMODE_OUT:
					outargs = lappend(outargs, n);
					break;
				case PROARGMODE_INOUT:
					inargs = lappend(inargs, n);
					outargs = lappend(outargs, copyObject(n));
					break;
				default:
					/* note we don't support PROARGMODE_TABLE */
					elog(ERROR, "invalid argmode %c for procedure",
						 argmodes[i]);
					break;
			}
			i++;
		}
		fexpr->args = inargs;
	}

	stmt->funcexpr = fexpr;
	stmt->outargs = outargs;

	ReleaseSysCache(proctup);

	/* represent the command as a utility Query */
	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) stmt;

	return result;
}

/*
 * Produce a string representation of a LockClauseStrength value.
 * This should only be applied to valid values (not LCS_NONE).
 */
const char *
LCS_asString(LockClauseStrength strength)
{
	switch (strength)
	{
		case LCS_NONE:
			Assert(false);
			break;
		case LCS_FORKEYSHARE:
			return "FOR KEY SHARE";
		case LCS_FORSHARE:
			return "FOR SHARE";
		case LCS_FORNOKEYUPDATE:
			return "FOR NO KEY UPDATE";
		case LCS_FORUPDATE:
			return "FOR UPDATE";
	}
	return "FOR some";			/* shouldn't happen */
}

/*
 * Check for features that are not supported with FOR [KEY] UPDATE/SHARE.
 *
 * exported so planner can check again after rewriting, query pullup, etc
 */
void
CheckSelectLocking(Query *qry, LockClauseStrength strength)
{
	Assert(strength != LCS_NONE);	/* else caller error */

	if (qry->setOperations)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with UNION/INTERSECT/EXCEPT",
						LCS_asString(strength))));
	if (qry->distinctClause != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with DISTINCT clause",
						LCS_asString(strength))));
	if (qry->groupClause != NIL || qry->groupingSets != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with GROUP BY clause",
						LCS_asString(strength))));
	if (qry->havingQual != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with HAVING clause",
						LCS_asString(strength))));
	if (qry->hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with aggregate functions",
						LCS_asString(strength))));
	if (qry->hasWindowFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with window functions",
						LCS_asString(strength))));
	if (qry->hasTargetSRFs)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		/*------
		  translator: %s is a SQL row locking clause such as FOR UPDATE */
				 errmsg("%s is not allowed with set-returning functions in the target list",
						LCS_asString(strength))));
}

/*
 * Transform a FOR [KEY] UPDATE/SHARE clause
 *
 * This basically involves replacing names by integer relids.
 *
 * NB: if you need to change this, see also markQueryForLocking()
 * in rewriteHandler.c, and isLockedRefname() in parse_relation.c.
 */
static void
transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc,
					   bool pushedDown)
{
	List	   *lockedRels = lc->lockedRels;
	ListCell   *l;
	ListCell   *rt;
	Index		i;
	LockingClause *allrels;

	CheckSelectLocking(qry, lc->strength);

	/* make a clause we can pass down to subqueries to select all rels */
	allrels = makeNode(LockingClause);
	allrels->lockedRels = NIL;	/* indicates all rels */
	allrels->strength = lc->strength;
	allrels->waitPolicy = lc->waitPolicy;

	if (lockedRels == NIL)
	{
		/* all regular tables used in query */
		i = 0;
		foreach(rt, qry->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

			++i;
			switch (rte->rtekind)
			{
				case RTE_RELATION:
					applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
									   pushedDown);
					rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
					break;
				case RTE_SUBQUERY:
					applyLockingClause(qry, i, lc->strength, lc->waitPolicy,
									   pushedDown);

					/*
					 * FOR UPDATE/SHARE of subquery is propagated to all of
					 * subquery's rels, too.  We could do this later (based on
					 * the marking of the subquery RTE) but it is convenient
					 * to have local knowledge in each query level about which
					 * rels need to be opened with RowShareLock.
					 */
					transformLockingClause(pstate, rte->subquery,
										   allrels, true);
					break;
				default:
					/* ignore JOIN, SPECIAL, FUNCTION, VALUES, CTE RTEs */
					break;
			}
		}
	}
	else
	{
		/* just the named tables */
		foreach(l, lockedRels)
		{
			RangeVar   *thisrel = (RangeVar *) lfirst(l);

			/* For simplicity we insist on unqualified alias names here */
			if (thisrel->catalogname || thisrel->schemaname)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				/*------
				  translator: %s is a SQL row locking clause such as FOR UPDATE */
						 errmsg("%s must specify unqualified relation names",
								LCS_asString(lc->strength)),
						 parser_errposition(pstate, thisrel->location)));

			i = 0;
			foreach(rt, qry->rtable)
			{
				RangeTblEntry *rte = (RangeTblEntry *) lfirst(rt);

				++i;
				if (strcmp(rte->eref->aliasname, thisrel->relname) == 0)
				{
					switch (rte->rtekind)
					{
						case RTE_RELATION:
							applyLockingClause(qry, i, lc->strength,
											   lc->waitPolicy, pushedDown);
							rte->requiredPerms |= ACL_SELECT_FOR_UPDATE;
							break;
						case RTE_SUBQUERY:
							applyLockingClause(qry, i, lc->strength,
											   lc->waitPolicy, pushedDown);
							/* see comment above */
							transformLockingClause(pstate, rte->subquery,
												   allrels, true);
							break;
						case RTE_JOIN:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a join",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_FUNCTION:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a function",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_TABLEFUNC:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a table function",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_VALUES:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to VALUES",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_CTE:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a WITH query",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;
						case RTE_NAMEDTUPLESTORE:
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							/*------
							  translator: %s is a SQL row locking clause such as FOR UPDATE */
									 errmsg("%s cannot be applied to a named tuplestore",
											LCS_asString(lc->strength)),
									 parser_errposition(pstate, thisrel->location)));
							break;

							/* Shouldn't be possible to see RTE_RESULT here */

						default:
							elog(ERROR, "unrecognized RTE type: %d",
								 (int) rte->rtekind);
							break;
					}
					break;		/* out of foreach loop */
				}
			}
			if (rt == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_TABLE),
				/*------
				  translator: %s is a SQL row locking clause such as FOR UPDATE */
						 errmsg("relation \"%s\" in %s clause not found in FROM clause",
								thisrel->relname,
								LCS_asString(lc->strength)),
						 parser_errposition(pstate, thisrel->location)));
		}
	}
}

/*
 * Record locking info for a single rangetable item
 */
void
applyLockingClause(Query *qry, Index rtindex,
				   LockClauseStrength strength, LockWaitPolicy waitPolicy,
				   bool pushedDown)
{
	RowMarkClause *rc;

	Assert(strength != LCS_NONE);	/* else caller error */

	/* If it's an explicit clause, make sure hasForUpdate gets set */
	if (!pushedDown)
		qry->hasForUpdate = true;

	/* Check for pre-existing entry for same rtindex */
	if ((rc = get_parse_rowmark(qry, rtindex)) != NULL)
	{
		/*
		 * If the same RTE is specified with more than one locking strength,
		 * use the strongest.  (Reasonable, since you can't take both a shared
		 * and exclusive lock at the same time; it'll end up being exclusive
		 * anyway.)
		 *
		 * Similarly, if the same RTE is specified with more than one lock
		 * wait policy, consider that NOWAIT wins over SKIP LOCKED, which in
		 * turn wins over waiting for the lock (the default).  This is a bit
		 * more debatable but raising an error doesn't seem helpful. (Consider
		 * for instance SELECT FOR UPDATE NOWAIT from a view that internally
		 * contains a plain FOR UPDATE spec.)  Having NOWAIT win over SKIP
		 * LOCKED is reasonable since the former throws an error in case of
		 * coming across a locked tuple, which may be undesirable in some
		 * cases but it seems better than silently returning inconsistent
		 * results.
		 *
		 * And of course pushedDown becomes false if any clause is explicit.
		 */
		rc->strength = Max(rc->strength, strength);
		rc->waitPolicy = Max(rc->waitPolicy, waitPolicy);
		rc->pushedDown &= pushedDown;
		return;
	}

	/* Make a new RowMarkClause */
	rc = makeNode(RowMarkClause);
	rc->rti = rtindex;
	rc->strength = strength;
	rc->waitPolicy = waitPolicy;
	rc->pushedDown = pushedDown;
	qry->rowMarks = lappend(qry->rowMarks, rc);
}

/*
 * Coverage testing for raw_expression_tree_walker().
 *
 * When enabled, we run raw_expression_tree_walker() over every DML statement
 * submitted to parse analysis.  Without this provision, that function is only
 * applied in limited cases involving CTEs, and we don't really want to have
 * to test everything inside as well as outside a CTE.
 */
#ifdef RAW_EXPRESSION_COVERAGE_TEST

static bool
test_raw_expression_coverage(Node *node, void *context)
{
	if (node == NULL)
		return false;
	return raw_expression_tree_walker(node,
									  test_raw_expression_coverage,
									  context);
}

#endif							/* RAW_EXPRESSION_COVERAGE_TEST */

#ifdef ADB_GRAM_ORA
typedef struct JoinExprInfo
{
	Node	   *expr;		/* join clause */
	JoinType	type;		/* */
	Index		lrtindex;
	Index		rrtindex;
	int			location;	/* of ColumnRefJoin location, or -1 */
}JoinExprInfo;

typedef struct GetOraColumnJoinContext
{
	ParseState	   *pstate;
	JoinExprInfo   *info;
}GetOraColumnJoinContext;

typedef struct PullupRelForJoinContext
{
	Node *larg;
	Node *rarg;
}PullupRelForJoinContext;

static bool
have_ora_column_join(Node *node, void *context)
{
	if(node == NULL)
	{
		return false;
	}else if(IsA(node, ColumnRefJoin))
	{
		return true;
	}
	return expression_tree_walker(node, have_ora_column_join, context);
}

static bool
get_ora_column_join_walker(Node *node, GetOraColumnJoinContext *context)
{
	AssertArg(context && context->info && context->pstate);
	if(node == NULL)
	{
		return false;
	}else if(IsA(node, ColumnRefJoin))
	{
		ColumnRefJoin *crj = (ColumnRefJoin*)node;
		JoinExprInfo *info = context->info;
		Assert(crj->var != NULL && IsA(crj->var, Var));
		if(info->rrtindex == 0)
		{
			Assert(info->type == JOIN_INNER);
			info->type = JOIN_LEFT;
			info->rrtindex = crj->var->varno;
			info->location = crj->location;
		}else if(info->rrtindex != crj->var->varno)
		{
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("a predicate may reference only one outer-joined table"),
					parser_errposition(context->pstate, crj->location));
		}
		return false;
	}else if(IsA(node, Var))
	{
		Var *var = (Var*)node;
		JoinExprInfo *info = context->info;
		Assert(var->varno != 0);
		if (info->lrtindex == var->varno ||
			info->rrtindex == var->varno)
		{
			return false;
		}

		if(info->lrtindex == 0)
		{
			info->lrtindex = var->varno;
		}else if(info->rrtindex == 0 &&
				 info->type == JOIN_INNER)
		{
			info->rrtindex = var->varno;
		}else if(info->rrtindex != var->varno &&
				 info->lrtindex != var->varno)
		{
			ereport(ERROR,
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("a predicate may reference only one outer-joined table"),
					parser_errposition(context->pstate, info->location));
		}
		return false;
	}
	return expression_tree_walker(node, get_ora_column_join_walker, context);
}

static JoinExprInfo*
get_ora_column_join(Node *expr, ParseState *pstate)
{
	GetOraColumnJoinContext context;
	JoinExprInfo *jinfo = palloc(sizeof(JoinExprInfo));

	jinfo->expr = expr;
	jinfo->type = JOIN_INNER;
	jinfo->rrtindex = jinfo->lrtindex = 0;
	context.info = jinfo;
	context.pstate = pstate;
	(void)get_ora_column_join_walker(expr, &context);

	return jinfo;
}

static Node*
remove_column_join_expr(Node *node, void *context)
{
	if(node == NULL)
		return NULL;
	else if(IsA(node, ColumnRefJoin))
		return (Node*)((ColumnRefJoin*)node)->var;
	return expression_tree_mutator(node, remove_column_join_expr, context);
}

static bool
combin_pullup_context(PullupRelForJoinContext *dest,
					  PullupRelForJoinContext *src)
{
	bool res = false;
	if(src->larg != NULL)
	{
		Assert(dest->larg == NULL);
		dest->larg = src->larg;
		res = true;
	}
	if(src->rarg != NULL)
	{
		Assert(dest->rarg == NULL);
		dest->rarg = src->rarg;
		res = true;
	}
	return res;
}

static bool
pullup_rel_for_join(Node *node, JoinExprInfo *jinfo,
					ParseState *pstate, PullupRelForJoinContext *context)
{
	AssertArg(node && jinfo && jinfo->expr && pstate);
	AssertArg(jinfo->lrtindex != 0 && jinfo->rrtindex != 0 &&
			  jinfo->lrtindex != jinfo->rrtindex &&
			  (jinfo->type == JOIN_INNER || jinfo->type == JOIN_LEFT));

	if(IsA(node, FromExpr))
	{
		ListCell *lc;
		JoinExpr *join;
		Node *item;
		PullupRelForJoinContext my_context;
		FromExpr *from = (FromExpr*)node;

		foreach (lc, from->fromlist)
		{
			item = lfirst(lc);
			Assert(item);

			my_context.larg = my_context.rarg = NULL;
			if(pullup_rel_for_join(item, jinfo, pstate, &my_context))
			{
				Assert(context->larg == NULL && context->rarg == NULL);
				return true;
			}

			if(combin_pullup_context(context, &my_context))
			{
				from->fromlist = foreach_delete_current(from->fromlist, lc);
				if(context->larg && context->rarg)
					break;
			}
		}

		/* return false when not found all */
		if(context->larg == NULL || context->rarg == NULL)
			return false;

		/* now make JoinExpr */
		join = makeNode(JoinExpr);
		join->jointype = jinfo->type;
		join->larg = context->larg;
		join->rarg = context->rarg;
		join->quals = jinfo->expr;
		from->fromlist = lappend(from->fromlist, join);
		return true;
	}else if(IsA(node, JoinExpr))
	{
		PullupRelForJoinContext my_context;
		JoinExpr *join = (JoinExpr*)node;

		my_context.larg = my_context.rarg = NULL;
		if(pullup_rel_for_join(join->larg, jinfo, pstate, &my_context))
		{
			Assert(context->larg == NULL && context->rarg == NULL);
			return true;
		}
		(void)combin_pullup_context(context, &my_context);

		if(context->larg == NULL || context->rarg == NULL)
		{
			my_context.larg = my_context.rarg = NULL;
			if(pullup_rel_for_join(join->rarg, jinfo, pstate, &my_context))
			{
				Assert(context->larg == NULL && context->rarg == NULL);
				return true;
			}
			(void)combin_pullup_context(context, &my_context);
		}

		if(context->larg != NULL && context->rarg != NULL)
		{
			/*
			 * all table found
			 * combin clause
			 */
			BoolExpr *bexpr;
			if(jinfo->type == JOIN_LEFT
				&& join->jointype != JOIN_LEFT
				&& join->jointype != JOIN_RIGHT)
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					, errmsg("a predicate may reference only one outer-joined table")
					, parser_errposition(pstate, jinfo->location)));
			}

			if(join->quals == NULL)
			{
				bexpr = (BoolExpr*)makeBoolExpr(AND_EXPR, NIL, -1);
				join->quals = (Node*)bexpr;
			}else if(is_andclause(join->quals))
			{
				bexpr = (BoolExpr*)(join->quals);
			}else
			{
				bexpr = (BoolExpr*)makeBoolExpr(AND_EXPR, list_make1(join->quals), -1);
				join->quals = (Node*)bexpr;
			}
			bexpr->args = lappend(bexpr->args, jinfo->expr);
			return true;
		}

		/* release [lr]arg to this join node */
		if(context->larg != NULL)
			context->larg = node;
		else if(context->rarg != NULL)
			context->rarg = node;

		return false;
	}else if(IsA(node, RangeTblRef))
	{
		RangeTblRef *rte = (RangeTblRef*)node;
		if(rte->rtindex == jinfo->lrtindex)
		{
			Assert(context->larg == NULL);
			context->larg = node;
		}else if(rte->rtindex == jinfo->rrtindex)
		{
			Assert(context->rarg == NULL);
			context->rarg = node;
		}
		return false;
	}else
	{
		ereport(ERROR,
			(errmsg("unrecognized node type: %d", (int)nodeTag(node))));
	}
	return false;
}

void check_joinon_column_join(Node *node, ParseState *pstate)
{
	if(node == NULL)
		return;

	switch(nodeTag(node))
	{
	case T_JoinExpr:
		{
			JoinExprInfo *jinfo;
			JoinExpr *join = (JoinExpr*)node;
			Assert(join->larg && join->rarg);
			check_joinon_column_join(join->larg, pstate);
			check_joinon_column_join(join->rarg, pstate);

			if(have_ora_column_join(join->quals, NULL) == false)
				return;

			jinfo = get_ora_column_join(join->quals, pstate);
			Assert(jinfo);

			if(jinfo->type != JOIN_INNER)
			{
				RangeTblRef *rte;
				bool failed = false;
				if(IsA(join->larg, RangeTblRef))
				{
					rte = (RangeTblRef*)(join->larg);
					if(rte->rtindex == jinfo->lrtindex && join->jointype != JOIN_LEFT)
						failed = true;
					if(rte->rtindex == jinfo->rrtindex && join->jointype != JOIN_RIGHT)
						failed = true;
				}
				if(IsA(join->rarg, RangeTblRef))
				{
					rte = (RangeTblRef*)(join->rarg);
					if(rte->rtindex == jinfo->lrtindex && join->jointype != JOIN_RIGHT)
						failed = true;
					if(rte->rtindex == jinfo->rrtindex && join->jointype != JOIN_LEFT)
						failed = true;
				}
				if(failed)
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
						,errmsg("a predicate may reference only on outer-joined table")
						,parser_errposition(pstate, jinfo->location)));
			}
			join->quals = remove_column_join_expr(join->quals, NULL);
		}
		break;
	case T_FromExpr:
		{
			ListCell *lc;
			FromExpr *from = (FromExpr*)node;
			foreach(lc, from->fromlist)
				check_joinon_column_join(lfirst(lc), pstate);
			if(have_ora_column_join(from->quals, NULL))
				from->quals = remove_column_join_expr(from->quals, NULL);
		}
		break;
	case T_RangeTblRef:
		break;
	default:
		elog(ERROR, "unrecognized node type: %d",(int) nodeTag(node));
	}
}

static ParseNamespaceItem*
find_namespace_item_for_rte(List *namespace, int rtindex)
{
	ListCell		   *lc;
	ParseNamespaceItem *pni = NULL;
	Assert(namespace != NIL);

	foreach(lc, namespace)
	{
		pni = lfirst(lc);
		Assert(pni);
		if(pni->p_rtindex == rtindex)
			return pni;
	}
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("can not found namespace item for range table %d", rtindex)));
	return NULL;	/* keep compiler quiet */
}

static void 
analyze_new_join(ParseState *pstate, Node *node,
				 ParseNamespaceItem **top_nsitem,
				 List **namespace)
{
	Assert(pstate && node && top_nsitem);
	if(IsA(node, JoinExpr))
	{
		JoinExpr *j = (JoinExpr*)node;
		if(j->rtindex == 0)
		{
			/* new join expr */
			ParseNamespaceItem *nsitem;
			ParseNamespaceItem *l_nsitem;
			ParseNamespaceItem *r_nsitem;
			ListCell		   *lc;
			List			   *l_namespace,
							   *r_namespace,
							   *my_namespace;
			int				k;
			int				res_colindex;

			Assert(j->jointype == JOIN_INNER || j->jointype == JOIN_LEFT);
			analyze_new_join(pstate, j->larg, &l_nsitem, &l_namespace);
			analyze_new_join(pstate, j->rarg, &r_nsitem, &r_namespace);
			Assert((checkNameSpaceConflicts(pstate, l_namespace, r_namespace), true));

			my_namespace = list_concat(l_namespace, r_namespace);

			nsitem = addSimpleTableEntryForJoin(pstate,
												j->alias,
												l_nsitem,
												r_nsitem,
												j->jointype);

			j->rtindex = nsitem->p_rtindex;

			/*
			 * Now that we know the join RTE's rangetable index, we can fix up the
			 * res_nscolumns data in places where it should contain that.
			 */
			res_colindex = list_length(nsitem->p_rte->eref->colnames);
			for (k = 0; k < res_colindex; k++)
			{
				ParseNamespaceColumn *nscol = nsitem->p_nscolumns + k;

				/* fill in join RTI for merged columns */
				if (nscol->p_varno == 0)
					nscol->p_varno = j->rtindex;
				if (nscol->p_varnosyn == 0)
					nscol->p_varnosyn = j->rtindex;
			}

			/* make a matching link to the JoinExpr for later use */
			for (k = list_length(pstate->p_joinexprs) + 1; k < j->rtindex; k++)
				pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
			pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
			Assert(list_length(pstate->p_joinexprs) == j->rtindex);

			/* inline setNamespaceColumnVisibility(my_namespace, false) */
			foreach(lc, my_namespace)
				((ParseNamespaceItem*)lfirst(lc))->p_cols_visible = false;

			nsitem->p_rel_visible = true;
			nsitem->p_cols_visible = true;
			nsitem->p_lateral_only = false;
			nsitem->p_lateral_ok = true;

			*top_nsitem = nsitem;
			*namespace = lappend(my_namespace, nsitem);
		}else
		{
			*top_nsitem = find_namespace_item_for_rte(pstate->p_namespace, j->rtindex);
			*namespace = list_make1(*top_nsitem);
		}
	}else if(IsA(node, FromExpr))
	{
		ParseNamespaceItem *nsitem;
		List			   *my_namelist;
		ListCell		   *lc;
		FromExpr		   *from_expr = (FromExpr*)node;
		foreach(lc, from_expr->fromlist)
		{
			analyze_new_join(pstate, lfirst(lc), &nsitem, &my_namelist);
			Assert((checkNameSpaceConflicts(pstate, *namespace, my_namelist), true));
			*namespace = list_concat(*namespace, my_namelist);
			list_free(my_namelist);
		}
	}else if(IsA(node, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef*)node;
		*top_nsitem = find_namespace_item_for_rte(pstate->p_namespace, rtr->rtindex);
		*namespace = list_make1(*top_nsitem);
	}else
	{
		ereport(ERROR, (errmsg("unknown node type %d", nodeTag(node))));
	}
}

/*
 * return list of JoinExprInfo
 *
 * t1.id=t2.id(+) and t1.name=t2.name and t1.id>10
 *          |
 *          V
 * t1.id=t2.id(+)
 * t1.name=t2.name
 * t1.id>10
 *          |
 *          V
 * t1.id=t2.id(+) and t1.name=t2.name
 * t1.id>10
 */
static List* get_join_qual_exprs(Node *quals, ParseState *pstate)
{
	ListCell *lc;
	List *result;
	List *qual_list;
	Node *expr;
	JoinExprInfo *jinfo, *jinfo2;
	if(quals == NULL)
		return NIL;

	quals = (Node*)canonicalize_qual((Expr*)quals, false);
	if(is_andclause(quals))
		qual_list = ((BoolExpr*)quals)->args;
	else
		qual_list = list_make1(quals);

	/*
	 * this loop, we get all column join expr clause
	 * and delete from qual_list
	 */
	result = NIL;
	foreach (lc, qual_list)
	{
		expr = lfirst(lc);
		if(have_ora_column_join(expr, NULL) == false)
			continue;

		jinfo = get_ora_column_join(expr, pstate);
		result = lappend(result, jinfo);
		qual_list = foreach_delete_current(qual_list, lc);
	}

	/*
	 * now, we combin exprs
	 */
	while(qual_list)
	{
		expr = linitial(qual_list);
		jinfo2 = get_ora_column_join(expr, pstate);

		foreach(lc, result)
		{
			jinfo = lfirst(lc);
			if(jinfo->type == jinfo2->type
				&& jinfo->lrtindex == jinfo2->lrtindex
				&& jinfo->rrtindex == jinfo2->rrtindex)
			{
				/* same table(s) clause, combin it */
				BoolExpr *bexpr;
				if(is_andclause(jinfo->expr))
				{
					bexpr = (BoolExpr*)(jinfo->expr);
				}else
				{
					bexpr = (BoolExpr*)makeBoolExpr(AND_EXPR, list_make1(jinfo->expr), -1);
					jinfo->expr = (Node*)bexpr;
				}
				bexpr->args = lappend(bexpr->args, jinfo2->expr);
				pfree(jinfo2);
				goto next_qual_list;
			}
		}
		/* not match in result */
		result = lappend(result, jinfo2);
next_qual_list:
		qual_list = list_delete_first(qual_list);
	}

	return result;
}

/*
 *  from t1,t2,t3,t4
 *  where t1.id=t2.id(+)
 *    and t1.id(+)=t3.id
 *    and t1.id=t4.id(+);
 *    and other
 *         |
 *         V
 *  from (t1,t3,t4) left join t2 on t1.id=t2.id
 *  where t1.id(+)=t3.id
 *    and t1.id=t4.id(+)
 *    and other
 *         |
 *         V
 *  from ((t1 left join t3 on t1.id=t3.id),t4) left join t2 on t1.id=t2.id
 *  where t1.id=t4.id(+)
 *    and other
 *         |
 *         V
 *  from ((t1 left join t3 on t1.id=t3.id) left join t4 on t1.id=t4.id) left join t2 on t1.id=t2.id
 *  where other
 */
static Node*
transformFromAndWhere(ParseState *pstate, Node *quals)
{
	List *qual_list,
		 *new_namelist;
	ListCell *lc;
	FromExpr *from;
	JoinExprInfo *jinfo;
	PullupRelForJoinContext context;

	if (pstate->p_joinlist == NIL ||
		have_ora_column_join(quals, NULL) == false)
		return quals;

	if(list_length(pstate->p_joinlist) == 1)
	{
		/* fast process */
		return remove_column_join_expr(quals, NULL);
	}

	qual_list = get_join_qual_exprs(quals, pstate);
	from = makeNode(FromExpr);
	from->fromlist = pstate->p_joinlist;

	foreach (lc, qual_list)
	{
		ListCell *tmp = lc;
		jinfo = lfirst(lc);

		if (jinfo->lrtindex == 0 ||
			jinfo->rrtindex == 0)
		{
			/* keep single table's clause and remove jinfo */
			lfirst(tmp) = remove_column_join_expr(jinfo->expr, NULL);
			pfree(jinfo);
			continue;
		}

		context.larg = context.rarg = NULL;
		jinfo->expr = remove_column_join_expr(jinfo->expr, NULL);
		if(pullup_rel_for_join((Node*)from, jinfo, pstate, &context) == false)
		{
			ereport(ERROR, (errmsg("move filter qual to join filter failed!")));
		}
		qual_list = foreach_delete_current(qual_list, lc);
		pfree(jinfo);
	}

	{
		ParseNamespaceItem *nsitem;
		new_namelist = NIL;
		analyze_new_join(pstate, (Node*)from, &nsitem, &new_namelist);
	}
	pstate->p_namespace = new_namelist;

	pstate->p_joinlist = from->fromlist;
	pfree(from);

	if(qual_list == NIL)
	{
		return NULL;
	}else if(list_length(qual_list) == 1)
	{
		return linitial(qual_list);
	}
	return (Node*)makeBoolExpr(AND_EXPR, qual_list, -1);
}

bool rewrite_rownum_query_enum(Node *node, void *context)
{
	if(node == NULL)
		return false;
	if(node_tree_walker(node,rewrite_rownum_query_enum, context))
		return true;
	if(IsA(node, Query))
	{
		rewrite_rownum_query((Query*)node);
	}
	return false;
}

/*
 * let "rownum <[=] CONST" or "CONST >[=] rownum"
 * to "limit N"
 * TODO: fix when "Const::consttypmod != -1"
 * TODO: fix when "rownum < 1 and rownum < 2" to "limit CASE WHEN 1<2 THEN 1 ELSE 2"
 */
static void rewrite_rownum_query(Query *query)
{
	List *qual_list,*args;
	ListCell *lc;
	Node *expr,*l,*r;
	Node *limitCount;
	Bitmapset *hints;
	char opname[4];
	Oid opno;
	Oid funcid;
	int i;
	int64 v64;

	Assert(query);
	if (query->jointree == NULL
		|| query->limitOffset != NULL
		|| query->limitCount != NULL
		|| query->hasAggs
		|| query->hasWindowFuncs
		|| contain_rownum(query->jointree->quals) == false)
		return;

	query->jointree->quals = expr = (Node*)canonicalize_qual((Expr*)(query->jointree->quals), false);
	if(is_andclause((Node*)expr))
		qual_list = ((BoolExpr*)expr)->args;
	else
		qual_list = list_make1(expr);

	/* find expr */
	limitCount = NULL;
	hints = NULL;
	for(i=0,lc=list_head(qual_list);lc;lc=lnext(qual_list, lc),++i)
	{
		expr = lfirst(lc);
		if(contain_rownum((Node*)expr) == false)
			continue;

		if(IsA(expr, OpExpr))
		{
			args = ((OpExpr*)expr)->args;
			opno = ((OpExpr*)expr)->opno;
			funcid = ((OpExpr*)expr)->opfuncid;
		}else if(IsA(expr, FuncExpr))
		{
			funcid = ((FuncExpr*)expr)->funcid;
			args = ((FuncExpr*)expr)->args;
			opno = InvalidOid;
		}else
		{
			return;
		}
		if(list_length(args) != 2)
			return;
		l = linitial(args);
		r = llast(args);
		Assert(l != NULL && r != NULL);
		if(!IsA(l,RownumExpr) && !IsA(r, RownumExpr))
			return;

		if(opno == InvalidOid)
		{
			/* get operator */
			Assert(OidIsValid(funcid));
			opno = get_operator_for_function(funcid);
			if(opno == InvalidOid)
				return;
		}

		if(IsA(r, RownumExpr))
		{
			/* exchange operator, like "10>rownum" to "rownum<10" */
			Node *tmp;
			opno = get_commutator(opno);
			if(opno == InvalidOid)
				return;
			tmp = l;
			l = r;
			r = tmp;
		}

		if(!IsA(l, RownumExpr))
			return;
		/* get operator name */
		{
			char *tmp = get_opname(opno);
			if(tmp == NULL)
				return;
			strncpy(opname, tmp, lengthof(opname));
			pfree(tmp);
		}

		if(opname[0] == '<')
		{
			if(contain_mutable_functions((Node*)r))
				return;

			if(const_get_int64((Expr*)r, &v64) == false)
				return;
			if(opname[1] == '=' && opname[2] == '\0')
			{
				/* rownum <= expr */
				if(v64 <= (int64)0)
				{
					/* rownum <= n, and (n<=0) */
					limitCount = (Node*)make_int8_const(Int64GetDatum(0));
					qual_list = NIL;
					break;
				}
				if(limitCount != NULL)
					return; /* has other operator */
				limitCount = r;
			}else if(opname[1] == '\0')
			{
				if(v64 <= (int64)1)
				{
					/* rownum < n, and (n<=1) */
					limitCount = (Node*)make_int8_const(Int64GetDatum(0));
					qual_list = NIL;
					break;
				}
				if(limitCount != NULL)
					return; /* has other operator */
				limitCount = (Node*)make_op2(NULL,
											 SystemFuncName("-"),
											 (Node*)r,
											 (Node*)make_int8_const(Int64GetDatum(1)),
											 NULL,
											 -1,
											 true);
				if(limitCount == NULL)
					return;
			}else if(opname[1] == '>' && opname[2] == '\0')
			{
				/* rownum <> expr */
				if(v64 <= (int64)0)
				{
					/* rownum <> n, and (n <= 0) ignore */
				}else if(limitCount != NULL)
				{
					return; /* has other operator */
				}else
				{
					/* for now, rownum <> n equal limit n-1 */
					limitCount = (Node*)make_op2(NULL,
												 SystemFuncName("-"),
												 (Node*)r,
												 (Node*)make_int8_const(Int64GetDatum(1)),
												 NULL,
												 -1,
												 true);
					if(limitCount == NULL)
						return;
				}
			}else
			{
				return; /* unknown operator */
			}
		}else if(opname[0] == '>')
		{
			if(const_get_int64((Expr*)r, &v64) == false)
				return;

			if(opname[1] == '=' && opname[2] == '\0')
			{
				/* rownum >= expr
				 *  only support rownum >= 1
				 */
				if(v64 == (int64)1)
					return;
			}else if(opname[1] == '\0')
			{
				/* rownum > expr
				 *  only support rownum > 0
				 */
				if(v64 != (int64)0)
					return;
			}else
			{
				return;
			}
		}else if(opname[0] == '=' && opname[1] == '\0')
		{
			if(const_get_int64((Expr*)r, &v64))
			{
				if(v64 == (int64)1)
				{
					limitCount = (Node*)make_int8_const(Int64GetDatum(v64));
				}else
				{
					/* if rownum != 1, return 0 records*/
					limitCount = (Node*)make_int8_const(Int64GetDatum(0));
				}

				if (list_length(qual_list) == 1)
					qual_list = NIL;
				else
					qual_list = list_delete(qual_list, (void *)expr);
				break;
			}
			else if(!IsA(r, RownumExpr))
				return;
			/* rownum = rownum ignore */
		}else
		{
			return;
		}

		hints = bms_add_member(hints, i);
	}

	if (limitCount)
	{
		query->limitCount = coerce_to_target_type(NULL,
												limitCount,
												exprType(limitCount),
												INT8OID,
												-1,
												COERCION_ASSIGNMENT,
												COERCE_IMPLICIT_CAST,
												-1);
		if (query->limitCount == NULL)
			return;	/* should not happen */
	}
	else
		query->limitCount = NULL;

	if(qual_list != NIL)
	{
		/* whe use args for get new quals */
		args = NIL;
		for(i=0,lc=list_head(qual_list);lc;lc=lnext(qual_list, lc),++i)
		{
			if(bms_is_member(i, hints))
				continue;
			Assert(contain_rownum(lfirst(lc)) == false);
			args = lappend(args, lfirst(lc));
		}
		if(args == NIL)
		{
			query->jointree->quals = NULL;
		}else if(list_length(args) == 1)
		{
			query->jointree->quals = linitial(args);
		}else
		{
			query->jointree->quals = (Node*)makeBoolExpr(AND_EXPR, args, -1);
		}
	}
	return;
}

static Expr* make_int8_const(Datum value)
{
	Const *result;
	result = makeNode(Const);
	result->consttype = INT8OID;
	result->consttypmod = -1;
	result->constcollid = InvalidOid;
	result->constlen = sizeof(int64);
	result->constvalue = value;
	result->constisnull = false;
	result->constbyval = FLOAT8PASSBYVAL;
	result->location = -1;
	return (Expr*)result;
}

/*
 * we should be find a cast and call it
 */
static bool const_get_int64(const Expr *expr, int64 *val)
{
	Const *c;
	AssertArg(expr && val);
	if(!IsA(expr, Const))
		return false;
	c = (Const*)expr;
	if(c->constisnull)
		return false;
	switch(c->consttype)
	{
	case INT8OID:
		*val = DatumGetInt64(c->constvalue);
		return true;
	case INT4OID:
		*val = (int64)(DatumGetInt32(c->constvalue));
		return true;
	case INT2OID:
		*val = (int64)(DatumGetInt16(c->constvalue));
		return true;
	default:
		break;
	}
	return false;
}

static Oid get_operator_for_function(Oid funcid)
{
	Relation rel;
	TableScanDesc scanDesc;
	HeapTuple	htup;
	ScanKeyData scanKeyData;
	Oid opno;

	if(funcid == InvalidOid)
		return InvalidOid;

	ScanKeyInit(&scanKeyData,
				Anum_pg_operator_oprcode,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(funcid));
	rel = table_open(OperatorRelationId, AccessShareLock);
	scanDesc = table_beginscan_catalog(rel, 1, &scanKeyData);
	htup = heap_getnext(scanDesc, ForwardScanDirection);
	if(HeapTupleIsValid(htup))
	{
		opno = ((Form_pg_operator)GETSTRUCT(htup))->oid;
	}else
	{
		opno = InvalidOid;
	}
	table_endscan(scanDesc);
	table_close(rel, AccessShareLock);
	return opno;
}

static Query *transformCreateFunctionStmt(ParseState *pstate, CreateFunctionStmt *cf)
{
	Query	   *result;
	ListCell   *lc;
	TypeName   *typeName;

	if (pstate->p_grammar == PARSE_GRAM_ORACLE)
	{
		/* convert user defined ref cursor to ref cursor */
		foreach(lc, cf->parameters)
		{
			typeName = lfirst_node(FunctionParameter, lc)->argType;
			typenameTypeIdAndMod(pstate,
								 typeName,
								 &typeName->typeOid,
								 &typeName->typemod);
			if (typeName->typeOid != REFCURSOROID &&
				getBaseType(typeName->typeOid) == REFCURSOROID)
			{
				typeName->typeOid = REFCURSOROID;
			}

			typeName->names = NIL;
			typeName->typmods = NIL;
		}
	}

	result = makeNode(Query);
	result->commandType = CMD_UTILITY;
	result->utilityStmt = (Node *) cf;

	return result;
}
#endif /* ADB_GRAM_ORA */

#ifdef ADB
static Query* makeAuxiliaryInsertQuery(Relation rel_aux, Alias *main_alias, List *main_vars, List *execNodes, double rows, int paramid);
static Query* makeAuxiliaryDeleteQuery(Relation rel_aux, Alias *main_alias, List *main_vars, List *execNodes, double rows, int paramid, bool filter_nulls);
static Expr* make_aux_delete_clause(Relation aux_rel, Index aux_relid, RangeTblEntry *pts_rte, Index pts_relid, bool filter_nulls);
static List* make_modify_query_insert_target(List *exprs, List * icolumns, List *attrnos, Bitmapset **inserted);
static List* make_aux_main_rel_need_result(Relation rel, Index mainrelid, List **colnames, Bitmapset **attrnos);
static Var* get_ts_scan_var_for_aux_key(RangeTblEntry *tsrte, const char *name, Index relid);
static List* make_aux_rel_result_vars_rel(RangeTblEntry *rte, Relation aux_rel, Index aux_relid);
static CommonTableExpr* add_modify_as_cte(Query *query, Query *subquery, const char *target_name);

void applyModifyToAuxiliaryTable(struct PlannerInfo *root, double rows, Index relid)
{
	Relation		rel;
	Relation		rel_aux;
	RangeTblEntry  *rte;
	Query		   *subparse;
	ListCell	   *lc;
	List		   *main_vars;
	Alias		   *main_alias;
	Param		   *param_old;
	Param		   *param_new;
	List		   *execNodes;
	CmdType			cmd_type;
	bool			generated;

	if (!IS_PGXC_COORDINATOR ||
		!IsConnFromApp())
		return;

	cmd_type = root->parse->commandType;
	if (cmd_type != CMD_UPDATE &&
		cmd_type != CMD_INSERT &&
		cmd_type != CMD_DELETE)
		return;

	rte = rt_fetch(relid, root->parse->rtable);
	rel = table_open(rte->relid, NoLock);
	if (rel->rd_auxlist == NIL)
	{
		table_close(rel, NoLock);
		return;
	}

	param_old = param_new = NULL;
	execNodes = main_vars = NIL;
	main_alias = NULL;

	generated = false;
	foreach(lc, rel->rd_auxlist)
	{
		rel_aux = table_open(lfirst_oid(lc), RowExclusiveLock);

		if (RELATION_IS_OTHER_TEMP(rel_aux))
		{
			table_close(rel_aux, RowExclusiveLock);
			continue;
		}

		if (main_alias == NULL)
		{
			if (cmd_type == CMD_UPDATE ||
				cmd_type == CMD_DELETE)
			{
				param_old = SS_make_initplan_output_param(root, INTERNALOID, -1, InvalidOid);
				rte->param_old = param_old->paramid;
			}

			if (cmd_type == CMD_UPDATE ||
				cmd_type == CMD_INSERT)
			{
				param_new = SS_make_initplan_output_param(root, INTERNALOID, -1, InvalidOid);
				rte->param_new = param_new->paramid;
			}
			main_alias = makeAlias(RelationGetRelationName(rel), NIL);
			main_vars = make_aux_main_rel_need_result(rel,
													  relid,
													  &main_alias->colnames,
													  &rte->mt_result);
			/* this is inexactitude for update and delete if where clause have distribute column */
			execNodes = list_copy(rel->rd_locator_info->nodeids);
		}

		/* delete old rows from aux_rel */
		if (cmd_type == CMD_UPDATE ||
			cmd_type == CMD_DELETE)
		{
			subparse = makeAuxiliaryDeleteQuery(rel_aux, main_alias, main_vars, execNodes, rows, param_old->paramid, true);
			add_modify_as_cte(root->parse, subparse, RelationGetRelationName(rel_aux));

			if (TupleDescAttr(RelationGetDescr(rel_aux), Anum_aux_table_key-1)->attnotnull == false)
			{
				subparse = makeAuxiliaryDeleteQuery(rel_aux, main_alias, main_vars, execNodes, rows, param_old->paramid, false);
				add_modify_as_cte(root->parse, subparse, RelationGetRelationName(rel_aux));
			}
		}

		/* insert into new rows to aux_rel */
		if (cmd_type == CMD_UPDATE ||
			cmd_type == CMD_INSERT)
		{
			subparse = makeAuxiliaryInsertQuery(rel_aux, main_alias, main_vars, execNodes, rows, param_new->paramid);
			add_modify_as_cte(root->parse, subparse, RelationGetRelationName(rel_aux));
		}

		generated = true;
		table_close(rel_aux, NoLock);
	}

	table_close(rel, NoLock);
	if (generated)
		root->parse->hasModifyingCTE = true;

	return;
}

static Query* makeAuxiliaryInsertQuery(Relation rel_aux, Alias *main_alias, List *main_vars, List *execNodes, double rows, int paramid)
{
	Query		   *subparse;
	ParseState	   *pstate;
	RangeTblEntry  *subrte;
	List		   *icolumns;
	List		   *attnos;
	List		   *exprList;
	ParseNamespaceItem *subnsi;

	pstate = make_parsestate(NULL);

	subparse = makeNode(Query);
	subparse->commandType = CMD_INSERT;
	subparse->canSetTag = false;	/* must be false */

	/* make insert target rte */
	subnsi = addRangeTableEntryForRelation(pstate, rel_aux, RowExclusiveLock, NULL, false, false);
	pstate->p_target_nsitem = subnsi;
	pstate->p_target_relation = rel_aux;
	subnsi->p_rte->requiredPerms = ACL_INSERT;
	subparse->resultRelation = subnsi->p_rtindex;

	/* build default list */
	icolumns = checkInsertTargets(pstate, NULL, &attnos);
	Assert(list_length(icolumns) == list_length(attnos));

	/* build tuplestore scan ParsenameSpace */
	subnsi = addRangeTableEntryForParamTupleStore(pstate,
												  main_vars,
												  main_alias,
												  paramid,
												  false);
	subrte = subnsi->p_rte;
	subrte->execNodes = execNodes;
	if (rows > 0.0)
		subrte->rows = rows;
	else
		subrte->rows = 1000.0;
	addNSItemToQuery(pstate, subnsi, true, false, false);

	/* search vars tuplestore scan result need */
	exprList = make_aux_rel_result_vars_rel(subrte, rel_aux, subnsi->p_rtindex);
	exprList = transformInsertRow(pstate, exprList, NULL, icolumns, attnos, false);

	subparse->targetList = make_modify_query_insert_target(exprList,
														   icolumns,
														   attnos,
														   &pstate->p_target_nsitem->p_rte->insertedCols);

	subparse->rtable = pstate->p_rtable;
	subparse->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	assign_query_collations(pstate, subparse);

	pstate->p_target_relation = NULL;
	free_parsestate(pstate);

	return subparse;
}


static Query* makeAuxiliaryDeleteQuery(Relation rel_aux, Alias *main_alias, List *main_vars, List *execNodes, double rows, int paramid, bool filter_nulls)
{
	Query		   *subparse;
	ParseState	   *pstate;
	RangeTblEntry  *pts_rte;
	Expr		   *qual;
	ParseNamespaceItem *aux_nsi;
	ParseNamespaceItem *pts_nsi;

	pstate = make_parsestate(NULL);

	subparse = makeNode(Query);
	subparse->commandType = CMD_DELETE;
	subparse->canSetTag = false;	/* must be false */

	/* make delete target rte */
	aux_nsi = addRangeTableEntryForRelation(pstate, rel_aux, RowExclusiveLock, NULL, false, false);
	pstate->p_target_nsitem = aux_nsi;
	pstate->p_target_relation = rel_aux;
	aux_nsi->p_rte->requiredPerms = ACL_DELETE|ACL_SELECT;
	subparse->resultRelation = aux_nsi->p_rtindex;
	/* add target ParseNamespaceItem to joinlist and namespace */
	addNSItemToQuery(pstate, aux_nsi, true, false, false);

	pts_nsi = addRangeTableEntryForParamTupleStore(pstate,
													main_vars,
													main_alias,
													paramid,
													false);
	pts_rte = pts_nsi->p_rte;
	pts_rte->execNodes = execNodes;
	if (rows > 0.0)
		pts_rte->rows = rows;
	else
		pts_rte->rows = 1000.0;

	/* make aux_rel inner join tuplestore */
	addNSItemToQuery(pstate, pts_nsi, true, false, false);

	/* make join clause */
	qual = make_aux_delete_clause(rel_aux, aux_nsi->p_rtindex, pts_rte, pts_nsi->p_rtindex, filter_nulls);

	subparse->rtable = pstate->p_rtable;
	subparse->jointree = makeFromExpr(pstate->p_joinlist, (Node*)qual);
	assign_query_collations(pstate, subparse);

	pstate->p_target_relation = NULL;
	free_parsestate(pstate);

	return subparse;
}

static Expr* make_aux_delete_clause(Relation aux_rel, Index aux_relid, RangeTblEntry *pts_rte, Index pts_relid, bool filter_nulls)
{
	Var				   *pts_var;
	Var				   *aux_var;
	Form_pg_attribute	attr;
	List			   *equals;
	Expr			   *expr;
	NullTest		   *null_test;
	List			   *equal_op_name = SystemFuncName((char*)"=");
	TupleDesc			desc = RelationGetDescr(aux_rel);

	attr = TupleDescAttr(desc, Anum_aux_table_key-1);
	aux_var = makeVar(aux_relid, Anum_aux_table_key, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
	pts_var = get_ts_scan_var_for_aux_key(pts_rte, NameStr(attr->attname), pts_relid);

	if (filter_nulls)
	{
		/*
		 * aux_rel.key = tuplestore.key_name
		 * this is not must, but aux_rel.key has index, so we add this operator
		 */
		expr = make_op(NULL, equal_op_name, (Node*)aux_var, (Node*)pts_var, NULL, -1);
		equals = list_make1(expr);

		/* tuplestore.key_name is not null */
		null_test = makeNullTest((Expr*)copyObject(pts_var), IS_NOT_NULL, false, -1);
		equals = lappend(equals, null_test);
	}else
	{
		/* tuplestore.key_name is null */
		null_test = makeNullTest((Expr*)copyObject(pts_var), IS_NULL, false, -1);
		equals = list_make1(null_test);

		/* aux_rel.key is null */
		null_test = makeNullTest((Expr*)copyObject(aux_var), IS_NULL, false, -1);
		equals = lappend(equals, null_test);
	}

	/* aux_rel.auxnodeid = tuplestore.xc_node_id */
	attr = TupleDescAttr(desc, Anum_aux_table_auxnodeid-1);
	aux_var = makeVar(aux_relid, Anum_aux_table_auxnodeid, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
	pts_var = get_ts_scan_var_for_aux_key(pts_rte, "xc_node_id", pts_relid);
	expr = make_op(NULL, equal_op_name, (Node*)aux_var, (Node*)pts_var, NULL, -1);
	equals = lappend(equals, expr);

	/* aux_rel.auxctid = tuplestore.ctid */
	attr = TupleDescAttr(desc, Anum_aux_table_auxctid-1);
	aux_var = makeVar(aux_relid, Anum_aux_table_auxctid, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
	pts_var = get_ts_scan_var_for_aux_key(pts_rte, "ctid", pts_relid);
	expr = make_op(NULL, equal_op_name, (Node*)aux_var, (Node*)pts_var, NULL, -1);
	equals = lappend(equals, expr);

	expr = makeBoolExpr(AND_EXPR, equals, -1);
	list_free(equal_op_name);

	return expr;
}

static List* make_modify_query_insert_target(List *exprs, List * icolumns, List *attrnos, Bitmapset **inserted)
{
	ListCell *lcExpr;
	ListCell *lcCol;
	ListCell *lcAttno;
	ResTarget *col;
	TargetEntry *tle;
	List *result = NULL;
	Bitmapset *insertedCols = NULL;
	AttrNumber attr;

	Assert(list_length(exprs) == list_length(icolumns));
	Assert(list_length(exprs) == list_length(attrnos));

	forthree(lcExpr, exprs,
			 lcCol, icolumns,
			 lcAttno, attrnos)
	{
		col = lfirst(lcCol);
		Assert(IsA(col, ResTarget));

		attr = (AttrNumber)lfirst_int(lcAttno);

		tle = makeTargetEntry(lfirst(lcExpr),
							  attr,
							  col->name,
							  false);
		result = lappend(result, tle);

		insertedCols = bms_add_member(insertedCols, attr - FirstLowInvalidHeapAttributeNumber);
	}

	*inserted = insertedCols;
	return result;
}

static List* make_aux_main_rel_need_result(Relation rel, Index mainrelid, List **colnames, Bitmapset **attrnos)
{
	TupleDesc			desc = RelationGetDescr(rel);
	const FormData_pg_attribute
					   *attr;
	List			   *list;
	List			   *names;
	Var				   *var;
	Bitmapset		   *varattnos;
	int					x;

	Assert(rel->rd_auxatt && rel->rd_locator_info);

	varattnos = MakeAuxMainRelResultAttnos(rel);

	x = -1;
	list = names = NIL;
	while ((x=bms_next_member(varattnos, x)) >= 0)
	{
		AttrNumber attno = (AttrNumber)(x + FirstLowInvalidHeapAttributeNumber);
		if (attno < 0)
			attr = SystemAttributeDefinition(attno);
		else
			attr = TupleDescAttr(desc, attno-1);

		var = makeVar(mainrelid, attno, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
		list = lappend(list, var);

		names = lappend(names, makeString(pstrdup(NameStr(attr->attname))));
	}

	*colnames = names;
	*attrnos = varattnos;
	return list;
}

static Var* get_ts_scan_var_for_aux_key(RangeTblEntry *tsrte, const char *name, Index relid)
{
	ListCell *lc;
	AttrNumber attno = 0;
	AssertArg(tsrte->rtekind == RTE_PARAMTS);

	foreach(lc, tsrte->eref->colnames)
	{
		if (strcmp(strVal(lfirst(lc)), name) == 0)
		{
			return makeVar(relid,
						   attno+1,
						   list_nth_oid(tsrte->coltypes, attno),
						   list_nth_int(tsrte->coltypmods, attno),
						   list_nth_oid(tsrte->colcollations, attno),
						   0);
		}
		++attno;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("can not found column \"%s\" in tuplestore scan", name)));
	return NULL;	/* keep compiler quiet */
}

static List* make_aux_rel_result_vars_rel(RangeTblEntry *rte, Relation aux_rel, Index aux_relid)
{
	List *result = NIL;
	const FormData_pg_attribute *attr;

#if (Anum_aux_table_auxnodeid == 1)
	attr = SystemAttributeDefinition(XC_NodeIdAttributeNumber);
	result = lappend(result,
					 get_ts_scan_var_for_aux_key(rte,
					 							 NameStr(attr->attname),
												 aux_relid));
#else
#error need change var list order
#endif

#if (Anum_aux_table_auxctid == 2)
	attr = SystemAttributeDefinition(SelfItemPointerAttributeNumber);
	result = lappend(result,
					 get_ts_scan_var_for_aux_key(rte,
					 							 NameStr(attr->attname),
												 aux_relid));
#else
#error need change var list order
#endif

#if (Anum_aux_table_key == 3)
	attr = TupleDescAttr(RelationGetDescr(aux_rel), Anum_aux_table_key - 1);
	result = lappend(result,
					 get_ts_scan_var_for_aux_key(rte,
					 							 NameStr(attr->attname),
												 aux_relid));
#else
#error need change var list order
#endif

	return result;
}

static CommonTableExpr* add_modify_as_cte(Query *query, Query *subquery, const char *target_name)
{
	const char *prefix;
	CommonTableExpr *cte;
	ListCell *lc;
	StringInfoData buf;
	uint32 n = 0;

	switch(subquery->commandType)
	{
	case CMD_INSERT:
		prefix = "insert.";
		break;
	case CMD_UPDATE:
		prefix = "update.";
		break;
	case CMD_DELETE:
		prefix = "delete.";
		break;
	default:
		prefix = "";
		break;
	}

	initStringInfo(&buf);
re_generate_name_:
	appendStringInfo(&buf, "%s%s", prefix, target_name);
	if (n)
		appendStringInfo(&buf, ".%u", n);

	foreach(lc, query->cteList)
	{
		cte = lfirst(lc);
		if (strcmp(cte->ctename, buf.data) == 0)
		{
			++n;
			resetStringInfo(&buf);
			goto re_generate_name_;
		}
	}

	Assert(lc == NULL);
	cte = makeNode(CommonTableExpr);
	cte->ctename = buf.data;
	cte->ctequery = (Node*)subquery;
	cte->location = -1;

	query->cteList = lappend(query->cteList, cte);
	return cte;
}
#endif /* ADB */
