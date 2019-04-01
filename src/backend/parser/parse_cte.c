/*-------------------------------------------------------------------------
 *
 * parse_cte.c
 *	  handle CTEs (common table expressions) in parser
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_cte.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_cte.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#ifdef ADB_GRAM_ORA
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parser.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/rel.h"
#endif /* ADB_GRAM_ORA */

/* Enumeration of contexts in which a self-reference is disallowed */
typedef enum
{
	RECURSION_OK,
	RECURSION_NONRECURSIVETERM, /* inside the left-hand term */
	RECURSION_SUBLINK,			/* inside a sublink */
	RECURSION_OUTERJOIN,		/* inside nullable side of an outer join */
	RECURSION_INTERSECT,		/* underneath INTERSECT (ALL) */
	RECURSION_EXCEPT			/* underneath EXCEPT (ALL) */
} RecursionContext;

/* Associated error messages --- each must have one %s for CTE name */
static const char *const recursion_errormsgs[] = {
	/* RECURSION_OK */
	NULL,
	/* RECURSION_NONRECURSIVETERM */
	gettext_noop("recursive reference to query \"%s\" must not appear within its non-recursive term"),
	/* RECURSION_SUBLINK */
	gettext_noop("recursive reference to query \"%s\" must not appear within a subquery"),
	/* RECURSION_OUTERJOIN */
	gettext_noop("recursive reference to query \"%s\" must not appear within an outer join"),
	/* RECURSION_INTERSECT */
	gettext_noop("recursive reference to query \"%s\" must not appear within INTERSECT"),
	/* RECURSION_EXCEPT */
	gettext_noop("recursive reference to query \"%s\" must not appear within EXCEPT")
};

/*
 * For WITH RECURSIVE, we have to find an ordering of the clause members
 * with no forward references, and determine which members are recursive
 * (i.e., self-referential).  It is convenient to do this with an array
 * of CteItems instead of a list of CommonTableExprs.
 */
typedef struct CteItem
{
	CommonTableExpr *cte;		/* One CTE to examine */
	int			id;				/* Its ID number for dependencies */
	Bitmapset  *depends_on;		/* CTEs depended on (not including self) */
} CteItem;

/* CteState is what we need to pass around in the tree walkers */
typedef struct CteState
{
	/* global state: */
	ParseState *pstate;			/* global parse state */
	CteItem    *items;			/* array of CTEs and extra data */
	int			numitems;		/* number of CTEs */
	/* working state during a tree walk: */
	int			curitem;		/* index of item currently being examined */
	List	   *innerwiths;		/* list of lists of CommonTableExpr */
	/* working state for checkWellFormedRecursion walk only: */
	int			selfrefcount;	/* number of self-references detected */
	RecursionContext context;	/* context to allow or disallow self-ref */
} CteState;

#ifdef ADB_GRAM_ORA
typedef struct ParseOracleConnectByContext
{
	CommonTableExpr	   *cte;
	ParseState		   *pstate;
	RangeTblEntry	   *rte;
	Bitmapset		   *using_attnos;
	char			   *cte_name;
	List			   *scbp_list;	/* sys_connect_by_path expressions */
	List			   *scbp_alias;	/* sys_connect_by_path alias "char*" */
	int					rtindex;
	bool				searching_rtindex;	/* is searching which rte need connect by? */
	bool				have_level;	/* have LevelExpr expression */

	/* for ParseFromItem hook */
	Node			   *source_item;
	RangeVar		   *union_all_larg;
	RangeVar		   *union_all_rarg;

	/* if not NULL, ParseOracleConnectByLarg need parse larg as CTE */
	CommonTableExpr	   *union_all_lcte;
	/* if union_all_lcte is not NULL, it using in ParseOracleConnectByLarg */
	RangeVar		   *source_rv;
	Index				lcte_levelsup;

	PreParseFromClauseItemHook
						prev_hook;
	void			   *prev_hook_state;
}ParseOracleConnectByContext;

#define CONNECT_BY_LEVELS_UP 2

static Node* parseOracleConnectByHook(ParseState *pstate, Node *n,
									  RangeTblEntry **top_rte, int *top_rti,
									  List **namespace, void *state);
#endif /* ADB_GRAM_ORA */

static void analyzeCTE(ParseState *pstate, CommonTableExpr *cte);

/* Dependency processing functions */
static void makeDependencyGraph(CteState *cstate);
static bool makeDependencyGraphWalker(Node *node, CteState *cstate);
static void TopologicalSort(ParseState *pstate, CteItem *items, int numitems);

/* Recursion validity checker functions */
static void checkWellFormedRecursion(CteState *cstate);
static bool checkWellFormedRecursionWalker(Node *node, CteState *cstate);
static void checkWellFormedSelectStmt(SelectStmt *stmt, CteState *cstate);

/*
 * transformWithClause -
 *	  Transform the list of WITH clause "common table expressions" into
 *	  Query nodes.
 *
 * The result is the list of transformed CTEs to be put into the output
 * Query.  (This is in fact the same as the ending value of p_ctenamespace,
 * but it seems cleaner to not expose that in the function's API.)
 */
List *
transformWithClause(ParseState *pstate, WithClause *withClause)
{
	ListCell   *lc;

	/* Only one WITH clause per query level */
	Assert(pstate->p_ctenamespace == NIL);
	Assert(pstate->p_future_ctes == NIL);

	/*
	 * For either type of WITH, there must not be duplicate CTE names in the
	 * list.  Check this right away so we needn't worry later.
	 *
	 * Also, tentatively mark each CTE as non-recursive, and initialize its
	 * reference count to zero, and set pstate->p_hasModifyingCTE if needed.
	 */
	foreach(lc, withClause->ctes)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
		ListCell   *rest;

		for_each_cell(rest, lnext(lc))
		{
			CommonTableExpr *cte2 = (CommonTableExpr *) lfirst(rest);

			if (strcmp(cte->ctename, cte2->ctename) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("WITH query name \"%s\" specified more than once",
								cte2->ctename),
						 parser_errposition(pstate, cte2->location)));
		}

		cte->cterecursive = false;
		cte->cterefcount = 0;

		if (!IsA(cte->ctequery, SelectStmt))
		{
			/* must be a data-modifying statement */
			Assert(IsA(cte->ctequery, InsertStmt) ||
				   IsA(cte->ctequery, UpdateStmt) ||
				   IsA(cte->ctequery, DeleteStmt));

			pstate->p_hasModifyingCTE = true;
		}
	}

	if (withClause->recursive)
	{
		/*
		 * For WITH RECURSIVE, we rearrange the list elements if needed to
		 * eliminate forward references.  First, build a work array and set up
		 * the data structure needed by the tree walkers.
		 */
		CteState	cstate;
		int			i;

		cstate.pstate = pstate;
		cstate.numitems = list_length(withClause->ctes);
		cstate.items = (CteItem *) palloc0(cstate.numitems * sizeof(CteItem));
		i = 0;
		foreach(lc, withClause->ctes)
		{
			cstate.items[i].cte = (CommonTableExpr *) lfirst(lc);
			cstate.items[i].id = i;
			i++;
		}

		/*
		 * Find all the dependencies and sort the CteItems into a safe
		 * processing order.  Also, mark CTEs that contain self-references.
		 */
		makeDependencyGraph(&cstate);

		/*
		 * Check that recursive queries are well-formed.
		 */
		checkWellFormedRecursion(&cstate);

		/*
		 * Set up the ctenamespace for parse analysis.  Per spec, all the WITH
		 * items are visible to all others, so stuff them all in before parse
		 * analysis.  We build the list in safe processing order so that the
		 * planner can process the queries in sequence.
		 */
		for (i = 0; i < cstate.numitems; i++)
		{
			CommonTableExpr *cte = cstate.items[i].cte;

			pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
		}

		/*
		 * Do parse analysis in the order determined by the topological sort.
		 */
		for (i = 0; i < cstate.numitems; i++)
		{
			CommonTableExpr *cte = cstate.items[i].cte;

			analyzeCTE(pstate, cte);
		}
	}
	else
	{
		/*
		 * For non-recursive WITH, just analyze each CTE in sequence and then
		 * add it to the ctenamespace.  This corresponds to the spec's
		 * definition of the scope of each WITH name.  However, to allow error
		 * reports to be aware of the possibility of an erroneous reference,
		 * we maintain a list in p_future_ctes of the not-yet-visible CTEs.
		 */
		pstate->p_future_ctes = list_copy(withClause->ctes);

		foreach(lc, withClause->ctes)
		{
			CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

			analyzeCTE(pstate, cte);
			pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, cte);
			pstate->p_future_ctes = list_delete_first(pstate->p_future_ctes);
		}
	}

	return pstate->p_ctenamespace;
}


/*
 * Perform the actual parse analysis transformation of one CTE.  All
 * CTEs it depends on have already been loaded into pstate->p_ctenamespace,
 * and have been marked with the correct output column names/types.
 */
static void
analyzeCTE(ParseState *pstate, CommonTableExpr *cte)
{
	Query	   *query;

	/* Analysis not done already */
	Assert(!IsA(cte->ctequery, Query));

	query = parse_sub_analyze(cte->ctequery, pstate, cte, false, true);
	cte->ctequery = (Node *) query;

	/*
	 * Check that we got something reasonable.  These first two cases should
	 * be prevented by the grammar.
	 */
	if (!IsA(query, Query))
		elog(ERROR, "unexpected non-Query statement in WITH");
	if (query->utilityStmt != NULL)
		elog(ERROR, "unexpected utility statement in WITH");

	/*
	 * We disallow data-modifying WITH except at the top level of a query,
	 * because it's not clear when such a modification should be executed.
	 */
	if (query->commandType != CMD_SELECT &&
		pstate->parentParseState != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("WITH clause containing a data-modifying statement must be at the top level"),
				 parser_errposition(pstate, cte->location)));

	/*
	 * CTE queries are always marked not canSetTag.  (Currently this only
	 * matters for data-modifying statements, for which the flag will be
	 * propagated to the ModifyTable plan node.)
	 */
	query->canSetTag = false;

	if (!cte->cterecursive)
	{
		/* Compute the output column names/types if not done yet */
		analyzeCTETargetList(pstate, cte, GetCTETargetList(cte));
	}
	else
	{
		/*
		 * Verify that the previously determined output column types and
		 * collations match what the query really produced.  We have to check
		 * this because the recursive term could have overridden the
		 * non-recursive term, and we don't have any easy way to fix that.
		 */
		ListCell   *lctlist,
				   *lctyp,
				   *lctypmod,
				   *lccoll;
		int			varattno;

		lctyp = list_head(cte->ctecoltypes);
		lctypmod = list_head(cte->ctecoltypmods);
		lccoll = list_head(cte->ctecolcollations);
		varattno = 0;
		foreach(lctlist, GetCTETargetList(cte))
		{
			TargetEntry *te = (TargetEntry *) lfirst(lctlist);
			Node	   *texpr;

			if (te->resjunk)
				continue;
			varattno++;
			Assert(varattno == te->resno);
			if (lctyp == NULL || lctypmod == NULL || lccoll == NULL)	/* shouldn't happen */
				elog(ERROR, "wrong number of output columns in WITH");
			texpr = (Node *) te->expr;
			if (exprType(texpr) != lfirst_oid(lctyp) ||
				exprTypmod(texpr) != lfirst_int(lctypmod))
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("recursive query \"%s\" column %d has type %s in non-recursive term but type %s overall",
								cte->ctename, varattno,
								format_type_with_typemod(lfirst_oid(lctyp),
														 lfirst_int(lctypmod)),
								format_type_with_typemod(exprType(texpr),
														 exprTypmod(texpr))),
						 errhint("Cast the output of the non-recursive term to the correct type."),
						 parser_errposition(pstate, exprLocation(texpr))));
			if (exprCollation(texpr) != lfirst_oid(lccoll))
				ereport(ERROR,
						(errcode(ERRCODE_COLLATION_MISMATCH),
						 errmsg("recursive query \"%s\" column %d has collation \"%s\" in non-recursive term but collation \"%s\" overall",
								cte->ctename, varattno,
								get_collation_name(lfirst_oid(lccoll)),
								get_collation_name(exprCollation(texpr))),
						 errhint("Use the COLLATE clause to set the collation of the non-recursive term."),
						 parser_errposition(pstate, exprLocation(texpr))));
			lctyp = lnext(lctyp);
			lctypmod = lnext(lctypmod);
			lccoll = lnext(lccoll);
		}
		if (lctyp != NULL || lctypmod != NULL || lccoll != NULL)	/* shouldn't happen */
			elog(ERROR, "wrong number of output columns in WITH");
	}
}

/*
 * Compute derived fields of a CTE, given the transformed output targetlist
 *
 * For a nonrecursive CTE, this is called after transforming the CTE's query.
 * For a recursive CTE, we call it after transforming the non-recursive term,
 * and pass the targetlist emitted by the non-recursive term only.
 *
 * Note: in the recursive case, the passed pstate is actually the one being
 * used to analyze the CTE's query, so it is one level lower down than in
 * the nonrecursive case.  This doesn't matter since we only use it for
 * error message context anyway.
 */
void
analyzeCTETargetList(ParseState *pstate, CommonTableExpr *cte, List *tlist)
{
	int			numaliases;
	int			varattno;
	ListCell   *tlistitem;

	/* Not done already ... */
	Assert(cte->ctecolnames == NIL);

	/*
	 * We need to determine column names, types, and collations.  The alias
	 * column names override anything coming from the query itself.  (Note:
	 * the SQL spec says that the alias list must be empty or exactly as long
	 * as the output column set; but we allow it to be shorter for consistency
	 * with Alias handling.)
	 */
	cte->ctecolnames = copyObject(cte->aliascolnames);
	cte->ctecoltypes = cte->ctecoltypmods = cte->ctecolcollations = NIL;
	numaliases = list_length(cte->aliascolnames);
	varattno = 0;
	foreach(tlistitem, tlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(tlistitem);
		Oid			coltype;
		int32		coltypmod;
		Oid			colcoll;

		if (te->resjunk)
			continue;
		varattno++;
		Assert(varattno == te->resno);
		if (varattno > numaliases)
		{
			char	   *attrname;

			attrname = pstrdup(te->resname);
			cte->ctecolnames = lappend(cte->ctecolnames, makeString(attrname));
		}
		coltype = exprType((Node *) te->expr);
		coltypmod = exprTypmod((Node *) te->expr);
		colcoll = exprCollation((Node *) te->expr);

		/*
		 * If the CTE is recursive, force the exposed column type of any
		 * "unknown" column to "text".  We must deal with this here because
		 * we're called on the non-recursive term before there's been any
		 * attempt to force unknown output columns to some other type.  We
		 * have to resolve unknowns before looking at the recursive term.
		 *
		 * The column might contain 'foo' COLLATE "bar", so don't override
		 * collation if it's already set.
		 */
		if (cte->cterecursive && coltype == UNKNOWNOID)
		{
			coltype = TEXTOID;
			coltypmod = -1;		/* should be -1 already, but be sure */
			if (!OidIsValid(colcoll))
				colcoll = DEFAULT_COLLATION_OID;
		}
		cte->ctecoltypes = lappend_oid(cte->ctecoltypes, coltype);
		cte->ctecoltypmods = lappend_int(cte->ctecoltypmods, coltypmod);
		cte->ctecolcollations = lappend_oid(cte->ctecolcollations, colcoll);
	}
	if (varattno < numaliases)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("WITH query \"%s\" has %d columns available but %d columns specified",
						cte->ctename, varattno, numaliases),
				 parser_errposition(pstate, cte->location)));
}


/*
 * Identify the cross-references of a list of WITH RECURSIVE items,
 * and sort into an order that has no forward references.
 */
static void
makeDependencyGraph(CteState *cstate)
{
	int			i;

	for (i = 0; i < cstate->numitems; i++)
	{
		CommonTableExpr *cte = cstate->items[i].cte;

		cstate->curitem = i;
		cstate->innerwiths = NIL;
		makeDependencyGraphWalker((Node *) cte->ctequery, cstate);
		Assert(cstate->innerwiths == NIL);
	}

	TopologicalSort(cstate->pstate, cstate->items, cstate->numitems);
}

/*
 * Tree walker function to detect cross-references and self-references of the
 * CTEs in a WITH RECURSIVE list.
 */
static bool
makeDependencyGraphWalker(Node *node, CteState *cstate)
{
	if (node == NULL)
		return false;
	if (IsA(node, RangeVar))
	{
		RangeVar   *rv = (RangeVar *) node;

		/* If unqualified name, might be a CTE reference */
		if (!rv->schemaname)
		{
			ListCell   *lc;
			int			i;

			/* ... but first see if it's captured by an inner WITH */
			foreach(lc, cstate->innerwiths)
			{
				List	   *withlist = (List *) lfirst(lc);
				ListCell   *lc2;

				foreach(lc2, withlist)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc2);

					if (strcmp(rv->relname, cte->ctename) == 0)
						return false;	/* yes, so bail out */
				}
			}

			/* No, could be a reference to the query level we are working on */
			for (i = 0; i < cstate->numitems; i++)
			{
				CommonTableExpr *cte = cstate->items[i].cte;

				if (strcmp(rv->relname, cte->ctename) == 0)
				{
					int			myindex = cstate->curitem;

					if (i != myindex)
					{
						/* Add cross-item dependency */
						cstate->items[myindex].depends_on =
							bms_add_member(cstate->items[myindex].depends_on,
										   cstate->items[i].id);
					}
					else
					{
						/* Found out this one is self-referential */
						cte->cterecursive = true;
					}
					break;
				}
			}
		}
		return false;
	}
	if (IsA(node, SelectStmt))
	{
		SelectStmt *stmt = (SelectStmt *) node;
		ListCell   *lc;

		if (stmt->withClause)
		{
			if (stmt->withClause->recursive)
			{
				/*
				 * In the RECURSIVE case, all query names of the WITH are
				 * visible to all WITH items as well as the main query. So
				 * push them all on, process, pop them all off.
				 */
				cstate->innerwiths = lcons(stmt->withClause->ctes,
										   cstate->innerwiths);
				foreach(lc, stmt->withClause->ctes)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

					(void) makeDependencyGraphWalker(cte->ctequery, cstate);
				}
				(void) raw_expression_tree_walker(node,
												  makeDependencyGraphWalker,
												  (void *) cstate);
				cstate->innerwiths = list_delete_first(cstate->innerwiths);
			}
			else
			{
				/*
				 * In the non-RECURSIVE case, query names are visible to the
				 * WITH items after them and to the main query.
				 */
				ListCell   *cell1;

				cstate->innerwiths = lcons(NIL, cstate->innerwiths);
				cell1 = list_head(cstate->innerwiths);
				foreach(lc, stmt->withClause->ctes)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

					(void) makeDependencyGraphWalker(cte->ctequery, cstate);
					lfirst(cell1) = lappend((List *) lfirst(cell1), cte);
				}
				(void) raw_expression_tree_walker(node,
												  makeDependencyGraphWalker,
												  (void *) cstate);
				cstate->innerwiths = list_delete_first(cstate->innerwiths);
			}
			/* We're done examining the SelectStmt */
			return false;
		}
		/* if no WITH clause, just fall through for normal processing */
	}
	if (IsA(node, WithClause))
	{
		/*
		 * Prevent raw_expression_tree_walker from recursing directly into a
		 * WITH clause.  We need that to happen only under the control of the
		 * code above.
		 */
		return false;
	}
	return raw_expression_tree_walker(node,
									  makeDependencyGraphWalker,
									  (void *) cstate);
}

/*
 * Sort by dependencies, using a standard topological sort operation
 */
static void
TopologicalSort(ParseState *pstate, CteItem *items, int numitems)
{
	int			i,
				j;

	/* for each position in sequence ... */
	for (i = 0; i < numitems; i++)
	{
		/* ... scan the remaining items to find one that has no dependencies */
		for (j = i; j < numitems; j++)
		{
			if (bms_is_empty(items[j].depends_on))
				break;
		}

		/* if we didn't find one, the dependency graph has a cycle */
		if (j >= numitems)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mutual recursion between WITH items is not implemented"),
					 parser_errposition(pstate, items[i].cte->location)));

		/*
		 * Found one.  Move it to front and remove it from every other item's
		 * dependencies.
		 */
		if (i != j)
		{
			CteItem		tmp;

			tmp = items[i];
			items[i] = items[j];
			items[j] = tmp;
		}

		/*
		 * Items up through i are known to have no dependencies left, so we
		 * can skip them in this loop.
		 */
		for (j = i + 1; j < numitems; j++)
		{
			items[j].depends_on = bms_del_member(items[j].depends_on,
												 items[i].id);
		}
	}
}


/*
 * Check that recursive queries are well-formed.
 */
static void
checkWellFormedRecursion(CteState *cstate)
{
	int			i;

	for (i = 0; i < cstate->numitems; i++)
	{
		CommonTableExpr *cte = cstate->items[i].cte;
		SelectStmt *stmt = (SelectStmt *) cte->ctequery;

		Assert(!IsA(stmt, Query));	/* not analyzed yet */

		/* Ignore items that weren't found to be recursive */
		if (!cte->cterecursive)
			continue;

		/* Must be a SELECT statement */
		if (!IsA(stmt, SelectStmt))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_RECURSION),
					 errmsg("recursive query \"%s\" must not contain data-modifying statements",
							cte->ctename),
					 parser_errposition(cstate->pstate, cte->location)));

		/* Must have top-level UNION */
		if (stmt->op != SETOP_UNION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_RECURSION),
					 errmsg("recursive query \"%s\" does not have the form non-recursive-term UNION [ALL] recursive-term",
							cte->ctename),
					 parser_errposition(cstate->pstate, cte->location)));

		/* The left-hand operand mustn't contain self-reference at all */
		cstate->curitem = i;
		cstate->innerwiths = NIL;
		cstate->selfrefcount = 0;
		cstate->context = RECURSION_NONRECURSIVETERM;
		checkWellFormedRecursionWalker((Node *) stmt->larg, cstate);
		Assert(cstate->innerwiths == NIL);

		/* Right-hand operand should contain one reference in a valid place */
		cstate->curitem = i;
		cstate->innerwiths = NIL;
		cstate->selfrefcount = 0;
		cstate->context = RECURSION_OK;
		checkWellFormedRecursionWalker((Node *) stmt->rarg, cstate);
		Assert(cstate->innerwiths == NIL);
		if (cstate->selfrefcount != 1)	/* shouldn't happen */
			elog(ERROR, "missing recursive reference");

		/* WITH mustn't contain self-reference, either */
		if (stmt->withClause)
		{
			cstate->curitem = i;
			cstate->innerwiths = NIL;
			cstate->selfrefcount = 0;
			cstate->context = RECURSION_SUBLINK;
			checkWellFormedRecursionWalker((Node *) stmt->withClause->ctes,
										   cstate);
			Assert(cstate->innerwiths == NIL);
		}

		/*
		 * Disallow ORDER BY and similar decoration atop the UNION. These
		 * don't make sense because it's impossible to figure out what they
		 * mean when we have only part of the recursive query's results. (If
		 * we did allow them, we'd have to check for recursive references
		 * inside these subtrees.)
		 */
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ORDER BY in a recursive query is not implemented"),
					 parser_errposition(cstate->pstate,
										exprLocation((Node *) stmt->sortClause))));
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("OFFSET in a recursive query is not implemented"),
					 parser_errposition(cstate->pstate,
										exprLocation(stmt->limitOffset))));
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("LIMIT in a recursive query is not implemented"),
					 parser_errposition(cstate->pstate,
										exprLocation(stmt->limitCount))));
		if (stmt->lockingClause)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("FOR UPDATE/SHARE in a recursive query is not implemented"),
					 parser_errposition(cstate->pstate,
										exprLocation((Node *) stmt->lockingClause))));
	}
}

/*
 * Tree walker function to detect invalid self-references in a recursive query.
 */
static bool
checkWellFormedRecursionWalker(Node *node, CteState *cstate)
{
	RecursionContext save_context = cstate->context;

	if (node == NULL)
		return false;
	if (IsA(node, RangeVar))
	{
		RangeVar   *rv = (RangeVar *) node;

		/* If unqualified name, might be a CTE reference */
		if (!rv->schemaname)
		{
			ListCell   *lc;
			CommonTableExpr *mycte;

			/* ... but first see if it's captured by an inner WITH */
			foreach(lc, cstate->innerwiths)
			{
				List	   *withlist = (List *) lfirst(lc);
				ListCell   *lc2;

				foreach(lc2, withlist)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc2);

					if (strcmp(rv->relname, cte->ctename) == 0)
						return false;	/* yes, so bail out */
				}
			}

			/* No, could be a reference to the query level we are working on */
			mycte = cstate->items[cstate->curitem].cte;
			if (strcmp(rv->relname, mycte->ctename) == 0)
			{
				/* Found a recursive reference to the active query */
				if (cstate->context != RECURSION_OK)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_RECURSION),
							 errmsg(recursion_errormsgs[cstate->context],
									mycte->ctename),
							 parser_errposition(cstate->pstate,
												rv->location)));
				/* Count references */
				if (++(cstate->selfrefcount) > 1)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_RECURSION),
							 errmsg("recursive reference to query \"%s\" must not appear more than once",
									mycte->ctename),
							 parser_errposition(cstate->pstate,
												rv->location)));
			}
		}
		return false;
	}
	if (IsA(node, SelectStmt))
	{
		SelectStmt *stmt = (SelectStmt *) node;
		ListCell   *lc;

		if (stmt->withClause)
		{
			if (stmt->withClause->recursive)
			{
				/*
				 * In the RECURSIVE case, all query names of the WITH are
				 * visible to all WITH items as well as the main query. So
				 * push them all on, process, pop them all off.
				 */
				cstate->innerwiths = lcons(stmt->withClause->ctes,
										   cstate->innerwiths);
				foreach(lc, stmt->withClause->ctes)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

					(void) checkWellFormedRecursionWalker(cte->ctequery, cstate);
				}
				checkWellFormedSelectStmt(stmt, cstate);
				cstate->innerwiths = list_delete_first(cstate->innerwiths);
			}
			else
			{
				/*
				 * In the non-RECURSIVE case, query names are visible to the
				 * WITH items after them and to the main query.
				 */
				ListCell   *cell1;

				cstate->innerwiths = lcons(NIL, cstate->innerwiths);
				cell1 = list_head(cstate->innerwiths);
				foreach(lc, stmt->withClause->ctes)
				{
					CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

					(void) checkWellFormedRecursionWalker(cte->ctequery, cstate);
					lfirst(cell1) = lappend((List *) lfirst(cell1), cte);
				}
				checkWellFormedSelectStmt(stmt, cstate);
				cstate->innerwiths = list_delete_first(cstate->innerwiths);
			}
		}
		else
			checkWellFormedSelectStmt(stmt, cstate);
		/* We're done examining the SelectStmt */
		return false;
	}
	if (IsA(node, WithClause))
	{
		/*
		 * Prevent raw_expression_tree_walker from recursing directly into a
		 * WITH clause.  We need that to happen only under the control of the
		 * code above.
		 */
		return false;
	}
	if (IsA(node, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) node;

		switch (j->jointype)
		{
			case JOIN_INNER:
				checkWellFormedRecursionWalker(j->larg, cstate);
				checkWellFormedRecursionWalker(j->rarg, cstate);
				checkWellFormedRecursionWalker(j->quals, cstate);
				break;
			case JOIN_LEFT:
				checkWellFormedRecursionWalker(j->larg, cstate);
				if (save_context == RECURSION_OK)
					cstate->context = RECURSION_OUTERJOIN;
				checkWellFormedRecursionWalker(j->rarg, cstate);
				cstate->context = save_context;
				checkWellFormedRecursionWalker(j->quals, cstate);
				break;
			case JOIN_FULL:
				if (save_context == RECURSION_OK)
					cstate->context = RECURSION_OUTERJOIN;
				checkWellFormedRecursionWalker(j->larg, cstate);
				checkWellFormedRecursionWalker(j->rarg, cstate);
				cstate->context = save_context;
				checkWellFormedRecursionWalker(j->quals, cstate);
				break;
			case JOIN_RIGHT:
				if (save_context == RECURSION_OK)
					cstate->context = RECURSION_OUTERJOIN;
				checkWellFormedRecursionWalker(j->larg, cstate);
				cstate->context = save_context;
				checkWellFormedRecursionWalker(j->rarg, cstate);
				checkWellFormedRecursionWalker(j->quals, cstate);
				break;
			default:
				elog(ERROR, "unrecognized join type: %d",
					 (int) j->jointype);
		}
		return false;
	}
	if (IsA(node, SubLink))
	{
		SubLink    *sl = (SubLink *) node;

		/*
		 * we intentionally override outer context, since subquery is
		 * independent
		 */
		cstate->context = RECURSION_SUBLINK;
		checkWellFormedRecursionWalker(sl->subselect, cstate);
		cstate->context = save_context;
		checkWellFormedRecursionWalker(sl->testexpr, cstate);
		return false;
	}
	return raw_expression_tree_walker(node,
									  checkWellFormedRecursionWalker,
									  (void *) cstate);
}

/*
 * subroutine for checkWellFormedRecursionWalker: process a SelectStmt
 * without worrying about its WITH clause
 */
static void
checkWellFormedSelectStmt(SelectStmt *stmt, CteState *cstate)
{
	RecursionContext save_context = cstate->context;

	if (save_context != RECURSION_OK)
	{
		/* just recurse without changing state */
		raw_expression_tree_walker((Node *) stmt,
								   checkWellFormedRecursionWalker,
								   (void *) cstate);
	}
	else
	{
		switch (stmt->op)
		{
			case SETOP_NONE:
			case SETOP_UNION:
				raw_expression_tree_walker((Node *) stmt,
										   checkWellFormedRecursionWalker,
										   (void *) cstate);
				break;
			case SETOP_INTERSECT:
				if (stmt->all)
					cstate->context = RECURSION_INTERSECT;
				checkWellFormedRecursionWalker((Node *) stmt->larg,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->rarg,
											   cstate);
				cstate->context = save_context;
				checkWellFormedRecursionWalker((Node *) stmt->sortClause,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->limitOffset,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->limitCount,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->lockingClause,
											   cstate);
				/* stmt->withClause is intentionally ignored here */
				break;
			case SETOP_EXCEPT:
				if (stmt->all)
					cstate->context = RECURSION_EXCEPT;
				checkWellFormedRecursionWalker((Node *) stmt->larg,
											   cstate);
				cstate->context = RECURSION_EXCEPT;
				checkWellFormedRecursionWalker((Node *) stmt->rarg,
											   cstate);
				cstate->context = save_context;
				checkWellFormedRecursionWalker((Node *) stmt->sortClause,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->limitOffset,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->limitCount,
											   cstate);
				checkWellFormedRecursionWalker((Node *) stmt->lockingClause,
											   cstate);
				/* stmt->withClause is intentionally ignored here */
				break;
			default:
				elog(ERROR, "unrecognized set op: %d",
					 (int) stmt->op);
		}
	}
}

#ifdef ADB_GRAM_ORA
bool is_sys_connect_by_path_expr(Node *node)
{
	if(node != NULL && IsA(node, FuncCall))
	{
		FuncCall *func = (FuncCall*)node;
		if(list_length(func->funcname) == 1
			&& list_length(func->args) == 2
			&& func->agg_order == NIL
			&& func->agg_star == false
			&& func->agg_distinct == false
			&& func->func_variadic == false
			&& func->over == NULL
			&& IsA(linitial(func->funcname), String)
			&& strcmp(strVal(linitial(func->funcname)), "sys_connect_by_path") == 0)
		{
			return true;
		}
	}
	return false;
}

static bool search_using_column(Node *node, ParseOracleConnectByContext *context)
{
	check_stack_depth();

	if (node == NULL)
	{
		return false;
	}else if (is_sys_connect_by_path_expr(node))
	{
		if (list_member(context->scbp_list, node) == false)
			context->scbp_list = lappend(context->scbp_list, node);
	}else if (IsA(node, LevelExpr))
	{
		context->have_level = true;
		return false;
	}else if (IsA(node, ColumnRef))
	{
		Var *var;
		ColumnRef *cref = (ColumnRef*)node;
		if (list_length(cref->fields) == 1 &&
			IsA(linitial(cref->fields), String) &&
			strcmp(strVal(linitial(cref->fields)), "level") == 0)
		{
			context->have_level = true;
			return false;
		}
		if (IsA(llast(cref->fields), A_Star))
		{
			context->using_attnos = bms_add_member(context->using_attnos,
												   InvalidAttrNumber-FirstLowInvalidHeapAttributeNumber);
			return false;
		}
		var = (Var*)transformExpr(context->pstate, node, EXPR_KIND_OTHER);
		if (var != NULL &&
			IsA(var, Var) &&
			var->varlevelsup == 0)
		{
			if (context->searching_rtindex)
			{
				if (context->rtindex <= 0)
				{
					context->rtindex = var->varno;
				}else if(context->rtindex != var->varno)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("connect by using relation is not unique"),
							 parser_errposition(context->pstate, exprLocation(node))));
				}
			}else if(context->rtindex != var->varno)
			{
				/* ignore other relation */
				return false;
			}

			/* when has whole, don't need add normale attribute */
			if (var->varattno <= 0 ||
				!bms_is_member(InvalidAttrNumber-FirstLowInvalidHeapAttributeNumber, context->using_attnos))
			{
				context->using_attnos = bms_add_member(context->using_attnos,
													   var->varattno - FirstLowInvalidHeapAttributeNumber);
			}
		}
		return false;
	}else if (IsA(node, SubLink))
	{
		SubLink *sublink = (SubLink*)node;
		/* ignore subselect */
		return search_using_column(sublink->testexpr, context);
	}else if (IsA(node, RangeSubselect))
	{
		return false;
	}

	return node_tree_walker(node, search_using_column, context);
}

static List* append_column_ref_target(List *list, const char *colname, bool on_right)
{
	ResTarget *target;
	ColumnRef *cref;

	cref = makeNode(ColumnRef);
	cref->fields = lappend(cref->fields, makeString(pstrdup(colname)));
	cref->location = -1;

	target = makeNode(ResTarget);
	target->val = (Node*)cref;
	target->location = -1;

	return lappend(list, target);
}

static bool pstate_has_column_name(const char *colname, ParseState *pstate)
{
	ListCell *lc;
	ListCell *lc2;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *rte;

	foreach(lc, pstate->p_namespace)
	{
		nsitem = lfirst(lc);
		rte = nsitem->p_rte;

		/* Ignore table-only items */
		if (!nsitem->p_cols_visible)
			continue;
		/* If not inside LATERAL, ignore lateral-only items */
		if (nsitem->p_lateral_only && !pstate->p_lateral_active)
			continue;

		foreach (lc2, rte->eref->colnames)
		{
			if (strcmp(strVal(lfirst(lc2)), colname) == 0)
				return true;
		}
	}

	return false;
}

static void generate_unique_scbp_names(ParseOracleConnectByContext *context)
{
	ParseState *pstate = context->pstate;
	ListCell *lc;
	int i = 0;
	char buf[NAMEDATALEN];

	strcpy(buf, "scbp");

	foreach (lc, context->scbp_list)
	{
		while (pstate_has_column_name(buf, pstate))
		{
			sprintf(buf, "scbp_%d", ++i);
		}

		context->scbp_alias = lappend(context->scbp_alias,
									  makeString(pstrdup(buf)));
	}
}

static List* make_union_all_targetlist(ParseOracleConnectByContext *context, bool on_right)
{
	List			   *result = NIL;
	Bitmapset		   *bms = context->using_attnos;
	List			   *colnames = context->rte->eref->colnames;
	ListCell		   *lc,*lc2;
	ResTarget		   *target;
	Node			   *expr;
	Form_pg_attribute	attr;
	const char		   *colname;
	int					x;
	AttrNumber			attno;

	/* process attribute */
	x = -1;
	while ((x = bms_next_member(bms, x)) >= 0)
	{
		attno = x + FirstLowInvalidHeapAttributeNumber;
		if (attno < 0)
		{
			/* sys attribute */
			attr = SystemAttributeDefinition(attno, true);
			colname = NameStr(attr->attname);
		}else if (attno == 0)
		{
			/* has whole */
			ListCell *lc;
			foreach(lc, colnames)
				result = append_column_ref_target(result, strVal(lfirst(lc)), on_right);
			/* don't need loop more */
			break;
		}else
		{
			/* normal attribute */
			Value *value = list_nth(colnames, attno-1);
			colname = strVal(value);
		}
		result = append_column_ref_target(result, colname, on_right);
	}

	/* has level expression? */
	if (context->have_level)
	{
		if (on_right)
		{
			PriorExpr *prior;

			/* "PRIOR LEVEL" */
			prior = makeNode(PriorExpr);
			prior->expr = makeColumnRef("level", NIL, -1, NULL);
			prior->location = -1;

			/* "(PRIOR LEVEL)+1" */
			expr = (Node*)makeSimpleA_Expr(AEXPR_OP, "+", (Node*)prior, makeIntConst(1, -1), -1);
		}else
		{
			/* 1::int8 as level */
			expr = makeIntConst(1, -1);
			expr = makeTypeCast(expr,
								makeTypeNameFromNameList(SystemFuncName("int8")),
								-1);
		}
		target = makeNode(ResTarget);
		target->val = expr;
		target->name = "level";
		target->location = -1;
		result = lappend(result, target);
	}

	/* have sys_conect_by_path? */
	Assert(list_length(context->scbp_list) == list_length(context->scbp_alias));
	forboth (lc, context->scbp_list, lc2, context->scbp_alias)
	{
		if (on_right)
		{
			PriorExpr *prior;
			/* PRIOR expr1 */
			expr = makeColumnRef(strVal(lfirst(lc2)), NIL, -1, NULL);
			prior = makeNode(PriorExpr);
			prior->expr = expr;
			prior->location = -1;
			/* (PRIOR expr1) || expr2 */
			expr = (Node*)makeA_Expr(AEXPR_OP,
									 list_make1(makeString("||")),
									 (Node*)prior,
									 llast(lfirst_node(FuncCall, lc)->args),
									 -1);

			/* ((PRIOR expr1) || expr2) || expr1 */
			expr = (Node*)makeA_Expr(AEXPR_OP,
									 list_make1(makeString("||")),
									 (Node*)expr,
									 linitial(lfirst_node(FuncCall, lc)->args),
									 -1);
		}else
		{
			expr = linitial(lfirst_node(FuncCall, lc)->args);
		}
		target = makeNode(ResTarget);
		target->val = expr;
		target->name = strVal(lfirst(lc2));
		target->location = -1;
		result = lappend(result, target);
	}

	return result;
}

static char *generate_new_cte_name(ParseState *pstate)
{
	ListCell *lc;
	CommonTableExpr *cte;
	int i=0;
	char buf[NAMEDATALEN];

	strcpy(buf, "connect_by_cte");
re_check_:
	foreach (lc, pstate->p_ctenamespace)
	{
		cte = lfirst_node(CommonTableExpr, lc);
		if (strcmp(buf, cte->ctename) == 0)
		{
			sprintf(buf, "connect_by_cte_%d", ++i);
			goto re_check_;
		}
	}

	return pstrdup(buf);
}

List* analyzeOracleConnectBy(List *cteList, ParseState *pstate, SelectStmt *stmt)
{
	ParseOracleConnectByContext context;
	SelectStmt		   *larg;
	SelectStmt		   *rarg;
	RangeVar		   *range_base;

	Assert(stmt->ora_connect_by != NULL &&
		   stmt->ora_connect_by->connect_by != NULL);

	MemSet(&context, 0, sizeof(context));
	context.pstate = make_parsestate(pstate);

	transformFromClause(context.pstate, copyObject(stmt->fromClause));
	/* search whitch rte we need transform with connect by */
	context.searching_rtindex = true;
	context.rtindex = -1;
	search_using_column(stmt->ora_connect_by->connect_by, &context);
	if (list_length(pstate->p_rtable) == 1)
	{
		context.rtindex = 1;
	}else if (context.rtindex <= 0)
	{
		/* should not be hapen */
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("no connect by relation found")));
	}
	context.searching_rtindex = false;
	context.rte = rt_fetch(context.rtindex, context.pstate->p_rtable);
	context.source_item = searchFromClauseItem(context.pstate, stmt->fromClause, context.rtindex);
	if (context.source_item == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Can not found from clause item for \"%s\"", context.rte->eref->aliasname)));
		return NULL; /* never run */
	}else if (IsA(context.source_item, RangeVar))
	{
		range_base = (RangeVar*)context.source_item;
		if (context.rte->rtekind == RTE_RELATION ||
			context.rte->rtekind == RTE_NAMEDTUPLESTORE)
		{
			/* nothing todo */
		}else if(context.rte->rtekind == RTE_CTE)
		{
			/* remember CTE */
			context.union_all_lcte = scanNameSpaceForCTE(pstate, range_base->relname, &context.lcte_levelsup);
			if (context.union_all_lcte == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Can not found CTE fro reference \"%s\"", range_base->relname)));
			}

			/* and we must generate a new CTE name */
			range_base = makeRangeVar(NULL, generate_new_cte_name(pstate), -1);
			range_base->alias = makeAlias(context.rte->eref->aliasname, NIL);
		}else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unknown RTE type %d for connect by",context.rte->rtekind)));
		}
		context.source_rv = range_base;
	}else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 parser_errposition(pstate, exprLocation(context.source_item)),
				 errmsg("connect by only support relation for now")));
		return NULL; /* never run */
	}

	/* search using column */
	search_using_column((Node*)stmt->distinctClause, &context);
	search_using_column((Node*)stmt->targetList, &context);
	search_using_column(stmt->whereClause, &context);
	search_using_column((Node*)stmt->groupClause, &context);
	search_using_column(stmt->havingClause, &context);
	search_using_column((Node*)stmt->windowClause, &context);
	search_using_column((Node*)stmt->sortClause, &context);
	search_using_column((Node*)stmt->fromClause, &context);

	if (context.have_level &&
		pstate_has_column_name("level", context.pstate))
	{
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_COLUMN),
				 errmsg("column reference \"%s\" is ambiguous", "level"),
				 err_generic_string(PG_DIAG_COLUMN_NAME, "level")));
	}
	if (context.scbp_list)
		generate_unique_scbp_names(&context);

	/* make union all left SelectStmt */
	larg = makeNode(SelectStmt);
	context.union_all_larg = copyObject(range_base);
	larg->fromClause = list_make1(context.union_all_larg);
	larg->whereClause = stmt->ora_connect_by->start_with;
	larg->targetList = make_union_all_targetlist(&context, false);

	/* cte name is RTE relation name */
	context.cte_name = pstrdup(range_base->relname);

	/* make union all right SelectStmt and target list */
	rarg = makeNode(SelectStmt);
	rarg->targetList = make_union_all_targetlist(&context, true);

	/* make union all right fromClause */
	context.union_all_rarg = copyObject(range_base);
	rarg->fromClause = list_make1(context.union_all_rarg);

	/* set "connect by" expression to right's where clause */
	rarg->whereClause = stmt->ora_connect_by->connect_by;

	/* generate final CommonTableExpr */
	context.cte = makeNode(CommonTableExpr);
	context.cte->cterecursive = true;
	context.cte->ctename = context.cte_name;
	context.cte->ctequery = makeSetOp(SETOP_UNION, true, (Node*)larg, (Node*)rarg);
	/* copy special info */
	context.cte->have_level = context.have_level;
	context.cte->scbp_list = context.scbp_list;
	context.cte->scbp_alias = context.scbp_alias;
	context.cte->from_connect_by = true;

	context.prev_hook = pstate->p_pre_from_item_hook;
	pstate->p_pre_from_item_hook = parseOracleConnectByHook;
	context.prev_hook_state = pstate->p_from_item_hook_state;
	pstate->p_from_item_hook_state = &context;
	PG_TRY();
	{
		pstate->p_ctenamespace = lappend(pstate->p_ctenamespace, context.cte);
		analyzeCTE(pstate, context.cte);
		transformFromClause(pstate, stmt->fromClause);
	}PG_CATCH();
	{
		pstate->p_pre_from_item_hook = context.prev_hook;
		pstate->p_from_item_hook_state = context.prev_hook_state;
		PG_RE_THROW();
	}PG_END_TRY();
	pstate->p_pre_from_item_hook = context.prev_hook;
	pstate->p_from_item_hook_state = context.prev_hook_state;

	free_parsestate(context.pstate);

	return pstate->p_ctenamespace;
}

static ParseNamespaceItem* make_simple_namespace_item(RangeTblEntry *rte, bool visiable)
{
	ParseNamespaceItem *nsitem = palloc0(sizeof(ParseNamespaceItem));

	nsitem->p_rte = rte;
	nsitem->p_rel_visible = nsitem->p_cols_visible = visiable;
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	return nsitem;
}

static Node* ParseOracleConnectByLarg(ParseState *pstate, Node *node,
									  RangeTblEntry **top_rte, int *top_rti,
									  List **namespace,
									  ParseOracleConnectByContext *context)
{
	/*
	 * like transformFromClauseItem(RangeVar)
	 */
	RangeTblEntry *rte;
	RangeTblRef *rtr;

	if (node == context->source_item)
	{
		/* top select stmt */
		rte = addRangeTableEntryForCTE(pstate,
									   context->cte,
									   0,
									   context->source_rv,
									   true);
	}else if (context->union_all_lcte)
	{
		/* "union all" left is from CTE */
		rte = addRangeTableEntryForCTE(pstate,
									   context->union_all_lcte,
									   context->lcte_levelsup + CONNECT_BY_LEVELS_UP,
									   context->source_rv,
									   true);
	}else
	{
		RangeVar *rv = castNode(RangeVar, node);
		rte = addRangeTableEntry(pstate, rv, rv->alias, rv->inh, true);
	}

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = list_length(pstate->p_rtable);
	Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));

	*top_rte = rte;
	*top_rti = rtr->rtindex;
	*namespace = list_make1(make_simple_namespace_item(rte, true));

	return (Node*)rtr;
}

static Node* ParseOracleConnectByRarg(ParseState *pstate, RangeVar *rv,
									  RangeTblEntry **top_rte, int *top_rti,
									  List **namespace,
									  ParseOracleConnectByContext *context)
{
	RangeTblEntry *rte;
	RangeTblRef *rtr;

	rte = addRangeTableEntryForCTE(pstate,
								   context->cte,
								   CONNECT_BY_LEVELS_UP,
								   rv, true);
	rtr = makeNode(RangeTblRef);
	rtr->rtindex = list_length(pstate->p_rtable);
	Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));
	/* add to join list */
	pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
	/* let rel and column not visiable */
	pstate->p_namespace = lappend(pstate->p_namespace,
								  make_simple_namespace_item(rte, false));

	return ParseOracleConnectByLarg(pstate,
									(Node*)rv,
									top_rte,
									top_rti,
									namespace,
									context);
}

/* like scanNameSpaceForCTE, but skip connect by generated CTE */
static CommonTableExpr*
scanNameSpaceForNormalCTE(ParseState *pstate, const char *refname, Index *ctelevelsup)
{
	Index		levelsup;

	for (levelsup = 0;
		 pstate != NULL;
		 pstate = pstate->parentParseState, levelsup++)
	{
		ListCell   *lc;

		foreach(lc, pstate->p_ctenamespace)
		{
			CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

			if (cte->from_connect_by == false &&
				strcmp(cte->ctename, refname) == 0)
			{
				*ctelevelsup = levelsup;
				return cte;
			}
		}
	}
	return NULL;
}

static Node* parseOracleConnectByHook(ParseState *pstate, Node *n,
									  RangeTblEntry **top_rte, int *top_rti,
									  List **namespace, void *state)
{
	ParseOracleConnectByContext *context = state;
	Node *result = NULL;

	if (n != NULL)
	{
		if ((void*)n == context->union_all_larg ||
			n == context->source_item)
		{
			result=ParseOracleConnectByLarg(pstate,
											n,
											top_rte,
											top_rti,
											namespace,
											context);
		}else if((void*)n == context->union_all_rarg)
		{
			result=ParseOracleConnectByRarg(pstate,
											(RangeVar*)n,
											top_rte,
											top_rti,
											namespace,
											context);
		}else if(IsA(n, RangeVar) &&
			((RangeVar*)n)->schemaname == NULL)
		{
			RangeVar *rv = (RangeVar*)n;
			CommonTableExpr *cte;
			RangeTblEntry *rte;
			RangeTblRef *rtr;
			Index		levelsup;

			cte = scanNameSpaceForNormalCTE(pstate, rv->relname, &levelsup);
			if (cte)
				rte = addRangeTableEntryForCTE(pstate, cte, levelsup, rv, true);
			else if (scanNameSpaceForENR(pstate, rv->relname))
				rte = addRangeTableEntryForENR(pstate, rv, true);
			else
				rte = addRangeTableEntry(pstate, rv, rv->alias, rv->inh, true);

			rtr = makeNode(RangeTblRef);
			rtr->rtindex = list_length(pstate->p_rtable);
			Assert(rte == rt_fetch(rtr->rtindex, pstate->p_rtable));

			*top_rte = rte;
			*top_rti = rtr->rtindex;
			*namespace = list_make1(make_simple_namespace_item(rte, true));

			result = (Node*)rtr;
		}
	}

	if (result == NULL &&
		context->prev_hook)
	{
		result = (*context->prev_hook)(pstate, n, top_rte,
									   top_rti, namespace,
									   context->prev_hook_state);
	}

	return result;
}

#endif /* ADB_GRAM_ORA */
