/*-------------------------------------------------------------------------
 *
 * clauses.h
 *	  prototypes for clauses.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/clauses.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLAUSES_H
#define CLAUSES_H

#include "nodes/pathnodes.h"

typedef struct
{
	int			numWindowFuncs; /* total number of WindowFuncs found */
	Index		maxWinRef;		/* windowFuncs[] is indexed 0 .. maxWinRef */
	List	  **windowFuncs;	/* lists of WindowFuncs for each winref */
} WindowFuncLists;

extern bool contain_agg_clause(Node *clause);

extern bool contain_window_function(Node *clause);
extern WindowFuncLists *find_window_functions(Node *clause, Index maxWinRef);

extern double expression_returns_set_rows(PlannerInfo *root, Node *clause);

extern bool contain_subplans(Node *clause);
#ifdef ADB_GRAM_ORA
extern bool contain_rownum(Node *clause);
#endif /* ADB */

extern char max_parallel_hazard(Query *parse);
extern bool is_parallel_safe(PlannerInfo *root, Node *node);
extern bool contain_nonstrict_functions(Node *clause);
extern bool contain_exec_param(Node *clause, List *param_ids);
extern bool contain_leaked_vars(Node *clause);

#ifdef ADB_GRAM_ORA
extern bool contain_volatile_functions_without_check_RownumExpr(Node *clause);
#endif
#ifdef ADB
extern bool has_cluster_hazard(Node *node, bool allow_restricted);
extern bool check_query_not_readonly_walker(Node *node, void *context);
#endif

extern Relids find_nonnullable_rels(Node *clause);
extern List *find_nonnullable_vars(Node *clause);
extern List *find_forced_null_vars(Node *clause);
extern Var *find_forced_null_var(Node *clause);

extern bool is_pseudo_constant_clause(Node *clause);
extern bool is_pseudo_constant_clause_relids(Node *clause, Relids relids);

extern int	NumRelids(PlannerInfo *root, Node *clause);

extern void CommuteOpExpr(OpExpr *clause);

extern Expr *evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
						   Oid result_collation);

extern Query *inline_set_returning_function(PlannerInfo *root,
											RangeTblEntry *rte);

#endif							/* CLAUSES_H */
