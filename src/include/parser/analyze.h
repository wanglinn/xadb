/*-------------------------------------------------------------------------
 *
 * analyze.h
 *		parse analysis for optimizable statements
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/analyze.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANALYZE_H
#define ANALYZE_H

#include "parser/parse_node.h"

/* Hook for plugins to get control at end of parse analysis */
typedef void (*post_parse_analyze_hook_type) (ParseState *pstate,
											  Query *query);
extern PGDLLIMPORT post_parse_analyze_hook_type post_parse_analyze_hook;


extern Query *parse_analyze(RawStmt *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams, QueryEnvironment *queryEnv);
extern Query *parse_analyze_varparams(RawStmt *parseTree, const char *sourceText,
						Oid **paramTypes, int *numParams);
#ifdef ADB_MULTI_GRAM
extern Query *parse_analyze_for_gram(RawStmt *parseTree, const char *sourceText,
									 Oid *paramTypes, int numParams, QueryEnvironment *queryEnv, ParseGrammar grammar);
extern Query *parse_analyze_varparams_for_gram(RawStmt *parseTree, const char *sourceText,
											   Oid **paramTypes, int *numParams, ParseGrammar grammar);
#endif
extern Query *parse_sub_analyze(Node *parseTree, ParseState *parentParseState,
				  CommonTableExpr *parentCTE,
				  bool locked_from_parent,
				  bool resolve_unknowns);

extern Query *transformTopLevelStmt(ParseState *pstate, RawStmt *parseTree);
extern Query *transformStmt(ParseState *pstate, Node *parseTree);

extern bool analyze_requires_snapshot(RawStmt *parseTree);

extern const char *LCS_asString(LockClauseStrength strength);
extern void CheckSelectLocking(Query *qry, LockClauseStrength strength);
extern void applyLockingClause(Query *qry, Index rtindex,
				   LockClauseStrength strength,
				   LockWaitPolicy waitPolicy, bool pushedDown);
#ifdef ADB
struct PlannerInfo;
extern void applyModifyToAuxiliaryTable(struct PlannerInfo *root, double rows, Index relid);
#endif /* ADB */
#ifdef ADB_GRAM_ORA
void check_joinon_column_join(Node *node, ParseState *pstate);
bool rewrite_rownum_query_enum(Node *node, void *context);
#endif /* ADB_GRAM_ORA */

extern List *BuildOnConflictExcludedTargetlist(Relation targetrel,
								  Index exclRelIndex);

#endif							/* ANALYZE_H */
