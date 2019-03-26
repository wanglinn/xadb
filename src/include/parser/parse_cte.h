/*-------------------------------------------------------------------------
 *
 * parse_cte.h
 *	  handle CTEs (common table expressions) in parser
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_cte.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_CTE_H
#define PARSE_CTE_H

#include "parser/parse_node.h"

extern List *transformWithClause(ParseState *pstate, WithClause *withClause);

extern void analyzeCTETargetList(ParseState *pstate, CommonTableExpr *cte,
					 List *tlist);

#ifdef ADB_GRAM_ORA
extern List* analyzeOracleConnectBy(List *cteList, ParseState *pstate, SelectStmt *stmt);
extern bool is_sys_connect_by_path_expr(Node *node);
#endif /* ADB_GRAM_ORA */

#endif							/* PARSE_CTE_H */
