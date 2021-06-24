/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *		parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_utilcmd.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "parser/parse_node.h"

struct AttrMap;					/* avoid including attmap.h here */


extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString ADB_ONLY_COMMA_ARG(Node **transform_stmt));
extern AlterTableStmt *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
											   const char *queryString,
											   List **beforeStmts,
											   List **afterStmts);
extern IndexStmt *transformIndexStmt(Oid relid, IndexStmt *stmt,
									 const char *queryString);
extern CreateStatsStmt *transformStatsStmt(Oid relid, CreateStatsStmt *stmt,
										   const char *queryString);
extern void transformRuleStmt(RuleStmt *stmt, const char *queryString,
							  List **actions, Node **whereClause);
extern List *transformCreateSchemaStmt(CreateSchemaStmt *stmt);
extern PartitionBoundSpec *transformPartitionBound(ParseState *pstate, Relation parent,
												   PartitionBoundSpec *spec);
extern List *expandTableLikeClause(RangeVar *heapRel,
								   TableLikeClause *table_like_clause);
extern IndexStmt *generateClonedIndexStmt(RangeVar *heapRel,
										  Relation source_idx,
										  const struct AttrMap *attmap,
										  Oid *constraintOid);
#ifdef ADB
extern int transformDistributeCluster(ParseState *pstate, Relation rel, PGXCSubCluster *cluster,
									  char loc_type, List **values, Oid **nodeoids);
#endif /* ADB */
#ifdef ADB_GRAM_ORA
extern List *ora_transformPartitionRangeBounds(ParseState *pstate, List *blist,
							  				   Relation parent ADB_ONLY_COMMA_ARG(PartitionKey key));
#endif /* ADB_GRAM_ORA */

#endif							/* PARSE_UTILCMD_H */
