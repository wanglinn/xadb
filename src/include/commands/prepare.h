/*-------------------------------------------------------------------------
 *
 * prepare.h
 *	  PREPARE, EXECUTE and DEALLOCATE commands, and prepared-stmt storage
 *
 *
 * Copyright (c) 2002-2021, PostgreSQL Global Development Group
 *
 * src/include/commands/prepare.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREPARE_H
#define PREPARE_H

#include "commands/explain.h"
#include "datatype/timestamp.h"
#include "utils/plancache.h"

/*
 * The data structure representing a prepared statement.  This is now just
 * a thin veneer over a plancache entry --- the main addition is that of
 * a name.
 *
 * Note: all subsidiary storage lives in the referenced plancache entry.
 */
typedef struct
{
	/* dynahash.c requires key to be first field */
	char		stmt_name[NAMEDATALEN];
	CachedPlanSource *plansource;	/* the actual cached plan */
	bool		from_sql;		/* prepared via SQL, not FE/BE protocol? */
	TimestampTz prepare_time;	/* the time when the stmt was prepared */
} PreparedStatement;

#ifdef ADB
typedef struct
{
	/* dynahash.c requires key to be first field */
	char		stmt_name[NAMEDATALEN];
	int			node_num;		/* number of nodes where statement is active */
	Oid 		node_ids[FLEXIBLE_ARRAY_MEMBER];	/* node ids where statement is active */
} DatanodeStatement;
#endif

/* Utility statements PREPARE, EXECUTE, DEALLOCATE, EXPLAIN EXECUTE */
extern void PrepareQuery(ParseState *pstate, PrepareStmt *stmt,
						 int stmt_location, int stmt_len);
extern void ExecuteQuery(ParseState *pstate,
						 ExecuteStmt *stmt, IntoClause *intoClause,
						 ParamListInfo params,
						 DestReceiver *dest, QueryCompletion *qc
						 ADB_ONLY_COMMA_ARG(bool cluster_safe));
extern void DeallocateQuery(DeallocateStmt *stmt);
extern void ExplainExecuteQuery(ExecuteStmt *execstmt, IntoClause *into,
								ExplainState *es, const char *queryString,
								ParamListInfo params, QueryEnvironment *queryEnv
								ADB_ONLY_COMMA_ARG(bool cluster_safe));

/* Low-level access to stored prepared statements */
extern void StorePreparedStatement(const char *stmt_name,
								   CachedPlanSource *plansource,
								   bool from_sql);
extern PreparedStatement *FetchPreparedStatement(const char *stmt_name,
												 bool throwError);
extern void DropPreparedStatement(const char *stmt_name, bool showError);
extern TupleDesc FetchPreparedStatementResultDesc(PreparedStatement *stmt);
extern List *FetchPreparedStatementTargetList(PreparedStatement *stmt);

extern void DropAllPreparedStatements(void);

#ifdef ADB
extern DatanodeStatement *FetchDatanodeStatement(const char *stmt_name, bool throwError);
extern bool ActivateDatanodeStatementOnNode(const char *stmt_name, Oid noid);
extern bool HaveActiveDatanodeStatements(void);
extern void DropDatanodeStatement(const char *stmt_name);
extern int SetRemoteStatementName(Plan *plan, const char *stmt_name, int num_params,
						Oid *param_types, int n);
#endif

#endif							/* PREPARE_H */
