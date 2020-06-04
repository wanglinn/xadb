/*-------------------------------------------------------------------------
 *
 * tcopprot.h
 *	  prototypes for postgres.c.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/tcopprot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TCOPPROT_H
#define TCOPPROT_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/queryenvironment.h"


/* Required daylight between max_stack_depth and the kernel limit, in bytes */
#define STACK_DEPTH_SLOP (512 * 1024L)

extern CommandDest whereToSendOutput;
extern PGDLLIMPORT const char *debug_query_string;
extern int	max_stack_depth;
extern int	PostAuthDelay;
#ifdef ADB
typedef enum
{
	SQLTYPE_UNINIT,
	SQLTYPE_READ,
	SQLTYPE_WRITE
}SqlTypeLevel;
extern SqlTypeLevel sql_readonly;
#endif
/* GUC-configurable parameters */

typedef enum
{
	LOGSTMT_NONE,				/* log no statements */
	LOGSTMT_DDL,				/* log data definition statements */
	LOGSTMT_MOD,				/* log modification statements, plus DDL */
	LOGSTMT_ALL					/* log all statements */
} LogStmtLevel;

extern PGDLLIMPORT int log_statement;
#ifdef ADB_MULTI_GRAM
extern PGDLLIMPORT int parse_grammar;
extern PGDLLIMPORT int current_grammar;

#define IsCurrentOracleGram() (current_grammar == PARSE_GRAM_ORACLE)
#endif

extern List *pg_parse_query(const char *query_string);
extern List *pg_analyze_and_rewrite(RawStmt *parsetree,
									const char *query_string,
									Oid *paramTypes, int numParams,
									QueryEnvironment *queryEnv);
#ifdef ADB_GRAM_ORA
extern List *ora_parse_query(const char *query_string);
#endif
#ifdef ADB_GRAM_DB2
extern List *db2_parse_query(const char *query_string);
#endif

#ifdef ADB_MULTI_GRAM
extern List *pg_analyze_and_rewrite_for_gram(RawStmt *parsetree, const char *query_string,
											 Oid *paramTypes, int numParams,
											 QueryEnvironment *queryEnv, ParseGrammar grammar);
extern List *parse_query_auto_gram(const char *query_string, ParseGrammar *gram);
extern List *parse_query_for_gram(const char *query_string, ParseGrammar grammer);
#endif

#ifdef ADBMGRD
extern List *mgr_parse_query(const char *query_string);
#endif /* ADBMGRD */
extern List *pg_analyze_and_rewrite_params(RawStmt *parsetree,
										   const char *query_string,
										   ParserSetupHook parserSetup,
										   void *parserSetupArg,
										   QueryEnvironment *queryEnv);
#ifdef ADB_MULTI_GRAM
extern List *pg_analyze_and_rewrite_params_for_gram(RawStmt *parsetree,
													const char *query_string,
													ParserSetupHook parserSetup,
													void *parserSetupArg,
													QueryEnvironment *queryEnv, ParseGrammar grammar);
#endif /* ADB_MULTI_GRAM */
extern PlannedStmt *pg_plan_query(Query *querytree, int cursorOptions,
								  ParamListInfo boundParams);
extern List *pg_plan_queries(List *querytrees, int cursorOptions,
							 ParamListInfo boundParams);

extern bool check_max_stack_depth(int *newval, void **extra, GucSource source);
extern void assign_max_stack_depth(int newval, void *extra);

extern void die(SIGNAL_ARGS);
extern void quickdie(SIGNAL_ARGS) pg_attribute_noreturn();
extern void StatementCancelHandler(SIGNAL_ARGS);
extern void FloatExceptionHandler(SIGNAL_ARGS) pg_attribute_noreturn();
extern void RecoveryConflictInterrupt(ProcSignalReason reason); /* called from SIGUSR1
																 * handler */
extern void ProcessClientReadInterrupt(bool blocked);
extern void ProcessClientWriteInterrupt(bool blocked);

extern void process_postgres_switches(int argc, char *argv[],
									  GucContext ctx, const char **dbname);
extern void PostgresMain(int argc, char *argv[],
						 const char *dbname,
						 const char *username) pg_attribute_noreturn();
extern long get_stack_depth_rlimit(void);
extern void ResetUsage(void);
extern void ShowUsage(const char *title);
extern int	check_log_duration(char *msec_str, bool was_logged);
extern void set_debug_options(int debug_flag,
							  GucContext context, GucSource source);
extern bool set_plan_disabling_options(const char *arg,
									   GucContext context, GucSource source);
extern const char *get_stats_option_name(const char *arg);

#endif							/* TCOPPROT_H */
