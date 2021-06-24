/*-------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#include "nodes/parsenodes.h"
#include "parser/scanner.h"


/*
 * RawParseMode determines the form of the string that raw_parser() accepts:
 *
 * RAW_PARSE_DEFAULT: parse a semicolon-separated list of SQL commands,
 * and return a List of RawStmt nodes.
 *
 * RAW_PARSE_TYPE_NAME: parse a type name, and return a one-element List
 * containing a TypeName node.
 *
 * RAW_PARSE_PLPGSQL_EXPR: parse a PL/pgSQL expression, and return
 * a one-element List containing a RawStmt node.
 *
 * RAW_PARSE_PLPGSQL_ASSIGNn: parse a PL/pgSQL assignment statement,
 * and return a one-element List containing a RawStmt node.  "n"
 * gives the number of dotted names comprising the target ColumnRef.
 */
typedef enum
{
	RAW_PARSE_DEFAULT = 0,
	RAW_PARSE_TYPE_NAME,
	RAW_PARSE_PLPGSQL_EXPR,
	RAW_PARSE_PLPGSQL_ASSIGN1,
	RAW_PARSE_PLPGSQL_ASSIGN2,
	RAW_PARSE_PLPGSQL_ASSIGN3
} RawParseMode;

/* Values for the backslash_quote GUC */
typedef enum
{
	BACKSLASH_QUOTE_OFF,
	BACKSLASH_QUOTE_ON,
	BACKSLASH_QUOTE_SAFE_ENCODING
}			BackslashQuoteType;

/* GUC variables in scan.l (every one of these is a bad idea :-() */
extern int	backslash_quote;
extern bool escape_string_warning;
extern PGDLLIMPORT bool standard_conforming_strings;


/* Primary entry point for the raw parsing functions */
extern List *raw_parser(const char *str, RawParseMode mode);

#ifdef ADB_MULTI_GRAM
extern List *raw_parser_for_gram(const char *str, RawParseMode mode, ParseGrammar grammar);
#endif
#ifdef ADB_GRAM_ORA
extern List *ora_raw_parser(const char *str, RawParseMode mode);
#endif
#ifdef ADB_GRAM_DB2
extern List *db2_raw_parser(const char *str, RawParseMode mode);
#endif /* ADB_GRAM_DB2 */

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List *SystemFuncName(char *name);
extern TypeName *SystemTypeName(char *name);
#ifdef ADB_MULTI_GRAM
extern TypeName *SystemTypeNameLocation(char *name, int location);
#endif /* ADB_MULTI_GRAM */

/* move from gram.y */
/* Private struct for the result of opt_select_limit production */
typedef struct SelectLimit
{
	Node *limitOffset;
	Node *limitCount;
	LimitOption limitOption;
} SelectLimit;
extern RawStmt *makeRawStmt(Node *stmt, int stmt_location);
extern void updateRawStmtEnd(RawStmt *rs, int end_location);
extern Node *makeColumnRef(char *colname, List *indirection,
						   int location, core_yyscan_t yyscanner);
extern Node *makeTypeCast(Node *arg, TypeName *typename, int location);
extern Node *makeStringConst(char *str, int location);
extern Node *makeStringConstCast(char *str, int location, TypeName *typename);
extern Node *makeIntConst(int val, int location);
extern Node *makeFloatConst(char *str, int location);
extern Node *makeBitStringConst(char *str, int location);
extern Node *makeNullAConst(int location);
extern Node *makeAConst(Value *v, int location);
extern Node *makeBoolAConst(bool state, int location);
extern RoleSpec *makeRoleSpec(RoleSpecType type, int location);
extern void check_qualified_name(List *names, core_yyscan_t yyscanner);
extern List *check_func_name(List *names, core_yyscan_t yyscanner);
extern List *check_indirection(List *indirection, core_yyscan_t yyscanner);
extern List *extractArgTypes(ObjectType objtype, List *parameters);
extern List *extractAggrArgTypes(List *aggrargs);
extern List *makeOrderedSetArgs(List *directargs, List *orderedargs,
								core_yyscan_t yyscanner);
extern void insertSelectOptions(SelectStmt *stmt,
								List *sortClause, List *lockingClause,
								SelectLimit *limitClause,
								WithClause *withClause,
								core_yyscan_t yyscanner);
extern Node *makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg);
extern Node *doNegate(Node *n, int location);
extern void doNegateFloat(Value *v);
extern Node *makeAndExpr(Node *lexpr, Node *rexpr, int location);
extern Node *makeOrExpr(Node *lexpr, Node *rexpr, int location);
extern Node *makeNotExpr(Node *expr, int location);
extern Node *makeAArrayExpr(List *elements, int location);
extern Node *makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod,
								  int location);
extern Node *makeXmlExpr(XmlExprOp op, char *name, List *named_args,
						 List *args, int location);
extern List *mergeTableFuncParameters(List *func_args, List *columns);
extern TypeName *TableFuncTypeName(List *columns);
extern RangeVar *makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner);
extern Node *makeRecursiveViewSelect(char *relname, List *aliases, Node *query);
extern ResTarget* make_star_target(int location);
extern void SplitColQualList(List *qualList,
							 List **constraintList, CollateClause **collClause,
							 core_yyscan_t yyscanner);
/* end from gram.y */
#ifdef ADB_GRAM_ORA
extern List *check_sequence_name(List *names, core_yyscan_t yyscanner, int location);
#endif

#ifdef ADB_GRAM_ORA
extern List *OracleFuncName(char *name);
extern TypeName *OracleTypeName(char *name);
extern TypeName *OracleTypeNameLocation(char *name, int location);
#endif

#endif							/* PARSER_H */
