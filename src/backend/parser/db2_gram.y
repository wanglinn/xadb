%{

/*#define YYDEBUG 1*/
/*-------------------------------------------------------------------------
 *
 * gram.y
 *	  POSTGRESQL BISON rules/actions
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2014-2019, ADB Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/db2_gram.y
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Sept, 1994		POSTQUEL to SQL conversion
 *	  Andrew Yu			Oct, 1994		lispy code conversion
 *
 * NOTES
 *	  CAPITALS are used to represent terminal symbols.
 *	  non-capitals are used to represent non-terminals.
 *
 *	  In general, nothing in this file should initiate database accesses
 *	  nor depend on changeable state (such as SET variables).  If you do
 *	  database accesses, your code will fail when we have aborted the
 *	  current transaction and are just parsing commands to find the next
 *	  ROLLBACK or COMMIT.  If you make use of SET variables, then you
 *	  will do the wrong thing in multi-query strings like this:
 *			SET constraint_exclusion TO off; SELECT * FROM foo;
 *	  because the entire string is parsed by gram.y before the SET gets
 *	  executed.  Anything that depends on the database or changeable state
 *	  should be handled during parse analysis so that it happens at the
 *	  right time not the wrong time.
 *
 * WARNINGS
 *	  If you use a list, make sure the datum is a node so that the printing
 *	  routines work.
 *
 *	  Sometimes we assign constants to makeStrings. Make sure we don't free
 *	  those.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "parser/db2_gramparse.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "storage/lmgr.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/xml.h"
#include "nodes/makefuncs.h"


/*
 * Location tracking support --- simpler than bison's default, since we only
 * want to track the start position not the end position of each nonterminal.
 */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if ((N) > 0) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (-1); \
	} while (0)

/*
 * The above macro assigns -1 (unknown) as the parse location of any
 * nonterminal that was reduced from an empty rule, or whose leftmost
 * component was reduced from an empty rule.  This is problematic
 * for nonterminals defined like
 *		OptFooList: / * EMPTY * / { ... } | OptFooList Foo { ... } ;
 * because we'll set -1 as the location during the first reduction and then
 * copy it during each subsequent reduction, leaving us with -1 for the
 * location even when the list is not empty.  To fix that, do this in the
 * action for the nonempty rule(s):
 *		if (@$ < 0) @$ = @2;
 * (Although we have many nonterminals that follow this pattern, we only
 * bother with fixing @$ like this when the nonterminal's parse location
 * is actually referenced in some rule.)
 *
 * A cleaner answer would be to make YYLLOC_DEFAULT scan all the Rhs
 * locations until it's found one that's not -1.  Then we'd get a correct
 * location for any nonterminal that isn't entirely empty.  But this way
 * would add overhead to every rule reduction, and so far there's not been
 * a compelling reason to pay that overhead.
 */

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)
#define ereport_pos(str, n)	ereport(ERROR,											\
								(errcode(ERRCODE_SYNTAX_ERROR),						\
								 errmsg("syntax error at or near \"%s\"", str),		\
								 parser_errposition(n)))



#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)
#define pg_yyget_extra(yyscanner) (*((base_yy_extra_type **) (yyscanner)))

union YYSTYPE;					/* need forward reference for tok_is_keyword */
static void db2_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg);
static int db2_yylex(union YYSTYPE *lvalp, YYLTYPE *lloc, core_yyscan_t yyscanner);

%}

%pure-parser
%expect 0
%name-prefix="db2_yy"
%locations

%parse-param {core_yyscan_t yyscanner}
%lex-param   {core_yyscan_t yyscanner}

%union
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs		*objwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
	PartitionElem		*partelem;
	PartitionSpec		*partspec;
	PartitionBoundSpec	*partboundspec;
	RoleSpec			*rolespec;
/* ADB_BEGIN */
#ifdef ADB
	DistributeBy		*distby;
	PGXCSubCluster		*subclus;
#endif
/* ADB_END */
}

/* ADB_BEGIN */
%type <keyword>	unreserved_keyword
/* ADB_END */
%type <keyword> reserved_keyword
%type <list>	stmtblock stmtmulti
%type <node>	stmt
%type <str>		ColLabel ColId NonReservedWord type_function_name

%type <node>	RenameStmt
%type <range> 	relation_expr qualified_name
%type <str> 	name
/*
 * Non-keyword token types.  These are hard-wired into the "flex" lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  PL/pgSQL depends on this so that it can share the
 * same lexer.  If you add/change tokens here, fix PL/pgSQL to match!
 *
 * DOT_DOT is unused in the core SQL grammar, and so will always provoke
 * parse errors.  It is needed by PL/pgSQL.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token			TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token			LESS_EQUALS GREATER_EQUALS NOT_EQUALS

/*
 * If you want to make any keyword changes, update the keyword table in
 * src/include/parser/kwlist.h and add new keywords to the appropriate one
 * of the reserved-or-not-so-reserved keyword lists, below; search
 * this file for "Keyword category lists".
 */

/* ordinary key words in alphabetical order */
%token <keyword> ADD_P AFTER ALL ALLOCATE ALLOW ALTER AND ANY AS
	ARRAY1 ARRAY_EXISTS1 
	ASENSITIVE ASSOCIATE ASUTIME AT AUDIT AUX AUXILIARY

	BEFORE BEGIN_P BETWEEN BUFFERPOOL BY

	CALL CAPTURE CASCADED CASE CAST CCSID CHAR_P CHARACTER CHECK CLONE CLOSE
	CLUSTER COLLECTION COLLID COLUMN COMMENT COMMIT CONCAT CONDITION CONNECT
	CONNECTION CONSTRAINT CONTAINS CONTENT_P CONTINUE_P CREATE CUBE CURRENT_P
	CURRENT_DATE CURRENT_LC_CTYPE CURRENT_PATH CURRENT_SCHEMA CURRENT_TIME
	CURRENT_TIMESTAMP CURRVAL CURSOR

	DATA_P DATABASE DAY_P DAYS DBINFO DECLARE DEFAULT DELETE_P DESCRIPTOR
	DETERMINISTIC DISABLE DISALLOW DISTINCT DO DOCUMENT_P DOUBLE_P DROP DSSIZE
	DYNAMIC

	EDITPROC ELSE ELSEIF ENCODING ENCRYPTION END_P ENDING //END-EXEC2
	ERASE ESCAPE EXCEPT EXCEPTION EXECUTE EXISTS EXIT EXPLAIN EXTERNAL

	FENCED FETCH FIELDPROC FINAL FIRST FOR FREE FROM FULL FUNCTION

	GENERATED GET GLOBAL GO GOTO GRANT GROUP_P

	HANDLER HAVING HOLD HOUR_P HOURS

	IF_P IMMEDIATE IN_P INCLUSIVE INDEX INHERIT INNER_P INOUT INSENSITIVE INSERT
	INTERSECT INTO IS ISOBID ITERATE

	JAR JOIN

	KEEP KEY

	LABEL LANGUAGE LAST_P LC_CTYPE_P LEAVE LEFT LIKE LOCAL LOCALE LOCATOR
	LOCATORS LOCK_P LOCKMAX LOCKSIZE LONG LOOP

	MAINTAINED MATERIALIZED MICROSECOND_P MICROSECONDS MINUTEMINUTES MODIFIES
	MONTH_P MONTHS

	NEXT NEXTVAL NO NONE NOT NULL_P NULLS_P NUMPARTS

	OBID OF OLD ON OPEN OPTIMIZATION OPTIMIZE OR ORDER ORGANIZATION
	OUT_P OUTER_P

	PACKAGE PARAMETER PART PADDED PARTITION PARTITIONED PARTITIONING PATH
	PIECESIZE PERIOD PLAN PRECISION PREPARE PREVVAL PRIOR PRIQTY PRIVILEGES
	PROCEDURE PROGRAM PSID PUBLIC

	QUERY QUERYNO

	READS REFERENCES REFRESH RESIGNAL RELEASE RENAME REPEAT RESTRICT RESULT
	RESULT_SET_LOCATOR RETURN RETURNS REVOKE RIGHT ROLE ROLLBACK ROLLUP1
	ROUND_CEILING ROUND_DOWN ROUND_FLOOR ROUND_HALF_DOWN ROUND_HALF_EVEN
	ROUND_HALF_UP ROUND_UP ROW ROWSET RUN

	SAVEPOINT SCHEMA SCRATCHPAD SECOND_P SECONDS SECQTY SECURITY SEQUENCE SELECT
	SENSITIVE SESSION_USER SET SIGNAL SIMPLE SOME SOURCE SPECIFIC STANDARD
	STATIC STATEMENT STAY STOGROUP STORES STYLE SUMMARY SYNONYM SYSDATE
	SYSTEM_P SYSTIMESTAMP

	TABLE TABLESPACE THEN TO TRIGGER TRUNCATE TYPE_P

	UNDO UNION UNIQUE UNTIL UPDATE USER USING

	VALIDPROC VALUE_P VALUES VARIABLE VARIANT VCAT VERSIONING1 VIEW VOLATILE
	VOLUMES

	WHEN WHENEVER WHERE WHILE WITH WLM

	XMLEXISTS XMLNAMESPACES XMLCAST

	YEAR_P YEARS

	ZONE

/* ADB_BEGIN */
	DIRECT DISTRIBUTE NODE
/* ADB_END */

%%

/*
 *	The target production for the whole parse.
 */
stmtblock:	stmtmulti
			{
				db2_yyget_extra(yyscanner)->parsetree = $1;
			}
		;

/*
 * At top level, we wrap each stmt with a RawStmt node carrying start location
 * and length of the stmt's text.  Notice that the start loc/len are driven
 * entirely from semicolon locations (@2).  It would seem natural to use
 * @1 or @3 to get the true start location of a stmt, but that doesn't work
 * for statements that can start with empty nonterminals (opt_with_clause is
 * the main offender here); as noted in the comments for YYLLOC_DEFAULT,
 * we'd get -1 for the location in such cases.
 * We also take care to discard empty statements entirely.
 */
stmtmulti:	stmtmulti ';' stmt
				{
					if ($1 != NIL)
					{
						/* update length of previous stmt */
						updateRawStmtEnd(llast_node(RawStmt, $1), @2);
					}
					if ($3 != NULL)
						$$ = lappend($1, makeRawStmt($3, @2 + 1));
					else
						$$ = $1;
				}
			| stmt
				{
					if ($1 != NULL)
						$$ = list_make1(makeRawStmt($1, 0));
					else
						$$ = NIL;
				}
		;

stmt:
			  /* just let bison no warning */
			  ColLabel ColId type_function_name NonReservedWord
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("syntax error")));
				}
			| RenameStmt
			| /*EMPTY*/
				{ $$ = NULL; }
		;


/*****************************************************************************
 *
 *		QUERY : 
 *				RENAME TABLE old_table_name to new_table_name
 *
 *****************************************************************************/


RenameStmt: RENAME TABLE relation_expr TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $5;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;
relation_expr:
			qualified_name
				{
					/* inheritance query, implicitly */
					$$ = $1;
					$$->inh = true;
					$$->alias = NULL;
				}
		;
qualified_name:
		  ColId { $$ = makeRangeVar(NULL, $1, @1); }
		;
name:		ColId { $$ = $1; };

/*
 * Name classification hierarchy.
 *
 * IDENT is the lexeme returned by the lexer for identifiers that match
 * no known keyword.  In most cases, we can accept certain keywords as
 * names, not only IDENTs.	We prefer to accept as many such keywords
 * as possible to minimize the impact of "reserved words" on programmers.
 * So, we divide names into several possible classes.  The classification
 * is chosen in part to make keywords acceptable as names wherever possible.
 */

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:		IDENT									{ $$ = $1; }
/* ADB_BEGIN */
			| unreserved_keyword					{ $$ = pstrdup($1); }
/* ADB_END */
		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	IDENT							{ $$ = $1; }
/* ADB_BEGIN */
			| unreserved_keyword					{ $$ = pstrdup($1); }
/* ADB_END */
		;

/* Any not-fully-reserved word --- these names can be, eg, role names.
 */
NonReservedWord:	IDENT							{ $$ = $1; }
/* ADB_BEGIN */
			| unreserved_keyword					{ $$ = pstrdup($1); }
/* ADB_END */
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENT									{ $$ = $1; }
/* ADB_BEGIN */
			| unreserved_keyword					{ $$ = pstrdup($1); }
/* ADB_END */
			| reserved_keyword						{ $$ = pstrdup($1); }
		;


/*
 * Keyword category lists.  Generally, every keyword present in
 * the Postgres grammar should appear in exactly one of these lists.
 *
 * Put a new keyword into the first list that it can go into without causing
 * shift or reduce conflicts.  The earlier lists define "less reserved"
 * categories of keywords.
 *
 * Make sure that each keyword's category in kwlist.h matches where
 * it is listed here.  (Someday we may be able to generate these lists and
 * kwlist.h's table from a common master list.)
 */

/* "Unreserved" keywords --- available for use as any kind of name.
 */
/* ADB_BEGIN */
unreserved_keyword:
			  DIRECT
			| DISTRIBUTE
		;
/* ADB_END */

/* Reserved keyword --- these keywords are usable only as a ColLabel.
 *
 * Keywords appear here if they could not be distinguished from variable,
 * type, or function names in some contexts.  Don't put things here unless
 * forced to.
 */
reserved_keyword:
			  ADD_P
			| AFTER
			| ALL
			| ALLOCATE
			| ALLOW
			| ALTER
			| AND
			| ANY
			| AS
			| ARRAY1
			| ARRAY_EXISTS1
			| ASENSITIVE
			| ASSOCIATE
			| ASUTIME
			| AT
			| AUDIT
			| AUX
			| AUXILIARY
			| BEFORE
			| BEGIN_P
			| BETWEEN
			| BUFFERPOOL
			| BY
			| CALL
			| CAPTURE
			| CASCADED
			| CASE
			| CAST
			| CCSID
			| CHAR_P
			| CHARACTER
			| CHECK
			| CLONE
			| CLOSE
			| CLUSTER
			| COLLECTION
			| COLLID
			| COLUMN
			| COMMENT
			| COMMIT
			| CONCAT
			| CONDITION
			| CONNECT
			| CONNECTION
			| CONSTRAINT
			| CONTAINS
			| CONTENT_P
			| CONTINUE_P
			| CREATE
			| CUBE
			| CURRENT_P
			| CURRENT_DATE
			| CURRENT_LC_CTYPE
			| CURRENT_PATH
			| CURRENT_SCHEMA
			| CURRENT_TIME
			| CURRENT_TIMESTAMP
			| CURRVAL
			| CURSOR
			| DATA_P
			| DATABASE
			| DAY_P
			| DAYS
			| DBINFO
			| DECLARE
			| DEFAULT
			| DELETE_P
			| DESCRIPTOR
			| DETERMINISTIC
			| DISABLE
			| DISALLOW
			| DISTINCT
			| DO
			| DOCUMENT_P
			| DOUBLE_P
			| DROP
			| DSSIZE
			| DYNAMIC
			| EDITPROC
			| ELSE
			| ELSEIF
			| ENCODING
			| ENCRYPTION
			| END_P
			| ENDING
			| ERASE
			| ESCAPE
			| EXCEPT
			| EXCEPTION
			| EXECUTE
			| EXISTS
			| EXIT
			| EXPLAIN
			| EXTERNAL
			| FENCED
			| FETCH
			| FIELDPROC
			| FINAL
			| FIRST
			| FOR
			| FREE
			| FROM
			| FULL
			| FUNCTION
			| GENERATED
			| GET
			| GLOBAL
			| GO
			| GOTO
			| GRANT
			| GROUP_P
			| HANDLER
			| HAVING
			| HOLD
			| HOUR_P
			| HOURS
			| IF_P
			| IMMEDIATE
			| IN_P
			| INCLUSIVE
			| INDEX
			| INHERIT
			| INNER_P
			| INOUT
			| INSENSITIVE
			| INSERT
			| INTERSECT
			| INTO
			| IS
			| ISOBID
			| ITERATE
			| JAR
			| JOIN
			| KEEP
			| KEY
			| LABEL
			| LANGUAGE
			| LAST_P
			| LC_CTYPE_P
			| LEAVE
			| LEFT
			| LIKE
			| LOCAL
			| LOCALE
			| LOCATOR
			| LOCATORS
			| LOCK_P
			| LOCKMAX
			| LOCKSIZE
			| LONG
			| LOOP
			| MAINTAINED
			| MATERIALIZED
			| MICROSECOND_P
			| MICROSECONDS
			| MINUTEMINUTES
			| MODIFIES
			| MONTH_P
			| MONTHS
			| NEXT
			| NEXTVAL
			| NO
			| NONE
			| NOT
			| NULL_P
			| NULLS_P
			| NUMPARTS
			| OBID
			| OF
			| OLD
			| ON
			| OPEN
			| OPTIMIZATION
			| OPTIMIZE
			| OR
			| ORDER
			| ORGANIZATION
			| OUT_P
			| OUTER_P
			| PACKAGE
			| PARAMETER
			| PART
			| PADDED
			| PARTITION
			| PARTITIONED
			| PARTITIONING
			| PATH
			| PIECESIZE
			| PERIOD
			| PLAN
			| PRECISION
			| PREPARE
			| PREVVAL
			| PRIOR
			| PRIQTY
			| PRIVILEGES
			| PROCEDURE
			| PROGRAM
			| PSID
			| PUBLIC
			| QUERY
			| QUERYNO
			| READS
			| REFERENCES
			| REFRESH
			| RESIGNAL
			| RELEASE
			| RENAME
			| REPEAT
			| RESTRICT
			| RESULT
			| RESULT_SET_LOCATOR
			| RETURN
			| RETURNS
			| REVOKE
			| RIGHT
			| ROLE
			| ROLLBACK
			| ROLLUP1
			| ROUND_CEILING
			| ROUND_DOWN
			| ROUND_FLOOR
			| ROUND_HALF_DOWN
			| ROUND_HALF_EVEN
			| ROUND_HALF_UP
			| ROUND_UP
			| ROW
			| ROWSET
			| RUN
			| SAVEPOINT
			| SCHEMA
			| SCRATCHPAD
			| SECOND_P
			| SECONDS
			| SECQTY
			| SECURITY
			| SEQUENCE
			| SELECT
			| SENSITIVE
			| SESSION_USER
			| SET
			| SIGNAL
			| SIMPLE
			| SOME
			| SOURCE
			| SPECIFIC
			| STANDARD
			| STATIC
			| STATEMENT
			| STAY
			| STOGROUP
			| STORES
			| STYLE
			| SUMMARY
			| SYNONYM
			| SYSDATE
			| SYSTEM_P
			| SYSTIMESTAMP
			| TABLE
			| TABLESPACE
			| THEN
			| TO
			| TRIGGER
			| TRUNCATE
			| TYPE_P
			| UNDO
			| UNION
			| UNIQUE
			| UNTIL
			| UPDATE
			| USER
			| USING
			| VALIDPROC
			| VALUE_P
			| VALUES
			| VARIABLE
			| VARIANT
			| VERSIONING1
			| VIEW
			| VOLATILE
			| VOLUMES
			| WHEN
			| WHENEVER
			| WHERE
			| WHILE
			| WITH
			| WLM
			| XMLEXISTS
			| XMLNAMESPACES
			| XMLCAST
			| YEAR_P
			| YEARS
			| ZONE
		;

%%

/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
db2_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

/* parser_init()
 * Initialize to parse one query string
 */
void
db2_parser_init(db2_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;		/* in case grammar forgets to set it */
}

static int db2_yylex(union YYSTYPE *lvalp, YYLTYPE *lloc, core_yyscan_t yyscanner)
{
	return core_yylex(&(lvalp->core_yystype), lloc, yyscanner);
}