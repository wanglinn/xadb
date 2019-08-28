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

#include "catalog/namespace.h"
#include "parser/db2_gramparse.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "parser/parse_expr.h"
#include "storage/lmgr.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/xml.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"

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

typedef struct OraclePartitionSpec
{
	PartitionSpec	   *partitionSpec;
	List			   *children;		/* list of CreateStmt */
}OraclePartitionSpec;

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
	OraclePartitionSpec		*partspec;
	PartitionBoundSpec	*partboundspec;
	RoleSpec			*rolespec;
/* ADB_BEGIN */
#ifdef ADB
	PGXCSubCluster		*subclus;
#endif
/* ADB_END */
}

%type <keyword>	unreserved_keyword
%type <keyword> reserved_keyword col_name_keyword
%type <list>	stmtblock stmtmulti
%type <node>	stmt
%type <str>		ColLabel ColId type_function_name 

%type <node>	RenameStmt
%type <range> 	relation_expr qualified_name
%type <str> 	name

%type <node>	CreateStmt TableElement columnDef generic_option_arg ColConstraint ColConstraintElem
				def_arg columnElem ConstraintAttr a_expr b_expr c_expr columnref indirection_el
				opt_slice_bound AexprConst ora_part_child
%type <list>	OptTableElementList TableElementList type_list opt_array_bounds opt_interval
				create_generic_options generic_option_list ColQualList
				opt_definition definition def_list qual_all_Op any_operator OptParenthesizedSeqOptList
				SeqOptList any_name opt_column_list columnList indirection opt_indirection expr_list
				interval_second opt_type_modifiers qual_Op attrs qualified_name_list OptInherit part_params
				opt_collate opt_class ora_part_child_list reloptions reloption_list OptWith
%type <typnam>	Typename SimpleTypename Numeric Bit ConstDatetime ConstInterval ConstTypename ConstBit
				BitWithLength BitWithoutLength Character ConstCharacter CharacterWithLength CharacterWithoutLength
				opt_float func_type
%type <ival>	Iconst generated_when SignedIconst key_match key_actions key_update key_delete OptTemp
				key_action 
%type <str>		Sconst character generic_option_name all_Op MathOp OptConsTableSpace attr_name
				part_strategy OptTableSpace
%type <boolean>	opt_timezone opt_no_inherit opt_varying 
%type <defelt>	def_elem SeqOptElem generic_option_elem reloption_elem
%type <value>	NumericOnly
%type <partspec>	PartitionSpec OptPartitionSpec
%type <partelem>	part_elem
%type <partboundspec> ForValues
%type <oncommit> OnCommitOption

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
	LOCATORS LOCK_P LOCKMAX LOCKSIZE LONG_P LOOP

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

%token <keyword> ACTION ALWAYS ARRAY BIGINT BIT BOOLEAN_P CACHE CASCADE COLLATE 
				CYCLE DEC DECIMAL_P DEFERRABLE DEFERRED FALSE_P FLOAT_P IDENTITY_P 
				INCREMENT INHERITS INITIALLY INT_P INTEGER INTERVAL MATCH MAXVALUE MINUTE_P 
				MINVALUE NAME_P NATIONAL NCHAR NUMERIC OIDS OPERATOR OPTIONS OWNED PARTIAL 
				PRIMARY POSTFIXOP REAL RESTART SETOF SMALLINT START STARTING TEMP TEMPORARY TIME 
				TIMESTAMP TRUE_P UNLOGGED VARCHAR VARYING WITH_LA WITHOUT UMINUS CLOB DBCLOB BLOB RANGE
				NUMBER_P DATE_P
				THAN LESS /* ora */


/* Precedence: lowest to highest */
/* ADB_BEGIN */
%nonassoc	TABLESPACE		/* see aux_opt_index_name */
/* ADB_END */
%nonassoc	SET				/* see relation_expr_opt_alias */
%left		UNION EXCEPT
%left		INTERSECT
%left		OR
%left		AND
%right		NOT
%nonassoc	IS ISNULL NOTNULL	/* IS sets precedence for IS NULL, etc */
%nonassoc	'<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc	BETWEEN IN_P LIKE ILIKE SIMILAR NOT_LA
%nonassoc	ESCAPE			/* ESCAPE must be just above LIKE/ILIKE/SIMILAR */
%left		POSTFIXOP		/* dummy for postfix Op rules */
/*
 * To support target_el without AS, we must give IDENT an explicit priority
 * between POSTFIXOP and Op.  We can safely assign the same priority to
 * various unreserved keywords as needed to resolve ambiguities (this can't
 * have any bad effects since obviously the keywords will still behave the
 * same as if they weren't keywords).  We need to do this for PARTITION,
 * RANGE, ROWS to support opt_existing_window_name; and for RANGE, ROWS
 * so that they can follow a_expr without creating postfix-operator problems;
 * for GENERATED so that it can follow b_expr;
 * and for NULL so that it can follow b_expr in ColQualList without creating
 * postfix-operator problems.
 *
 * To support CUBE and ROLLUP in GROUP BY without reserving them, we give them
 * an explicit priority lower than '(', so that a rule with CUBE '(' will shift
 * rather than reducing a conflicting rule that takes CUBE as a function name.
 * Using the same precedence as IDENT seems right for the reasons given above.
 *
 * The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
 * are even messier: since UNBOUNDED is an unreserved keyword (per spec!),
 * there is no principled way to distinguish these from the productions
 * a_expr PRECEDING/FOLLOWING.  We hack this up by giving UNBOUNDED slightly
 * lower precedence than PRECEDING and FOLLOWING.  At present this doesn't
 * appear to cause UNBOUNDED to be treated differently from other unreserved
 * keywords anywhere else in the grammar, but it's definitely risky.  We can
 * blame any funny behavior of UNBOUNDED on the SQL standard, though.
 */
%nonassoc	UNBOUNDED		/* ideally should have same precedence as IDENT */
%nonassoc	IDENT GENERATED NULL_P PARTITION RANGE ROWS PRECEDING FOLLOWING CUBE ROLLUP
%left		Op OPERATOR		/* multi-character ops and user-defined operators */
%left		'+' '-'
%left		'*' '/' '%'
%left		'^'
/* Unary Operators */
%left		AT				/* sets precedence for AT TIME ZONE */
%left		COLLATE
%right		UMINUS
%left		'[' ']'
%left		'(' ')'
%left		TYPECAST
%left		'.'
/*
 * These might seem to be low-precedence, but actually they are not part
 * of the arithmetic hierarchy at all in their use as JOIN operators.
 * We make them high-precedence to support their use as function names.
 * They wouldn't be given a precedence at all, were it not that we need
 * left-associativity among the JOIN rules themselves.
 */
%left		JOIN CROSS LEFT FULL RIGHT INNER_P NATURAL
/* kluge to keep xml_whitespace_option from causing shift/reduce conflicts */
%right		PRESERVE STRIP_P

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
			
			 CreateStmt
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
/*
 * The production for a qualified relation name has to exactly match the
 * production for a qualified func_name, because in a FROM clause we cannot
 * tell which we are parsing until we see what comes after it ('(' for a
 * func_name, something else for a relation). Therefore we allow 'indirection'
 * which may contain subscripts, and reject that case in the C code.
 */
qualified_name:
			ColId
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| ColId indirection
				{
					check_qualified_name($2, yyscanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))),
									 parser_errposition(@1)));
							break;
					}
				}
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
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			//| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENT	
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| reserved_keyword						{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			//| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 			CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptPartitionSpec OptWith OnCommitOption OptTableSpace
			OptDistributeBy OptSubCluster

 *****************************************************************************/

CreateStmt:	CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptPartitionSpec
				{
					CreateStmt *n = makeNode(CreateStmt);
#if defined(ADB_MULTI_GRAM)
					n->grammar = PARSE_GRAM_DB2;
#endif /* ADB */
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = NIL;
					n->partspec = NULL;
					n->ofTypename = NULL;
					n->constraints = NIL;
					n->options = NIL;
					n->oncommit = ONCOMMIT_NOOP;
					n->tablespacename = NULL;
					n->if_not_exists = false;
/* ADB_BEGIN */
					n->distributeby = NULL;
					n->subcluster = NULL;
					if (n->inhRelations != NIL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* ADB_END */
					if ($9)
					{
						n->partspec = $9->partitionSpec;
						n->child_rels = $9->children;
					}
					$$ = (Node *)n;
				}
			;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior, so warn about that.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 */
OptTemp:	TEMPORARY					{ $$ = RELPERSISTENCE_TEMP; }
			| TEMP						{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMPORARY			{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMP				{ $$ = RELPERSISTENCE_TEMP; }
			| GLOBAL TEMPORARY
				{
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMP
				{
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = RELPERSISTENCE_TEMP;
				}
			| UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;

OptTableElementList:
			TableElementList					{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;
TableElementList:
			TableElement
				{
					$$ = list_make1($1);
				}
			| TableElementList ',' TableElement
				{
					$$ = lappend($1, $3);
				}
		;
TableElement:
			columnDef							{ $$ = $1; }
			// | TableLikeClause					{ $$ = $1; }
			// | TableConstraint					{ $$ = $1; }
		;
columnDef:	ColId Typename create_generic_options ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $3;
					SplitColQualList($4, &n->constraints, &n->collClause,
									 yyscanner);
					n->location = @1;
					$$ = (Node *)n;
				}
		;
/*****************************************************************************
 *
 *	Type syntax
 *		SQL introduces a large amount of type-specific syntax.
 *		Define individual clauses to handle these cases, and use
 *		 the generic case to handle regular type-extensible Postgres syntax.
 *		- thomas 1997-10-10
 *
 *****************************************************************************/

Typename:	SimpleTypename opt_array_bounds
				{
					$$ = $1;
					$$->arrayBounds = $2;
				}
			| SETOF SimpleTypename opt_array_bounds
				{
					$$ = $2;
					$$->arrayBounds = $3;
					$$->setof = true;
				}
			/* SQL standard syntax, currently only one-dimensional */
			| SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger($4));
				}
			| SETOF SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger($5));
					$$->setof = true;
				}
			| SimpleTypename ARRAY
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger(-1));
				}
			| SETOF SimpleTypename ARRAY
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger(-1));
					$$->setof = true;
				}
		;
type_list:	Typename								{ $$ = list_make1($1); }
			| type_list ',' Typename				{ $$ = lappend($1, $3); }
		;
opt_array_bounds:
			opt_array_bounds '[' ']'
					{  $$ = lappend($1, makeInteger(-1)); }
			| opt_array_bounds '[' Iconst ']'
					{  $$ = lappend($1, makeInteger($3)); }
			| /*EMPTY*/
					{  $$ = NIL; }
		;
SimpleTypename:
			//GenericType								{ $$ = $1; }
			Numeric									{ $$ = $1; }
			| Bit									{ $$ = $1; }
			| Character								{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
			| ConstInterval opt_interval
				{
					$$ = $1;
					$$->typmods = $2;
				}
			| ConstInterval '(' Iconst ')'
				{
					$$ = $1;
					$$->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											 makeIntConst($3, @3));
				}
		;
Iconst:		ICONST									{ $$ = $1; };
Sconst:		SCONST									{ $$ = $1; };
/*
 * SQL numeric data types
 */
Numeric:	INT_P
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| INTEGER
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| SMALLINT
				{
					$$ = SystemTypeName("int2");
					$$->location = @1;
				}
			| BIGINT
				{
					$$ = SystemTypeName("int8");
					$$->location = @1;
				}
			| REAL
				{
					$$ = SystemTypeName("float4");
					$$->location = @1;
				}
			| FLOAT_P opt_float
				{
					$$ = $2;
					$$->location = @1;
				}
			| DOUBLE_P
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
			| DOUBLE_P PRECISION
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
			| DECIMAL_P opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| DEC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| NUMERIC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| BOOLEAN_P
				{
					$$ = SystemTypeName("bool");
					$$->location = @1;
				}
			| NUMBER_P opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
		;
/*
 * SQL bit-field data types
 * The following implements BIT() and BIT VARYING().
 */
Bit:		BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
				}
			| BLOB					
				{ $$ = SystemTypeNameLocation("bytea", @1); }
		;
/*
 * SQL date/time types
 */
ConstDatetime:
			DATE_P
				{
					$$ = OracleTypeNameLocation("date", @1);
				}
			| TIMESTAMP '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIMESTAMP opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					$$->location = @1;
				}
			| TIME '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIME opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->location = @1;
				}
		;
opt_timezone:
			WITH_LA TIME ZONE						{ $$ = true; }
			| WITHOUT TIME ZONE						{ $$ = false; }
			| /*EMPTY*/								{ $$ = false; }
		;
ConstInterval:
			INTERVAL
				{
					$$ = SystemTypeName("interval");
					$$->location = @1;
				}
		;
opt_interval:
			YEAR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR), @1)); }
			| MONTH_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MONTH), @1)); }
			| DAY_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY), @1)); }
			| HOUR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR), @1)); }
			| MINUTE_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MINUTE), @1)); }
			| interval_second
				{ $$ = $1; }
			| YEAR_P TO MONTH_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
												 INTERVAL_MASK(MONTH), @1));
				}
			| DAY_P TO HOUR_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR), @1));
				}
			| DAY_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| DAY_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| HOUR_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| HOUR_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| MINUTE_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;
/* Options definition for CREATE FDW, SERVER and USER MAPPING */
create_generic_options:
			OPTIONS '(' generic_option_list ')'			{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NIL; }
		;

generic_option_list:
			generic_option_elem
				{
					$$ = list_make1($1);
				}
			| generic_option_list ',' generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;
generic_option_elem:
			generic_option_name generic_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;
generic_option_name:
				ColLabel			{ $$ = $1; }
		;
/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
				Sconst				{ $$ = (Node *) makeString($1); }
		;
ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;
ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					Constraint *n = castNode(Constraint, $3);
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
			| COLLATE any_name
				{
					/*
					 * Note: the CollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
		;
/* DEFAULT NULL is already the default for Postgres.
 * But define it here and carry it forward into the system
 * to make it explicit.
 * - thomas 1998-09-13
 *
 * WITH NULL and NULL are not SQL-standard syntax elements,
 * so leave them out. Use DEFAULT NULL to explicitly indicate
 * that a column may have that value. WITH NULL leads to
 * shift/reduce conflicts with WITH TIME ZONE anyway.
 * - thomas 1999-01-08
 *
 * DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
 * conflict on NOT (since NOT might start a subsequent NOT NULL constraint,
 * or be part of a_expr NOT LIKE or similar constructs).
 */
ColConstraintElem:
			NOT NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					n->indexspace = $3;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					n->indexspace = $4;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')' opt_no_inherit
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					n->skip_validation = false;
					n->initially_valid = true;
					$$ = (Node *)n;
				}
			| DEFAULT b_expr
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_DEFAULT;
					n->location = @1;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| GENERATED generated_when AS IDENTITY_P OptParenthesizedSeqOptList
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_IDENTITY;
					n->generated_when = $2;
					n->options = $5;
					n->location = @1;
					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid  = true;
					$$ = (Node *)n;
				}
		;
generated_when:
			ALWAYS			{ $$ = ATTRIBUTE_IDENTITY_ALWAYS; }
			| BY DEFAULT	{ $$ = ATTRIBUTE_IDENTITY_BY_DEFAULT; }
		;
opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
definition: '(' def_list ')'						{ $$ = $2; }
		;
def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;
def_elem:	ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;
/* Note : any simple identifier will be returned as a type name! */
// TODO : func_type
def_arg:	  func_type						{ $$ = (Node *)$1; }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
			//| NONE							{ $$ = (Node *)makeString(pstrdup($1)); }
		;
qual_Op:	Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;
qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;
any_operator:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| ColId '.' any_operator
					{ $$ = lcons(makeString($1), $3); }
		;
all_Op:		Op										{ $$ = $1; }
			| MathOp								{ $$ = $1; }
		;
MathOp:		 '+'									{ $$ = "+"; }
			| '-'									{ $$ = "-"; }
			| '*'									{ $$ = "*"; }
			| '/'									{ $$ = "/"; }
			| '%'									{ $$ = "%"; }
			| '^'									{ $$ = "^"; }
			| '<'									{ $$ = "<"; }
			| '>'									{ $$ = ">"; }
			| '='									{ $$ = "="; }
			| LESS_EQUALS							{ $$ = "<="; }
			| GREATER_EQUALS						{ $$ = ">="; }
			| NOT_EQUALS							{ $$ = "<>"; }
		;
NumericOnly:
			FCONST								{ $$ = makeFloat($1); }
			| '+' FCONST						{ $$ = makeFloat($2); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;
SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;
OptConsTableSpace:   USING INDEX TABLESPACE name	{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
opt_no_inherit:	NO INHERIT							{  $$ = true; }
			| /* EMPTY */							{  $$ = false; }
		;
OptParenthesizedSeqOptList: '(' SeqOptList ')'		{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;

SeqOptElem: AS SimpleTypename
				{
					$$ = makeDefElem("as", (Node *)$2, @1);
				}
			| CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2, @1);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(true), @1);
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(false), @1);
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (Node *)$3, @1);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (Node *)$2, @1);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (Node *)$2, @1);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL, @1);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3, @1);
				}
			| SEQUENCE NAME_P any_name
				{
					/* not documented, only used by pg_dump */
					$$ = makeDefElem("sequence_name", (Node *)$3, @1);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (Node *)$3, @1);
				}
			| RESTART
				{
					$$ = makeDefElem("restart", NULL, @1);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3, @1);
				}
		;
opt_by:		BY				{}
			| /* empty */	{}
	  ;
any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
		;
opt_with:	WITH									{}
			| WITH_LA								{}
			| /*EMPTY*/								{}
		;

opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;
columnElem: ColId
				{
					$$ = (Node *) makeString($1);
				}
		;		
key_match:  MATCH FULL
			{
				$$ = FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented"),
						 parser_errposition(@1)));
				$$ = FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		| /*EMPTY*/
			{
				$$ = FKCONSTR_MATCH_SIMPLE;
			}
		;
/*
 * We combine the update and delete actions into one value temporarily
 * for simplicity of parsing, and then break them down again in the
 * calling production.  update is in the left 8 bits, delete in the right.
 * Note that NOACTION is the default.
 */
key_actions:
			key_update
				{ $$ = ($1 << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
		;
key_update: ON UPDATE key_action		{ $$ = $3; }
		;
key_delete: ON DELETE_P key_action		{ $$ = $3; }
		;
/*
 * ConstraintAttr represents constraint attributes, which we parse as if
 * they were independent constraint clauses, in order to avoid shift/reduce
 * conflicts (since NOT might start either an independent NOT NULL clause
 * or an attribute).  parse_utilcmd.c is responsible for attaching the
 * attribute information to the preceding "real" constraint node, and for
 * complaining if attribute clauses appear in the wrong place or wrong
 * combinations.
 *
 * See also ConstraintAttributeSpec, which can be used in places where
 * there is no parsing conflict.  (Note : currently, NOT VALID and NO INHERIT
 * are allowed clauses in ConstraintAttributeSpec, but not here.  Someday we
 * might need to allow them here too, but for the moment it doesn`t seem
 * useful in the statements that use ConstraintAttr.)
 */

ConstraintAttr:
			DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NOT DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_NOT_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY DEFERRED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRED;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY IMMEDIATE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_IMMEDIATE;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

expr_list:	a_expr
				{
					$$ = list_make1($1);
				}
			| expr_list ',' a_expr
				{
					$$ = lappend($1, $3);
				}
		;
a_expr: 	c_expr ;

/*
 * Restricted expressions
 *
 * b_expr is a subset of the complete expression syntax defined by a_expr.
 *
 * Presently, AND, NOT, IS, and IN are the a_expr keywords that would
 * cause trouble in the places where b_expr is used.  For simplicity, we
 * just eliminate all the boolean-keyword-operator productions from b_expr.
 */
b_expr:		c_expr
				{ $$ = $1; }
			| b_expr TYPECAST Typename
				{ $$ = makeTypeCast($1, $3, @2); }
			| '+' b_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' b_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| b_expr '+' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| b_expr '-' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| b_expr '*' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| b_expr '/' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| b_expr '%' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| b_expr '^' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| b_expr '<' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| b_expr '>' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| b_expr '=' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| b_expr LESS_EQUALS b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
			| b_expr GREATER_EQUALS b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
			| b_expr NOT_EQUALS b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }
			| b_expr qual_Op b_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op b_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| b_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }
			| b_expr IS DISTINCT FROM b_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| b_expr IS NOT DISTINCT FROM b_expr	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_DISTINCT, "=", $1, $6, @2);
				}
			| b_expr IS OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| b_expr IS NOT OF '(' type_list ')'	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			| b_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| b_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = makeNotExpr(makeXmlExpr(IS_DOCUMENT, NULL, NIL,
												 list_make1($1), @2),
									 @2);
				}
		;
/*
 * Productions that can be used in both a_expr and b_expr.
 *
 * Note: productions that refer recursively to a_expr or b_expr mostly
 * cannot appear here.	However, it`s OK to refer to a_exprs that occur
 * inside parentheses, such as function arguments; that cannot introduce
 * ambiguity to the b_expr syntax.
 */
c_expr:		columnref								{ $$ = $1; }
			| AexprConst							{ $$ = $1; }
			| PARAM opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = check_indirection($2, yyscanner);
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| '(' a_expr ')' opt_indirection
				{
					if ($4)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = $2;
						n->indirection = check_indirection($4, yyscanner);
						$$ = (Node *)n;
					}
					else if (operator_precedence_warning)
					{
						/*
						 * If precedence warnings are enabled, insert
						 * AEXPR_PAREN nodes wrapping all explicitly
						 * parenthesized subexpressions; this prevents bogus
						 * warnings from being issued when the ordering has
						 * been forced by parentheses.  Take care that an
						 * AEXPR_PAREN node has the same exprLocation as its
						 * child, so as not to cause surprising changes in
						 * error cursor positioning.
						 *
						 * In principle we should not be relying on a GUC to
						 * decide whether to insert AEXPR_PAREN nodes.
						 * However, since they have no effect except to
						 * suppress warnings, it's probably safe enough; and
						 * we'd just as soon not waste cycles on dummy parse
						 * nodes if we don't have to.
						 */
						$$ = (Node *) makeA_Expr(AEXPR_PAREN, NIL, $2, NULL,
												 exprLocation($2));
					}
					else
						$$ = $2;
				}
// 			| case_expr
// 				{ $$ = $1; }
// 			| func_expr
// 				{ $$ = $1; }
// 			| select_with_parens			%prec UMINUS
// 				{
// 					SubLink *n = makeNode(SubLink);
// 					n->subLinkType = EXPR_SUBLINK;
// 					n->subLinkId = 0;
// 					n->testexpr = NULL;
// 					n->operName = NIL;
// 					n->subselect = $1;
// 					n->location = @1;
// 					$$ = (Node *)n;
// 				}
// 			| select_with_parens indirection
// 				{
// 					/*
// 					 * Because the select_with_parens nonterminal is designed
// 					 * to "eat" as many levels of parens as possible, the
// 					 * '(' a_expr ')' opt_indirection production above will
// 					 * fail to match a sub-SELECT with indirection decoration;
// 					 * the sub-SELECT won't be regarded as an a_expr as long
// 					 * as there are parens around it.  To support applying
// 					 * subscripting or field selection to a sub-SELECT result,
// 					 * we need this redundant-looking production.
// 					 */
// 					SubLink *n = makeNode(SubLink);
// 					A_Indirection *a = makeNode(A_Indirection);
// 					n->subLinkType = EXPR_SUBLINK;
// 					n->subLinkId = 0;
// 					n->testexpr = NULL;
// 					n->operName = NIL;
// 					n->subselect = $1;
// 					n->location = @1;
// 					a->arg = (Node *)n;
// 					a->indirection = check_indirection($2, yyscanner);
// 					$$ = (Node *)a;
// 				}
// 			| EXISTS select_with_parens
// 				{
// 					SubLink *n = makeNode(SubLink);
// 					n->subLinkType = EXISTS_SUBLINK;
// 					n->subLinkId = 0;
// 					n->testexpr = NULL;
// 					n->operName = NIL;
// 					n->subselect = $2;
// 					n->location = @1;
// 					$$ = (Node *)n;
// 				}
// 			| ARRAY select_with_parens
// 				{
// 					SubLink *n = makeNode(SubLink);
// 					n->subLinkType = ARRAY_SUBLINK;
// 					n->subLinkId = 0;
// 					n->testexpr = NULL;
// 					n->operName = NIL;
// 					n->subselect = $2;
// 					n->location = @1;
// 					$$ = (Node *)n;
// 				}
// 			| ARRAY array_expr
// 				{
// 					A_ArrayExpr *n = castNode(A_ArrayExpr, $2);
// 					/* point outermost A_ArrayExpr to the ARRAY keyword */
// 					n->location = @1;
// 					$$ = (Node *)n;
// 				}
// 			| explicit_row
// 				{
// 					RowExpr *r = makeNode(RowExpr);
// 					r->args = $1;
// 					r->row_typeid = InvalidOid;	/* not analyzed yet */
// 					r->colnames = NIL;	/* to be filled in during analysis */
// 					r->row_format = COERCE_EXPLICIT_CALL; /* abuse */
// 					r->location = @1;
// 					$$ = (Node *)r;
// 				}
// 			| implicit_row
// 				{
// 					RowExpr *r = makeNode(RowExpr);
// 					r->args = $1;
// 					r->row_typeid = InvalidOid;	/* not analyzed yet */
// 					r->colnames = NIL;	/* to be filled in during analysis */
// 					r->row_format = COERCE_IMPLICIT_CAST; /* abuse */
// 					r->location = @1;
// 					$$ = (Node *)r;
// 				}
// 			| GROUPING '(' expr_list ')'
// 			  {
// 				  GroupingFunc *g = makeNode(GroupingFunc);
// 				  g->args = $3;
// 				  g->location = @1;
// 				  $$ = (Node *)g;
// 			  }
// 			| '.' ROW
// 				{
// #ifdef ADB_GRAM_ORA
// 					RownumExpr *r = makeNode(RownumExpr);
// 					r->location = @1;
// 					$$ = (Node*)r;
// #else
// 					ereport_pos(".", @1);
// #endif
// 				}
		;
columnref:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| ColId indirection
				{
					$$ = makeColumnRef($1, $2, @1, yyscanner);
				}
		;
opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
		;		
indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;
indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
			| '.' '*'
				{
					$$ = (Node *) makeNode(A_Star);
				}
			| '[' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->is_slice = false;
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' opt_slice_bound ':' opt_slice_bound ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->is_slice = true;
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;
opt_slice_bound:
			a_expr									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
attr_name:	ColLabel								{ $$ = $1; };

/*
 * Constants
 */
AexprConst: Iconst
				{
					$$ = makeIntConst($1, @1);
				}
			| FCONST
				{
					$$ = makeFloatConst($1, @1);
				}
			| Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| BCONST
				{
					$$ = makeBitStringConst($1, @1);
				}
			| XCONST
				{
					/* This is a bit constant per SQL99:
					 * Without Feature F511, "BIT data type",
					 * a <general literal> shall not be a
					 * <bit string literal> or a <hex string literal>.
					 */
					$$ = makeBitStringConst($1, @1);
				}
			// | func_name Sconst
			// 	{
			// 		/* generic type 'literal' syntax */
			// 		TypeName *t = makeTypeNameFromNameList($1);
			// 		t->location = @1;
			// 		$$ = makeStringConstCast($2, @2, t);
			// 	}
			// | func_name '(' func_arg_list opt_sort_clause ')' Sconst
			// 	{
			// 		/* generic syntax with a type modifier */
			// 		TypeName *t = makeTypeNameFromNameList($1);
			// 		ListCell *lc;

			// 		/*
			// 		 * We must use func_arg_list and opt_sort_clause in the
			// 		 * production to avoid reduce/reduce conflicts, but we
			// 		 * don't actually wish to allow NamedArgExpr in this
			// 		 * context, nor ORDER BY.
			// 		 */
			// 		foreach(lc, $3)
			// 		{
			// 			NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

			// 			if (IsA(arg, NamedArgExpr))
			// 				ereport(ERROR,
			// 						(errcode(ERRCODE_SYNTAX_ERROR),
			// 						 errmsg("type modifier cannot have parameter name"),
			// 						 parser_errposition(arg->location)));
			// 		}
			// 		if ($4 != NIL)
			// 				ereport(ERROR,
			// 						(errcode(ERRCODE_SYNTAX_ERROR),
			// 						 errmsg("type modifier cannot have ORDER BY"),
			// 						 parser_errposition(@4)));

			// 		t->typmods = $3;
			// 		t->location = @1;
			// 		$$ = makeStringConstCast($6, @6, t);
			// 	}
			| ConstTypename Sconst
				{
					$$ = makeStringConstCast($2, @2, $1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					TypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| TRUE_P
				{
					$$ = makeBoolAConst(true, @1);
				}
			| FALSE_P
				{
					$$ = makeBoolAConst(false, @1);
				}
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
		;
ConstTypename:
			Numeric									{ $$ = $1; }
			| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
		;
/* ConstBit is like Bit except "BIT" defaults to unspecified length */
/* See notes for ConstCharacter, which addresses same issue for "CHAR" */
ConstBit:	BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
					$$->typmods = NIL;
				}
		;
BitWithLength:
			BIT opt_varying '(' expr_list ')'
				{
					char *typname;

					typname = $2 ? "varbit" : "bit";
					$$ = SystemTypeName(typname);
					$$->typmods = $4;
					$$->location = @1;
				}
		;
BitWithoutLength:
			BIT opt_varying
				{
					/* bit defaults to bit(1), varbit to no limit */
					if ($2)
					{
						$$ = SystemTypeName("varbit");
					}
					else
					{
						$$ = SystemTypeName("bit");
						$$->typmods = list_make1(makeIntConst(1, -1));
					}
					$$->location = @1;
				}
		;
/*
 * SQL character data types
 * The following implements CHAR() and VARCHAR().
 */
Character:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					$$ = $1;
				}
			| CLOB		{ $$ = SystemTypeNameLocation("text", @1); }
			| DBCLOB	{ $$ = SystemTypeNameLocation("text", @1); }
		;
ConstCharacter:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					/* Length was not specified so allow to be unrestricted.
					 * This handles problems with fixed-length (bpchar) strings
					 * which in column definitions must default to a length
					 * of one, but should not be constrained if the length
					 * was not specified.
					 */
					$$ = $1;
					$$->typmods = NIL;
				}
		;
CharacterWithLength:  character '(' Iconst ')'
				{
					$$ = SystemTypeName($1);
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
		;
CharacterWithoutLength:	 character
				{
					$$ = SystemTypeName($1);
					/* char defaults to char(1), varchar to no limit */
					if (strcmp($1, "bpchar") == 0)
						$$->typmods = list_make1(makeIntConst(1, -1));
					$$->location = @1;
				}
		;
character:	
			CHARACTER opt_varying
										{ $$ = $2 ? "varchar": "bpchar"; }
			| CHAR_P opt_varying
										{ $$ = $2 ? "varchar": "bpchar"; }
			| VARCHAR
										{ $$ = "varchar"; }
			| NATIONAL CHARACTER opt_varying
										{ $$ = $3 ? "varchar": "bpchar"; }
			| NATIONAL CHAR_P opt_varying
										{ $$ = $3 ? "varchar": "bpchar"; }
			| NCHAR opt_varying
										{ $$ = $2 ? "varchar": "bpchar"; }
			| LONG_P VARCHAR				
										{ $$ = "text";}
		;
opt_varying:
			VARYING									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;
attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;
interval_second:
			SECOND_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(SECOND), @1));
				}
			| SECOND_P '(' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(SECOND), @1),
									makeIntConst($3, @3));
				}
		;
key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }
		;		
opt_float:	'(' Iconst ')'
				{
					/*
					 * Check FLOAT() precision limits assuming IEEE floating
					 * types - thomas 1997-09-18
					 */
					if ($2 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be at least 1 bit"),
								 parser_errposition(@2)));
					else if ($2 <= 24)
						$$ = SystemTypeName("float4");
					else if ($2 <= 53)
						$$ = SystemTypeName("float8");
					else
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be less than 54 bits"),
								 parser_errposition(@2)));
				}
			| /*EMPTY*/
				{
					$$ = SystemTypeName("float8");
				}
		;
opt_type_modifiers: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
		;

/*
 * We would like to make the %TYPE productions here be ColId attrs etc,
 * but that causes reduce/reduce conflicts.  type_function_name
 * is next best choice.
 */
func_type:	Typename								{ $$ = $1; }
			| type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = true;
					$$->location = @1;
				}
			| SETOF type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($2), $3));
					$$->pct_type = true;
					$$->setof = true;
					$$->location = @2;
				}
		;
//OptInherit=======================================================================================
OptInherit: INHERITS '(' qualified_name_list ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
/*****************************************************************************
 *
 *	Names and constants
 *
 *****************************************************************************/

qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

//OptPartitionSpec================================================================================
/* Optional partition key specification */
OptPartitionSpec: 
			PartitionSpec	
				{ 
					$$ = $1; 
				}
			| PartitionSpec '(' ora_part_child_list ')'
			  	{
					$$ =  $1;
					$$->children = $3;
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
		;
PartitionSpec: PARTITION BY part_strategy '(' part_params ')'
				{
					PartitionSpec *n = makeNode(PartitionSpec);

					n->strategy = $3;
					n->partParams = $5;
					n->location = @1;

					/* oracle */
					$$ = palloc0(sizeof(OraclePartitionSpec));
					$$->partitionSpec = n;
					/* pg */
					//$$ = n;
				}
		;
part_strategy:	IDENT					{ $$ = $1; }
				| unreserved_keyword	{ $$ = pstrdup($1); }
		;
part_params:	part_elem						{ $$ = list_make1($1); }
			| part_params ',' part_elem			{ $$ = lappend($1, $3); }
		;
part_elem: ColId opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = $1;
					n->expr = NULL;
					n->collation = $2;
					n->opclass = $3;
					n->location = @1;
					$$ = n;
				}
			// | func_expr_windowless opt_collate opt_class
			// 	{
			// 		PartitionElem *n = makeNode(PartitionElem);

			// 		n->name = NULL;
			// 		n->expr = $1;
			// 		n->collation = $2;
			// 		n->opclass = $3;
			// 		n->location = @1;
			// 		$$ = n;
			// 	}
			| '(' a_expr ')' opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = NULL;
					n->expr = $2;
					n->collation = $4;
					n->opclass = $5;
					n->location = @1;
					$$ = n;
				}
		;

opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
opt_class:	any_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
//--ora_part_child_list---------------------------------------------------------------------------
ora_part_child_list:
		  ora_part_child							{ $$ = list_make1($1); }
		| ora_part_child_list ',' ora_part_child	{ $$ = lappend($1, $3); }
		;
ora_part_child:
		PARTITION qualified_name ForValues OptWith OnCommitOption OptTableSpace
			{
				CreateStmt *n = makeNode(CreateStmt);
				n->grammar = PARSE_GRAM_DB2;
				n->relation = $2;
				n->partbound = $3;
				n->options = $4;
				n->oncommit = $5;
				n->tablespacename = $6;

				$$ = (Node*)n;
			}
		;
ForValues:
			/* starting 1 ending 100 */
			STARTING a_expr ENDING a_expr
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_RANGE;
					n->lowerdatums = list_make1($2);
					n->upperdatums = list_make1($4);
					n->location = @2;

					$$ = n;
				}
			| STARTING FROM a_expr ENDING AT a_expr
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_RANGE;
					n->lowerdatums = list_make1($3);
					n->upperdatums = list_make1($6);
					n->location = @2;

					$$ = n;
				}
		;
/* WITH (options) is preferred, WITH OIDS and WITHOUT OIDS are legacy forms */
OptWith:
			WITH reloptions				{ $$ = $2; }
			| WITH OIDS					{ $$ = list_make1(makeDefElem("oids", (Node *) makeInteger(true), @1)); }
			| WITHOUT OIDS				{ $$ = list_make1(makeDefElem("oids", (Node *) makeInteger(false), @1)); }
			| /*EMPTY*/					{ $$ = NIL; }
		;
OnCommitOption:  ON COMMIT DROP				{ $$ = ONCOMMIT_DROP; }
			| ON COMMIT DELETE_P ROWS		{ $$ = ONCOMMIT_DELETE_ROWS; }
			| ON COMMIT PRESERVE ROWS		{ $$ = ONCOMMIT_PRESERVE_ROWS; }
			| /*EMPTY*/						{ $$ = ONCOMMIT_NOOP; }
		;
OptTableSpace:   
			TABLESPACE name					{ $$ = $2; }
			| IN_P name						{ $$ = $2; }
			| /*EMPTY*/						{ $$ = NULL; }
		;
reloptions:
			'(' reloption_list ')'					{ $$ = $2; }
		;
reloption_list:
			reloption_elem							{ $$ = list_make1($1); }
			| reloption_list ',' reloption_elem		{ $$ = lappend($1, $3); }
		;
/* This should match def_elem and also allow qualified names */
reloption_elem:
			ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3, @1);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
			| ColLabel '.' ColLabel '=' def_arg
				{
					$$ = makeDefElemExtended($1, $3, (Node *) $5,
											 DEFELEM_UNSPEC, @1);
				}
			| ColLabel '.' ColLabel
				{
					$$ = makeDefElemExtended($1, $3, NULL, DEFELEM_UNSPEC, @1);
				}
		;

//<<<OptInherit====================================================================================


/* Column identifier --- keywords that can be column, table, etc names.
 *
 * Many of these keywords will in fact be recognized as type or function
 * names too; but they have special productions for the purpose, and so
 * can't be treated as "generic" type or function names.
 *
 * The type names appearing here are not usable as function names
 * because they can be followed by '(' in typename productions, which
 * looks too much like a function call for an LR(1) parser.
 */
col_name_keyword:
			  BIGINT
			| BLOB
			| BOOLEAN_P
			| CHAR_P
			| CHARACTER
			| CLOB
			| DATE_P
			| DBCLOB
			| DEC
			| DECIMAL_P
			| FLOAT_P
			| INT_P
			| INTEGER
			| NUMBER_P
			| NUMERIC
			| REAL
			| SMALLINT
			| VARCHAR
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
unreserved_keyword:
			  LESS
			| RANGE
			| THAN
/* ADB_BEGIN */
			| DIRECT
			| DISTRIBUTE
/* ADB_END */
		;

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
			| LONG_P
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
			| STARTING
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