%{
/*-------------------------------------------------------------------------
 *
 * pl_gram.y			- Parser for the PL/pgSQL procedural language
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_gram.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "utils/builtins.h"

#include "plpgsql.h"


/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (Rhs)[0]; \
	} while (0)

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
#define plorasql_yyerror plpgsql_yyerror

typedef struct
{
	int			location;
	int			leaderlen;
} sql_error_callback_arg;

#define parser_errposition(pos)  plpgsql_scanner_errposition(pos)

#ifdef USE_ASSERT_CHECKING
static inline PLpgSQL_stmt *
castStmtImpl(PLpgSQL_stmt_type type, void *ptr)
{
	Assert(ptr == NULL || ((PLpgSQL_stmt*)ptr)->cmd_type == type);
	return (PLpgSQL_stmt *) ptr;
}
#define castStmt(_name_, _type_, nodeptr) ((PLpgSQL_stmt_##_name_ *) castStmtImpl(PLPGSQL_STMT_##_type_, nodeptr))
#else
#define castStmt(_name_, _type_, nodeptr) ((PLpgSQL_stmt_##_name_ *) (nodeptr))
#endif							/* USE_ASSERT_CHECKING */


union YYSTYPE;					/* need forward reference for tok_is_keyword */

static	bool			tok_is_keyword(int token, union YYSTYPE *lval,
									   int kw_token, const char *kw_str);
static	void			word_is_not_variable(PLword *word, int location);
static	void			cword_is_not_variable(PLcword *cword, int location);
static	void			current_token_is_not_variable(int tok);
static	PLpgSQL_expr	*read_sql_construct(int until,
											int until2,
											int until3,
											const char *expected,
											const char *sqlstart,
											bool isexpression,
											bool valid_sql,
											bool trim,
											int *startloc,
											int *endtoken);
static	PLpgSQL_expr	*read_sql_expression(int until,
											 const char *expected);
static	PLpgSQL_expr	*read_sql_expression2(int until, int until2,
											  const char *expected,
											  int *endtoken);
static	PLpgSQL_expr	*read_sql_stmt(const char *sqlstart);
static	PLpgSQL_type	*read_datatype(int tok);
/*static	PLpgSQL_stmt	*make_execsql_stmt(int firsttoken, int location);*/
static	PLpgSQL_stmt_fetch *read_fetch_direction(void);
/*static	void			 complete_direction(PLpgSQL_stmt_fetch *fetch,
											bool *check_FROM);*/
static	PLpgSQL_stmt	*make_return_stmt(int location);
static	PLpgSQL_stmt	*make_return_next_stmt(int location);
static	PLpgSQL_stmt	*make_return_query_stmt(int location);
static  PLpgSQL_stmt	*make_case(int location, PLpgSQL_expr *t_expr,
								   List *case_when_list, List *else_stmts);
static	char			*NameOfDatum(PLwdatum *wdatum);
static	void			 check_assignable(PLpgSQL_datum *datum, int location);
/*static	void			 read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row,
										  bool *strict);*/
static	PLpgSQL_row		*read_into_scalar_list(char *initial_name,
											   PLpgSQL_datum *initial_datum,
											   int initial_location);
static	PLpgSQL_row		*make_scalar_list1(char *initial_name,
										   PLpgSQL_datum *initial_datum,
										   int lineno, int location);
static	void			 check_sql_expr(const char *stmt, int location,
										int leaderlen);
static	void			 plpgsql_sql_error_callback(void *arg);
static	PLpgSQL_type	*parse_datatype(const char *string, int location);
static	void			 check_labels(const char *start_label,
									  const char *end_label,
									  int end_location);
static PLpgSQL_stmt		*make_stmt_raise(int lloc, int elevel, char *name,
										 char *message, List *params);
%}

%expect 0
%name-prefix="plorasql_yy"
%locations

%union {
		core_YYSTYPE			core_yystype;
		/* these fields must match core_YYSTYPE: */
		int						ival;
		char					*str;
		const char				*keyword;

		PLword					word;
		PLcword					cword;
		PLwdatum				wdatum;
		bool					boolean;
		Oid						oid;
		struct
		{
			char *name;
			int  lineno;
		}						varname;
		struct
		{
			char *name;
			int  lineno;
			PLpgSQL_datum   *scalar;
			PLpgSQL_rec		*rec;
			PLpgSQL_row		*row;
		}						forvariable;
		struct
		{
			char *label;
			int  n_initvars;
			int  *initvarnos;
		}						declhdr;
		struct
		{
			List *stmts;
			char *end_label;
			int   end_label_location;
		}						loop_body;
		List					*list;
		PLpgSQL_type			*dtype;
		PLpgSQL_datum			*datum;
		PLpgSQL_var				*var;
		PLpgSQL_expr			*expr;
		PLpgSQL_stmt			*stmt;
		PLpgSQL_condition		*condition;
		PLpgSQL_exception		*exception;
		PLpgSQL_exception_block	*exception_block;
		PLpgSQL_nsitem			*nsitem;
		PLpgSQL_diag_item		*diagitem;
		PLpgSQL_stmt_fetch		*fetch;
		PLpgSQL_case_when		*casewhen;
}

%type <datum>	assign_var

%type <declhdr> decl_sect
%type <varname> decl_varname
%type <boolean>	decl_const decl_notnull exit_type
%type <expr>	decl_defval
%type <dtype>	decl_datatype

%type <expr>	expr_until_semi/* expr_until_rightbracket*/
%type <expr>	expr_until_then /*expr_until_loop opt_expr_until_when*/
%type <expr>	opt_exitcond

%type <str>		any_identifier opt_block_label /*opt_loop_label*/ opt_label
/*%type <str>		option_value raise_exception*/
%type <str>		raise_exception
%type <ival>	raise_level opt_raise_level

%type <list>	proc_sect stmt_elsifs stmt_else
%type <stmt>	proc_stmt pl_block
%type <stmt>	stmt_assign stmt_if /*stmt_loop stmt_while stmt_exit*/
%type <stmt>	stmt_return stmt_raise /*stmt_assert stmt_execsql
%type <stmt>	stmt_dynexecute stmt_for stmt_perform stmt_getdiag
%type <stmt>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <stmt>	stmt_case stmt_foreach_a*/
%type <stmt>	stmt_exit
%type <stmt>	stmt_goto

/*%type <list>	proc_exceptions*/
%type <exception_block> exception_sect
/*%type <exception>	proc_exception
%type <condition>	proc_conditions proc_condition*/

%type <keyword>	unreserved_keyword


/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token			TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token			LESS_EQUALS GREATER_EQUALS NOT_EQUALS

/*
 * Other tokens recognized by plpgsql's lexer interface layer (pl_scanner.c).
 */
%token <word>		T_WORD		/* unrecognized simple identifier */
%token <cword>		T_CWORD		/* unrecognized composite identifier */
%token <wdatum>		T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
%token				LESS_LESS
%token				GREATER_GREATER

/*
  reserved keywords, POK mean is Pl Oracle Key
  https://docs.oracle.com/database/121/LNPLS/reservedwords.htm#LNPLS019
 */
%token <keyword> POK_ALL POK_ALTER POK_AND POK_ANY POK_AS POK_ASC POK_AT

%token <keyword> POK_BEGIN POK_BETWEEN POK_BY

%token <keyword> POK_CASE POK_CHECK POK_CLUSTERS POK_CLUSTER POK_COLAUTH
				 POK_COLUMNS POK_COMPRESS POK_CONNECT POK_CRASH POK_CREATE POK_CURSOR

%token <keyword> POK_DECLARE POK_DEFAULT POK_DESC POK_DISTINCT POK_DROP

%token <keyword> POK_ELSE POK_END POK_EXCEPTION POK_EXCLUSIVE

%token <keyword> POK_FETCH POK_FOR POK_FROM POK_FUNCTION

%token <keyword> POK_GOTO POK_GRANT POK_GROUP

%token <keyword> POK_HAVING

%token <keyword> POK_IDENTIFIED POK_IF POK_IN POK_INDEX POK_INDEXES POK_INSERT POK_INTERSECT POK_INTO POK_IS

%token <keyword> POK_LIKE POK_LOCK

%token <keyword> POK_MINUS POK_MODE

%token <keyword> POK_NOCOMPRESS POK_NOT POK_NOWAIT POK_NULL

%token <keyword> POK_OF POK_ON POK_OPTION POK_OR POK_ORDER POK_OVERLAPS

%token <keyword> POK_PROCEDURE POK_PUBLIC

%token <keyword> POK_RESOURCE POK_REVOKE

%token <keyword> POK_SELECT POK_SHARE POK_SIZE POK_SQL POK_START POK_SUBTYPE

%token <keyword> POK_TABAUTH POK_TABLE POK_THEN POK_TO POK_TYPE

%token <keyword> POK_UNION POK_UNIQUE POK_UPDATE

%token <keyword> POK_VALUES POK_VIEW POK_VIEWS

%token <keyword> POK_WHEN POK_WHERE POK_WITH

/* unreserved keywords, POK mean is Pl Oracle Key */
%token <keyword> POK_A POK_ADD POK_ACCESSIBLE POK_AGENT POK_AGGREGATE POK_ARRAY
				 POK_ATTRIBUTE POK_AUTHID POK_AVG

%token <keyword> POK_BFILE_BASE POK_BINARY POK_BLOB_BASE POK_BLOCK POK_BODY
				 POK_BOTH POK_BOUND POK_BULK POK_BYTE

%token <keyword> POK_C POK_CALL POK_CALLING POK_CASCADE POK_CHAR POK_CHAR_BASE
				 POK_CHARACTER POK_CHARSET POK_CHARSETFORM POK_CHARSETID
				 POK_CLOB_BASE POK_CLONE POK_CLOSE POK_COLLECT POK_COMMENT
				 POK_COMMIT POK_COMMITTED POK_COMPILED POK_CONSTANT
				 POK_CONSTRUCTOR POK_CONTEXT POK_CONTINUE POK_CONVERT POK_COUNT
				 POK_CREDENTIAL POK_CURRENT POK_CUSTOMDATUM

%token <keyword> POK_DANGLING POK_DATA POK_DATE POK_DATE_BASE POK_DAY POK_DEFINE
				 POK_DELETE POK_DETERMINISTIC POK_DIRECTORY POK_DOUBLE POK_DURATION

%token <keyword> POK_ELEMENT POK_ELSIF POK_EMPTY POK_ESCAPE POK_EXCEPT POK_EXCEPTIONS
				 POK_EXECUTE POK_EXISTS POK_EXIT POK_EXTERNAL

%token <keyword> POK_FINAL POK_FIRST POK_FIXED POK_FLOAT POK_FORALL POK_FORCE

%token <keyword> POK_GENERAL

%token <keyword> POK_HASH POK_HEAP POK_HIDDEN POK_HOUR

%token <keyword> POK_IMMEDIATE POK_INCLUDING POK_INDICATOR POK_INDICES POK_INFINITE
				 POK_INSTANTIABLE POK_INT POK_INTERFACE POK_INTERVAL POK_INVALIDATE POK_ISOLATION

%token <keyword> POK_JAVA

%token <keyword> POK_LANGUAGE POK_LARGE POK_LEADING POK_LENGTH POK_LEVEL POK_LIBRARY
				 POK_LIKE2 POK_LIKE4 POK_LIKEC POK_LIMIT POK_LIMITED POK_LOCAL POK_LONG POK_LOOP

%token <keyword> POK_MAP POK_MAX POK_MAXLEN POK_MEMBER POK_MERGE POK_MIN POK_MINUTE POK_MOD POK_MODIFY POK_MONTH POK_MULTISET

%token <keyword> POK_NAME POK_NAN POK_NATIONAL POK_NATIVE POK_NCHAR POK_NEW POK_NOCOPY POK_NUMBER_BASE

%token <keyword> POK_OBJECT POK_OCICOLL POK_OCIDATE POK_OCIDATETIME POK_OCIDURATION POK_OCIINTERVAL
				 POK_OCILOBLOCATOR POK_OCINUMBER POK_OCIRAW POK_OCIREF POK_OCIREFCURSOR POK_OCIROWID
				 POK_OCISTRING POK_OCITYPE POK_OLD POK_ONLY POK_OPAQUE POK_OPEN POK_OPERATOR POK_ORACLE
				 POK_ORADATA POK_ORGANIZATION POK_ORLANY POK_ORLVARY POK_OTHERS POK_OUT POK_OVERRIDING

%token <keyword> POK_PACKAGE POK_PARALLEL_ENABLE POK_PARAMETER POK_PARAMETERS POK_PARENT POK_PARTITION
				 POK_PASCAL POK_PIPE POK_PIPELINED POK_PLUGGABLE POK_PRAGMA POK_PRECISION POK_PRIOR POK_PRIVATE

%token <keyword> POK_RAISE POK_RANGE POK_RAW POK_READ POK_RECORD POK_REF POK_REFERENCE POK_RELIES_ON
				 POK_REM POK_REMAINDER POK_RENAME POK_RESULT POK_RESULT_CACHE POK_RETURN POK_RETURNING
				 POK_REVERSE POK_ROLLBACK POK_ROW

%token <keyword> POK_SAMPLE POK_SAVE POK_SAVEPOINT POK_SB1 POK_SB2 POK_SB4 POK_SECOND POK_SEGMENT
				 POK_SELF POK_SEPARATE POK_SEQUENCE POK_SERIALIZABLE POK_SET POK_SHORT POK_SIZE_T
				 POK_SOME POK_SPARSE POK_SQLCODE POK_SQLDATA POK_SQLNAME POK_SQLSTATE POK_STANDARD
				 POK_STATIC POK_STDDEV POK_STORED POK_STRING POK_STRUCT POK_STYLE POK_SUBMULTISET
				 POK_SUBPARTITION POK_SUBSTITUTABLE POK_SUM POK_SYNONYM

%token <keyword> POK_TDO POK_THE POK_TIME POK_TIMESTAMP POK_TIMEZONE_ABBR POK_TIMEZONE_HOUR
				 POK_TIMEZONE_MINUTE POK_TIMEZONE_REGION POK_TRAILING POK_TRANSACTION POK_TRANSACTIONAL POK_TRUSTED

%token <keyword> POK_UB1 POK_UB2 POK_UB4 POK_UNDER POK_UNPLUG POK_UNSIGNED POK_UNTRUSTED POK_USE POK_USING

%token <keyword> POK_VALIST POK_VALUE POK_VARIABLE POK_VARIANCE POK_VARRAY POK_VARYING POK_VOID

%token <keyword> POK_WHILE POK_WORK POK_WRAPPED POK_WRITE

%token <keyword> POK_YEAR

%token <keyword> POK_ZONE
%%

pl_function		: pl_block opt_semi
					{
						plpgsql_parse_result = (PLpgSQL_stmt_block *) $1;
					}
				;

opt_semi		:
				| ';'
				;

pl_block		: decl_sect POK_BEGIN proc_sect exception_sect POK_END opt_label
					{
						PLpgSQL_stmt_block *new;

						new = palloc0(sizeof(PLpgSQL_stmt_block));

						new->cmd_type	= PLPGSQL_STMT_BLOCK;
						new->lineno		= plpgsql_location_to_lineno(@2);
						new->label		= $1.label;
						new->n_initvars = $1.n_initvars;
						new->initvarnos = $1.initvarnos;
						new->body		= $3;
						new->exceptions	= $4;

						check_labels($1.label, $6, @6);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;


decl_sect		: opt_block_label
					{
						/* done with decls, so resume identifier lookup */
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label decl_start
					{
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label decl_start decl_stmts
					{
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						/* Remember variables declared in decl_stmts */
						$$.n_initvars = plpgsql_add_initdatums(&($$.initvarnos));
					}
				;

decl_start		: POK_DECLARE
					{
						/* Forget any variables created before block */
						plpgsql_add_initdatums(NULL);
						/*
						 * Disable scanner lookup of identifiers while
						 * we process the decl_stmts
						 */
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
					}
				;

decl_stmts		: decl_stmts decl_stmt
				| decl_stmt
				;

decl_stmt		: decl_statement
				| POK_DECLARE
					{
						/* We allow useless extra DECLAREs */
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
						/*
						 * Throw a helpful error if user tries to put block
						 * label just before BEGIN, instead of before DECLARE.
						 */
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("block label must be placed before DECLARE, not after"),
								 parser_errposition(@1)));
					}
				;

decl_statement	: decl_varname decl_const decl_datatype decl_notnull decl_defval
					{
						PLpgSQL_variable	*var;

						var = plpgsql_build_variable($1.name, $1.lineno,
													 $3, true);
						if ($2)
						{
							if (var->dtype == PLPGSQL_DTYPE_VAR)
								((PLpgSQL_var *) var)->isconst = $2;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("row or record variable cannot be CONSTANT"),
										 parser_errposition(@2)));
						}
						if ($5)
						{
							if (var->dtype == PLPGSQL_DTYPE_VAR)
								((PLpgSQL_var *) var)->notnull = $4;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("row or record variable cannot be NOT NULL"),
										 parser_errposition(@4)));

						}
						if ($5 != NULL)
						{
							if (var->dtype == PLPGSQL_DTYPE_VAR)
								((PLpgSQL_var *) var)->default_val = $5;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("default value for row or record variable is not supported"),
										 parser_errposition(@5)));
						}
					}

decl_varname	: T_WORD
					{
						$$.name = $1.ident;
						$$.lineno = plpgsql_location_to_lineno(@1);
						/*
						 * Check to make sure name isn't already declared
						 * in the current block.
						 */
						if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
											  $1.ident, NULL, NULL,
											  NULL) != NULL)
							yyerror("duplicate declaration");

						if (plpgsql_curr_compile->extra_warnings & PLPGSQL_XCHECK_SHADOWVAR ||
							plpgsql_curr_compile->extra_errors & PLPGSQL_XCHECK_SHADOWVAR)
						{
							PLpgSQL_nsitem *nsi;
							nsi = plpgsql_ns_lookup(plpgsql_ns_top(), false,
													$1.ident, NULL, NULL, NULL);
							if (nsi != NULL)
								ereport(plpgsql_curr_compile->extra_errors & PLPGSQL_XCHECK_SHADOWVAR ? ERROR : WARNING,
										(errcode(ERRCODE_DUPLICATE_ALIAS),
										 errmsg("variable \"%s\" shadows a previously defined variable",
												$1.ident),
										 parser_errposition(@1)));
						}

					}
				| unreserved_keyword
					{
						$$.name = pstrdup($1);
						$$.lineno = plpgsql_location_to_lineno(@1);
						/*
						 * Check to make sure name isn't already declared
						 * in the current block.
						 */
						if (plpgsql_ns_lookup(plpgsql_ns_top(), true,
											  $1, NULL, NULL,
											  NULL) != NULL)
							yyerror("duplicate declaration");

						if (plpgsql_curr_compile->extra_warnings & PLPGSQL_XCHECK_SHADOWVAR ||
							plpgsql_curr_compile->extra_errors & PLPGSQL_XCHECK_SHADOWVAR)
						{
							PLpgSQL_nsitem *nsi;
							nsi = plpgsql_ns_lookup(plpgsql_ns_top(), false,
													$1, NULL, NULL, NULL);
							if (nsi != NULL)
								ereport(plpgsql_curr_compile->extra_errors & PLPGSQL_XCHECK_SHADOWVAR ? ERROR : WARNING,
										(errcode(ERRCODE_DUPLICATE_ALIAS),
										 errmsg("variable \"%s\" shadows a previously defined variable",
												$1),
										 parser_errposition(@1)));
						}

					}
				;

decl_const		:
					{ $$ = false; }
				| POK_CONSTANT
					{ $$ = true; }
				;

decl_datatype	:
					{
						/*
						 * If there's a lookahead token, read_datatype
						 * should consume it.
						 */
						$$ = read_datatype(yychar);
						yyclearin;
					}
				;

decl_notnull	:
					{ $$ = false; }
				| POK_NOT POK_NULL
					{ $$ = true; }
				;

decl_defval		: ';'
					{ $$ = NULL; }
				| decl_defkey
					{
						$$ = read_sql_expression(';', ";");
					}
				;

decl_defkey		: assign_operator
				| POK_DEFAULT
				;

/*
 * Ada-based PL/SQL uses := for assignment and variable defaults, while
 * the SQL standard uses equals for these cases and for GET
 * DIAGNOSTICS, so we support both.  FOR and OPEN only support :=.
 */
assign_operator	: '='
				| COLON_EQUALS
				;

proc_sect		:
					{ $$ = NIL; }
				| proc_sect proc_stmt
					{
						/* don't bother linking null statements into list */
						if ($2 == NULL)
							$$ = $1;
						else
							$$ = lappend($1, $2);
					}
				;

proc_stmt		: pl_block ';'
						{ $$ = $1; }
				| stmt_assign
						{ $$ = $1; }
				| opt_block_label stmt_if
						{ $$ = $2; castStmt(if, IF, $$)->label = $1; }
				| opt_block_label stmt_exit
						{ $$ = $2; castStmt(exit, EXIT, $$)->block_name = $1; }
				| opt_block_label stmt_return
						{
							$$ = $2;
							if ($1)
							{
								switch($$->cmd_type)
								{
								case PLPGSQL_STMT_RETURN:
									((PLpgSQL_stmt_return*)$$)->label = $1;
									break;
								case PLPGSQL_STMT_RETURN_NEXT:
									((PLpgSQL_stmt_return_next*)$$)->label = $1;
									break;
								case PLPGSQL_STMT_RETURN_QUERY:
									((PLpgSQL_stmt_return_query*)$$)->label = $1;
									break;
								default:
									ereport(ERROR,
											(errcode(ERRCODE_INTERNAL_ERROR),
											 errmsg("Unknown Pl/pgsql return type for stmt %d", $$->cmd_type)));
								}
							}
						}
				| opt_block_label stmt_raise
						{ $$ = $2; castStmt(raise, RAISE, $$)->label = $1; }
				| stmt_goto
						{ $$ = $1; }
				;

stmt_assign		: assign_var assign_operator expr_until_semi
					{
						PLpgSQL_stmt_assign *new;

						new = palloc0(sizeof(PLpgSQL_stmt_assign));
						new->cmd_type = PLPGSQL_STMT_ASSIGN;
						new->lineno   = plpgsql_location_to_lineno(@1);
						new->varno = $1->dno;
						new->expr  = $3;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

assign_var		: T_DATUM
					{
						check_assignable($1.datum, @1);
						$$ = $1.datum;
					}
				;

stmt_if			: POK_IF expr_until_then proc_sect stmt_elsifs stmt_else POK_END POK_IF ';'
					{
						PLpgSQL_stmt_if *new;

						new = palloc0(sizeof(PLpgSQL_stmt_if));
						new->cmd_type	= PLPGSQL_STMT_IF;
						new->lineno		= plpgsql_location_to_lineno(@1);
						new->cond		= $2;
						new->then_body	= $3;
						new->elsif_list = $4;
						new->else_body  = $5;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_elsifs		:
					{
						$$ = NIL;
					}
				| stmt_elsifs POK_ELSIF expr_until_then proc_sect
					{
						PLpgSQL_if_elsif *new;

						new = palloc0(sizeof(PLpgSQL_if_elsif));
						new->lineno = plpgsql_location_to_lineno(@2);
						new->cond   = $3;
						new->stmts  = $4;

						$$ = lappend($1, new);
					}
				;

stmt_else		:
					{
						$$ = NIL;
					}
				| POK_ELSE proc_sect
					{
						$$ = $2;
					}
				;

stmt_exit		: exit_type opt_label opt_exitcond
					{
						PLpgSQL_stmt_exit *new;

						new = palloc0(sizeof(PLpgSQL_stmt_exit));
						new->cmd_type = PLPGSQL_STMT_EXIT;
						new->is_exit  = $1;
						new->lineno	  = plpgsql_location_to_lineno(@1);
						new->label	  = $2;
						new->cond	  = $3;

						if ($2)
						{
							/* We have a label, so verify it exists */
							PLpgSQL_nsitem *label;

							label = plpgsql_ns_lookup_label(plpgsql_ns_top(), $2);
							if (label == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("there is no label \"%s\" "
												"attached to any block or loop enclosing this statement",
												$2),
										 parser_errposition(@2)));
							/* CONTINUE only allows loop labels */
							if (label->itemno != PLPGSQL_LABEL_LOOP && !new->is_exit)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("block label \"%s\" cannot be used in CONTINUE",
												$2),
										 parser_errposition(@2)));
						}
						else
						{
							/*
							 * No label, so make sure there is some loop (an
							 * unlabelled EXIT does not match a block, so this
							 * is the same test for both EXIT and CONTINUE)
							 */
							if (plpgsql_ns_find_nearest_loop(plpgsql_ns_top()) == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 new->is_exit ?
										 errmsg("EXIT cannot be used outside a loop, unless it has a label") :
										 errmsg("CONTINUE cannot be used outside a loop"),
										 parser_errposition(@1)));
						}

						$$ = (PLpgSQL_stmt *)new;
					}
				;

exit_type		: POK_EXIT
					{
						$$ = true;
					}
				| POK_CONTINUE
					{
						$$ = false;
					}
				;

stmt_return		: POK_RETURN
					{
						int	tok;

						tok = yylex();
						if (tok == 0)
							yyerror("unexpected end of function definition");

						plpgsql_push_back_token(tok);
						$$ = make_return_stmt(@1);
					}
				;

stmt_raise		: POK_RAISE raise_exception ';'
					{
						$$ = make_stmt_raise(@1, ERROR, $2, NULL, NIL);
					}
				| POK_RAISE raise_level raise_exception ';'
					{
						$$ = make_stmt_raise(@1, $2, $3, NULL, NIL);
					}
				| POK_RAISE opt_raise_level SCONST
					{
						List *params = NIL;
						int tok = yylex();
						if (tok != ',' && tok != ';')
							yyerror("syntax error");

						while (tok == ',')
						{
							PLpgSQL_expr *expr;

							expr = read_sql_construct(',', ';', 0,
														", or ;",
														"SELECT ",
														true, true, true,
														NULL, &tok);
							params = lappend(params, expr);
						}
						$$ = make_stmt_raise(@1, $2, NULL, $3, params);
					}
				;

raise_exception	: T_WORD				{ $$ = yylval.word.ident; }
				| unreserved_keyword	{ $$ = pstrdup(yylval.keyword); }
				;

raise_level		: POK_EXCEPTION			{ $$ = ERROR; }
				| T_WORD
					{
						if (strcmp($1.ident, "warning") == 0)
							$$ = WARNING;
						else if (strcmp($1.ident, "notice") == 0)
							$$ = NOTICE;
						else if (strcmp($1.ident, "info") == 0)
							$$ = INFO;
						else if (strcmp($1.ident, "log") == 0)
							$$ = LOG;
						else if (strcmp($1.ident, "debug") == 0)
							$$ = DEBUG1;
						else
							yyerror("syntax error");
					}
				;

opt_raise_level	: raise_level		{ $$ = $1; }
				| /* empty */		{ $$ = ERROR; }
				;

stmt_goto		: opt_block_label POK_GOTO any_identifier ';'
					{
						PLpgSQL_stmt_goto *new;

						new = palloc0(sizeof(PLpgSQL_stmt_goto));
						new->cmd_type = PLPGSQL_STMT_GOTO;
						new->lineno = plpgsql_location_to_lineno(@2);
						new->label_goto = $3;
						new->label = $1;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

exception_sect	:
					{ $$ = NULL; }

expr_until_semi :
					{ $$ = read_sql_expression(';', ";"); }
				;

/*expr_until_rightbracket :
					{ $$ = read_sql_expression(']', "]"); }
				;*/

expr_until_then :
					{ $$ = read_sql_expression(POK_THEN, "THEN"); }
				;

/*expr_until_loop :
					{ $$ = read_sql_expression(POK_LOOP, "LOOP"); }
				;*/

opt_block_label	:
					{
						plpgsql_ns_push(NULL, PLPGSQL_LABEL_BLOCK);
						$$ = NULL;
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
						plpgsql_ns_push($2, PLPGSQL_LABEL_BLOCK);
						$$ = $2;
					}
				;

/*opt_loop_label	:
					{
						plpgsql_ns_push(NULL, PLPGSQL_LABEL_LOOP);
						$$ = NULL;
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
						plpgsql_ns_push($2, PLPGSQL_LABEL_LOOP);
						$$ = $2;
					}
				;*/

opt_label	:
					{
						$$ = NULL;
					}
				| any_identifier
					{
						/* label validity will be checked by outer production */
						$$ = $1;
					}
				;

opt_exitcond	: ';'
					{ $$ = NULL; }
				| POK_WHEN expr_until_semi
					{ $$ = $2; }
				;

/*
 * need to allow DATUM because scanner will have tried to resolve as variable
 */
any_identifier	: T_WORD
					{
						$$ = $1.ident;
					}
				| unreserved_keyword
					{
						$$ = pstrdup($1);
					}
				| T_DATUM
					{
						if ($1.ident == NULL) /* composite name not OK */
							yyerror("syntax error");
						$$ = $1.ident;
					}
				;

unreserved_keyword	:
				  POK_A
				| POK_ADD
				| POK_ACCESSIBLE
				| POK_AGENT
				| POK_AGGREGATE
				| POK_ARRAY
				| POK_ATTRIBUTE
				| POK_AUTHID
				| POK_AVG
				| POK_BFILE_BASE
				| POK_BINARY
				| POK_BLOB_BASE
				| POK_BLOCK
				| POK_BODY
				| POK_BOTH
				| POK_BOUND
				| POK_BULK
				| POK_BYTE
				| POK_C
				| POK_CALL
				| POK_CALLING
				| POK_CASCADE
				| POK_CHAR
				| POK_CHAR_BASE
				| POK_CHARACTER
				| POK_CHARSET
				| POK_CHARSETFORM
				| POK_CHARSETID
				| POK_CLOB_BASE
				| POK_CLONE
				| POK_CLOSE
				| POK_COLLECT
				| POK_COMMENT
				| POK_COMMIT
				| POK_COMMITTED
				| POK_COMPILED
				| POK_CONSTANT
				| POK_CONSTRUCTOR
				| POK_CONTEXT
				| POK_CONTINUE
				| POK_CONVERT
				| POK_COUNT
				| POK_CREDENTIAL
				| POK_CURRENT
				| POK_CUSTOMDATUM
				| POK_DANGLING
				| POK_DATA
				| POK_DATE
				| POK_DATE_BASE
				| POK_DAY
				| POK_DEFINE
				| POK_DELETE
				| POK_DETERMINISTIC
				| POK_DIRECTORY
				| POK_DOUBLE
				| POK_DURATION
				| POK_ELEMENT
				| POK_ELSIF
				| POK_EMPTY
				| POK_ESCAPE
				| POK_EXCEPT
				| POK_EXCEPTIONS
				| POK_EXECUTE
				| POK_EXISTS
				| POK_EXIT
				| POK_EXTERNAL
				| POK_FINAL
				| POK_FIRST
				| POK_FIXED
				| POK_FLOAT
				| POK_FORALL
				| POK_FORCE
				| POK_GENERAL
				| POK_HASH
				| POK_HEAP
				| POK_HIDDEN
				| POK_HOUR
				| POK_IMMEDIATE
				| POK_INCLUDING
				| POK_INDICATOR
				| POK_INDICES
				| POK_INFINITE
				| POK_INSTANTIABLE
				| POK_INT
				| POK_INTERFACE
				| POK_INTERVAL
				| POK_INVALIDATE
				| POK_ISOLATION
				| POK_JAVA
				| POK_LANGUAGE
				| POK_LARGE
				| POK_LEADING
				| POK_LENGTH
				| POK_LEVEL
				| POK_LIBRARY
				| POK_LIKE2
				| POK_LIKE4
				| POK_LIKEC
				| POK_LIMIT
				| POK_LIMITED
				| POK_LOCAL
				| POK_LONG
				| POK_LOOP
				| POK_MAP
				| POK_MAX
				| POK_MAXLEN
				| POK_MEMBER
				| POK_MERGE
				| POK_MIN
				| POK_MINUTE
				| POK_MOD
				| POK_MODIFY
				| POK_MONTH
				| POK_MULTISET
				| POK_NAME
				| POK_NAN
				| POK_NATIONAL
				| POK_NATIVE
				| POK_NCHAR
				| POK_NEW
				| POK_NOCOPY
				| POK_NUMBER_BASE
				| POK_OBJECT
				| POK_OCICOLL
				| POK_OCIDATE
				| POK_OCIDATETIME
				| POK_OCIDURATION
				| POK_OCIINTERVAL
				| POK_OCILOBLOCATOR
				| POK_OCINUMBER
				| POK_OCIRAW
				| POK_OCIREF
				| POK_OCIREFCURSOR
				| POK_OCIROWID
				| POK_OCISTRING
				| POK_OCITYPE
				| POK_OLD
				| POK_ONLY
				| POK_OPAQUE
				| POK_OPEN
				| POK_OPERATOR
				| POK_ORACLE
				| POK_ORADATA
				| POK_ORGANIZATION
				| POK_ORLANY
				| POK_ORLVARY
				| POK_OTHERS
				| POK_OUT
				| POK_OVERRIDING
				| POK_PACKAGE
				| POK_PARALLEL_ENABLE
				| POK_PARAMETER
				| POK_PARAMETERS
				| POK_PARENT
				| POK_PARTITION
				| POK_PASCAL
				| POK_PIPE
				| POK_PIPELINED
				| POK_PLUGGABLE
				| POK_PRAGMA
				| POK_PRECISION
				| POK_PRIOR
				| POK_PRIVATE
				| POK_RAISE
				| POK_RANGE
				| POK_RAW
				| POK_READ
				| POK_RECORD
				| POK_REF
				| POK_REFERENCE
				| POK_RELIES_ON
				| POK_REM
				| POK_REMAINDER
				| POK_RENAME
				| POK_RESULT
				| POK_RESULT_CACHE
				| POK_RETURN
				| POK_RETURNING
				| POK_REVERSE
				| POK_ROLLBACK
				| POK_ROW
				| POK_SAMPLE
				| POK_SAVE
				| POK_SAVEPOINT
				| POK_SB1
				| POK_SB2
				| POK_SB4
				| POK_SECOND
				| POK_SEGMENT
				| POK_SELF
				| POK_SEPARATE
				| POK_SEQUENCE
				| POK_SERIALIZABLE
				| POK_SET
				| POK_SHORT
				| POK_SIZE_T
				| POK_SOME
				| POK_SPARSE
				| POK_SQLCODE
				| POK_SQLDATA
				| POK_SQLNAME
				| POK_SQLSTATE
				| POK_STANDARD
				| POK_STATIC
				| POK_STDDEV
				| POK_STORED
				| POK_STRING
				| POK_STRUCT
				| POK_STYLE
				| POK_SUBMULTISET
				| POK_SUBPARTITION
				| POK_SUBSTITUTABLE
				| POK_SUM
				| POK_SYNONYM
				| POK_TDO
				| POK_THE
				| POK_TIME
				| POK_TIMESTAMP
				| POK_TIMEZONE_ABBR
				| POK_TIMEZONE_HOUR
				| POK_TIMEZONE_MINUTE
				| POK_TIMEZONE_REGION
				| POK_TRAILING
				| POK_TRANSACTION
				| POK_TRANSACTIONAL
				| POK_TRUSTED
				| POK_UB1
				| POK_UB2
				| POK_UB4
				| POK_UNDER
				| POK_UNPLUG
				| POK_UNSIGNED
				| POK_UNTRUSTED
				| POK_USE
				| POK_USING
				| POK_VALIST
				| POK_VALUE
				| POK_VARIABLE
				| POK_VARIANCE
				| POK_VARRAY
				| POK_VARYING
				| POK_VOID
				| POK_WHILE
				| POK_WORK
				| POK_WRAPPED
				| POK_WRITE
				| POK_YEAR
				| POK_ZONE
			;

%%

/*
 * Check whether a token represents an "unreserved keyword".
 * We have various places where we want to recognize a keyword in preference
 * to a variable name, but not reserve that keyword in other contexts.
 * Hence, this kluge.
 */
static bool
tok_is_keyword(int token, union YYSTYPE *lval,
			   int kw_token, const char *kw_str)
{
	if (token == kw_token)
	{
		/* Normal case, was recognized by scanner (no conflicting variable) */
		return true;
	}
	else if (token == T_DATUM)
	{
		/*
		 * It's a variable, so recheck the string name.  Note we will not
		 * match composite names (hence an unreserved word followed by "."
		 * will not be recognized).
		 */
		if (!lval->wdatum.quoted && lval->wdatum.ident != NULL &&
			strcmp(lval->wdatum.ident, kw_str) == 0)
			return true;
	}
	return false;				/* not the keyword */
}

/*
 * Convenience routine to complain when we expected T_DATUM and got T_WORD,
 * ie, unrecognized variable.
 */
static void
word_is_not_variable(PLword *word, int location)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("\"%s\" is not a known variable",
					word->ident),
			 parser_errposition(location)));
}

/* Same, for a CWORD */
static void
cword_is_not_variable(PLcword *cword, int location)
{
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("\"%s\" is not a known variable",
					NameListToString(cword->idents)),
			 parser_errposition(location)));
}

/*
 * Convenience routine to complain when we expected T_DATUM and got
 * something else.  "tok" must be the current token, since we also
 * look at yylval and yylloc.
 */
static void
current_token_is_not_variable(int tok)
{
	if (tok == T_WORD)
		word_is_not_variable(&(yylval.word), yylloc);
	else if (tok == T_CWORD)
		cword_is_not_variable(&(yylval.cword), yylloc);
	else
		yyerror("syntax error");
}

/* Convenience routine to read an expression with one possible terminator */
static PLpgSQL_expr *
read_sql_expression(int until, const char *expected)
{
	return read_sql_construct(until, 0, 0, expected,
							  "SELECT ", true, true, true, NULL, NULL);
}

/* Convenience routine to read an expression with two possible terminators */
static PLpgSQL_expr *
read_sql_expression2(int until, int until2, const char *expected,
					 int *endtoken)
{
	return read_sql_construct(until, until2, 0, expected,
							  "SELECT ", true, true, true, NULL, endtoken);
}

/* Convenience routine to read a SQL statement that must end with ';' */
static PLpgSQL_expr *
read_sql_stmt(const char *sqlstart)
{
	return read_sql_construct(';', 0, 0, ";",
							  sqlstart, false, true, true, NULL, NULL);
}

/*
 * Read a SQL construct and build a PLpgSQL_expr for it.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * sqlstart:	text to prefix to the accumulated SQL text
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr (prefixed with sqlstart)
 * trim:		trim trailing whitespace
 * startloc:	if not NULL, location of first token is stored at *startloc
 * endtoken:	if not NULL, ending token is stored at *endtoken
 *				(this is only interesting if until2 or until3 isn't zero)
 */
static PLpgSQL_expr *
read_sql_construct(int until,
				   int until2,
				   int until3,
				   const char *expected,
				   const char *sqlstart,
				   bool isexpression,
				   bool valid_sql,
				   bool trim,
				   int *startloc,
				   int *endtoken)
{
	int					tok;
	StringInfoData		ds;
	IdentifierLookup	save_IdentifierLookup;
	int					startlocation = -1;
	int					parenlevel = 0;
	PLpgSQL_expr		*expr;

	initStringInfo(&ds);
	appendStringInfoString(&ds, sqlstart);

	/* special lookup mode for identifiers within the SQL text */
	save_IdentifierLookup = plpgsql_IdentifierLookup;
	plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

	for (;;)
	{
		tok = yylex();
		if (startlocation < 0)			/* remember loc of first token */
			startlocation = yylloc;
		if (tok == until && parenlevel == 0)
			break;
		if (tok == until2 && parenlevel == 0)
			break;
		if (tok == until3 && parenlevel == 0)
			break;
		if (tok == '(' || tok == '[')
			parenlevel++;
		else if (tok == ')' || tok == ']')
		{
			parenlevel--;
			if (parenlevel < 0)
				yyerror("mismatched parentheses");
		}
		/*
		 * End of function definition is an error, and we don't expect to
		 * hit a semicolon either (unless it's the until symbol, in which
		 * case we should have fallen out above).
		 */
		if (tok == 0 || tok == ';')
		{
			if (parenlevel != 0)
				yyerror("mismatched parentheses");
			if (isexpression)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing \"%s\" at end of SQL expression",
								expected),
						 parser_errposition(yylloc)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing \"%s\" at end of SQL statement",
								expected),
						 parser_errposition(yylloc)));
		}
	}

	plpgsql_IdentifierLookup = save_IdentifierLookup;

	if (startloc)
		*startloc = startlocation;
	if (endtoken)
		*endtoken = tok;

	/* give helpful complaint about empty input */
	if (startlocation >= yylloc)
	{
		if (isexpression)
			yyerror("missing expression");
		else
			yyerror("missing SQL statement");
	}

	plpgsql_append_source_text(&ds, startlocation, yylloc);

	/* trim any trailing whitespace, for neatness */
	if (trim)
	{
		while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
			ds.data[--ds.len] = '\0';
	}

	expr = palloc0(sizeof(PLpgSQL_expr));
	expr->query			= pstrdup(ds.data);
	expr->plan			= NULL;
	expr->paramnos		= NULL;
	expr->rwparam		= -1;
	expr->ns			= plpgsql_ns_top();
	pfree(ds.data);

	if (valid_sql)
		check_sql_expr(expr->query, startlocation, strlen(sqlstart));

	return expr;
}

static PLpgSQL_type *
read_datatype(int tok)
{
	StringInfoData		ds;
	char			   *type_name;
	int					startlocation;
	PLpgSQL_type		*result;
	int					parenlevel = 0;

	/* Should only be called while parsing DECLARE sections */
	Assert(plpgsql_IdentifierLookup == IDENTIFIER_LOOKUP_DECLARE);

	/* Often there will be a lookahead token, but if not, get one */
	if (tok == YYEMPTY)
		tok = yylex();

	startlocation = yylloc;

	/*
	 * If we have a simple or composite identifier, check for %TYPE
	 * and %ROWTYPE constructs.
	 */
	if (tok == T_WORD)
	{
		char   *dtname = yylval.word.ident;

		tok = yylex();
		if (tok == '%')
		{
			tok = yylex();
			if (tok_is_keyword(tok, &yylval,
							   POK_TYPE, "type"))
			{
				result = plpgsql_parse_wordtype(dtname);
				if (result)
					return result;
			}
		}
	}
	else if (plorasql_token_is_unreserved_keyword(tok))
	{
		char   *dtname = pstrdup(yylval.keyword);

		tok = yylex();
		if (tok == '%')
		{
			tok = yylex();
			if (tok_is_keyword(tok, &yylval,
							   POK_TYPE, "type"))
			{
				result = plpgsql_parse_wordtype(dtname);
				if (result)
					return result;
			}
		}
	}
	else if (tok == T_CWORD)
	{
		List   *dtnames = yylval.cword.idents;

		tok = yylex();
		if (tok == '%')
		{
			tok = yylex();
			if (tok_is_keyword(tok, &yylval,
							   POK_TYPE, "type"))
			{
				result = plpgsql_parse_cwordtype(dtnames);
				if (result)
					return result;
			}
		}
	}

	while (tok != ';')
	{
		if (tok == 0)
		{
			if (parenlevel != 0)
				yyerror("mismatched parentheses");
			else
				yyerror("incomplete data type declaration");
		}
		/* Possible followers for datatype in a declaration */
		if (tok == POK_NOT ||
			tok == '=' || tok == COLON_EQUALS || tok == POK_DEFAULT)
			break;
		/* Possible followers for datatype in a cursor_arg list */
		if ((tok == ',' || tok == ')') && parenlevel == 0)
			break;
		if (tok == '(')
			parenlevel++;
		else if (tok == ')')
			parenlevel--;

		tok = yylex();
	}

	/* set up ds to contain complete typename text */
	initStringInfo(&ds);
	plpgsql_append_source_text(&ds, startlocation, yylloc);
	type_name = ds.data;

	if (type_name[0] == '\0')
		yyerror("missing data type declaration");

	result = parse_datatype(type_name, startlocation);

	pfree(ds.data);

	plpgsql_push_back_token(tok);

	return result;
}

//static PLpgSQL_stmt *
//make_execsql_stmt(int firsttoken, int location)
//{
//	StringInfoData		ds;
//	IdentifierLookup	save_IdentifierLookup;
//	PLpgSQL_stmt_execsql *execsql;
//	PLpgSQL_expr		*expr;
//	PLpgSQL_row			*row = NULL;
//	PLpgSQL_rec			*rec = NULL;
//	int					tok;
//	int					prev_tok;
//	bool				have_into = false;
//	bool				have_strict = false;
//	int					into_start_loc = -1;
//	int					into_end_loc = -1;
//
//	initStringInfo(&ds);
//
//	/* special lookup mode for identifiers within the SQL text */
//	save_IdentifierLookup = plpgsql_IdentifierLookup;
//	plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;
//
//	/*
//	 * Scan to the end of the SQL command.  Identify any INTO-variables
//	 * clause lurking within it, and parse that via read_into_target().
//	 *
//	 * Because INTO is sometimes used in the main SQL grammar, we have to be
//	 * careful not to take any such usage of INTO as a PL/pgSQL INTO clause.
//	 * There are currently three such cases:
//	 *
//	 * 1. SELECT ... INTO.  We don't care, we just override that with the
//	 * PL/pgSQL definition.
//	 *
//	 * 2. INSERT INTO.  This is relatively easy to recognize since the words
//	 * must appear adjacently; but we can't assume INSERT starts the command,
//	 * because it can appear in CREATE RULE or WITH.  Unfortunately, INSERT is
//	 * *not* fully reserved, so that means there is a chance of a false match;
//	 * but it's not very likely.
//	 *
//	 * 3. IMPORT FOREIGN SCHEMA ... INTO.  This is not allowed in CREATE RULE
//	 * or WITH, so we just check for IMPORT as the command's first token.
//	 * (If IMPORT FOREIGN SCHEMA returned data someone might wish to capture
//	 * with an INTO-variables clause, we'd have to work much harder here.)
//	 *
//	 * Fortunately, INTO is a fully reserved word in the main grammar, so
//	 * at least we need not worry about it appearing as an identifier.
//	 *
//	 * Any future additional uses of INTO in the main grammar will doubtless
//	 * break this logic again ... beware!
//	 */
//	tok = firsttoken;
//	for (;;)
//	{
//		prev_tok = tok;
//		tok = yylex();
//		if (have_into && into_end_loc < 0)
//			into_end_loc = yylloc;		/* token after the INTO part */
//		if (tok == ';')
//			break;
//		if (tok == 0)
//			yyerror("unexpected end of function definition");
//		if (tok == POK_INTO)
//		{
//			if (prev_tok == POK_INSERT)
//				continue;		/* INSERT INTO is not an INTO-target */
//			if (firsttoken == POK_IMPORT)
//				continue;		/* IMPORT ... INTO is not an INTO-target */
//			if (have_into)
//				yyerror("INTO specified more than once");
//			have_into = true;
//			into_start_loc = yylloc;
//			plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
//			read_into_target(&rec, &row, &have_strict);
//			plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;
//		}
//	}
//
//	plpgsql_IdentifierLookup = save_IdentifierLookup;
//
//	if (have_into)
//	{
//		/*
//		 * Insert an appropriate number of spaces corresponding to the
//		 * INTO text, so that locations within the redacted SQL statement
//		 * still line up with those in the original source text.
//		 */
//		plpgsql_append_source_text(&ds, location, into_start_loc);
//		appendStringInfoSpaces(&ds, into_end_loc - into_start_loc);
//		plpgsql_append_source_text(&ds, into_end_loc, yylloc);
//	}
//	else
//		plpgsql_append_source_text(&ds, location, yylloc);
//
//	/* trim any trailing whitespace, for neatness */
//	while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
//		ds.data[--ds.len] = '\0';
//
//	expr = palloc0(sizeof(PLpgSQL_expr));
//	expr->dtype			= PLPGSQL_DTYPE_EXPR;
//	expr->query			= pstrdup(ds.data);
//	expr->plan			= NULL;
//	expr->paramnos		= NULL;
//	expr->rwparam		= -1;
//	expr->ns			= plpgsql_ns_top();
//	pfree(ds.data);
//
//	check_sql_expr(expr->query, location, 0);
//
//	execsql = palloc(sizeof(PLpgSQL_stmt_execsql));
//	execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
//	execsql->lineno  = plpgsql_location_to_lineno(location);
//	execsql->sqlstmt = expr;
//	execsql->into	 = have_into;
//	execsql->strict	 = have_strict;
//	execsql->rec	 = rec;
//	execsql->row	 = row;
//
//	return (PLpgSQL_stmt *) execsql;
//}

/*
 * Process remainder of FETCH/MOVE direction after FORWARD or BACKWARD.
 * Allows these cases:
 *   FORWARD expr,  FORWARD ALL,  FORWARD
 *   BACKWARD expr, BACKWARD ALL, BACKWARD
 */
//static void
//complete_direction(PLpgSQL_stmt_fetch *fetch,  bool *check_FROM)
//{
//	int			tok;
//
//	tok = yylex();
//	if (tok == 0)
//		yyerror("unexpected end of function definition");
//
//	if (tok == POK_FROM || tok == POK_IN)
//	{
//		*check_FROM = false;
//		return;
//	}
//
//	if (tok == POK_ALL)
//	{
//		fetch->how_many = FETCH_ALL;
//		fetch->returns_multiple_rows = true;
//		*check_FROM = true;
//		return;
//	}
//
//	plpgsql_push_back_token(tok);
//	fetch->expr = read_sql_expression2(K_FROM, POK_IN,
//									   "FROM or IN",
//									   NULL);
//	fetch->returns_multiple_rows = true;
//	*check_FROM = false;
//}


static PLpgSQL_stmt *
make_return_stmt(int location)
{
	PLpgSQL_stmt_return *new;

	new = palloc0(sizeof(PLpgSQL_stmt_return));
	new->cmd_type = PLPGSQL_STMT_RETURN;
	new->lineno   = plpgsql_location_to_lineno(location);
	new->expr	  = NULL;
	new->retvarno = -1;

	if (plpgsql_curr_compile->fn_retset)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN cannot have a parameter in function returning set"),
					 errhint("Use RETURN NEXT or RETURN QUERY."),
					 parser_errposition(yylloc)));
	}
	else if (plpgsql_curr_compile->out_param_varno >= 0)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN cannot have a parameter in function with OUT parameters"),
					 parser_errposition(yylloc)));
		new->retvarno = plpgsql_curr_compile->out_param_varno;
	}
	else if (plpgsql_curr_compile->fn_rettype == VOIDOID)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN cannot have a parameter in function returning void"),
					 parser_errposition(yylloc)));
	}
	else
	{
		/*
		 * We want to special-case simple variable references for efficiency.
		 * So peek ahead to see if that's what we have.
		 */
		int		tok = yylex();

		if (tok == T_DATUM && plpgsql_peek() == ';' &&
			(yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR ||
			 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
			 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC))
		{
			new->retvarno = yylval.wdatum.datum->dno;
			/* eat the semicolon token that we only peeked at above */
			tok = yylex();
			Assert(tok == ';');
		}
		else
		{
			/*
			 * Not (just) a variable name, so treat as expression.
			 *
			 * Note that a well-formed expression is _required_ here;
			 * anything else is a compile-time error.
			 */
			plpgsql_push_back_token(tok);
			new->expr = read_sql_expression(';', ";");
		}
	}

	return (PLpgSQL_stmt *) new;
}


static PLpgSQL_stmt *
make_return_next_stmt(int location)
{
	PLpgSQL_stmt_return_next *new;

	if (!plpgsql_curr_compile->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot use RETURN NEXT in a non-SETOF function"),
				 parser_errposition(location)));

	new = palloc0(sizeof(PLpgSQL_stmt_return_next));
	new->cmd_type	= PLPGSQL_STMT_RETURN_NEXT;
	new->lineno		= plpgsql_location_to_lineno(location);
	new->expr		= NULL;
	new->retvarno	= -1;

	if (plpgsql_curr_compile->out_param_varno >= 0)
	{
		if (yylex() != ';')
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("RETURN NEXT cannot have a parameter in function with OUT parameters"),
					 parser_errposition(yylloc)));
		new->retvarno = plpgsql_curr_compile->out_param_varno;
	}
	else
	{
		/*
		 * We want to special-case simple variable references for efficiency.
		 * So peek ahead to see if that's what we have.
		 */
		int		tok = yylex();

		if (tok == T_DATUM && plpgsql_peek() == ';' &&
			(yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR ||
			 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
			 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC))
		{
			new->retvarno = yylval.wdatum.datum->dno;
			/* eat the semicolon token that we only peeked at above */
			tok = yylex();
			Assert(tok == ';');
		}
		else
		{
			/*
			 * Not (just) a variable name, so treat as expression.
			 *
			 * Note that a well-formed expression is _required_ here;
			 * anything else is a compile-time error.
			 */
			plpgsql_push_back_token(tok);
			new->expr = read_sql_expression(';', ";");
		}
	}

	return (PLpgSQL_stmt *) new;
}


static PLpgSQL_stmt *
make_return_query_stmt(int location)
{
	PLpgSQL_stmt_return_query *new;
	int			tok;

	if (!plpgsql_curr_compile->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot use RETURN QUERY in a non-SETOF function"),
				 parser_errposition(location)));

	new = palloc0(sizeof(PLpgSQL_stmt_return_query));
	new->cmd_type = PLPGSQL_STMT_RETURN_QUERY;
	new->lineno = plpgsql_location_to_lineno(location);

	/* check for RETURN QUERY EXECUTE */
	if ((tok = yylex()) != POK_EXECUTE)
	{
		/* ordinary static query */
		plpgsql_push_back_token(tok);
		new->query = read_sql_stmt("");
	}
	else
	{
		/* dynamic SQL */
		int		term;

		new->dynquery = read_sql_expression2(';', POK_USING, "; or USING",
											 &term);
		if (term == POK_USING)
		{
			do
			{
				PLpgSQL_expr *expr;

				expr = read_sql_expression2(',', ';', ", or ;", &term);
				new->params = lappend(new->params, expr);
			} while (term == ',');
		}
	}

	return (PLpgSQL_stmt *) new;
}


/* convenience routine to fetch the name of a T_DATUM */
static char *
NameOfDatum(PLwdatum *wdatum)
{
	if (wdatum->ident)
		return wdatum->ident;
	Assert(wdatum->idents != NIL);
	return NameListToString(wdatum->idents);
}

static void
check_assignable(PLpgSQL_datum *datum, int location)
{
	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			if (((PLpgSQL_var *) datum)->isconst)
				ereport(ERROR,
						(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
						 errmsg("\"%s\" is declared CONSTANT",
								((PLpgSQL_var *) datum)->refname),
						 parser_errposition(location)));
			break;
		case PLPGSQL_DTYPE_ROW:
			/* always assignable? */
			break;
		case PLPGSQL_DTYPE_REC:
			/* always assignable?  What about NEW/OLD? */
			break;
		case PLPGSQL_DTYPE_RECFIELD:
			/* always assignable? */
			break;
		case PLPGSQL_DTYPE_ARRAYELEM:
			/* always assignable? */
			break;
		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			break;
	}
}

/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword.
 */
//static void
//read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row, bool *strict)
//{
//	int			tok;
//
//	/* Set default results */
//	*rec = NULL;
//	*row = NULL;
//	if (strict)
//		*strict = false;
//
//	tok = yylex();
//	if (strict && tok == POK_STRICT)
//	{
//		*strict = true;
//		tok = yylex();
//	}
//
//	/*
//	 * Currently, a row or record variable can be the single INTO target,
//	 * but not a member of a multi-target list.  So we throw error if there
//	 * is a comma after it, because that probably means the user tried to
//	 * write a multi-target list.  If this ever gets generalized, we should
//	 * probably refactor read_into_scalar_list so it handles all cases.
//	 */
//	switch (tok)
//	{
//		case T_DATUM:
//			if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW)
//			{
//				check_assignable(yylval.wdatum.datum, yylloc);
//				*row = (PLpgSQL_row *) yylval.wdatum.datum;
//
//				if ((tok = yylex()) == ',')
//					ereport(ERROR,
//							(errcode(ERRCODE_SYNTAX_ERROR),
//							 errmsg("record or row variable cannot be part of multiple-item INTO list"),
//							 parser_errposition(yylloc)));
//				plpgsql_push_back_token(tok);
//			}
//			else if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
//			{
//				check_assignable(yylval.wdatum.datum, yylloc);
//				*rec = (PLpgSQL_rec *) yylval.wdatum.datum;
//
//				if ((tok = yylex()) == ',')
//					ereport(ERROR,
//							(errcode(ERRCODE_SYNTAX_ERROR),
//							 errmsg("record or row variable cannot be part of multiple-item INTO list"),
//							 parser_errposition(yylloc)));
//				plpgsql_push_back_token(tok);
//			}
//			else
//			{
//				*row = read_into_scalar_list(NameOfDatum(&(yylval.wdatum)),
//											 yylval.wdatum.datum, yylloc);
//			}
//			break;
//
//		default:
//			/* just to give a better message than "syntax error" */
//			current_token_is_not_variable(tok);
//	}
//}

/*
 * Given the first datum and name in the INTO list, continue to read
 * comma-separated scalar variables until we run out. Then construct
 * and return a fake "row" variable that represents the list of
 * scalars.
 */
static PLpgSQL_row *
read_into_scalar_list(char *initial_name,
					  PLpgSQL_datum *initial_datum,
					  int initial_location)
{
	int				 nfields;
	char			*fieldnames[1024];
	int				 varnos[1024];
	PLpgSQL_row		*row;
	int				 tok;

	check_assignable(initial_datum, initial_location);
	fieldnames[0] = initial_name;
	varnos[0]	  = initial_datum->dno;
	nfields		  = 1;

	while ((tok = yylex()) == ',')
	{
		/* Check for array overflow */
		if (nfields >= 1024)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("too many INTO variables specified"),
					 parser_errposition(yylloc)));

		tok = yylex();
		switch (tok)
		{
			case T_DATUM:
				check_assignable(yylval.wdatum.datum, yylloc);
				if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
					yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("\"%s\" is not a scalar variable",
									NameOfDatum(&(yylval.wdatum))),
							 parser_errposition(yylloc)));
				fieldnames[nfields] = NameOfDatum(&(yylval.wdatum));
				varnos[nfields++]	= yylval.wdatum.datum->dno;
				break;

			default:
				/* just to give a better message than "syntax error" */
				current_token_is_not_variable(tok);
		}
	}

	/*
	 * We read an extra, non-comma token from yylex(), so push it
	 * back onto the input stream
	 */
	plpgsql_push_back_token(tok);

	row = palloc(sizeof(PLpgSQL_row));
	row->dtype = PLPGSQL_DTYPE_ROW;
	row->refname = pstrdup("*internal*");
	row->lineno = plpgsql_location_to_lineno(initial_location);
	row->rowtupdesc = NULL;
	row->nfields = nfields;
	row->fieldnames = palloc(sizeof(char *) * nfields);
	row->varnos = palloc(sizeof(int) * nfields);
	while (--nfields >= 0)
	{
		row->fieldnames[nfields] = fieldnames[nfields];
		row->varnos[nfields] = varnos[nfields];
	}

	plpgsql_adddatum((PLpgSQL_datum *)row);

	return row;
}

/*
 * Convert a single scalar into a "row" list.  This is exactly
 * like read_into_scalar_list except we never consume any input.
 *
 * Note: lineno could be computed from location, but since callers
 * have it at hand already, we may as well pass it in.
 */
static PLpgSQL_row *
make_scalar_list1(char *initial_name,
				  PLpgSQL_datum *initial_datum,
				  int lineno, int location)
{
	PLpgSQL_row		*row;

	check_assignable(initial_datum, location);

	row = palloc(sizeof(PLpgSQL_row));
	row->dtype = PLPGSQL_DTYPE_ROW;
	row->refname = pstrdup("*internal*");
	row->lineno = lineno;
	row->rowtupdesc = NULL;
	row->nfields = 1;
	row->fieldnames = palloc(sizeof(char *));
	row->varnos = palloc(sizeof(int));
	row->fieldnames[0] = initial_name;
	row->varnos[0] = initial_datum->dno;

	plpgsql_adddatum((PLpgSQL_datum *)row);

	return row;
}

/*
 * When the PL/pgSQL parser expects to see a SQL statement, it is very
 * liberal in what it accepts; for example, we often assume an
 * unrecognized keyword is the beginning of a SQL statement. This
 * avoids the need to duplicate parts of the SQL grammar in the
 * PL/pgSQL grammar, but it means we can accept wildly malformed
 * input. To try and catch some of the more obviously invalid input,
 * we run the strings we expect to be SQL statements through the main
 * SQL parser.
 *
 * We only invoke the raw parser (not the analyzer); this doesn't do
 * any database access and does not check any semantic rules, it just
 * checks for basic syntactic correctness. We do this here, rather
 * than after parsing has finished, because a malformed SQL statement
 * may cause the PL/pgSQL parser to become confused about statement
 * borders. So it is best to bail out as early as we can.
 *
 * It is assumed that "stmt" represents a copy of the function source text
 * beginning at offset "location", with leader text of length "leaderlen"
 * (typically "SELECT ") prefixed to the source text.  We use this assumption
 * to transpose any error cursor position back to the function source text.
 * If no error cursor is provided, we'll just point at "location".
 */
static void
check_sql_expr(const char *stmt, int location, int leaderlen)
{
	sql_error_callback_arg cbarg;
	ErrorContextCallback  syntax_errcontext;
	MemoryContext oldCxt;

	if (!plpgsql_check_syntax)
		return;

	cbarg.location = location;
	cbarg.leaderlen = leaderlen;

	syntax_errcontext.callback = plpgsql_sql_error_callback;
	syntax_errcontext.arg = &cbarg;
	syntax_errcontext.previous = error_context_stack;
	error_context_stack = &syntax_errcontext;

	oldCxt = MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
	(void) raw_parser(stmt);
	MemoryContextSwitchTo(oldCxt);

	/* Restore former ereport callback */
	error_context_stack = syntax_errcontext.previous;
}

static void
plpgsql_sql_error_callback(void *arg)
{
	sql_error_callback_arg *cbarg = (sql_error_callback_arg *) arg;
	int			errpos;

	/*
	 * First, set up internalerrposition to point to the start of the
	 * statement text within the function text.  Note this converts
	 * location (a byte offset) to a character number.
	 */
	parser_errposition(cbarg->location);

	/*
	 * If the core parser provided an error position, transpose it.
	 * Note we are dealing with 1-based character numbers at this point.
	 */
	errpos = geterrposition();
	if (errpos > cbarg->leaderlen)
	{
		int		myerrpos = getinternalerrposition();

		if (myerrpos > 0)		/* safety check */
			internalerrposition(myerrpos + errpos - cbarg->leaderlen - 1);
	}

	/* In any case, flush errposition --- we want internalerrpos only */
	errposition(0);
}

/*
 * Parse a SQL datatype name and produce a PLpgSQL_type structure.
 *
 * The heavy lifting is done elsewhere.  Here we are only concerned
 * with setting up an errcontext link that will let us give an error
 * cursor pointing into the plpgsql function source, if necessary.
 * This is handled the same as in check_sql_expr(), and we likewise
 * expect that the given string is a copy from the source text.
 */
static PLpgSQL_type *
parse_datatype(const char *string, int location)
{
	Oid			type_id;
	int32		typmod;
	sql_error_callback_arg cbarg;
	ErrorContextCallback  syntax_errcontext;

	cbarg.location = location;
	cbarg.leaderlen = 0;

	syntax_errcontext.callback = plpgsql_sql_error_callback;
	syntax_errcontext.arg = &cbarg;
	syntax_errcontext.previous = error_context_stack;
	error_context_stack = &syntax_errcontext;

	/* Let the main parser try to parse it under standard SQL rules */
	parseTypeStringForGrammar(string, &type_id, &typmod, false, PARSE_GRAM_ORACLE);

	/* Restore former ereport callback */
	error_context_stack = syntax_errcontext.previous;

	/* Okay, build a PLpgSQL_type data structure for it */
	return plpgsql_build_datatype(type_id, typmod,
								  plpgsql_curr_compile->fn_input_collation);
}

/*
 * Check block starting and ending labels match.
 */
static void
check_labels(const char *start_label, const char *end_label, int end_location)
{
	if (end_label)
	{
		if (!start_label)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("end label \"%s\" specified for unlabelled block",
							end_label),
					 parser_errposition(end_location)));

		if (strcmp(start_label, end_label) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("end label \"%s\" differs from block's label \"%s\"",
							end_label, start_label),
					 parser_errposition(end_location)));
	}
}

/*
 * Fix up CASE statement
 */
//static PLpgSQL_stmt *
//make_case(int location, PLpgSQL_expr *t_expr,
//		  List *case_when_list, List *else_stmts)
//{
//	PLpgSQL_stmt_case	*new;
//
//	new = palloc(sizeof(PLpgSQL_stmt_case));
//	new->cmd_type = PLPGSQL_STMT_CASE;
//	new->lineno = plpgsql_location_to_lineno(location);
//	new->t_expr = t_expr;
//	new->t_varno = 0;
//	new->case_when_list = case_when_list;
//	new->have_else = (else_stmts != NIL);
//	/* Get rid of list-with-NULL hack */
//	if (list_length(else_stmts) == 1 && linitial(else_stmts) == NULL)
//		new->else_stmts = NIL;
//	else
//		new->else_stmts = else_stmts;
//
//	/*
//	 * When test expression is present, we create a var for it and then
//	 * convert all the WHEN expressions to "VAR IN (original_expression)".
//	 * This is a bit klugy, but okay since we haven't yet done more than
//	 * read the expressions as text.  (Note that previous parsing won't
//	 * have complained if the WHEN ... THEN expression contained multiple
//	 * comma-separated values.)
//	 */
//	if (t_expr)
//	{
//		char	varname[32];
//		PLpgSQL_var *t_var;
//		ListCell *l;
//
//		/* use a name unlikely to collide with any user names */
//		snprintf(varname, sizeof(varname), "__Case__Variable_%d__",
//				 plpgsql_nDatums);
//
//		/*
//		 * We don't yet know the result datatype of t_expr.  Build the
//		 * variable as if it were INT4; we'll fix this at runtime if needed.
//		 */
//		t_var = (PLpgSQL_var *)
//			plpgsql_build_variable(varname, new->lineno,
//								   plpgsql_build_datatype(INT4OID,
//														  -1,
//														  InvalidOid),
//								   true);
//		new->t_varno = t_var->dno;
//
//		foreach(l, case_when_list)
//		{
//			PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);
//			PLpgSQL_expr *expr = cwt->expr;
//			StringInfoData	ds;
//
//			/* copy expression query without SELECT keyword (expr->query + 7) */
//			Assert(strncmp(expr->query, "SELECT ", 7) == 0);
//
//			/* And do the string hacking */
//			initStringInfo(&ds);
//
//			appendStringInfo(&ds, "SELECT \"%s\" IN (%s)",
//							 varname, expr->query + 7);
//
//			pfree(expr->query);
//			expr->query = pstrdup(ds.data);
//			/* Adjust expr's namespace to include the case variable */
//			expr->ns = plpgsql_ns_top();
//
//			pfree(ds.data);
//		}
//	}
//
//	return (PLpgSQL_stmt *) new;
//}

static PLpgSQL_stmt *make_stmt_raise(int lloc, int elevel, char *name,
										 char *message, List *params)
{
	PLpgSQL_stmt_raise *new;
	int					expected_nparams;

	if (name)
		plpgsql_recognize_err_condition(name, false);
	new = palloc0(sizeof(PLpgSQL_stmt_raise));

	new->cmd_type = PLPGSQL_STMT_RAISE;
	new->lineno = plpgsql_location_to_lineno(lloc);
	new->elog_level = elevel;
	new->condname = name;
	new->message = message;
	new->params = params;

	/* check raise parameters */
	if (message)
	{
		expected_nparams = 0;
		for (;*message;++message)
		{
			if (message[0] == '%')
			{
				/* ignore literal % characters */
				if (message[1] == '%')
					message++;
				else
					expected_nparams++;
			}
		}

		if (expected_nparams < list_length(params))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("too many parameters specified for RAISE")));
		if (expected_nparams > list_length(params))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("too few parameters specified for RAISE")));
	}

	return (PLpgSQL_stmt*)new;
}
