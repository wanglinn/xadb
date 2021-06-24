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
#include "utils/lsyscache.h"

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
											RawParseMode parsemode,
											bool isexpression,
											bool valid_sql,
											bool trim,
											int *startloc,
											int *endtoken);
static PLpgSQL_expr *make_plpg_sql_expr(const char *query, RawParseMode parseMode);
static	PLpgSQL_expr	*read_sql_expression(int until,
											 const char *expected);
static	PLpgSQL_expr	*read_sql_expression2(int until, int until2,
											  const char *expected,
											  int *endtoken);
static	PLpgSQL_expr	*read_sql_stmt(void);
static	PLpgSQL_type	*read_datatype(int tok);
static	PLpgSQL_stmt	*make_execsql_stmt(int firsttoken, int location);
static	PLpgSQL_stmt_fetch *read_fetch_direction(void);
static	void			 complete_direction(PLpgSQL_stmt_fetch *fetch,
											bool *check_FROM);
static	PLpgSQL_stmt	*make_return_stmt(int location);
static  PLpgSQL_stmt	*make_case(int location, PLpgSQL_expr *t_expr,
								   List *case_when_list, List *else_stmts);
static	char			*NameOfDatum(PLwdatum *wdatum);
static	void			 check_assignable(PLpgSQL_datum *datum, int location);
static	void			 read_into_target(PLpgSQL_variable **target, bool bulk_collect, bool *strict);
static	PLpgSQL_row		*read_into_scalar_list(char *initial_name,
											   PLpgSQL_datum *initial_datum,
											   int initial_location);
static	PLpgSQL_row		*make_scalar_list1(char *initial_name,
										   PLpgSQL_datum *initial_datum,
										   int lineno, int location);
static	void			 check_sql_expr(const char *stmt,
										RawParseMode parseMode, int location);
static	void			 plpgsql_sql_error_callback(void *arg);
static	PLpgSQL_type	*parse_datatype(const char *string, int location);
static	void			 check_labels(const char *start_label,
									  const char *end_label,
									  int end_location);
static PLpgSQL_stmt		*make_stmt_raise(int lloc, int elevel, char *name,
										 char *message, List *params);
static PLpgSQL_expr    *read_cursor_args(PLpgSQL_var *cursor, int until, const char *expected);
static PLpgSQL_stmt_func *read_func_stmt(int startloc, int endloc);
static PLoraSQL_type   *plora_build_type(char *name, int location, Oid oid, int typmod);
static PLoraSQL_type   *read_type_define(char *name, int location);
static PLpgSQL_stmt	*make_piperow_stmt(int location);
static bool list_have_sub_transaction(List *list);

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
			PLpgSQL_datum   *row;
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

%type <declhdr> decl_sect decl_sect_top
%type <varname> decl_varname
%type <boolean>	decl_const decl_notnull exit_type
%type <expr>	decl_defval decl_cursor_query
%type <dtype>	decl_datatype

%type <expr>	expr_until_semi/* expr_until_rightbracket*/
%type <expr>	expr_until_then expr_until_loop opt_expr_until_when
%type <expr>	opt_exitcond

%type <str>		any_identifier opt_block_label opt_block_label_top opt_loop_label opt_label
%type <str>		/*option_value */raise_exception
%type <ival>	raise_level opt_raise_level

%type <list>	proc_sect stmt_elsifs stmt_else
%type <stmt>	proc_stmt pl_block pl_block_top
%type <stmt>	stmt_assign stmt_commit stmt_if stmt_loop stmt_while stmt_exit
%type <stmt>	stmt_return stmt_raise stmt_rollback /*stmt_assert*/ stmt_execsql
/*%type <stmt>	stmt_dynexecute stmt_for stmt_perform stmt_getdiag*/
%type <stmt>	/*stmt_open stmt_fetch stmt_move stmt_close*/ stmt_null
/*%type <stmt>	stmt_case stmt_foreach_a*/
%type <stmt>	stmt_goto stmt_case
%type <stmt>	for_control stmt_dynexecute
%type <stmt>	stmt_for stmt_func stmt_open stmt_close stmt_fetch stmt_piperow
%type <loop_body>	loop_body
%type <forvariable>	for_variable

%type <casewhen>	case_when
%type <list>	case_when_list opt_case_else

%type <list>	proc_exceptions
%type <exception_block> exception_sect
%type <exception>	proc_exception
%type <condition>	proc_conditions proc_condition

%type <keyword>	unreserved_keyword

%type <var>		cursor_variable
%type <fetch>	opt_fetch_direction
%type <datum>	decl_cursor_args
%type <datum>	decl_cursor_arg
%type <list>	decl_cursor_arglist

/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str>	IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
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

%token <keyword> POK_SELECT POK_SHARE POK_SIZE POK_SQL POK_START POK_STRICT POK_SUBTYPE

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

%token <keyword> POK_ZONE POK_IMPORT POK_NEXT POK_LAST POK_ABSOLUTE POK_RELATIVE POK_FORWARD POK_BACKWARD

/* special keywords */
%token <keyword> POKS_REF_CURSOR POKS_IS_ARRAY

%%

pl_function		: pl_block_top opt_semi
					{
						plpgsql_parse_result = (PLpgSQL_stmt_block *) $1;
					}
				;

opt_semi		:
				| ';'
				;

pl_block_top	: decl_sect_top POK_BEGIN proc_sect exception_sect POK_END opt_label
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
						new->have_sub_transaction = list_have_sub_transaction(new->body);

						if ($1.label == NULL &&
							$6 != NULL)
							$1.label = $6;

						check_labels($1.label, $6, @6);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;

/* top block DECLARE is optional */
decl_sect_top	: opt_block_label_top
					{
						/* done with decls, so resume identifier lookup */
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label_top POK_DECLARE
					{
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label_top POK_DECLARE decl_stmts_top
					{
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						/* Remember variables declared in decl_stmts */
						$$.n_initvars = plpgsql_add_initdatums(&($$.initvarnos));
					}
				| opt_block_label_top decl_stmts_top
					{
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
						$$.label	  = $1;
						/* Remember variables declared in decl_stmts */
						$$.n_initvars = plpgsql_add_initdatums(&($$.initvarnos));
					}
				;

opt_block_label_top: opt_block_label
					{
						/* Forget any variables created before block */
						plpgsql_add_initdatums(NULL);
						/*
						 * Disable scanner lookup of identifiers while
						 * we process the decl_stmts
						 */
						plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;

						$$ = $1;
					}

decl_stmts_top	: decl_stmts_top decl_statement_top
				| decl_statement_top
				;

decl_statement_top: decl_statement
				| POK_PRAGMA T_WORD ';'
					{
						if ($2.quoted)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									errmsg("syntax error"),
									parser_errposition(@2)));

						if (strcmp($2.ident, "autonomous_transaction") == 0)
						{
							/* for now ignore "PRAGMA AUTONOMOUS_TRANSACTION" */
							;
						}else
						{
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									errmsg("syntax error"),
									parser_errposition(@2)));
						}
					}
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
						new->have_sub_transaction = list_have_sub_transaction(new->body);

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
						if ($4)
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
				| POK_TYPE decl_varname POK_IS
					{
						read_type_define($2.name, @2);
					}
				| POK_CURSOR decl_varname
					{ plpgsql_ns_push($2.name, PLPGSQL_LABEL_OTHER); }
				  decl_cursor_args decl_is_for decl_cursor_query
					{
						PLpgSQL_var *new;
						PLpgSQL_expr *curname_def;
						char		buf[1024];
						char		*cp1;
						char		*cp2;

						/* pop local namespace for cursor args */
						plpgsql_ns_pop();

						new = (PLpgSQL_var *)
							plpgsql_build_variable($2.name, $2.lineno,
												   plpgsql_build_datatype(REFCURSOROID,
																		  -1,
																		  InvalidOid,
																		  NULL),
												   true);

						curname_def = palloc0(sizeof(PLpgSQL_expr));

						strcpy(buf, "SELECT ");
						cp1 = new->refname;
						cp2 = buf + strlen(buf);
						/*
						 * Don't trust standard_conforming_strings here;
						 * it might change before we use the string.
						 */
						if (strchr(cp1, '\\') != NULL)
							*cp2++ = ESCAPE_STRING_SYNTAX;
						*cp2++ = '\'';
						while (*cp1)
						{
							if (SQL_STR_DOUBLE(*cp1, true))
								*cp2++ = *cp1;
							*cp2++ = *cp1++;
						}
						/*strcpy(cp2, "'::pg_catalog.refcursor");*/
						/* need modify */
						strcpy(cp2, "'");
						curname_def->query = pstrdup(buf);
						new->default_val = curname_def;

						new->cursor_explicit_expr = $6;
						if ($4 == NULL)
							new->cursor_explicit_argrow = -1;
						else
							new->cursor_explicit_argrow = $4->dno;
						new->cursor_options = CURSOR_OPT_FAST_PLAN;
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
				| opt_block_label stmt_assign
						{ $$ = $2; castStmt(assign, ASSIGN, $$)->label = $1; plpgsql_ns_pop(); }
				| opt_block_label stmt_if
						{ $$ = $2; castStmt(if, IF, $$)->label = $1; plpgsql_ns_pop(); }
				| opt_block_label stmt_exit
						{ $$ = $2; castStmt(exit, EXIT, $$)->block_name = $1; plpgsql_ns_pop(); }
				| stmt_func
						{ $$ = $1; }
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
							plpgsql_ns_pop();
						}
				| opt_block_label stmt_piperow
						{
							$$ = $2;
							switch($$->cmd_type)
							{
							case PLPGSQL_STMT_RETURN_NEXT:
								((PLpgSQL_stmt_return_next*)$$)->label = $1;
								break;
							default:
								ereport(ERROR,
										(errcode(ERRCODE_INTERNAL_ERROR),
										 errmsg("Unknown Pl/pgsql return type for stmt %d", $$->cmd_type)));
							}
							plpgsql_ns_pop();
						}
				| opt_block_label stmt_raise
						{ $$ = $2; castStmt(raise, RAISE, $$)->label = $1; plpgsql_ns_pop(); }
				| stmt_goto
						{ $$ = $1; }
				| stmt_loop
						{ $$ = $1; }
				| stmt_while
						{ $$ = $1; }
				| stmt_for
						{ $$ = $1; }
				| opt_block_label stmt_case
						{ $$ = $2; castStmt(case, CASE, $$)->label = $1; plpgsql_ns_pop(); }
				| stmt_null
						{ $$ = $1; }
				| opt_block_label stmt_execsql
						{ $$ = $2; castStmt(execsql, EXECSQL, $$)->label = $1; plpgsql_ns_pop(); }
				| opt_block_label stmt_open
						{ $$ = $2; castStmt(open, OPEN, $$)->label = $1; plpgsql_ns_pop(); }
				| opt_block_label stmt_close
						{ $$ = $2; castStmt(close, CLOSE, $$)->label = $1; plpgsql_ns_pop(); }
				| opt_block_label stmt_fetch
						{ $$ = $2; castStmt(fetch, FETCH, $$)->label = $1; plpgsql_ns_pop(); }
				| opt_block_label stmt_commit
						{
							$$ = $2;
							castStmt(sub_commit, SUB_COMMIT, $$)->label = $1;
							plpgsql_ns_pop();
						}
				| opt_block_label stmt_rollback
						{
							$$ = $2;
							castStmt(sub_rollback, SUB_ROLLBACK, $$)->label = $1;
							plpgsql_ns_pop();
						}
				| opt_block_label stmt_dynexecute
						{ $$ = $2; castStmt(dynexecute, DYNEXECUTE, $$)->label = $1; plpgsql_ns_pop(); }
				;

stmt_assign		: T_DATUM
					{
						PLpgSQL_stmt_assign *new;
						RawParseMode pmode;

						/* see how many names identify the datum */
						switch ($1.ident ? 1 : list_length($1.idents))
						{
							case 1:
								pmode = RAW_PARSE_PLPGSQL_ASSIGN1;
								break;
							case 2:
								pmode = RAW_PARSE_PLPGSQL_ASSIGN2;
								break;
							case 3:
								pmode = RAW_PARSE_PLPGSQL_ASSIGN3;
								break;
							default:
								elog(ERROR, "unexpected number of names");
								pmode = 0; /* keep compiler quiet */
						}

						check_assignable($1.datum, @1);
						new = palloc0(sizeof(PLpgSQL_stmt_assign));
						new->cmd_type = PLPGSQL_STMT_ASSIGN;
						new->lineno   = plpgsql_location_to_lineno(@1);
						new->varno = $1.datum->dno;
						/* Push back the head name to include it in the stmt */
						plpgsql_push_back_token(T_DATUM);
						new->expr = read_sql_construct(';', 0, 0, ";",
													   pmode,
													   false, true, true,
													   NULL, NULL);

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_func		: opt_block_label T_CWORD '('
					{
						$$ = NULL;
						if (list_length($2.idents) == 2 &&
							plpgsql_peek() == ')' )
						{
							/* test is varray.extend() */
							PLpgSQL_nsitem *nsi;
							PLpgSQL_var *var;
							nsi = plpgsql_ns_lookup(plpgsql_ns_top(),
													false,
													strVal(linitial($2.idents)),
													NULL,
													NULL,
													NULL);
							if (nsi->itemtype == PLPGSQL_NSTYPE_VAR &&
								(var = (PLpgSQL_var*)plpgsql_Datums[nsi->itemno]) != NULL &&
								var->datatype->typisarray &&
								strcmp(strVal(llast($2.idents)), "extend") == 0)
							{
								PLpgSQL_stmt_assign *assign;
								char *sql = psprintf("pg_catalog.array_append(\"%s\", NULL)", strVal(linitial($2.idents)));

								assign = palloc0(sizeof(PLpgSQL_stmt_assign));
								assign->cmd_type = PLPGSQL_STMT_ASSIGN;
								assign->lineno   = plpgsql_location_to_lineno(@2);
								assign->varno = var->dno;
								assign->expr  = make_plpg_sql_expr(sql, RAW_PARSE_PLPGSQL_EXPR);
								assign->label = $1;
								pfree(sql);

								yylex();	/* token is ')' */
								if (yylex() != ';')
									yyerror("syntax error");

								$$ = (PLpgSQL_stmt *)assign;
							}
						}
						if ($$ == NULL)
						{
							PLpgSQL_stmt_func *func = read_func_stmt(@2, @3);
							func->label = $1;
							$$ = (PLpgSQL_stmt *)func;
						}
						plpgsql_ns_pop();
					}
				| opt_block_label T_WORD '('
					{
						PLpgSQL_stmt_func *func = read_func_stmt(@2, @3);
						func->label = $1;
						$$ = (PLpgSQL_stmt *)func;
						plpgsql_ns_pop();
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
														RAW_PARSE_PLPGSQL_EXPR,
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
						plpgsql_ns_pop();
					}
				;

exception_sect	:
					{ $$ = NULL; }
				| POK_EXCEPTION
					{
						/*
						 * We use a mid-rule action to add these
						 * special variables to the namespace before
						 * parsing the WHEN clauses themselves.  The
						 * scope of the names extends to the end of the
						 * current block.
						 */
						int			lineno = plpgsql_location_to_lineno(@1);
						PLpgSQL_exception_block *new = palloc(sizeof(PLpgSQL_exception_block));
						PLpgSQL_variable *var;

						var = plpgsql_build_variable("sqlstate", lineno,
													 plpgsql_build_datatype(TEXTOID,
																			-1,
																			plpgsql_curr_compile->fn_input_collation,
																			NULL),
													 true);
						((PLpgSQL_var *) var)->isconst = true;
						new->sqlstate_varno = var->dno;

						var = plpgsql_build_variable("sqlerrm", lineno,
													 plpgsql_build_datatype(TEXTOID,
																			-1,
																			plpgsql_curr_compile->fn_input_collation,
																			NULL),
													 true);
						((PLpgSQL_var *) var)->isconst = true;
						new->sqlerrm_varno = var->dno;

						var = plpgsql_build_variable("sqlcode", lineno,
													 plpgsql_build_datatype(INT4OID,
																			-1,
																			plpgsql_curr_compile->fn_input_collation,
																			NULL),
													 true);
						((PLpgSQL_var *) var)->isconst = true;
						new->sqlcode_varno = var->dno;

						$<exception_block>$ = new;
					}
					proc_exceptions
					{
						PLpgSQL_exception_block *new = $<exception_block>2;
						new->exc_list = $3;

						$$ = new;
					}
				;

proc_exceptions	: proc_exceptions proc_exception
						{
							$$ = lappend($1, $2);
						}
				| proc_exception
						{
							$$ = list_make1($1);
						}
				;

proc_exception	: POK_WHEN proc_conditions POK_THEN proc_sect
					{
						PLpgSQL_exception *new;

						new = palloc0(sizeof(PLpgSQL_exception));
						new->lineno = plpgsql_location_to_lineno(@1);
						new->conditions = $2;
						new->action = $4;

						$$ = new;
					}
				;

proc_conditions	: proc_conditions POK_OR proc_condition
						{
							PLpgSQL_condition	*old;

							for (old = $1; old->next != NULL; old = old->next)
								/* skip */ ;
							old->next = $3;
							$$ = $1;
						}
				| proc_condition
						{
							$$ = $1;
						}
				;

proc_condition	: any_identifier
						{
							if (strcmp($1, "sqlstate") != 0)
							{
								if (strcmp($1, "zero_divide") == 0)
									$$ = plpgsql_parse_err_condition("division_by_zero");
								else
									$$ = plpgsql_parse_err_condition($1);
							}
							else
							{
								PLpgSQL_condition *new;
								char   *sqlstatestr;

								/* next token should be a string literal */
								if (yylex() != SCONST)
									yyerror("syntax error");
								sqlstatestr = yylval.str;

								if (strlen(sqlstatestr) != 5)
									yyerror("invalid SQLSTATE code");
								if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
									yyerror("invalid SQLSTATE code");

								new = palloc(sizeof(PLpgSQL_condition));
								new->sqlerrstate =
									MAKE_SQLSTATE(sqlstatestr[0],
												  sqlstatestr[1],
												  sqlstatestr[2],
												  sqlstatestr[3],
												  sqlstatestr[4]);
								new->condname = sqlstatestr;
								new->next = NULL;

								$$ = new;
							}
						}
				;

expr_until_semi :
					{ $$ = read_sql_expression(';', ";"); }
				;

expr_until_then :
					{ $$ = read_sql_expression(POK_THEN, "THEN"); }
				;

expr_until_loop :
					{ $$ = read_sql_expression(POK_LOOP, "LOOP"); }
				;

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

opt_loop_label	:
					{
						plpgsql_ns_push(NULL, PLPGSQL_LABEL_LOOP);
						$$ = NULL;
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
						plpgsql_ns_push($2, PLPGSQL_LABEL_LOOP);
						$$ = $2;
					}
				;

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

stmt_loop		: opt_loop_label POK_LOOP loop_body
					{
						PLpgSQL_stmt_loop *new;

						new = palloc0(sizeof(PLpgSQL_stmt_loop));
						new->cmd_type = PLPGSQL_STMT_LOOP;
						new->lineno   = plpgsql_location_to_lineno(@2);
						new->label	  = $1;
						new->body	  = $3.stmts;

						check_labels($1, $3.end_label, $3.end_label_location);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_while		: opt_loop_label POK_WHILE expr_until_loop loop_body
					{
						PLpgSQL_stmt_while *new;

						new = palloc0(sizeof(PLpgSQL_stmt_while));
						new->cmd_type = PLPGSQL_STMT_WHILE;
						new->lineno   = plpgsql_location_to_lineno(@2);
						new->label	  = $1;
						new->cond	  = $3;
						new->body	  = $4.stmts;

						check_labels($1, $4.end_label, $4.end_label_location);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_for		: opt_loop_label POK_FOR for_control loop_body
					{
						/* This runs after we've scanned the loop body */
						if ($3->cmd_type == PLPGSQL_STMT_FORI)
						{
							PLpgSQL_stmt_fori		*new;

							new = (PLpgSQL_stmt_fori *) $3;
							new->lineno   = plpgsql_location_to_lineno(@2);
							new->label	  = $1;
							new->body	  = $4.stmts;
							$$ = (PLpgSQL_stmt *) new;
						}
						else
						{
							PLpgSQL_stmt_forq		*new;

							Assert($3->cmd_type == PLPGSQL_STMT_FORS ||
								   $3->cmd_type == PLPGSQL_STMT_FORC ||
								   $3->cmd_type == PLPGSQL_STMT_DYNFORS);
							/* forq is the common supertype of all three */
							new = (PLpgSQL_stmt_forq *) $3;
							new->lineno   = plpgsql_location_to_lineno(@2);
							new->label	  = $1;
							new->body	  = $4.stmts;
							$$ = (PLpgSQL_stmt *) new;
						}

						check_labels($1, $4.end_label, $4.end_label_location);
						/* close namespace started in opt_loop_label */
						plpgsql_ns_pop();
					}
				;

for_control		: for_variable POK_IN
					{
						int			tok = yylex();
						int			tokloc = yylloc;

						if (tok == POK_EXECUTE)
						{
							/* EXECUTE means it's a dynamic FOR loop */
							PLpgSQL_stmt_dynfors	*new;
							PLpgSQL_expr			*expr;
							int						term;

							expr = read_sql_expression2(POK_LOOP, POK_USING,
														"LOOP or USING",
														&term);

							new = palloc0(sizeof(PLpgSQL_stmt_dynfors));
							new->cmd_type = PLPGSQL_STMT_DYNFORS;
							if ($1.row)
							{
								new->var = (PLpgSQL_variable *) $1.row;
								check_assignable($1.row, @1);
							}
							else if ($1.scalar)
							{
								/* convert single scalar to list */
								new->var = (PLpgSQL_variable *)
									make_scalar_list1($1.name, $1.scalar,
													  $1.lineno, @1);
								/* make_scalar_list1 did check_assignable */
							}
							else
							{
								ereport(ERROR,
										(errcode(ERRCODE_DATATYPE_MISMATCH),
										 errmsg("loop variable of loop over rows must be a record or row variable or list of scalar variables"),
										 parser_errposition(@1)));
							}
							new->query = expr;

							if (term == POK_USING)
							{
								do
								{
									expr = read_sql_expression2(',', POK_LOOP,
																", or LOOP",
																&term);
									new->params = lappend(new->params, expr);
								} while (term == ',');
							}

							$$ = (PLpgSQL_stmt *) new;
						}
						else if (tok == T_DATUM &&
								 yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR &&
								 ((PLpgSQL_var *) yylval.wdatum.datum)->datatype->typoid == REFCURSOROID)
						{
							/* It's FOR var IN cursor */
							PLpgSQL_stmt_forc	*new;
							PLpgSQL_var			*cursor = (PLpgSQL_var *) yylval.wdatum.datum;

							new = (PLpgSQL_stmt_forc *) palloc0(sizeof(PLpgSQL_stmt_forc));
							new->cmd_type = PLPGSQL_STMT_FORC;
							new->curvar = cursor->dno;

							/* Should have had a single variable name */
							if ($1.scalar && $1.row)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("cursor FOR loop must have only one target variable"),
										 parser_errposition(@1)));

							/* can't use an unbound cursor this way */
							if (cursor->cursor_explicit_expr == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("cursor FOR loop must use a bound cursor variable"),
										 parser_errposition(tokloc)));

							/* collect cursor's parameters if any */
							new->argquery = read_cursor_args(cursor,
															 POK_LOOP,
															 "LOOP");

							/* create loop's private RECORD variable */
							new->var = (PLpgSQL_variable *)
								plpgsql_build_record($1.name,
													 $1.lineno,
													 NULL,
													 RECORDOID,
													 true);

							$$ = (PLpgSQL_stmt *) new;
						}
						else
						{
							PLpgSQL_expr	*expr1;
							int				expr1loc;
							bool			reverse = false;

							/*
							 * We have to distinguish between two
							 * alternatives: FOR var IN a .. b and FOR
							 * var IN query. Unfortunately this is
							 * tricky, since the query in the second
							 * form needn't start with a SELECT
							 * keyword.  We use the ugly hack of
							 * looking for two periods after the first
							 * token. We also check for the REVERSE
							 * keyword, which means it must be an
							 * integer loop.
							 */
							if (tok_is_keyword(tok, &yylval,
											   POK_REVERSE, "reverse"))
								reverse = true;
							else
								plpgsql_push_back_token(tok);

							/*
							 * Read tokens until we see either a ".."
							 * or a LOOP.  The text we read may be either
							 * an expression or a whole SQL statement, so
							 * we need to invoke read_sql_construct directly,
							 * and tell it not to check syntax yet.
							 */
							expr1 = read_sql_construct(DOT_DOT,
													   POK_LOOP,
													   0,
													   "LOOP",
													   RAW_PARSE_DEFAULT,
													   true,
													   false,
													   true,
													   &expr1loc,
													   &tok);

							if (tok == DOT_DOT)
							{
								/* Saw "..", so it must be an integer loop */
								PLpgSQL_expr		*expr2;
								PLpgSQL_expr		*expr_by;
								PLpgSQL_var			*fvar;
								PLpgSQL_stmt_fori	*new;

								/*
								 * Relabel first expression as an expression;
								 * then we can check its syntax.
								 */
								expr1->parseMode = RAW_PARSE_PLPGSQL_EXPR;
								check_sql_expr(expr1->query, expr1->parseMode,
											   expr1loc);

								/* Read and check the second one */
								expr2 = read_sql_expression2(POK_LOOP, POK_BY,
															 "LOOP",
															 &tok);

								/* Get the BY clause if any */
								if (tok == POK_BY)
									expr_by = read_sql_expression(POK_LOOP,
																  "LOOP");
								else
									expr_by = NULL;

								/* Should have had a single variable name */
								if ($1.scalar && $1.row)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("integer FOR loop must have only one target variable"),
											 parser_errposition(@1)));

								/* create loop's private variable */
								fvar = (PLpgSQL_var *)
									plpgsql_build_variable($1.name,
														   $1.lineno,
														   plpgsql_build_datatype(INT4OID,
																				  -1,
																				  InvalidOid,
																				  NULL),
														   true);

								new = palloc0(sizeof(PLpgSQL_stmt_fori));
								new->cmd_type = PLPGSQL_STMT_FORI;
								new->var	  = fvar;
								new->reverse  = reverse;
								if (reverse)
								{
									new->lower	  = expr2;
									new->upper	  = expr1;
								}
								else
								{
									new->lower	  = expr1;
									new->upper	  = expr2;
								}
								new->step	  = expr_by;

								$$ = (PLpgSQL_stmt *) new;
							}
							else
							{
								/*
								 * No "..", so it must be a query loop.
								 */
								PLpgSQL_stmt_fors	*new;

								if (reverse)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("cannot specify REVERSE in query FOR loop"),
											 parser_errposition(tokloc)));

								/* Check syntax as a regular query */
								check_sql_expr(expr1->query, expr1->parseMode,
											   expr1loc);

								new = palloc0(sizeof(PLpgSQL_stmt_fors));
								new->cmd_type = PLPGSQL_STMT_FORS;
								if ($1.row)
								{
									new->var = (PLpgSQL_variable *) $1.row;
									check_assignable($1.row, @1);
								}
								else if ($1.scalar)
								{
									/* convert single scalar to list */
									new->var = (PLpgSQL_variable *)
										make_scalar_list1($1.name, $1.scalar,
														  $1.lineno, @1);
									/* make_scalar_list1 did check_assignable */
								}
								else
								{
									/* auto create loop's variable */
									new->var = (PLpgSQL_variable *)plpgsql_build_record($1.name, $1.lineno, NULL, RECORDOID, true);
								}

								new->query = expr1;
								$$ = (PLpgSQL_stmt *) new;
							}
						}
					}
				;

/*
 * Processing the for_variable is tricky because we don't yet know if the
 * FOR is an integer FOR loop or a loop over query results.  In the former
 * case, the variable is just a name that we must instantiate as a loop
 * local variable, regardless of any other definition it might have.
 * Therefore, we always save the actual identifier into $$.name where it
 * can be used for that case.  We also save the outer-variable definition,
 * if any, because that's what we need for the loop-over-query case.  Note
 * that we must NOT apply check_assignable() or any other semantic check
 * until we know what's what.
 *
 * However, if we see a comma-separated list of names, we know that it
 * can't be an integer FOR loop and so it's OK to check the variables
 * immediately.  In particular, for T_WORD followed by comma, we should
 * complain that the name is not known rather than say it's a syntax error.
 * Note that the non-error result of this case sets *both* $$.scalar and
 * $$.row; see the for_control production.
 */
for_variable	: T_DATUM
					{
						$$.name = NameOfDatum(&($1));
						$$.lineno = plpgsql_location_to_lineno(@1);
						if ($1.datum->dtype == PLPGSQL_DTYPE_ROW
							|| $1.datum->dtype == PLPGSQL_DTYPE_REC)
						{
							$$.scalar = NULL;
							$$.row = $1.datum;
						}
						else
						{
							int			tok;

							$$.scalar = $1.datum;
							$$.row = NULL;
							/* check for comma-separated list */
							tok = yylex();
							plpgsql_push_back_token(tok);
							if (tok == ',')
								$$.row = (PLpgSQL_datum *)
									read_into_scalar_list($$.name,
														  $$.scalar,
														  @1);
						}
					}
				| T_WORD
					{
						int			tok;

						$$.name = $1.ident;
						$$.lineno = plpgsql_location_to_lineno(@1);
						$$.scalar = NULL;
						$$.row = NULL;
						/* check for comma-separated list */
						tok = yylex();
						plpgsql_push_back_token(tok);
						if (tok == ',')
							word_is_not_variable(&($1), @1);
					}
				| T_CWORD
					{
						/* just to give a better message than "syntax error" */
						cword_is_not_variable(&($1), @1);
					}
				;

loop_body		: proc_sect POK_END POK_LOOP opt_label ';'
					{
						$$.stmts = $1;
						$$.end_label = $4;
						$$.end_label_location = @4;
					}
				;

stmt_case		: POK_CASE opt_expr_until_when case_when_list opt_case_else POK_END POK_CASE ';'
					{
						$$ = make_case(@1, $2, $3, $4);
					}
				;

opt_expr_until_when	:
					{
						PLpgSQL_expr *expr = NULL;
						int	tok = yylex();

						if (tok != POK_WHEN)
						{
							plpgsql_push_back_token(tok);
							expr = read_sql_expression(POK_WHEN, "WHEN");
						}
						plpgsql_push_back_token(POK_WHEN);
						$$ = expr;
					}
				;

case_when_list	: case_when_list case_when
					{
						$$ = lappend($1, $2);
					}
				| case_when
					{
						$$ = list_make1($1);
					}
				;

case_when		: POK_WHEN expr_until_then proc_sect
					{
						PLpgSQL_case_when *new = palloc(sizeof(PLpgSQL_case_when));

						new->lineno	= plpgsql_location_to_lineno(@1);
						new->expr	= $2;
						new->stmts	= $3;
						$$ = new;
					}
				;

opt_case_else	:
					{
						$$ = NIL;
					}
				| POK_ELSE proc_sect
					{
						/*
						 * proc_sect could return an empty list, but we
						 * must distinguish that from not having ELSE at all.
						 * Simplest fix is to return a list with one NULL
						 * pointer, which make_case() must take care of.
						 */
						if ($2 != NIL)
							$$ = $2;
						else
							$$ = list_make1(NULL);
					}
				;

stmt_null		: POK_NULL ';'
					{
						/* We do not bother building a node for NULL */
						$$ = NULL;
					}
				;

stmt_commit		: POK_COMMIT ';'
					{
						/*
						 * for now PG not support transaction in pl sql,
						 * so we let "COMMIT" is sub transaction commit
						 */
						PLpgSQL_stmt_sub_commit *new = palloc0(sizeof(PLpgSQL_stmt_sub_commit));

						new->cmd_type = PLPGSQL_STMT_SUB_COMMIT;
						new->lineno   = plpgsql_location_to_lineno(@1);

						$$ = (PLpgSQL_stmt*)new;
					}
				;

stmt_rollback	: POK_ROLLBACK ';'
					{
						/*
						 * for now PG not support transaction in pl sql,
						 * so we let "rollback" is sub transaction rollback
						 */
						PLpgSQL_stmt_sub_rollback *new = palloc0(sizeof(PLpgSQL_stmt_sub_rollback));

						new->cmd_type = PLPGSQL_STMT_SUB_ROLLBACK;
						new->lineno   = plpgsql_location_to_lineno(@1);

						$$ = (PLpgSQL_stmt*)new;
					}
				;

/*
 * T_WORD+T_CWORD match any initial identifier that is not a known plpgsql
 * variable.  (The composite case is probably a syntax error, but we'll let
 * the core parser decide that.)  Normally, we should assume that such a
 * word is a SQL statement keyword that isn't also a plpgsql keyword.
 * However, if the next token is assignment or '[', it can't be a valid
 * SQL statement, and what we're probably looking at is an intended variable
 * assignment.  Give an appropriate complaint for that, instead of letting
 * the core parser throw an unhelpful "syntax error".
 */
stmt_execsql	: POK_IMPORT
					{
						$$ = make_execsql_stmt(POK_IMPORT, @1);
					}
				| POK_INSERT
					{
						$$ = make_execsql_stmt(POK_INSERT, @1);
					}
				| POK_SELECT
					{
						$$ = make_execsql_stmt(POK_SELECT, @1);
					}
				| POK_UPDATE
					{
						$$ = make_execsql_stmt(POK_UPDATE, @1);
					}
				| POK_DELETE
					{
						$$ = make_execsql_stmt(POK_DELETE, @1);
					}
				| POK_WITH
					{
						$$ = make_execsql_stmt(POK_WITH, @1);
					}
				;

stmt_open		: POK_OPEN cursor_variable
					{
						PLpgSQL_stmt_open *new;
						int				  tok;

						new = palloc0(sizeof(PLpgSQL_stmt_open));
						new->cmd_type = PLPGSQL_STMT_OPEN;
						new->lineno = plpgsql_location_to_lineno(@1);
						new->curvar = $2->dno;
						new->cursor_options = CURSOR_OPT_FAST_PLAN;

						if ($2->cursor_explicit_expr == NULL)
						{
							/* be nice if we could use opt_scrollable here */
							tok = yylex();

							if (tok != POK_FOR)
								yyerror("syntax error, expected \"FOR\"");

							tok = plpgsql_peek();
							if (tok == POK_SELECT ||
								tok == POK_WITH)
							{
								new->query = read_sql_stmt();
							}else
							{
								int		endtoken;

								new->dynquery =
									read_sql_expression2(POK_USING, ';',
														 "USING or ;",
														 &endtoken);

								/* If we found "USING", collect argument(s) */
								if (endtoken == POK_USING)
								{
									PLpgSQL_expr *expr;

									do
									{
										expr = read_sql_expression2(',', ';',
																	", or ;",
																	&endtoken);
										new->params = lappend(new->params,
															  expr);
									} while (endtoken == ',');
								}
							}
						}
						else
						{
							/* predefined cursor query, so read args */
							new->argquery = read_cursor_args($2, ';', ";");
						}

						$$ = (PLpgSQL_stmt *)new;
					}
				;

cursor_variable	: T_DATUM
					{
						/*
						 * In principle we should support a cursor_variable
						 * that is an array element, but for now we don't, so
						 * just throw an error if next token is '['.
						 */
						if ($1.datum->dtype != PLPGSQL_DTYPE_VAR ||
							plpgsql_peek() == '[')
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("cursor variable must be a simple variable"),
									 parser_errposition(@1)));

						if (((PLpgSQL_var *) $1.datum)->datatype->typoid != REFCURSOROID)
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("variable \"%s\" must be of type cursor or refcursor",
											((PLpgSQL_var *) $1.datum)->refname),
									 parser_errposition(@1)));
						$$ = (PLpgSQL_var *) $1.datum;
					}
				| T_WORD
					{
						/* just to give a better message than "syntax error" */
						word_is_not_variable(&($1), @1);
					}
				| T_CWORD
					{
						/* just to give a better message than "syntax error" */
						cword_is_not_variable(&($1), @1);
					}
				;

stmt_close		: POK_CLOSE cursor_variable ';'
					{
						PLpgSQL_stmt_close *new;

						new = palloc(sizeof(PLpgSQL_stmt_close));
						new->cmd_type = PLPGSQL_STMT_CLOSE;
						new->lineno = plpgsql_location_to_lineno(@1);
						new->curvar = $2->dno;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_fetch		: POK_FETCH opt_fetch_direction cursor_variable POK_INTO
					{
						PLpgSQL_stmt_fetch *fetch = $2;
						PLpgSQL_variable *target;

						/* We have already parsed everything through the INTO keyword */
						read_into_target(&target, false, NULL);

						if (yylex() != ';')
							yyerror("syntax error");

						/*
						 * We don't allow multiple rows in PL/pgSQL's FETCH
						 * statement, only in MOVE.
						 */
						if (fetch->returns_multiple_rows)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("FETCH statement cannot return multiple rows"),
									 parser_errposition(@1)));

						fetch->lineno = plpgsql_location_to_lineno(@1);
						fetch->target	= target;
						fetch->curvar	= $3->dno;
						fetch->is_move	= false;

						$$ = (PLpgSQL_stmt *)fetch;
					}
				;

opt_fetch_direction	:
					{
						$$ = read_fetch_direction();
					}
				;

decl_cursor_args :
					{
						$$ = NULL;
					}
				| '(' decl_cursor_arglist ')'
					{
						PLpgSQL_row *new;
						int i;
						ListCell *l;

						new = palloc0(sizeof(PLpgSQL_row));
						new->dtype = PLPGSQL_DTYPE_ROW;
						new->lineno = plpgsql_location_to_lineno(@1);
						new->rowtupdesc = NULL;
						new->nfields = list_length($2);
						new->fieldnames = palloc(new->nfields * sizeof(char *));
						new->varnos = palloc(new->nfields * sizeof(int));

						i = 0;
						foreach (l, $2)
						{
							PLpgSQL_variable *arg = (PLpgSQL_variable *) lfirst(l);
							new->fieldnames[i] = arg->refname;
							new->varnos[i] = arg->dno;
							i++;
						}
						list_free($2);

						plpgsql_adddatum((PLpgSQL_datum *) new);
						$$ = (PLpgSQL_datum *) new;
					}
				;

decl_cursor_arglist : decl_cursor_arg
					{
						$$ = list_make1($1);
					}
				| decl_cursor_arglist ',' decl_cursor_arg
					{
						$$ = lappend($1, $3);
					}
				;

decl_cursor_arg : decl_varname decl_datatype
					{
						$$ = (PLpgSQL_datum *)
							plpgsql_build_variable($1.name, $1.lineno,
												   $2, true);
					}
				;

decl_cursor_query :
					{
						$$ = read_sql_stmt();
					}
				;

decl_is_for		:	POK_IS |		/* Oracle */
					POK_FOR;		/* SQL standard */

stmt_dynexecute : POK_EXECUTE POK_IMMEDIATE
					{
						PLpgSQL_stmt_dynexecute *new;
						PLpgSQL_expr *expr;
						int endtoken;

						expr = read_sql_construct(POK_INTO, POK_USING, ';',
												  "INTO or USING or ;",
												  RAW_PARSE_PLPGSQL_EXPR,
												  true, true, true,
												  NULL, &endtoken);

						new = palloc(sizeof(PLpgSQL_stmt_dynexecute));
						new->cmd_type = PLPGSQL_STMT_DYNEXECUTE;
						new->lineno = plpgsql_location_to_lineno(@1);
						new->query = expr;
						new->into = false;
						new->strict = false;
						new->target = NULL;
						new->params = NIL;

						/*
						 * We loop to allow the INTO and USING clauses to
						 * appear in either order, since people easily get
						 * that wrong.  This coding also prevents "INTO foo"
						 * from getting absorbed into a USING expression,
						 * which is *really* confusing.
						 */
						for (;;)
						{
							if (endtoken == POK_INTO)
							{
								if (new->into)			/* multiple INTO */
									yyerror("syntax error");
								new->into = true;
								read_into_target(&new->target, false, NULL);
								endtoken = yylex();
							}
							else if (endtoken == POK_USING)
							{
								if (new->params)		/* multiple USING */
									yyerror("syntax error");
								do
								{
									expr = read_sql_construct(',', ';', POK_INTO,
												", or ; or INTO",
												RAW_PARSE_PLPGSQL_EXPR,
												true, true, true,
												NULL, &endtoken);
									new->params = lappend(new->params, expr);
								} while (endtoken == ',');
							}
							else if (endtoken == ';')
								break;
							else
								yyerror("syntax error");
						}

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_piperow : POK_PIPE
				{
					int	tok;

					tok = yylex();
					if (tok == 0)
						yyerror("unexpected end of function definition");

					if (tok_is_keyword(tok, &yylval,
									   POK_ROW, "row"))
					{
						$$ = make_piperow_stmt(@1);
					}
					else
					{
						plpgsql_push_back_token(tok);
						$$ = make_return_stmt(@1);
					}
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

static bool
tok_is_reserved_keyword(int token, union YYSTYPE *lval,
			   int kw_token, const char *kw_str)
{
	if (token == kw_token)
	{
		/* Normal case, was recognized by scanner (no conflicting variable) */
		return true;
	}
	else if (token == T_WORD)
	{
		/*
		 * It's a variable, so recheck the string name.  Note we will not
		 * match composite names (hence an reserved word followed by "."
		 * will not be recognized).
		 */
		if (!lval->word.quoted &&
			lval->word.ident != NULL &&
			strcmp(lval->word.ident, kw_str) == 0)
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
							  RAW_PARSE_PLPGSQL_EXPR,
							  true, true, true, NULL, NULL);
}

/* Convenience routine to read an expression with two possible terminators */
static PLpgSQL_expr *
read_sql_expression2(int until, int until2, const char *expected,
					 int *endtoken)
{
	return read_sql_construct(until, until2, 0, expected,
							  RAW_PARSE_PLPGSQL_EXPR,
							  true, true, true, NULL, endtoken);
}

/* Convenience routine to read a SQL statement that must end with ';' */
static PLpgSQL_expr *
read_sql_stmt(void)
{
	return read_sql_construct(';', 0, 0, ";",
							  RAW_PARSE_DEFAULT,
							  false, true, true, NULL, NULL);
}

/*
 * Read a SQL construct and build a PLpgSQL_expr for it.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * parsemode:	ora_raw_parser() mode to use
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr
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
				   RawParseMode parsemode,
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

	expr = make_plpg_sql_expr(ds.data, RAW_PARSE_PLPGSQL_EXPR);
	pfree(ds.data);

	if (valid_sql)
		check_sql_expr(expr->query, expr->parseMode, startlocation);

	return expr;
}

static PLpgSQL_expr *make_plpg_sql_expr(const char *query, RawParseMode parseMode)
{
	PLpgSQL_expr *expr;

	expr = palloc0(sizeof(PLpgSQL_expr));
	expr->query			= pstrdup(query);
	expr->plan			= NULL;
	expr->paramnos		= NULL;
	expr->target_param	= -1;
	expr->ns			= plpgsql_ns_top();

	return expr;
}

static PLpgSQL_type *
read_datatype(int tok)
{
	StringInfoData		ds;
	char			   *type_name;
	PLpgSQL_type	   *result;
	char			   *dtname;
	int					startlocation;
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
	dtname = NULL;
	if (tok == T_WORD)
	{
		dtname = yylval.word.ident;
	}else if(plorasql_token_is_unreserved_keyword(tok))
	{
		dtname = pstrdup(yylval.keyword);
	}
	if (dtname != NULL)
	{
		tok = yylex();
		if (tok == '%')
		{
			tok = yylex();
			if (tok_is_reserved_keyword(tok, &yylval,
										POK_TYPE, "type"))
			{
				result = plpgsql_parse_wordtype(dtname);
				if (result)
					goto end_read_datatype_;
			}else if (tok == T_WORD &&
				yylval.word.quoted == false &&
				strcmp(yylval.word.ident, "rowtype") == 0)
			{
				PLpgSQL_var *var;
				PLpgSQL_nsitem *item = plpgsql_ns_lookup(plpgsql_ns_top(),
														 false,
														 dtname,
														 NULL,
														 NULL,
														 NULL);
				if (item != NULL &&
					item->itemtype == PLPGSQL_NSTYPE_VAR &&
					(var = (PLpgSQL_var*)plpgsql_Datums[item->itemno]) != NULL)
				{
					result = NULL;
					if (var->datatype->typoid == RECORDOID)
					{
						result = var->datatype;
					}else if(var->datatype->typoid == REFCURSOROID)
					{
						result = plpgsql_build_datatype(RECORDOID, -1, InvalidOid, NULL);
						result->cursor_dno = var->dno;
					}
					if (result)
						goto end_read_datatype_;
				}
				result = plpgsql_parse_wordrowtype(dtname);
				if (result)
					goto end_read_datatype_;
			}
		}else
		{
			result = plpgsql_find_wordtype(dtname);
			if (result)
			{
				plpgsql_push_back_token(tok);
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
			if (tok_is_reserved_keyword(tok, &yylval,
										POK_TYPE, "type"))
			{
				result = plpgsql_parse_cwordtype(dtnames);
				if (result)
					goto end_read_datatype_;
			}
		}else if (tok == T_WORD &&
			yylval.word.quoted == false &&
			strcmp(yylval.word.ident, "rowtype") == 0)
		{
			result = plpgsql_parse_cwordrowtype(dtnames);
			if (result)
				goto end_read_datatype_;
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

end_read_datatype_:
	Assert(result != NULL);
	if (result->typoid != REFCURSOROID &&
		getBaseType(result->typoid) == REFCURSOROID)
		result->typoid = REFCURSOROID;

	return result;
}

static PLpgSQL_stmt *
make_execsql_stmt(int firsttoken, int location)
{
	StringInfoData		ds;
	IdentifierLookup	save_IdentifierLookup;
	PLpgSQL_stmt_execsql *execsql;
	PLpgSQL_expr		*expr;
	PLpgSQL_variable	*target = NULL;
	int					tok;
	int					prev_tok;
	bool				have_into = false;
	int					into_start_loc = -1;
	int					into_end_loc = -1;
	bool				bulk_collect = false;
	bool				have_strict = false;

	initStringInfo(&ds);

	/* special lookup mode for identifiers within the SQL text */
	save_IdentifierLookup = plpgsql_IdentifierLookup;
	plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;

	/*
	 * Scan to the end of the SQL command.  Identify any INTO-variables
	 * clause lurking within it, and parse that via read_into_target().
	 *
	 * Because INTO is sometimes used in the main SQL grammar, we have to be
	 * careful not to take any such usage of INTO as a PL/pgSQL INTO clause.
	 * There are currently three such cases:
	 *
	 * 1. SELECT ... INTO.  We don't care, we just override that with the
	 * PL/pgSQL definition.
	 *
	 * 2. INSERT INTO.  This is relatively easy to recognize since the words
	 * must appear adjacently; but we can't assume INSERT starts the command,
	 * because it can appear in CREATE RULE or WITH.  Unfortunately, INSERT is
	 * *not* fully reserved, so that means there is a chance of a false match;
	 * but it's not very likely.
	 *
	 * 3. IMPORT FOREIGN SCHEMA ... INTO.  This is not allowed in CREATE RULE
	 * or WITH, so we just check for IMPORT as the command's first token.
	 * (If IMPORT FOREIGN SCHEMA returned data someone might wish to capture
	 * with an INTO-variables clause, we'd have to work much harder here.)
	 *
	 * Fortunately, INTO is a fully reserved word in the main grammar, so
	 * at least we need not worry about it appearing as an identifier.
	 *
	 * Any future additional uses of INTO in the main grammar will doubtless
	 * break this logic again ... beware!
	 */
	tok = firsttoken;
	for (;;)
	{
		prev_tok = tok;
		tok = yylex();
		if (have_into && into_end_loc < 0)
			into_end_loc = yylloc;		/* token after the INTO part */
		if (tok == ';')
			break;
		if (tok == 0)
			yyerror("unexpected end of function definition");
		if (tok == POK_BULK)
		{
			/* [BULK COLLECT] INTO */
			into_start_loc = yylloc;
			if ((tok = yylex()) != POK_COLLECT ||
				(tok = yylex()) != POK_INTO)
			{
				yyerror("syntax error");
			}
			bulk_collect = true;
		}
		if (tok == POK_INTO)
		{
			if (prev_tok == POK_INSERT)
				continue;		/* INSERT INTO is not an INTO-target */
			if (firsttoken == POK_IMPORT)
				continue;		/* IMPORT ... INTO is not an INTO-target */
			if (have_into)
				yyerror("INTO specified more than once");
			have_into = true;
			if (into_start_loc < 0)		/* maybe have [BULK COLLECT] */
				into_start_loc = yylloc;

			plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;
			read_into_target(&target, bulk_collect, &have_strict);
			plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_EXPR;
		}
	}

	plpgsql_IdentifierLookup = save_IdentifierLookup;

	if (have_into)
	{
		/*
		 * Insert an appropriate number of spaces corresponding to the
		 * INTO text, so that locations within the redacted SQL statement
		 * still line up with those in the original source text.
		 */
		plpgsql_append_source_text(&ds, location, into_start_loc);
		appendStringInfoSpaces(&ds, into_end_loc - into_start_loc);
		plpgsql_append_source_text(&ds, into_end_loc, yylloc);
	}
	else
		plpgsql_append_source_text(&ds, location, yylloc);

	/* trim any trailing whitespace, for neatness */
	while (ds.len > 0 && scanner_isspace(ds.data[ds.len - 1]))
		ds.data[--ds.len] = '\0';

	expr = make_plpg_sql_expr(ds.data, RAW_PARSE_DEFAULT);
	pfree(ds.data);

	check_sql_expr(expr->query, expr->parseMode, location);

	execsql = palloc(sizeof(PLpgSQL_stmt_execsql));
	execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
	execsql->lineno  = plpgsql_location_to_lineno(location);
	execsql->sqlstmt = expr;
	execsql->into	 = have_into;
	execsql->strict	 = have_strict;
	execsql->target	 = target;
	execsql->bulk_collect = bulk_collect;

	return (PLpgSQL_stmt *) execsql;
}

/*
 * Process remainder of FETCH/MOVE direction after FORWARD or BACKWARD.
 * Allows these cases:
 *   FORWARD expr,  FORWARD ALL,  FORWARD
 *   BACKWARD expr, BACKWARD ALL, BACKWARD
 */
static void
complete_direction(PLpgSQL_stmt_fetch *fetch,  bool *check_FROM)
{
	int			tok;

	tok = yylex();
	if (tok == 0)
		yyerror("unexpected end of function definition");

	if (tok == POK_FROM || tok == POK_IN)
	{
		*check_FROM = false;
		return;
	}

	if (tok == POK_ALL)
	{
		fetch->how_many = FETCH_ALL;
		fetch->returns_multiple_rows = true;
		*check_FROM = true;
		return;
	}

	plpgsql_push_back_token(tok);
	fetch->expr = read_sql_expression2(POK_FROM, POK_IN,
									   "FROM or IN",
									   NULL);
	fetch->returns_multiple_rows = true;
	*check_FROM = false;
}


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
		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			break;
	}
}

/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword.
 */
static void
read_into_target(PLpgSQL_variable **target, bool bulk_collect, bool *strict)
{
	int			tok;

	/* Set default results */
	*target = NULL;

	tok = yylex();
	if (tok == POK_STRICT)
	{
		tok = yylex();
		if (strict)
			*strict = true;
	}

	/*
	 * Currently, a row or record variable can be the single INTO target,
	 * but not a member of a multi-target list.  So we throw error if there
	 * is a comma after it, because that probably means the user tried to
	 * write a multi-target list.  If this ever gets generalized, we should
	 * probably refactor read_into_scalar_list so it handles all cases.
	 */
	switch (tok)
	{
		case T_DATUM:
			if (bulk_collect &&
				yylval.wdatum.datum->dtype != PLPGSQL_DTYPE_VAR)
			{
				ereport(ERROR,
						(errmsg("vairable \"%s\" is not an array", NameOfDatum(&(yylval.wdatum))),
						 errcode(ERRCODE_SYNTAX_ERROR),
						 parser_errposition(yylloc)));
			}

			if (yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_ROW ||
				yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_REC)
			{
				check_assignable(yylval.wdatum.datum, yylloc);
				*target = (PLpgSQL_variable *) yylval.wdatum.datum;

				if ((tok = yylex()) == ',')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("record variable cannot be part of multiple-item INTO list"),
							 parser_errposition(yylloc)));
				plpgsql_push_back_token(tok);
			}
			else if (bulk_collect)
			{
				PLpgSQL_var *var = (PLpgSQL_var*)yylval.wdatum.datum;
				PLpgSQL_type *elem_type;
				PLpgSQL_variable *elem_var;
				Oid elem_type_oid;
				Assert(yylval.wdatum.datum->dtype == PLPGSQL_DTYPE_VAR);	/* we checked */

				elem_type_oid = get_element_type(var->datatype->typoid);
				if (!OidIsValid(elem_type_oid))
				{
					ereport(ERROR,
							(errmsg("vairable \"%s\" is not an array", var->refname),
							 errcode(ERRCODE_SYNTAX_ERROR),
							 parser_errposition(yylloc)));
				}
				elem_type = plpgsql_build_datatype(elem_type_oid,
												   var->datatype->atttypmod,
												   var->datatype->collation,
												   NULL);
				elem_var = plpgsql_build_variable("*internal*",
												  var->lineno,
												  elem_type,
												  false);
				if (elem_var->dtype == PLPGSQL_DTYPE_REC)
				{
					((PLpgSQL_rec*)elem_var)->parent_dno = var->dno;
				}else if (elem_var->dtype == PLPGSQL_DTYPE_ROW)
				{
					((PLpgSQL_row*)elem_var)->parent_dno = var->dno;
				}else
				{
					PLpgSQL_row *row = read_into_scalar_list(var->refname,
															 (PLpgSQL_datum*)elem_var,
															 yylloc);
					row->parent_dno = var->dno;
					elem_var = (PLpgSQL_variable*)row;
				}
				*target = elem_var;
			}
			else
			{
				*target = (PLpgSQL_variable *)
					read_into_scalar_list(NameOfDatum(&(yylval.wdatum)),
										  yylval.wdatum.datum, yylloc);
			}
			break;

		default:
			/* just to give a better message than "syntax error" */
			current_token_is_not_variable(tok);
	}
}

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

	row = palloc0(sizeof(PLpgSQL_row));
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

	row = palloc0(sizeof(PLpgSQL_row));
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
 * beginning at offset "location".  We use this assumption to transpose
 * any error cursor position back to the function source text.
 * If no error cursor is provided, we'll just point at "location".
 */
static void
check_sql_expr(const char *stmt, RawParseMode parseMode, int location)
{
	sql_error_callback_arg cbarg;
	ErrorContextCallback  syntax_errcontext;
	MemoryContext oldCxt;

	if (!plpgsql_check_syntax)
		return;

	cbarg.location = location;

	syntax_errcontext.callback = plpgsql_sql_error_callback;
	syntax_errcontext.arg = &cbarg;
	syntax_errcontext.previous = error_context_stack;
	error_context_stack = &syntax_errcontext;

	oldCxt = MemoryContextSwitchTo(plpgsql_compile_tmp_cxt);
	(void) ora_raw_parser(stmt, RAW_PARSE_DEFAULT);
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
	if (errpos > 0)
	{
		int		myerrpos = getinternalerrposition();

		if (myerrpos > 0)		/* safety check */
			internalerrposition(myerrpos + errpos - 1);
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

	syntax_errcontext.callback = plpgsql_sql_error_callback;
	syntax_errcontext.arg = &cbarg;
	syntax_errcontext.previous = error_context_stack;
	error_context_stack = &syntax_errcontext;

	/* Let the main parser try to parse it under standard SQL rules */
	parseTypeStringForGrammar(string, &type_id, &typmod, false, PARSE_GRAM_ORACLE);
	if (type_id != REFCURSOROID &&
		getBaseType(type_id) == REFCURSOROID)
		type_id = REFCURSOROID;

	/* Restore former ereport callback */
	error_context_stack = syntax_errcontext.previous;

	/* Okay, build a PLpgSQL_type data structure for it */
	return plpgsql_build_datatype(type_id, typmod,
								  plpgsql_curr_compile->fn_input_collation,
								  NULL);
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
static PLpgSQL_stmt *
make_case(int location, PLpgSQL_expr *t_expr,
		  List *case_when_list, List *else_stmts)
{
	PLpgSQL_stmt_case	*new;

	new = palloc(sizeof(PLpgSQL_stmt_case));
	new->cmd_type = PLPGSQL_STMT_CASE;
	new->lineno = plpgsql_location_to_lineno(location);
	new->t_expr = t_expr;
	new->t_varno = 0;
	new->case_when_list = case_when_list;
	new->have_else = (else_stmts != NIL);
	/* Get rid of list-with-NULL hack */
	if (list_length(else_stmts) == 1 && linitial(else_stmts) == NULL)
		new->else_stmts = NIL;
	else
		new->else_stmts = else_stmts;

	/*
	 * When test expression is present, we create a var for it and then
	 * convert all the WHEN expressions to "VAR IN (original_expression)".
	 * This is a bit klugy, but okay since we haven't yet done more than
	 * read the expressions as text.  (Note that previous parsing won't
	 * have complained if the WHEN ... THEN expression contained multiple
	 * comma-separated values.)
	 */
	if (t_expr)
	{
		char	varname[32];
		PLpgSQL_var *t_var;
		ListCell *l;

		/* use a name unlikely to collide with any user names */
		snprintf(varname, sizeof(varname), "__Case__Variable_%d__",
				 plpgsql_nDatums);

		/*
		 * We don't yet know the result datatype of t_expr.  Build the
		 * variable as if it were INT4; we'll fix this at runtime if needed.
		 */
		t_var = (PLpgSQL_var *)
			plpgsql_build_variable(varname, new->lineno,
								   plpgsql_build_datatype(INT4OID,
														  -1,
														  InvalidOid,
														  NULL),
								   true);
		new->t_varno = t_var->dno;

		foreach(l, case_when_list)
		{
			PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);
			PLpgSQL_expr *expr = cwt->expr;
			StringInfoData	ds;

			/* copy expression query without SELECT keyword (expr->query + 7) */
			Assert(strncmp(expr->query, "SELECT ", 7) == 0);

			/* And do the string hacking */
			initStringInfo(&ds);

			appendStringInfo(&ds, "SELECT \"%s\" IN (%s)",
							 varname, expr->query + 7);

			pfree(expr->query);
			expr->query = pstrdup(ds.data);
			/* Adjust expr's namespace to include the case variable */
			expr->ns = plpgsql_ns_top();

			pfree(ds.data);
		}
	}

	return (PLpgSQL_stmt *) new;
}

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

/*
 * Read the arguments (if any) for a cursor, followed by the until token
 *
 * If cursor has no args, just swallow the until token and return NULL.
 * If it does have args, we expect to see "( arg [, arg ...] )" followed
 * by the until token, where arg may be a plain expression, or a named
 * parameter assignment of the form argname := expr. Consume all that and
 * return a SELECT query that evaluates the expression(s) (without the outer
 * parens).
 */
static PLpgSQL_expr *
read_cursor_args(PLpgSQL_var *cursor, int until, const char *expected)
{
	PLpgSQL_expr *expr;
	PLpgSQL_row *row;
	int			tok;
	int			argc;
	char	  **argv;
	StringInfoData ds;
	bool		any_named = false;

	tok = yylex();
	if (cursor->cursor_explicit_argrow < 0)
	{
		/* No arguments expected */
		if (tok == '(')
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cursor \"%s\" has no arguments",
							cursor->refname),
					 parser_errposition(yylloc)));

		if (tok != until)
			yyerror("syntax error");

		return NULL;
	}

	/* Else better provide arguments */
	if (tok != '(')
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cursor \"%s\" has arguments",
						cursor->refname),
				 parser_errposition(yylloc)));

	/*
	 * Read the arguments, one by one.
	 */
	row = (PLpgSQL_row *) plpgsql_Datums[cursor->cursor_explicit_argrow];
	argv = (char **) palloc0(row->nfields * sizeof(char *));

	for (argc = 0; argc < row->nfields; argc++)
	{
		PLpgSQL_expr *item;
		int		endtoken;
		int		argpos;
		int		tok1,
				tok2;
		int		arglocation;

		/* Check if it's a named parameter: "param := value" */
		plpgsql_peek2(&tok1, &tok2, &arglocation, NULL);
		if (tok1 == IDENT && tok2 == COLON_EQUALS)
		{
			char   *argname;
			IdentifierLookup save_IdentifierLookup;

			/* Read the argument name, ignoring any matching variable */
			save_IdentifierLookup = plpgsql_IdentifierLookup;
			plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
			yylex();
			argname = yylval.str;
			plpgsql_IdentifierLookup = save_IdentifierLookup;

			/* Match argument name to cursor arguments */
			for (argpos = 0; argpos < row->nfields; argpos++)
			{
				if (strcmp(row->fieldnames[argpos], argname) == 0)
					break;
			}
			if (argpos == row->nfields)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cursor \"%s\" has no argument named \"%s\"",
								cursor->refname, argname),
						 parser_errposition(yylloc)));

			/*
			 * Eat the ":=". We already peeked, so the error should never
			 * happen.
			 */
			tok2 = yylex();
			if (tok2 != COLON_EQUALS)
				yyerror("syntax error");

			any_named = true;
		}
		else
			argpos = argc;

		if (argv[argpos] != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("value for parameter \"%s\" of cursor \"%s\" specified more than once",
							row->fieldnames[argpos], cursor->refname),
					 parser_errposition(arglocation)));

		/*
		 * Read the value expression. To provide the user with meaningful
		 * parse error positions, we check the syntax immediately, instead of
		 * checking the final expression that may have the arguments
		 * reordered. Trailing whitespace must not be trimmed, because
		 * otherwise input of the form (param -- comment\n, param) would be
		 * translated into a form where the second parameter is commented
		 * out.
		 */
		item = read_sql_construct(',', ')', 0,
								  ",\" or \")",
								  RAW_PARSE_PLPGSQL_EXPR,
								  true, true,
								  false, /* do not trim */
								  NULL, &endtoken);

		argv[argpos] = item->query;

		if (endtoken == ')' && !(argc == row->nfields - 1))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("not enough arguments for cursor \"%s\"",
							cursor->refname),
					 parser_errposition(yylloc)));

		if (endtoken == ',' && (argc == row->nfields - 1))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("too many arguments for cursor \"%s\"",
							cursor->refname),
					 parser_errposition(yylloc)));
	}

	/* Make positional argument list */
	initStringInfo(&ds);
	for (argc = 0; argc < row->nfields; argc++)
	{
		Assert(argv[argc] != NULL);

		/*
		 * Because named notation allows permutated argument lists, include
		 * the parameter name for meaningful runtime errors.
		 */
		appendStringInfoString(&ds, argv[argc]);
		if (any_named)
			appendStringInfo(&ds, " AS %s",
							 quote_identifier(row->fieldnames[argc]));
		if (argc < row->nfields - 1)
			appendStringInfoString(&ds, ", ");
	}

	expr = make_plpg_sql_expr(ds.data, RAW_PARSE_PLPGSQL_EXPR);
	pfree(ds.data);

	/* Next we'd better find the until token */
	tok = yylex();
	if (tok != until)
		yyerror("syntax error");

	return expr;
}

static PLpgSQL_stmt_func *read_func_stmt(int startloc, int endloc)
{
	PLpgSQL_stmt_func *new;
	StringInfoData buf;

	new = palloc0(sizeof(PLpgSQL_stmt_func));
	new->cmd_type = PLPGSQL_STMT_FUNC;
	new->lineno = plpgsql_location_to_lineno(startloc);

	initStringInfo(&buf);
	plpgsql_append_source_text(&buf, startloc, endloc);

	plpgsql_push_back_token('(');
	new->expr = read_sql_construct(';', 0, 0,
									";", RAW_PARSE_PLPGSQL_EXPR,
									true, true, true, NULL, NULL);

	pfree(buf.data);

	return new;
}

/*
 * Read FETCH or MOVE direction clause (everything through FROM/IN).
 */
static PLpgSQL_stmt_fetch *
read_fetch_direction(void)
{
	PLpgSQL_stmt_fetch *fetch;
	int			tok;
	bool		check_FROM = true;

	/*
	 * We create the PLpgSQL_stmt_fetch struct here, but only fill in
	 * the fields arising from the optional direction clause
	 */
	fetch = (PLpgSQL_stmt_fetch *) palloc0(sizeof(PLpgSQL_stmt_fetch));
	fetch->cmd_type = PLPGSQL_STMT_FETCH;
	/* set direction defaults: */
	fetch->direction = FETCH_FORWARD;
	fetch->how_many  = 1;
	fetch->expr		 = NULL;
	fetch->returns_multiple_rows = false;

	tok = yylex();
	if (tok == 0)
		yyerror("unexpected end of function definition");

	if (tok_is_keyword(tok, &yylval,
					   POK_NEXT, "next"))
	{
		/* use defaults */
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_PRIOR, "prior"))
	{
		fetch->direction = FETCH_BACKWARD;
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_FIRST, "first"))
	{
		fetch->direction = FETCH_ABSOLUTE;
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_LAST, "last"))
	{
		fetch->direction = FETCH_ABSOLUTE;
		fetch->how_many  = -1;
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_ABSOLUTE, "absolute"))
	{
		fetch->direction = FETCH_ABSOLUTE;
		fetch->expr = read_sql_expression2(POK_FROM, POK_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_RELATIVE, "relative"))
	{
		fetch->direction = FETCH_RELATIVE;
		fetch->expr = read_sql_expression2(POK_FROM, POK_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_ALL, "all"))
	{
		fetch->how_many = FETCH_ALL;
		fetch->returns_multiple_rows = true;
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_FORWARD, "forward"))
	{
		complete_direction(fetch, &check_FROM);
	}
	else if (tok_is_keyword(tok, &yylval,
							POK_BACKWARD, "backward"))
	{
		fetch->direction = FETCH_BACKWARD;
		complete_direction(fetch, &check_FROM);
	}
	else if (tok == POK_FROM || tok == POK_IN)
	{
		/* empty direction */
		check_FROM = false;
	}
	else if (tok == T_DATUM)
	{
		/* Assume there's no direction clause and tok is a cursor name */
		plpgsql_push_back_token(tok);
		check_FROM = false;
	}
	else
	{
		/*
		 * Assume it's a count expression with no preceding keyword.
		 * Note: we allow this syntax because core SQL does, but we don't
		 * document it because of the ambiguity with the omitted-direction
		 * case.  For instance, "MOVE n IN c" will fail if n is a variable.
		 * Perhaps this can be improved someday, but it's hardly worth a
		 * lot of work.
		 */
		plpgsql_push_back_token(tok);
		fetch->expr = read_sql_expression2(POK_FROM, POK_IN,
										   "FROM or IN",
										   NULL);
		fetch->returns_multiple_rows = true;
		check_FROM = false;
	}

	/* check FROM or IN keyword after direction's specification */
	if (check_FROM)
	{
		tok = yylex();
		if (tok != POK_FROM && tok != POK_IN)
			yyerror("expected FROM or IN");
	}

	return fetch;
}

static PLoraSQL_type   *plora_build_type(char *name, int location, Oid oid, int typmod)
{
	PLpgSQL_type *typ;
	PLoraSQL_type *oraTyp;
	if (plpgsql_find_wordtype(name) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("duplicate declaration"),
					plpgsql_scanner_errposition(location)));

	if (oid != REFCURSOROID &&
		getBaseType(oid) == REFCURSOROID)
		oid = REFCURSOROID;

	typ = plpgsql_build_datatype(oid, typmod, InvalidOid, NULL);
	oraTyp = palloc0(sizeof(PLoraSQL_type));
	pfree(typ->typname);
	typ->typname = name;
	oraTyp->type = typ;
	oraTyp->dtype = PLPGSQL_DTYPE_TYPE;
	plpgsql_adddatum((PLpgSQL_datum*) oraTyp);
	plpgsql_ns_additem(PLPGSQL_NSTYPE_TYPE, oraTyp->dno, typ->typname);

	return oraTyp;
}

static PLoraSQL_type* read_type_define(char *name, int location)
{
	PLpgSQL_type *typ;
	Oid typeid;
	int arr_loc = -1;
	int tok = yylex();
	bool is_array = false;

	if (tok == POK_REF)
	{
		if (yylex() != POK_CURSOR)
			goto read_error_;
		if (yylex() != ';')
			goto read_error_;

		return plora_build_type(name, location, REFCURSOROID, -1);
	}else if (tok == POK_ARRAY ||
			  tok == POK_VARRAY)
	{
		arr_loc = yylloc;
		/* ( ICONST ) */
		if (yylex() != '(' )
			goto read_error_;
		if (yylex() != ICONST)
			goto read_error_;
		if (yylval.ival <= 0)
			goto read_error_;
		is_array = true;
		if (yylex() != ')' )
			goto read_error_;

		/* OF */
		if (yylex() != POK_OF)
			goto read_error_;

		tok = yylex();

		if (tok == POK_REF)
		{
			/* array of refcursor */
			if (yylex() != POK_CURSOR)
				goto read_error_;
			if (yylex() != ';')
				goto read_error_;

			typeid = get_array_type(REFCURSOROID);
			if (!OidIsValid(typeid))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("type REF CURSOR of array does not exist"),
						 plpgsql_scanner_errposition(arr_loc)));
			return plora_build_type(name, location, typeid, -1);
		}
	}else if (tok == POK_TABLE)
	{
		if (yylex() != POK_OF)
			goto read_error_;
		is_array = true;
		tok = yylex();
	}

	typ = read_datatype(tok);
	if (yylex() != ';')
		goto read_error_;

	if (is_array)
	{
		typeid = get_array_type(typ->typoid);
		if (!OidIsValid(typeid))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("type \"%s\" of array does not exist", typ->typname),
						plpgsql_scanner_errposition(arr_loc)));
	}else
	{
		typeid = typ->typoid;
	}

	return plora_build_type(name, location, typeid, typ->atttypmod);

read_error_:
	yyerror("syntax error");
	return NULL;
}

static PLpgSQL_stmt *
make_piperow_stmt(int location)
{
	PLpgSQL_stmt_return_next *new;

	if (!plpgsql_curr_compile->fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot use PIPE ROW in a non-PIPELINED function"),
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
					 errmsg("PIPE ROW cannot have a parameter in function with OUT parameters"),
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

static bool list_have_sub_transaction(List *list)
{
	ListCell *lc;
	PLpgSQL_stmt *stmt;
	foreach (lc, list)
	{
		stmt = lfirst(lc);
		if (stmt->cmd_type == PLPGSQL_STMT_SUB_COMMIT ||
			stmt->cmd_type == PLPGSQL_STMT_SUB_ROLLBACK)
			return true;
	}

	return false;
}
