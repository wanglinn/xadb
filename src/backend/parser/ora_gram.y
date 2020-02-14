%{

//#define YYDEBUG 1

/*
 */

#include "postgres.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/ora_convert_d.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/ora_gramparse.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/guc.h"

/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = YYRHSLOC(Rhs, 1); \
		else \
			(Current) = YYRHSLOC(Rhs, 0); \
	} while (0)

/*
 * The above macro assigns -1 (unknown) as the parse location of any
 * nonterminal that was reduced from an empty rule.  This is problematic
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
 */

/* ConstraintAttributeSpec yields an integer bitmask of these flags: */
#define CAS_NOT_DEFERRABLE			0x01
#define CAS_DEFERRABLE				0x02
#define CAS_INITIALLY_IMMEDIATE		0x04
#define CAS_INITIALLY_DEFERRED		0x08
#define CAS_NOT_VALID				0x10
#define CAS_NO_INHERIT				0x20

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
#define pg_yyget_extra(yyscanner) (*((base_yy_extra_type **) (yyscanner)))
#define ereport_pos(str, n)	ereport(ERROR,											\
								(errcode(ERRCODE_SYNTAX_ERROR),						\
								 errmsg("syntax error at or near \"%s\"", str),		\
								 parser_errposition(n)))

union YYSTYPE;					/* need forward reference for tok_is_keyword */

typedef struct OraclePartitionSpec
{
	PartitionSpec	   *partitionSpec;
	List			   *children;		/* list of CreateStmt */
}OraclePartitionSpec;

typedef struct SelectSortClause
{
	List	   *sortClause;
	int			sort_loc;		/* location for "order" keyword */
	int			siblings_loc;	/* location for "siblings" if exists, or -1 */
}SelectSortClause;

static void ora_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg);
static int ora_yylex(union YYSTYPE *lvalp, YYLTYPE *yylloc, core_yyscan_t yyscanner);
static Node *makeDeleteStmt(RangeVar *range, Alias *alias, WithClause *with, Node *where, List *returning);
static Node *reparse_decode_func(List *args, int location);
static void processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner);
static void add_alias_if_need(List *parseTree);
static Node* make_any_sublink(Node *testexpr, const char *operName, Node *subselect, int location);
#define MAKE_ANY_A_EXPR(name_, l_, r_, loc_) (Node*)makeA_Expr(AEXPR_OP_ANY, list_make1(makeString(pstrdup(name_))), l_, r_, loc_)
static void oracleInsertSelectOptions(SelectStmt *stmt,
									  SelectSortClause *sortClause, List *lockingClause,
									  Node *limitOffset, Node *limitCount,
									  WithClause *withClause,
									  core_yyscan_t yyscanner);
static A_Indirection* listToIndirection(A_Indirection *in, ListCell *lc);
%}

%expect 0
%pure-parser
%name-prefix="ora_yy"
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
	OnCommitAction		oncommit;
	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
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
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
	PartitionElem		*partelem;
	OraclePartitionSpec *orapartspec;
	PartitionBoundSpec	*partboundspec;
	OracleConnectBy		*connectby;
	SelectSortClause	*select_order_by;
/* ADB_BEGIN */
	PartitionSpec		*partspec;
	PGXCSubCluster		*subclus;
/* ADB_END */
/* ADB_EXT */
	KeepClause			*keep;
/* ADB_EXT */
}

/*
 * Non-keyword token types.  These are hard-wired into the "flex" lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  PL/pgsql depends on this so that it can share the
 * same lexer.  If you add/change tokens here, fix PL/pgsql to match!
 *
 * DOT_DOT is unused in the core SQL grammar, and so will always provoke
 * parse errors.  It is needed by PL/pgsql.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token			TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token			LESS_EQUALS GREATER_EQUALS NOT_EQUALS

%type <ielem>	index_elem

%type <list>	stmtblock stmtmulti opt_column_list columnList alter_table_cmds
				OptRoleList convert_type_list

%type <list>	OptSeqOptList SeqOptList
/* %type <list>	NumericOnly_list */

%type <list>	ExclusionConstraintList ExclusionConstraintElem
%type <list>	/* generic_option_list*/ alter_generic_option_list

%type <str>		ExistingIndex
%type <str>		generic_option_name
%type <node>	generic_option_arg

%type <node>	stmt ViewStmt alter_table_cmd

%type <node>	TableConstraint TableLikeClause

%type <node>	columnOptions convert_type

%type <alias>	alias_clause opt_alias_clause

%type <boolean>	opt_all opt_byte_char opt_restart_seqs opt_verbose opt_no_inherit
				opt_unique opt_concurrently opt_with_data opt_or_replace
				connect_by_opt_nocycle opt_nowait

%type <dbehavior>	opt_drop_behavior

%type <defelt>	transaction_mode_item explain_option_elem def_elem reloption_elem
				SeqOptElem AlterOptRoleElem CreateOptRoleElem

%type <istmt>	insert_rest insert_rest_no_cols

%type <into>  into_clause create_as_target create_mv_target /*into_clause*/

%type <ival>
	document_or_content
	Iconst
	opt_asc_desc
	OptTemp
	SignedIconst sub_type

%type <ival> ConstraintAttributeElem ConstraintAttributeSpec opt_function_or_common

%type <ival>	cursor_options
				key_actions key_delete key_match key_update key_action
				for_locking_strength
				opt_hold opt_nulls_order opt_column opt_set_data TableLikeOptionList
				TableLikeOption opt_nowait_or_skip OptNoLog

%type <objwithargs> aggregate_with_argtypes function_with_argtypes operator_with_argtypes

%type <jexpr>	joined_table

%type <node>	join_qual
%type <jtype>	join_type

%type <objtype>	comment_type_any_name drop_type comment_type_name

%type <keyword>
	col_name_keyword
	reserved_keyword
	type_func_name_keyword
	unreserved_keyword

%type <list>
	aggr_args aggr_args_list any_operator any_name any_name_list attrs alter_generic_options
	case_when_list create_generic_options cte_list ctext_expr_list ctext_row
	ColQualList
	definition def_list
	explain_option_list expr_list extract_list
	from_clause from_list func_arg_list func_args_with_defaults func_args_with_defaults_list
	function_with_argtypes_list func_args func_args_list
	func_alias_clause func_name for_locking_clause for_locking_items
	group_clause
	indirection insert_column_list interval_second index_params
	locked_rels_list
	multiple_set_clause
	name_list
	oper_argtypes OptTableElementList opt_distinct opt_for_locking_clause
	opt_indirection opt_interval opt_name_list opt_sort_clause
	OptWith OptTypedTableElementList
	opt_type_mod opt_type_modifiers opt_definition opt_collate opt_class opt_select_limit
	opt_partition_clause
	opt_reloptions OptInherit
	qual_Op qual_all_Op qualified_name_list
	relation_expr_list returning_clause returning_item reloption_list
	reloptions role_list implicit_row
	select_limit set_clause_list set_clause set_expr_list set_expr_row
	set_target_list sortby_list sort_clause subquery_Op
	TableElementList TableFuncElementList target_list transaction_mode_list_or_empty TypedTableElementList
	transaction_mode_list /*transaction_mode_list_or_empty*/ trim_list
	var_list within_group_clause

%type <list>	group_by_list
%type <node>	group_by_item rollup_clause empty_grouping_set cube_clause grouping_sets_clause

%type <node>
	AexprConst a_expr AlterTableStmt alter_column_default AlterObjectSchemaStmt
	alter_using
	b_expr
	BlockCodeStmt
	ClosePortalStmt CreateMatViewStmt RefreshMatViewStmt
	common_table_expr columnDef columnref CreateStmt ctext_expr columnElem
	ColConstraint ColConstraintElem CommentStmt ConstraintAttr CreateProcedureStmt DropProcedureStmt CreateRoleStmt
	case_default case_expr /*case_when*/ case_when_item c_expr
	ConstraintElem CreateSeqStmt CreateAsStmt
	DeclareCursorStmt DeleteStmt DropStmt def_arg
	ExplainStmt ExplainableStmt explain_option_arg ExclusionWhereClause
	FetchStmt fetch_args
	func_application func_application_normal func_expr_common_subexpr func_arg_expr
	func_expr func_table for_locking_item func_expr_windowless
	having_clause
	indirection_el InsertStmt IndexStmt OraImplicitConvertStmt
	join_outer
	limit_clause
	offset_clause opt_collate_clause opt_start_with_clause
	SelectStmt select_clause select_no_parens select_with_parens set_expr
	simple_select select_limit_value select_offset_value start_with_clause
	TableElement TableFuncElement TypedTableElement table_ref TruncateStmt
	TransactionStmt
	UpdateStmt UpdateSelectStmt
	RenameStmt
	values_clause VariableResetStmt VariableSetStmt var_value VariableShowStmt
	where_clause where_or_current_clause
	zone_value

%type <range> OptTempTableName qualified_name relation_expr relation_expr_opt_alias

%type <sortby>	sortby

%type <defelt>	generic_option_elem alter_generic_option_elem

%type <str> all_Op attr_name access_method_clause access_method comment_text
	ColId ColLabel cursor_name
	explain_option_name extract_arg
	iso_level index_name
	MathOp
	name NonReservedWord NonReservedWord_or_Sconst
	opt_boolean_or_string opt_encoding OptConsTableSpace opt_index_name
	opt_existing_window_name
	OptTableSpace
	param_name
	RoleId
	Sconst
	type_function_name convert_functon_name
	var_name

%type <target>	insert_column_item single_set_clause set_target target_item

%type <typnam>	Bit Character ConstDatetime ConstInterval ConstTypename
				Numeric SimpleTypename Typename func_type

%type <value>	NumericOnly

%type <oncommit> OnCommitOption

%type <vsetstmt> set_rest set_rest_more
%type <windef> over_clause window_specification opt_frame_clause frame_extent frame_bound
%type <with> with_clause opt_with_clause
%type <fun_param> aggr_arg func_arg func_arg_with_default
%type <fun_param_mode> arg_class

%type <orapartspec>	PartitionSpec OptPartitionSpecNotEmpty
/* ADB_BEGIN */
%type <orapartspec>	OptPartitionSpec
/* ADB_END */
%type <str>			part_strategy
%type <partelem>	part_elem
%type <list>		part_params
%type <partboundspec> ForValues
%type <list>		ora_part_child_list range_datum_list
%type <node>		ora_part_child PartitionRangeDatum
%type <connectby>	opt_connect_by_clause connect_by_clause
%type <select_order_by> opt_select_sort_clause select_sort_clause
%type <keep>		keep_clause

/* ADB_BEGIN */
%type <defelt>	SubClusterNodeElem
%type <partspec>	OptDistributeBy OptDistributeByInternal
%type <list>	pgxcnodes pgxcnode_list SubClusterNodeList
%type <str>		pgxcgroup_name pgxcnode_name
%type <subclus> OptSubCluster OptSubClusterInternal
/* ADB_END */

%token <keyword> ABSOLUTE_P ACCESS ADD_P AGGREGATE ALL ALTER ANALYZE ANALYSE AND ABORT_P
	ANY AS ASC AUDIT AUTHORIZATION ACTION ALWAYS AT
	ADMIN AUTHID
	BACKWARD BEGIN_P BETWEEN BFILE BIGINT BINARY BINARY_FLOAT BINARY_DOUBLE
	BLOB BOOLEAN_P BOTH BY BYTE_P
	CASCADE CASE CAST CATALOG_P CHAR_P CHARACTERISTICS CHECK CLASS CLOSE CLUSTER
	COLUMN COMMIT COMMENT COLLATION CONVERSION_P CONNECT_BY_ROOT CONNECTION COMMON
	COMMITTED COMPRESS COLLATE CONNECT CONSTRAINT CONVERT CYCLE NOCYCLE
	CONSTRAINTS CLOB COALESCE CONTENT_P CONTINUE_P CREATE CROSS CUBE CURRENT_DATE
	CURRENT_P CURRENT_TIMESTAMP CURRENT_USER CURRVAL CURSOR CONCURRENTLY CONFIGURATION
	CACHE NOCACHE COMMENTS
	DATABASE DATE_P DAY_P DBTIMEZONE_P DEC DECIMAL_P DECLARE DEFAULT DEFERRABLE DELETE_P DESC DISTINCT
	DO DOCUMENT_P DOUBLE_P DROP DEFERRED DATA_P DEFAULTS DEFINER DETERMINISTIC
	DISABLE_P PREPARE PREPARED DOMAIN_P DICTIONARY DENSE_RANK

	/* ADB_BEGIN */
	DISTRIBUTE
	/* ADB_END */

	ELSE END_P ESCAPE EXCLUSIVE EXISTS EXPLAIN EXTRACT
	ENABLE_P EXCLUDE EVENT EXTENSION EXCLUDING ENCRYPTED
	FALSE_P FAMILY FETCH FILE_P FIRST_P FLOAT_P FOLLOWING FOR FORWARD FROM FOREIGN FULL FUNCTION
	GLOBAL GRANT GREATEST GROUP_P GROUPING HAVING
	HOLD HOUR_P
	IDENTIFIED IF_P IMMEDIATE IN_P INOUT INCREMENT INDEX INITIAL_P INSERT INHERIT INITIALLY
	INHERITS INCLUDING INDEXES INNER_P INSENSITIVE
	IDENTITY_P INTEGER INTERSECT INTO INTERVAL INT_P IS ISOLATION
	LAST_P LESS
	JOIN
	KEEP KEY
	LANGUAGE LARGE_P LEADING LEAST LEFT LEVEL LIMIT LIKE LOCAL LOCALTIMESTAMP LOCK_P LOG_P LONG_P
	MATERIALIZED MAXEXTENTS MINUS MINUTE_P MLSLABEL MOD MODE MODIFY MONTH_P MOVE
	MATCH MAXVALUE METHOD NOMAXVALUE  MINVALUE NOMINVALUE
	NAMES NCHAR NCLOB NEXT NEXTVAL NOAUDIT NOCOMPRESS NOT NOWAIT NULL_P NULLIF NUMBER_P
	NUMERIC NVARCHAR2 NO NONE
	/* PGXC add NODE token */
	NODE NULLS_P
	OBJECT_P OF OFF OFFLINE OFFSET ON ONLINE ONLY OPERATOR OPTION OR ORDER OUT_P OUTER_P
	OWNER OIDS OPTIONS OVER OWNED
	PCTFREE PIPELINED PRECISION PRESERVE PRIOR PRIVILEGES PUBLIC PUBLICATION PURGE
	PARTITION PRECEDING PROCEDURAL PROCEDURE PARTIAL PRIMARY PARSER PASSWORD PARALLEL_ENABLE POLICY
	RANGE RAW READ REAL RECURSIVE REFRESH RENAME REPLACE REPEATABLE RESET RESOURCE RESTART RESTRICT
	RETURNING RETURN_P REVOKE REUSE RIGHT ROLE ROLLBACK ROLLUP ROW ROWID ROWNUM ROWS
	REFERENCES REPLICA RULE RELATIVE_P RELEASE RESULT_CACHE
	SCHEMA SECOND_P SELECT SERIALIZABLE SERVER SESSION SESSIONTIMEZONE SET SETS SHARE SHOW SIBLINGS SIZE SEARCH
	SMALLINT SIMPLE SETOF STATISTICS SAVEPOINT SEQUENCE SYSID SOME SCROLL
	SNAPSHOT SPECIAL START STORAGE SUBSCRIPTION SUCCESSFUL SYNONYM SYSDATE SYSTIMESTAMP
	TABLE TEMP TEMPLATE TEMPORARY THAN THEN TIME TIMESTAMP TO TRAILING
	TRANSACTION TRANSFORM TREAT TRIM TRUNCATE TRIGGER TRUE_P TABLESPACE
	TYPE_P TEXT_P
	UID UNCOMMITTED UNION UNIQUE UPDATE USER USING UNLOGGED
	UNENCRYPTED UNTIL UNBOUNDED
	VALIDATE VALUES VARCHAR VARCHAR2 VERBOSE VIEW VALID
	WHEN WHENEVER WHERE WITH WRAPPER WRITE WITHIN WITHOUT WORK
	XML_P
	YEAR_P
	ZONE

/*
 * same specific token
 */
%token	ORACLE_JOIN_OP CONNECT_BY CONNECT_BY_NOCYCLE NULLS_LA DROP_PARTITION LIMIT_LA

/* Precedence: lowest to highest */
%right	RETURN_P RETURNING PRIMARY
%right	HI_THEN_RETURN LO_THEN_LIMIT
%right	LIMIT OFFSET
%left	HI_THEN_LIMIT
%left		UNION MINUS /*EXCEPT*/
//%left		INTERSECT
%nonassoc	CASE CAST SOME ROLLUP CUBE
%left		WHEN END_P
%left		OR
%left		AND
%right		NOT
%right		'='
%nonassoc	'<' '>' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc	LIKE //ILIKE SIMILAR
%nonassoc	ESCAPE
//%nonassoc	OVERLAPS
%nonassoc	BETWEEN
%nonassoc	IN_P
%left		POSTFIXOP		/* dummy for postfix Op rules */
/*
 * To support target_el without AS, we must give IDENT an explicit priority
 * between POSTFIXOP and Op.  We can safely assign the same priority to
 * various unreserved keywords as needed to resolve ambiguities (this can't
 * have any bad effects since obviously the keywords will still behave the
 * same as if they weren't keywords).  We need to do this for PARTITION,
 * RANGE, ROWS to support opt_existing_window_name; and for RANGE, ROWS
 * so that they can follow a_expr without creating postfix-operator problems;
 * and for NULL so that it can follow b_expr in ColQualList without creating
 * postfix-operator problems.
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
%nonassoc	IDENT NULL_P PARTITION RANGE ROWS PRECEDING FOLLOWING
%left		Op OPERATOR		/* multi-character ops and user-defined operators */
//%nonassoc	NOTNULL
//%nonassoc	ISNULL
%nonassoc	IS				/* sets precedence for IS NULL, etc */
%left		'+' '-'
%left		'*' '/' '%' MOD
%left		'^'
//%right SOME
/* Unary Operators */
%left		AT				/* sets precedence for AT TIME ZONE */
%left		COLLATE
%right		UMINUS
//%left		'[' ']'
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
%left		JOIN CROSS LEFT FULL RIGHT INNER_P //NATURAL
/* kluge to keep xml_whitespace_option from causing shift/reduce conflicts */
//%right		PRESERVE STRIP_P

%%
stmtblock: stmtmulti
		{
			ora_yyget_extra(yyscanner)->parsetree = $1;
			if(ora_yyget_extra(yyscanner)->has_no_alias_subquery)
				add_alias_if_need($1);
		}
	;

stmtmulti: stmtmulti ';' stmt
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
	  AlterTableStmt
	| AlterObjectSchemaStmt
	| BlockCodeStmt
	| ClosePortalStmt
	| CommentStmt
	| CreateAsStmt
	| CreateMatViewStmt
	| CreateStmt
	| CreateSeqStmt
	| CreateRoleStmt
	| CreateProcedureStmt
	| DeclareCursorStmt
	| DeleteStmt
	| DropProcedureStmt
	| DropStmt
	| ExplainStmt
	| FetchStmt
	| InsertStmt
	| IndexStmt
	| OraImplicitConvertStmt
	| RefreshMatViewStmt
	| RenameStmt
	| SelectStmt
	| TruncateStmt
	| TransactionStmt
	| UpdateStmt
	| VariableResetStmt
	| VariableShowStmt
	| VariableSetStmt
	| ViewStmt
	| /* empty */	{ $$ = NULL; }
	;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname AS SelectStmt [ WITH [NO] DATA ]
 *
 *
 * Note: SELECT ... INTO is a now-deprecated alternative for this.
 *
 *****************************************************************************/

CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
#ifdef ADB
					ctas->grammar = PARSE_GRAM_POSTGRES;
#endif /* ADB */
					ctas->query = $6;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
					$4->skipData = !($7);
					$$ = (Node *) ctas;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $9;
					ctas->into = $7;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					ctas->if_not_exists = true;
					/* cram additional flags into the IntoClause */
					$7->rel->relpersistence = $2;
					$7->skipData = !($10);
					$$ = (Node *) ctas;
				}
		;

create_as_target:
			qualified_name opt_column_list OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptDistributeBy OptSubCluster
/* ADB_END */
				{
					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;
					$$->tableSpaceName = $5;
					$$->viewQuery = NULL;
					$$->skipData = false;		/* might get changed later */
/* ADB_BEGIN */
					$$->distributeby = $6;
					$$->subcluster = $7;
/* ADB_END */
				}
		;

opt_with_data:
			 /*EMPTY*/								{ $$ = true; }
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS role
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE ROLE RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_ROLE;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

OptRoleList:
			OptRoleList CreateOptRoleElem			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

CreateOptRoleElem:
			AlterOptRoleElem			{ $$ = $1; }
			/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (Node *)makeInteger($2), @1);
				}
			| ADMIN role_list
				{
					$$ = makeDefElem("adminmembers", (Node *)$2, @1);
				}
			| ROLE role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2, @1);
				}
			| IN_P ROLE role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3, @1);
				}
			| IN_P GROUP_P role_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3, @1);
				}
		;

role_list:	RoleId
					{ $$ = list_make1(makeString($1)); }
			| role_list ',' RoleId
					{ $$ = lappend($1, makeString($3)); }
		;

AlterOptRoleElem:
			PASSWORD Sconst
				{
					$$ = makeDefElem("password",
									 (Node *)makeString($2),
									 @1);
				}
			| PASSWORD NULL_P
				{
					$$ = makeDefElem("password", NULL, @1);
				}
			| ENCRYPTED PASSWORD Sconst
				{
					$$ = makeDefElem("encryptedPassword",
									 (Node *)makeString($3),
									 @1);
				}
			| UNENCRYPTED PASSWORD Sconst
				{
					$$ = makeDefElem("unencryptedPassword",
									 (Node *)makeString($3),
									 @1);
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(true), @1);
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($3), @1);
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (Node *)makeString($3), @1);
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER role_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2, @1);
				}
			| IDENT
				{
					/*
					 * We handle identifiers that aren't parser keywords with
					 * the following special-case codes, to avoid bloating the
					 * size of the main parser.
					 */
					if (strcmp($1, "superuser") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nosuperuser") == 0)
						$$ = makeDefElem("superuser", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "createuser") == 0)
					{
						/* For backwards compatibility, synonym for SUPERUSER */
						$$ = makeDefElem("superuser", (Node *)makeInteger(true), @1);
					}
					else if (strcmp($1, "nocreateuser") == 0)
					{
						/* For backwards compatibility, synonym for SUPERUSER */
						$$ = makeDefElem("superuser", (Node *)makeInteger(false), @1);
					}
					else if (strcmp($1, "createrole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nocreaterole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "replication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "noreplication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "createdb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nocreatedb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "login") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(true), @1);
					else if (strcmp($1, "nologin") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(false), @1);
					else if (strcmp($1, "noinherit") == 0)
					{
						/*
						 * Note that INHERIT is a keyword, so it's handled by main parser, but
						 * NOINHERIT is handled here.
						 */
						$$ = makeDefElem("inherit", (Node *)makeInteger(false), @1);
					}
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized role option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;

/*****************************************************************************
 *
 * CREATE PROCEDURE
 *
 *****************************************************************************/

CreateProcedureStmt:
			CREATE opt_or_replace PROCEDURE func_name func_args_with_defaults
			create_procedure_invoker_rights_clause create_procedure_is_or_as Sconst
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					ListCell		   *lc;
					TypeName		   *type_name;
					int					out_count;

					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->options = list_make2(makeDefElem("as", (Node *)list_make1(makeString($8)), @8),
											makeDefElem("language", (Node *)makeString("plorasql"), -1));

					/* what type we need return of procedure */
					out_count = 0;
					type_name = NULL;
					foreach(lc, n->parameters)
					{
						FunctionParameter *parm = lfirst(lc);
						if (parm->mode == FUNC_PARAM_OUT ||
							parm->mode == FUNC_PARAM_INOUT)
						{
							if(out_count == 0)
								type_name = parm->argType;
							++out_count;
						}
					}
					if (out_count == 0)
					{
						n->returnType = makeTypeNameFromNameList(SystemFuncName("void"));
					}else if(out_count == 1)
					{
						Assert(type_name != NULL);
						n->returnType = type_name;
					}else
					{
						Assert(out_count > 1);
						n->returnType = makeTypeNameFromNameList(SystemFuncName("record"));
					}

					/* ignore invoker_rights_clause */
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name func_args_with_defaults
			  create_function_attrs RETURN_P Typename
			  create_procedure_is_or_as Sconst
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);

					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = $8;
					n->options = list_make2(makeDefElem("as", (Node *)list_make1(makeString($10)), @10),
											makeDefElem("language", (Node *)makeString("plorasql"), -1));

					$$ = (Node*)n;
				}
			;

opt_or_replace:
			OR REPLACE								{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

create_procedure_invoker_rights_clause:
			  invoker_rights_clause
			| /* empty */
			;

invoker_rights_clause:
			  AUTHID CURRENT_USER
			| AUTHID DEFINER

create_procedure_is_or_as: is_or_as
				{
					ora_yyget_extra(yyscanner)->parsing_code_block = true;
				}
			;

is_or_as:
			  AS
			| IS
			;

create_function_attrs:
			  create_function_attrs create_function_attr_item
			| /* empty */
			;

create_function_attr_item:
			  invoker_rights_clause
			| DETERMINISTIC
			| PARALLEL_ENABLE /* not whole of parallel_enable_clause */
			| RESULT_CACHE /* not whole of result_cache_clause */
			;


/*
 * func_args_with_defaults is separate because we only want to accept
 * defaults in CREATE FUNCTION, not in ALTER etc.
 */
func_args_with_defaults:
		'(' func_args_with_defaults_list ')'		{ $$ = $2; }
		| '(' ')'									{ $$ = NIL; }
		;

func_args_with_defaults_list:
		func_arg_with_default						{ $$ = list_make1($1); }
		| func_args_with_defaults_list ',' func_arg_with_default
													{ $$ = lappend($1, $3); }
		;

/*
 * The style with arg_class first is SQL99 standard, but Oracle puts
 * param_name first; accept both since it's likely people will try both
 * anyway.  Don't bother trying to save productions by letting arg_class
 * have an empty alternative ... you'll get shift/reduce conflicts.
 *
 * We can catch over-specified arguments here if we want to,
 * but for now better to silently swallow typmod, etc.
 * - thomas 2000-03-22
 */
func_arg:
			arg_class param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $2;
					n->argType = $3;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $3;
					n->mode = $2;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_IN;
					n->defexpr = NULL;
					$$ = n;
				}
		;

func_arg_with_default:
		func_arg
				{
					$$ = $1;
				}
		| func_arg DEFAULT a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg '=' a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg COLON_EQUALS a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		;

/* INOUT is SQL99 standard, IN OUT is for Oracle compatibility */
arg_class:	IN_P								{ $$ = FUNC_PARAM_IN; }
			| OUT_P								{ $$ = FUNC_PARAM_OUT; }
			| INOUT								{ $$ = FUNC_PARAM_INOUT; }
			| IN_P OUT_P						{ $$ = FUNC_PARAM_INOUT; }
		;

/*
 * Ideally param_name should be ColId, but that causes too many conflicts.
 */
param_name:	type_function_name
		;

/*****************************************************************************
 *
 * ALTER THING name SET SCHEMA name
 *
 *****************************************************************************/

AlterObjectSchemaStmt:
			ALTER VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $6;
					n->newschema = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name RENAME TO newname
 *
 *****************************************************************************/

RenameStmt: ALTER INDEX qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_MATVIEW;
					n->relation = $4;
					n->subname = NULL;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_MATVIEW;
					n->relation = $6;
					n->subname = NULL;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_MATVIEW;
					n->relation = $4;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_MATVIEW;
					n->relation = $6;
					n->subname = $9;
					n->newname = $11;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *	ALTER [ TABLE | INDEX | SEQUENCE | VIEW | MATERIALIZED VIEW ] variations
 *
 * Note: we accept all subcommands for each of the five variants, and sort
 * out what's really legal at execution time.
 *****************************************************************************/

AlterTableStmt:
				ALTER INDEX qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER INDEX IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			|	ALTER VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			|	ALTER MATERIALIZED VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
#if defined(ADB_MULTI_GRAM)
					n->grammar = PARSE_GRAM_POSTGRES;
#endif /* ADB */
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_MATVIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
#if defined(ADB_MULTI_GRAM)
					n->grammar = PARSE_GRAM_POSTGRES;
#endif /* ADB */
					n->relation = $6;
					n->cmds = $7;
					n->relkind = OBJECT_MATVIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			|	ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $7;
					n->objtype = OBJECT_MATVIEW;
					n->roles = NIL;
					n->new_tablespacename = $10;
					n->nowait = $11;
					$$ = (Node *)n;
				}
			|	ALTER MATERIALIZED VIEW ALL IN_P TABLESPACE name OWNED BY role_list SET TABLESPACE name opt_nowait
				{
					AlterTableMoveAllStmt *n =
						makeNode(AlterTableMoveAllStmt);
					n->orig_tablespacename = $7;
					n->objtype = OBJECT_MATVIEW;
					n->roles = $10;
					n->new_tablespacename = $13;
					n->nowait = $14;
					$$ = (Node *)n;
				}
			|	ALTER TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			|	ALTER TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <table_name> DROP PARTITION sub_table_name */
			|	ALTER TABLE relation_expr DROP_PARTITION any_name_list
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TABLE;
					n->missing_ok = false;
					n->objects = $5;
					n->behavior = DROP_RESTRICT;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			/*|ALTER TABLE <table_name> TRUNCATE PARTITION sub_table_name */
			| ALTER TABLE relation_expr TRUNCATE PARTITION relation_expr_list
				{
					TruncateStmt *n = makeNode(TruncateStmt);
					n->relations = $6;
					n->restart_seqs = false;
					n->behavior = DROP_RESTRICT;
					$$ = (Node *)n;
				}

		;

alter_table_cmds:
			alter_table_cmd							{ $$ = list_make1($1); }
			| alter_table_cmds ',' alter_table_cmd	{ $$ = lappend($1, $3); }
		;

alter_table_cmd:
			/* ALTER TABLE <name> ADD <coldef> */
			ADD_P '(' columnDef ')'
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN <coldef> */
			| ADD_P COLUMN columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
			| ALTER opt_column ColId alter_column_default
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ColumnDefault;
					n->name = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL */
			| ALTER opt_column ColId DROP NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL */
			| ALTER opt_column ColId SET NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS <SignedIconst> */
			| ALTER opt_column ColId SET STATISTICS SignedIconst
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStatistics;
					n->name = $3;
					n->def = (Node *) makeInteger($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
			| ALTER opt_column ColId SET STORAGE ColId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStorage;
					n->name = $3;
					n->def = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE] */
			| DROP opt_column IF_P EXISTS ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
			| DROP opt_column ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/*
			 * ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
			 *		[ USING <expression> ]
			 */
			| ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					/* We only use these three fields of the ColumnDef node */
					def->typeName = $6;
					def->collClause = (CollateClause *) $7;
					def->raw_default = $8;
					$$ = (Node *)n;
				}
			| MODIFY opt_column ColId Typename opt_collate_clause alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					/* We only use these three fields of the ColumnDef node */
					def->typeName = $4;
					def->collClause = (CollateClause *) $5;
					def->raw_default = $6;
					$$ = (Node *)n;
				}
			/* ALTER FOREIGN TABLE <name> ALTER [COLUMN] <colname> OPTIONS */
			| ALTER opt_column ColId alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AlterColumnGenericOptions;
					n->name = $3;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD CONSTRAINT ... */
			| ADD_P TableConstraint
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddConstraint;
					n->def = $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> VALIDATE CONSTRAINT ... */
			| VALIDATE CONSTRAINT name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ValidateConstraint;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITH OIDS  */
			| SET WITH OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT OIDS  */
			| SET WITHOUT OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> CLUSTER ON <indexname> */
			| CLUSTER ON name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ClusterOn;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT CLUSTER */
			| SET WITHOUT CLUSTER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropCluster;
					n->name = NULL;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER <trig> */
			| ENABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS TRIGGER <trig> */
			| ENABLE_P ALWAYS TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA TRIGGER <trig> */
			| ENABLE_P REPLICA TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER ALL */
			| ENABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER USER */
			| ENABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER <trig> */
			| DISABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER ALL */
			| DISABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER USER */
			| DISABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE RULE <rule> */
			| ENABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS RULE <rule> */
			| ENABLE_P ALWAYS RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA RULE <rule> */
			| ENABLE_P REPLICA RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE RULE <rule> */
			| DISABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> INHERIT <parent> */
			| INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddInherit;
					n->def = (Node *) $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO INHERIT <parent> */
			| NO INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropInherit;
					n->def = (Node *) $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OF <type_name> */
			| OF any_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					TypeName *def = makeTypeNameFromNameList($2);
					def->location = @2;
					n->subtype = AT_AddOf;
					n->def = (Node *) def;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NOT OF */
			| NOT OF
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOf;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OWNER TO RoleId */
			| OWNER TO RoleId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ChangeOwner;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET TABLESPACE <tablespacename> */
			| SET TABLESPACE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetTableSpace;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET (...) */
			| SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> RESET (...) */
			| RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			| alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_GenericOptions;
					n->def = (Node *)$1;
					$$ = (Node *) n;
				}
/* ADB_BEGIN */
			/* ALTER TABLE <name> DISTRIBUTE BY ... */
			| OptDistributeByInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DistributeBy;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> TO [ NODE (nodelist) | GROUP groupname ] */
			| OptSubClusterInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SubCluster;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD NODE (nodelist) */
			| ADD_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DELETE NODE (nodelist) */
			| DELETE_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DeleteNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
/* ADB_END */
		;

opt_set_data: SET DATA_P							{ $$ = 1; }
			| /*EMPTY*/								{ $$ = 0; }
		;

alter_using:
			USING a_expr				{ $$ = $2; }
			| /* EMPTY */				{ $$ = NULL; }
		;

/* Options definition for ALTER FDW, SERVER and USER MAPPING */
alter_generic_options:
			OPTIONS	'(' alter_generic_option_list ')'		{ $$ = $3; }
		;

alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = list_make1($1);
				}
			| alter_generic_option_list ',' alter_generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1;
				}
			| SET generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_SET;
				}
			| ADD_P generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_ADD;
				}
			| DROP generic_option_name
				{
					$$ = makeDefElemExtended(NULL, $2, NULL, DEFELEM_DROP, @2);
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

opt_collate_clause:
			COLLATE any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| /* EMPTY */				{ $$ = NULL; }
		;

/* ADB_BEGIN */
OptDistributeByInternal:  DISTRIBUTE BY part_strategy
				{
					PartitionSpec *n = makeNode(PartitionSpec);

					n->strategy = $3;
					n->location = @1;

					$$ = n;
				}
			| DISTRIBUTE BY part_strategy '(' part_elem ')'
				{
					PartitionSpec *n = makeNode(PartitionSpec);

					n->strategy = $3;
					n->partParams = list_make1($5);
					n->location = @1;

					$$ = n;
				}
		;

OptSubCluster: OptSubClusterInternal				{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSubClusterInternal:
			TO NODE '(' SubClusterNodeList ')'
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_NODE;
					n->members = $4;
					n->modulus = n->mod_loc = -1;
					$$ = n;
				}
			| TO NODE NonReservedWord Iconst '(' SubClusterNodeList ')'
				{				
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_NODE;
					n->members = $6;
					if (strcmp($3, "modulus") == 0)
					{
						n->modulus = $4;
						if (n->modulus == 0)
							ereport_pos("0", @4);
						n->mod_loc = @4;
					}else
					{
						ereport_pos($3, @3);
					}
					$$ = n;
				}
			| TO GROUP_P pgxcgroup_name
				{					
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_GROUP;
					n->members = list_make1(makeDefElem($3, NULL, @3));
					n->modulus = n->mod_loc = -1;
					$$ = n;
				}
		;

SubClusterNodeElem:
		  pgxcnode_name
			{
				$$ = makeDefElem($1, NULL, @1);
			}
		| pgxcnode_name FOR NULLS_P
			{
				PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

				n->is_default = true;
				n->location = @3;

				$$ = makeDefElem($1, (Node*)n, @1);
			}
		| pgxcnode_name FOR VALUES IN_P '(' expr_list ')'
			{
				PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

				n->strategy = PARTITION_STRATEGY_LIST;
				n->is_default = false;
				n->listdatums = $6;
				n->location = @4;

				$$ = makeDefElem($1, (Node*)n, @1);
			}
		| pgxcnode_name FOR VALUES FROM '(' a_expr ')' TO '(' a_expr ')'
			{
				PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

				n->strategy = PARTITION_STRATEGY_RANGE;
				n->is_default = false;
				n->lowerdatums = list_make1($6);
				n->upperdatums = list_make1($10);
				n->location = @4;

				$$ = makeDefElem($1, (Node*)n, @1);
			}
		;

SubClusterNodeList:
		  SubClusterNodeList ',' SubClusterNodeElem	{ $$ = lappend($1, $3); }
		| SubClusterNodeElem						{ $$ = list_make1($1); }
		;

pgxcnode_name:
			ColId							{ $$ = $1; }
			;

pgxcgroup_name:
			ColId							{ $$ = $1; }
		;

pgxcnodes:
			'(' pgxcnode_list ')'			{ $$ = $2; }
		;

pgxcnode_list:
			pgxcnode_list ',' pgxcnode_name		{ $$ = lappend($1, makeString($3)); }
			| pgxcnode_name						{ $$ = list_make1(makeString($1)); }
		;
/* ADB_END */

/* ConstraintElem specifies constraint syntax which is not embedded into
 *	a column definition. ColConstraintElem specifies the embedded form.
 * - thomas 1997-12-03
 */
TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					Constraint *n = (Constraint *) $3;
					Assert(IsA(n, Constraint));
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ConstraintElem						{ $$ = $1; }
		;

ConstraintElem:
			CHECK '(' a_expr ')' ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					processCASbits($5, @5, "CHECK",
								   NULL, NULL, &n->skip_validation,
								   &n->is_no_inherit, yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
			| UNIQUE '(' columnList ')' opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->options = $5;
					n->indexname = NULL;
					n->indexspace = $6;
					processCASbits($7, @7, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| UNIQUE ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $2;
					n->indexspace = NULL;
					processCASbits($3, @3, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY '(' columnList ')' opt_definition OptConsTableSpace
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $4;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = $7;
					processCASbits($8, @8, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| PRIMARY KEY ExistingIndex ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NIL;
					n->options = NIL;
					n->indexname = $3;
					n->indexspace = NULL;
					processCASbits($4, @4, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| EXCLUDE access_method_clause '(' ExclusionConstraintList ')'
				opt_definition OptConsTableSpace ExclusionWhereClause
				ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_EXCLUSION;
					n->location = @1;
					n->access_method	= $2;
					n->exclusions		= $4;
					n->options			= $6;
					n->indexname		= NULL;
					n->indexspace		= $7;
					n->where_clause		= $8;
					processCASbits($9, @9, "EXCLUDE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					processCASbits($11, @11, "FOREIGN KEY",
								   &n->deferrable, &n->initdeferred,
								   &n->skip_validation, NULL,
								   yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
		;

ExistingIndex:   USING INDEX index_name				{ $$ = $3; }
		;

ExclusionWhereClause:
			WHERE '(' a_expr ')'					{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ExclusionConstraintList:
			ExclusionConstraintElem					{ $$ = list_make1($1); }
			| ExclusionConstraintList ',' ExclusionConstraintElem
													{ $$ = lappend($1, $3); }
		;

ExclusionConstraintElem: index_elem WITH any_operator
			{
				$$ = list_make2($1, $3);
			}
			/* allow OPERATOR() decoration for the benefit of ruleutils.c */
			| index_elem WITH OPERATOR '(' any_operator ')'
			{
				$$ = list_make2($1, $5);
			}
		;

ConstraintAttributeSpec:
			/*EMPTY*/
				{ $$ = 0; }
			| ConstraintAttributeSpec ConstraintAttributeElem
				{
					/*
					 * We must complain about conflicting options.
					 * We could, but choose not to, complain about redundant
					 * options (ie, where $2's bit is already set in $1).
					 */
					int		newspec = $1 | $2;

					/* special message for this case */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)) == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 parser_errposition(@2)));
					/* generic message for other conflicts */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE) ||
						(newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)) == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting constraint properties"),
								 parser_errposition(@2)));
					$$ = newspec;
				}
		;

ConstraintAttributeElem:
			NOT DEFERRABLE					{ $$ = CAS_NOT_DEFERRABLE; }
			| DEFERRABLE					{ $$ = CAS_DEFERRABLE; }
			| INITIALLY IMMEDIATE			{ $$ = CAS_INITIALLY_IMMEDIATE; }
			| INITIALLY DEFERRED			{ $$ = CAS_INITIALLY_DEFERRED; }
			| NOT VALID						{ $$ = CAS_NOT_VALID; }
			| NO INHERIT					{ $$ = CAS_NO_INHERIT; }
		;

RoleId:		NonReservedWord							{ $$ = $1; }
		;

opt_column: COLUMN									{ $$ = COLUMN; }
			| /*EMPTY*/								{ $$ = 0; }
		;

alter_column_default:
			SET DEFAULT a_expr			{ $$ = $3; }
			| DROP DEFAULT				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE MATERIALIZED VIEW relname AS SelectStmt
 *
 *****************************************************************************/

CreateMatViewStmt:
		CREATE OptNoLog MATERIALIZED VIEW create_mv_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $7;
					ctas->into = $5;
					ctas->relkind = OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->if_not_exists = false;
					/* cram additional flags into the IntoClause */
					$5->rel->relpersistence = $2;
					$5->skipData = !($8);
					$$ = (Node *) ctas;
				}
		| CREATE OptNoLog MATERIALIZED VIEW IF_P NOT EXISTS create_mv_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $10;
					ctas->into = $8;
					ctas->relkind = OBJECT_MATVIEW;
					ctas->is_select_into = false;
					ctas->if_not_exists = true;
					/* cram additional flags into the IntoClause */
					$8->rel->relpersistence = $2;
					$8->skipData = !($11);
					$$ = (Node *) ctas;
				}
		;

create_mv_target:
			qualified_name opt_column_list opt_reloptions OptTableSpace
				{
					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = ONCOMMIT_NOOP;
					$$->tableSpaceName = $4;
					$$->viewQuery = NULL;		/* filled at analysis time */
					$$->skipData = false;		/* might get changed later */
				}
		;

OptNoLog:	UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;


/*****************************************************************************
 *
 *		QUERY :
 *				REFRESH MATERIALIZED VIEW qualified_name
 *
 *****************************************************************************/

RefreshMatViewStmt:
			REFRESH MATERIALIZED VIEW opt_concurrently qualified_name opt_with_data
				{
					RefreshMatViewStmt *n = makeNode(RefreshMatViewStmt);
					n->concurrent = $4;
					n->relation = $5;
					n->skipData = !($6);
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/

CreateSeqStmt:
			CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$4->relpersistence = $2;
					n->sequence = $4;
					n->options = $5;
					n->ownerId = InvalidOid;
/* ADB_BEGIN */
					n->is_serial = false;
/* ADB_END */
					$$ = (Node *)n;
				}
		;



OptSeqOptList: SeqOptList							{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;

SeqOptElem: CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2, @1);
				}
			| NOCACHE
				{
					$$ = makeDefElem("cache", (Node *)makeInteger(20), @1);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(true), @1);
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(false), @1);
				}
			| NOCYCLE
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
			| NOMAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL, @1);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| NOMINVALUE
				{
					$$ = makeDefElem("minvalue", NULL, @1);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3, @1);
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

opt_with:	WITH									{}
			| /*EMPTY*/								{}
		;

opt_by:		BY				{}
			| /* empty */	{}
	  ;

/* NumericOnly_list:	NumericOnly						{ $$ = list_make1($1); }
				| NumericOnly_list ',' NumericOnly	{ $$ = lappend($1, $3); }
		; */

/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 * Note: we cannot put TABLESPACE clause after WHERE clause unless we are
 * willing to make TABLESPACE a fully reserved word.
 *****************************************************************************/

IndexStmt:	CREATE opt_unique INDEX opt_concurrently opt_index_name
			ON qualified_name access_method_clause '(' index_params ')'
		    opt_reloptions OptTableSpace where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->unique = $2;
					n->concurrent = $4;
					n->idxname = $5;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
					n->options = $12;
					n->tableSpace = $13;
					n->whereClause = $14;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
		;

opt_unique:
			UNIQUE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;
opt_concurrently:
			CONCURRENTLY							{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;
opt_index_name:
			index_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
index_name: ColId									{ $$ = $1; };
access_method_clause:
			USING access_method						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = DEFAULT_INDEX_TYPE; }
		;
access_method:
			ColId									{ $$ = $1; };

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }

/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem:	ColId opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| func_expr_windowless opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->indexcolname = NULL;
					$$->collation = $4;
					$$->opclass = $5;
					$$->ordering = $6;
					$$->nulls_ordering = $7;
				}
		;

opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_class:	any_name								{ $$ = $1; }
			| USING any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_nulls_order: NULLS_LA FIRST_P				{ $$ = SORTBY_NULLS_FIRST; }
			| NULLS_LA LAST_P					{ $$ = SORTBY_NULLS_LAST; }
			| /*EMPTY*/						{ $$ = SORTBY_NULLS_DEFAULT; }
		;

reloptions:
			'(' reloption_list ')'					{ $$ = $2; }
		;

opt_reloptions:		WITH reloptions					{ $$ = $2; }
			 |		/* EMPTY */						{ $$ = NIL; }
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

/* Note: any simple identifier will be returned as a type name! */
def_arg:	func_type						{ $$ = (Node *)$1; }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
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

qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

sub_type:	ANY										{ $$ = ANY_SUBLINK; }
			/*
			 * in oracle "some" is not keyword, we can not put here
			 * | SOME									{ $$ = ANY_SUBLINK; }*/
			| ALL									{ $$ = ALL_SUBLINK; }
		;

subquery_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
			| LIKE
					{ $$ = list_make1(makeString("~~")); }
			| NOT LIKE
					{ $$ = list_make1(makeString("!~~")); }
			/*| ILIKE
					{ $$ = list_make1(makeString("~~*")); }
			| NOT ILIKE
					{ $$ = list_make1(makeString("!~~*")); }*/
/* cannot put SIMILAR TO here, because SIMILAR TO is a hack.
 * the regular expression is preprocessed by a function (similar_escape),
 * and the ~ operator for posix regular expressions is used.
 *        x SIMILAR TO y     ->    x ~ similar_escape(y)
 * this transformation is made on the fly by the parser upwards.
 * however the SubLink structure which handles any/some/all stuff
 * is not ready for such a thing.
 */
			;

OptTableSpace:   TABLESPACE name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/**********************************************************/

alias_clause:
			AS ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					$$->colnames = $4;
				}
			| AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
				}
			| ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
					$$->colnames = $3;
				}
			| ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
				}
		;

all_Op: Op
	| MathOp
	;

any_operator: all_Op 			{ $$ = list_make1(makeString($1)); }
	| ColId '.' any_operator 	{ $$ = lcons(makeString($1), $3); }
	;

analyze_keyword:
		ANALYZE									{}
		| ANALYSE /* British */					{}
	;

a_expr:	c_expr
	| a_expr TYPECAST Typename
		{ $$ = makeTypeCast($1, $3, @2); }
	| a_expr AT TIME ZONE a_expr			%prec AT
		{
			FuncCall *n = makeNode(FuncCall);
			n->funcname = SystemFuncName("timezone");
			n->args = list_make2($5, $1);
			n->agg_order = NIL;
			n->agg_star = false;
			n->agg_distinct = false;
			n->func_variadic = false;
			n->over = NULL;
			n->location = @2;
			$$ = (Node *) n;
		}
	| '+' a_expr						%prec UMINUS
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
	| '-' a_expr						%prec UMINUS
		{ $$ = doNegate($2, @1); }
	| a_expr '+' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
	| a_expr '-' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
	| a_expr '*' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
	| a_expr '/' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
	| a_expr '%' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
	| a_expr MOD a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
	| a_expr '^' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
	| a_expr '<' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
	| a_expr '>' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
	| a_expr '=' a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
	| a_expr LESS_EQUALS a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $3, @2); }
	| a_expr GREATER_EQUALS a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $3, @2); }
	| a_expr NOT_EQUALS a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<>", $1, $3, @2); }

	| a_expr qual_Op a_expr				%prec Op
		{
			char *nspname = NULL;
			char *objname = NULL;

			DeconstructQualifiedName($2, &nspname, &objname);
			if (objname &&
				strncmp(objname, "||", strlen(objname)) == 0)
			{
				if (nodeTag($1) == T_A_Const && nodeTag($3) == T_A_Const &&
						((A_Const*)$1)->val.type == T_Null && ((A_Const*)$3)->val.type == T_Null )
				{
					$$ = $1;
					pfree($3);
				}
				else
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("concat");
					n->args = list_make2($1, $3);
					n->location = @1;
					$$ = (Node *)n;
				}
			} else
			{
				$$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2);
			}
		}
	| qual_Op a_expr					%prec Op
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
	| a_expr qual_Op					%prec POSTFIXOP
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }

	| a_expr AND a_expr
		{ $$ = makeAndExpr($1, $3, @2); }
	| a_expr OR a_expr
		{ $$ = makeOrExpr($1, $3, @2); }
	| NOT a_expr
		{ $$ = makeNotExpr($2, @1); }
	| a_expr LIKE a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, $3, @2); }
	| a_expr LIKE a_expr ESCAPE a_expr
		{
			FuncCall *n = makeNode(FuncCall);
			n->funcname = SystemFuncName("like_escape");
			n->args = list_make2($3, $5);
			n->agg_order = NIL;
			n->agg_star = false;
			n->agg_distinct = false;
			n->func_variadic = false;
			n->over = NULL;
			n->location = @2;
			$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, (Node *) n, @2);
		}
	| a_expr NOT LIKE a_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, $4, @2); }
	| a_expr NOT LIKE a_expr ESCAPE a_expr
		{
			FuncCall *n = makeNode(FuncCall);
			n->funcname = SystemFuncName("like_escape");
			n->args = list_make2($4, $6);
			n->agg_order = NIL;
			n->agg_star = false;
			n->agg_distinct = false;
			n->func_variadic = false;
			n->over = NULL;
			n->location = @2;
			$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, (Node *) n, @2);
		}
	| a_expr IS NULL_P							%prec IS
		{
			NullTest *n = makeNode(NullTest);
			n->arg = (Expr *) $1;
			n->nulltesttype = IS_NULL;
			n->location = @2;
			$$ = (Node *) n;
		}
	| a_expr IS NOT NULL_P						%prec IS
		{
			NullTest *n = makeNode(NullTest);
			n->arg = (Expr *) $1;
			n->nulltesttype = IS_NOT_NULL;
			n->location = @2;
			$$ = (Node *) n;
		}
	| a_expr BETWEEN b_expr AND b_expr			%prec BETWEEN
		{
			$$ = (Node *) makeSimpleA_Expr(AEXPR_BETWEEN,
										   "BETWEEN",
										   $1,
										   (Node *) list_make2($3, $5),
										   @2);
		}
	| a_expr NOT BETWEEN b_expr AND b_expr		%prec BETWEEN
		{
			$$ = (Node *) makeSimpleA_Expr(AEXPR_NOT_BETWEEN,
										   "NOT BETWEEN",
										   $1,
										   (Node *) list_make2($4, $6),
										   @2);
		}
	| a_expr IN_P select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subselect = $3;
			n->subLinkType = ANY_SUBLINK;
			n->testexpr = $1;
			n->operName = list_make1(makeString("="));
			n->location = @2;
			$$ = (Node*)n;
		}
	| a_expr IN_P '(' expr_list ')'
		{
			$$ = (Node*)makeSimpleA_Expr(AEXPR_IN, "=", $1, (Node*)$4, @2);
		}
	| a_expr NOT IN_P select_with_parens
		{
			/* generate NOT (foo = ANY (subquery)) */
			/* Make an = ANY node */
			SubLink *n = makeNode(SubLink);
			n->subselect = $4;
			n->subLinkType = ANY_SUBLINK;
			n->testexpr = $1;
			n->operName = NIL;
			n->location = @2;
			/* Stick a NOT on top; must have same parse location */
			$$ = makeNotExpr((Node *) n, @2);
		}
	| a_expr NOT IN_P '(' expr_list ')'
		{
			/* generate scalar NOT IN expression */
			$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, (Node*)$5, @2);
		}
	| a_expr subquery_Op sub_type select_with_parens	%prec Op
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = $3;
			n->testexpr = $1;
			n->operName = $2;
			n->subselect = $4;
			n->location = @2;
			$$ = (Node *)n;
		}
	| a_expr qual_Op SOME select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = ANY_SUBLINK;
			n->testexpr = $1;
			n->operName = $2;
			n->subselect = $4;
			n->location = @2;
			$$ = (Node *)n;
		}
	| a_expr '+' SOME select_with_parens		{ $$ = make_any_sublink($1, "+", $4, @2); }
	| a_expr '-' SOME select_with_parens		{ $$ = make_any_sublink($1, "-", $4, @2); }
	| a_expr '*' SOME select_with_parens		{ $$ = make_any_sublink($1, "*", $4, @2); }
	| a_expr '/' SOME select_with_parens		{ $$ = make_any_sublink($1, "/", $4, @2); }
	| a_expr '%' SOME select_with_parens		{ $$ = make_any_sublink($1, "%", $4, @2); }
	| a_expr '^' SOME select_with_parens		{ $$ = make_any_sublink($1, "^", $4, @2); }
	| a_expr '<' SOME select_with_parens		{ $$ = make_any_sublink($1, "<", $4, @2); }
	| a_expr '>' SOME select_with_parens		{ $$ = make_any_sublink($1, ">", $4, @2); }
	| a_expr '=' SOME select_with_parens		{ $$ = make_any_sublink($1, "=", $4, @2); }
	| a_expr LIKE SOME select_with_parens		{ $$ = make_any_sublink($1, "~~", $4, @2); }
	| a_expr NOT LIKE SOME select_with_parens	{ $$ = make_any_sublink($1, "!~~", $5, @2); }
	| a_expr subquery_Op sub_type '(' a_expr ')'
		{
			if ($3 == ANY_SUBLINK)
				$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
			else
				$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, $5, @2);
		}
	| a_expr qual_Op SOME '(' a_expr ')'
		{
			$$ = (Node*) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
		}
	| a_expr '+' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("+", $1, $5, @2); }
	| a_expr '*' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("*", $1, $5, @2); }
	| a_expr '/' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("/", $1, $5, @2); }
	| a_expr '%' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("%", $1, $5, @2); }
	| a_expr '^' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("^", $1, $5, @2); }
	| a_expr '<' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("<", $1, $5, @2); }
	| a_expr '>' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR(">", $1, $5, @2); }
	| a_expr '=' SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("=", $1, $5, @2); }
	| a_expr LIKE SOME '(' a_expr ')'			{ $$ = MAKE_ANY_A_EXPR("~~", $1, $5, @2); }
	| a_expr NOT LIKE SOME '(' a_expr ')'		{ $$ = MAKE_ANY_A_EXPR("!~~", $1, $6, @2); }
	| PRIOR a_expr								%prec UMINUS
		{
			PriorExpr *prior = makeNode(PriorExpr);
			prior->location = @1;
			prior->expr = $2;
			$$ = (Node*)prior;
		}
	| CONNECT_BY_ROOT a_expr					%prec UMINUS
		{
			ConnectByRootExpr *node = makeNode(ConnectByRootExpr);
			node->location = @1;
			node->expr = $2;
			$$ = (Node*)node;
		}
	;

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
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
			| func_name Sconst
				{
					/* generic type 'literal' syntax */
					TypeName *t = makeTypeNameFromNameList($1);
					t->location = @1;
					$$ = makeStringConstCast($2, @2, t);
				}
			| func_name '(' func_arg_list ')' Sconst
				{
					/* generic syntax with a type modifier */
					TypeName *t = makeTypeNameFromNameList($1);
					ListCell *lc;

					/*
					 * We must use func_arg_list in the production to avoid
					 * reduce/reduce conflicts, but we don't actually wish
					 * to allow NamedArgExpr in this context.
					 */
					foreach(lc, $3)
					{
						NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

						if (IsA(arg, NamedArgExpr))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have parameter name"),
									 parser_errposition(arg->location)));
					}
					t->typmods = $3;
					t->location = @1;
					$$ = makeStringConstCast($5, @5, t);
				}
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
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					TypeName *t = $1;
					if ($6 != NIL)
					{
						if (list_length($6) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						t->typmods = lappend($6, makeIntConst($3, @3));
					}
					else
						t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
		;

attr_name: ColLabel
	;

Bit:  BFILE					{ $$ = SystemTypeNameLocation("bytea", @1); }
	| BLOB					{ $$ = SystemTypeNameLocation("bytea", @1); }
	| LONG_P RAW			{ $$ = SystemTypeNameLocation("bytea", @1); }
	| RAW '(' Iconst ')'
		{
			$$ = SystemTypeNameLocation("bytea", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	;

b_expr: c_expr
	| b_expr TYPECAST Typename
		{ $$ = makeTypeCast($1, $3, @2); }
	| '+' b_expr
		{ $$ = (Node*)makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
	| '-' b_expr
		{ $$ = doNegate($2, @1); }
	| b_expr '+' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
	| b_expr '-' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
	| b_expr '*' b_expr
		{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
	| b_expr '/' b_expr
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
		{
			char *nspname = NULL;
			char *objname = NULL;

			DeconstructQualifiedName($2, &nspname, &objname);
			if (objname &&
				strncmp(objname, "||", strlen(objname)) == 0)
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("concat");
				n->args = list_make2($1, $3);
				n->location = @1;
				$$ = (Node *)n;
			} else
			{
				$$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2);
			}
		}
	| qual_Op b_expr					%prec Op
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
	| b_expr qual_Op					%prec POSTFIXOP
		{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }
	| b_expr IS DISTINCT FROM b_expr		%prec IS
		{
			$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
		}
	;

//case_arg: a_expr
//	| /* empty */ { $$ = NULL;}
//	;
//
case_default: ELSE a_expr		{ $$ = $2; }
	| /* empty */				{ $$ = NULL; }
	;

case_expr:
	CASE case_when_list case_default END_P
		{
			CaseExpr *c = makeNode(CaseExpr);
			c->casetype = InvalidOid;
			c->arg = NULL;
			c->args = $2;
			c->defresult = (Expr*)$3;
			c->location = @1;
			$$ = (Node*)c;
		}
	| CASE a_expr case_when_list case_default END_P
		{
			CaseExpr *c = makeNode(CaseExpr);
			c->casetype = InvalidOid;
			c->arg = (Expr*)$2;
			c->args = $3;
			c->defresult = (Expr*)$4;
			c->location = @1;
			$$ = (Node*)c;
		}
	;

case_when_list: case_when_item			{ $$ = list_make1($1); }
		| case_when_list case_when_item	{ $$ = lappend($1, $2); }
	;

case_when_item: WHEN a_expr THEN a_expr
		{
			CaseWhen *w = makeNode(CaseWhen);
			w->expr = (Expr *) $2;
			w->result = (Expr *) $4;
			w->location = @1;
			$$ = (Node *)w;
		}
	| WHEN IS NULL_P THEN a_expr
		{
			CaseWhen *w = makeNode(CaseWhen);
			NullTest *n = makeNode(NullTest);
			n->arg = NULL;
			n->nulltesttype = IS_NULL;
			n->location = @2;
			w->expr = (Expr*)n;
			w->result = (Expr*)$5;
			w->location = @1;
			$$ = (Node *)w;
		}
	| WHEN IS NOT NULL_P THEN a_expr
		{
			CaseWhen *w = makeNode(CaseWhen);
			NullTest *n = makeNode(NullTest);
			n->arg = NULL;
			n->nulltesttype = IS_NOT_NULL;
			n->location = @2;
			w->expr = (Expr*)n;
			w->result = (Expr*)$6;
			w->location = @1;
			$$ = (Node *)w;
		}
	;

Character:
	  VARCHAR opt_type_mod
		{
			$$ = SystemTypeNameLocation("varchar", @1);
			$$->typmods = $2;
		}
	| VARCHAR2
		{
			$$ = OracleTypeNameLocation("varchar2", @1);
		}
	| VARCHAR2 '(' Iconst opt_byte_char ')'
		{
			$$ = OracleTypeNameLocation("varchar2", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| NVARCHAR2 '(' Iconst ')'
		{
			$$ = OracleTypeNameLocation("nvarchar2", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| CHAR_P '(' Iconst opt_byte_char ')'
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| CHAR_P
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			/* char defaults to char(1) */
			$$->typmods = list_make1(makeIntConst(1, -1));
		}
	| NCHAR '(' Iconst ')'
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			$$->typmods = list_make1(makeIntConst($3, @3));
		}
	| NCHAR
		{
			$$ = SystemTypeNameLocation("bpchar", @1);
			/* nchar defaults to char(1) */
			$$->typmods = list_make1(makeIntConst(1, -1));
		}
	| CLOB		{ $$ = SystemTypeNameLocation("text", @1); }
	| NCLOB		{ $$ = SystemTypeNameLocation("text", @1); }
	;

c_expr: columnref
	| AexprConst
	| func_expr
	| case_expr
	| '(' a_expr ')' { $$ = $2; }
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
	| select_with_parens			%prec UMINUS
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = EXPR_SUBLINK;
			n->testexpr = NULL;
			n->operName = NIL;
			n->subselect = $1;
			n->location = @1;
			$$ = (Node *)n;
		}
	| EXISTS select_with_parens
		{
			SubLink *n = makeNode(SubLink);
			n->subLinkType = EXISTS_SUBLINK;
			/*n->testexpr = NULL;
			n->operName = NIL;*/
			n->subselect = $2;
			n->location = @1;
			$$ = (Node *)n;
		}
	| implicit_row
		{
			RowExpr *r = makeNode(RowExpr);
			r->args = $1;
			r->row_typeid = InvalidOid;	/* not analyzed yet */
			r->colnames = NIL;	/* to be filled in during analysis */
			r->row_format = COERCE_IMPLICIT_CAST; /* abuse */
			r->location = @1;
			$$ = (Node *)r;
		}
	| GROUPING '(' expr_list ')'
			{
				GroupingFunc *g = makeNode(GroupingFunc);
				g->args = $3;
				g->location = @1;
				$$ = (Node *)g;
			}
	;

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:		IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
			| reserved_keyword						{ $$ = pstrdup($1); }
		;

common_table_expr: name opt_name_list AS '(' SelectStmt ')'
		{
			CommonTableExpr *n = makeNode(CommonTableExpr);
			n->ctename = $1;
			n->aliascolnames = $2;
			n->ctequery = $5;
			n->location = @1;
			$$ = (Node *) n;
		}
	;


columnDef: ColId Typename create_generic_options ColQualList
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
					$$ = (Node *)n;
		}
	;

ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					Constraint *n = (Constraint *) $3;
					Assert(IsA(n, Constraint));
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

any_name_list: any_name						{ $$ = list_make1($1); }
			| any_name_list ',' any_name	{ $$ = lappend($1,$3); }
		;

any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
		;

attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;
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
opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
definition: '(' def_list ')'						{ $$ = $2; }
		;

def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;

def_elem:	ColLabel
				{
					$$ = makeDefElem($1, NULL, @1);
				}
		;


OptConsTableSpace:   USING INDEX TABLESPACE name	{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_no_inherit:	NO INHERIT							{  $$ = true; }
			| /* EMPTY */							{  $$ = false; }
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

key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }
		;

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

columnref:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| ROWID
				{
					$$ = makeColumnRef(pstrdup($1), NIL, @1, yyscanner);
				}
			| LEVEL
				{
					LevelExpr *n = makeNode(LevelExpr);
					n->location = @1;
					$$ = (Node*)n;
				}
			| ColId indirection
				{	
					ListCell		*lc1, *lc2;
					List 			*list;
					//Grammar support: select a.b.c.d =>> select ((a.b).c).d
					if(list_length($2) >= 2)
					{
						A_Indirection *in = makeNode(A_Indirection);
						lc1 = list_head($2);
						lc2 = lnext(lc1);
						list = list_make1(lfirst(lc1));
						in->arg = makeColumnRef($1, list, @1 + (list_length($2)-1), yyscanner);
						in->indirection = list_make1(lfirst(lc2));
						$$ = (Node *)listToIndirection(in, lnext(lc2));
					}else
					{
						$$ = makeColumnRef($1, $2, @1, yyscanner);
					}
				}
			| columnref ORACLE_JOIN_OP
				{
					ColumnRefJoin *n = makeNode(ColumnRefJoin);
					n->column = (ColumnRef*)$1;
					n->location = @2;
					$$ = (Node*)n;
				}
		;

ConstInterval:
	  INTERVAL		{ $$ = SystemTypeNameLocation("interval", @1); }
	;

ConstDatetime:
	  DATE_P
	  	{
	  		$$ = OracleTypeNameLocation("date", @1);
	  	}
	| TIMESTAMP opt_type_mod
		{
			$$ = SystemTypeNameLocation("timestamp", @1);
			$$->typmods = $2;
		}
	| TIMESTAMP opt_type_mod WITH TIME ZONE
		{
			$$ = SystemTypeNameLocation("timestamptz", @1);
			$$->typmods = $2;
		}
	| TIMESTAMP opt_type_mod WITH LOCAL TIME ZONE
		{
			$$ = SystemTypeNameLocation("timestamptz", @1);
			$$->typmods = $2;
		}
	;

/* We have a separate ConstTypename to allow defaulting fixed-length
 * types such as CHAR() and BIT() to an unspecified length.
 * SQL9x requires that these default to a length of one, but this
 * makes no sense for constructs like CHAR 'hi' and BIT '0101',
 * where there is an obvious better choice to make.
 * Note that ConstInterval is not included here since it must
 * be pushed up higher in the rules to accommodate the postfix
 * options (e.g. INTERVAL '1' YEAR). Likewise, we have to handle
 * the generic-type-name case in AExprConst to avoid premature
 * reduce/reduce conflicts against function names.
 */
ConstTypename:
			Numeric									{ $$ = $1; }
			/*| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }*/
			| ConstDatetime							{ $$ = $1; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *		PGXC-related extensions:
 *		1) Distribution type of a table:
 *			DISTRIBUTE BY ( HASH(column) | MODULO(column) | REPLICATION | RANDOM )
 *		2) Subcluster for table
 *			TO ( GROUP groupname | NODE nodename1,...,nodenameN )
 *
 *****************************************************************************/

CreateStmt:	CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = $8;
					n->constraints = NIL;
					n->options = $9;
					n->oncommit = $10;
					n->tablespacename = $11;
					n->if_not_exists = false;
/* ADB_BEGIN */
					n->distributeby = NULL;
					n->subcluster = $12;
/* ADB_END */					
					n->partspec = NULL;
					n->child_rels = NIL;
				
					$$ = (Node *)n;
				}		               
        | CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptPartitionSpecNotEmpty OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptDistributeBy OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = $8;
					n->constraints = NIL;
					n->options = $10;
					n->oncommit = $11;
					n->tablespacename = $12;
					n->if_not_exists = false;
/* ADB_BEGIN */
					n->distributeby = $13;
					n->subcluster = $14;
/* ADB_END */
					if ($9)
					{
						n->partspec = $9->partitionSpec;
						n->child_rels = $9->children;
					}
					$$ = (Node *)n;
				}
/* ADB_BEGIN */
		| CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
		    OptInherit OptDistributeByInternal OptSubCluster
			OptPartitionSpec OptWith OnCommitOption OptTableSpace 
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $6;
                    n->inhRelations = $8;	
					n->distributeby = $9;
					n->subcluster = $10;
					n->constraints = NIL;
					n->options = $12;
					n->oncommit = $13;
					n->tablespacename = $14;
					n->if_not_exists = false;

					if ($11)
					{
						n->partspec = $11->partitionSpec;
						n->child_rels = $11->children;
					}
					$$ = (Node *)n;
				}
/* ADB_END */
        | CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('        
			OptTableElementList ')' OptInherit OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->inhRelations = $11;
					n->constraints = NIL;
					n->options = $12;
					n->oncommit = $13;
					n->tablespacename = $14;
					n->if_not_exists = true;
/* ADB_BEGIN */
					n->distributeby = NULL;
					n->subcluster = $15;					
/* ADB_END */
                    n->partspec = NULL;
					n->child_rels = NIL;

					$$ = (Node *)n;
				}		
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('  
			OptTableElementList ')' OptInherit OptPartitionSpecNotEmpty OptWith OnCommitOption
			OptTableSpace
/* ADB_BEGIN */
			OptDistributeBy OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->inhRelations = $11;
					n->constraints = NIL;
					n->options = $13;
					n->oncommit = $14;
					n->tablespacename = $15;
					n->if_not_exists = true;
/* ADB_BEGIN */
					n->distributeby = $16;
					n->subcluster = $17;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* ADB_END */
					if ($12)
					{
						n->partspec = $12->partitionSpec;
						n->child_rels = $12->children;
					}
					$$ = (Node *)n;
				}		
/* ADB_BEGIN */
        | CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('
			OptTableElementList ')' OptInherit OptDistributeByInternal OptSubCluster
			OptPartitionSpec OptWith OnCommitOption	OptTableSpace
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->inhRelations = $11;
					n->constraints = NIL;
					n->distributeby = $12;
					n->subcluster = $13;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
					n->options = $15;
					n->oncommit = $16;
					n->tablespacename = $17;
					n->if_not_exists = true;

					if ($14)
					{
						n->partspec = $14->partitionSpec;
						n->child_rels = $14->children;
					}
					$$ = (Node *)n;
				}
/* ADB_END */
		| CREATE OptTemp TABLE qualified_name OF any_name 
		    OptTypedTableElementList OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $7;
					n->ofTypename = makeTypeNameFromNameList($6);
					n->ofTypename->location = @6;
					n->constraints = NIL;
					n->options = $8;
					n->oncommit = $9;
					n->tablespacename = $10;
					n->if_not_exists = false;
/* ADB_BEGIN */
					n->distributeby = NULL;
					n->subcluster = $11;
/* ADB_END */
					n->partspec = NULL;
					n->child_rels = NIL;
				
					$$ = (Node *)n;
				}	
		| CREATE OptTemp TABLE qualified_name OF any_name 
		    OptTypedTableElementList OptPartitionSpecNotEmpty OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptDistributeBy OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $7;
					n->ofTypename = makeTypeNameFromNameList($6);
					n->ofTypename->location = @6;
					n->constraints = NIL;
					n->options = $9;
					n->oncommit = $10;
					n->tablespacename = $11;
					n->if_not_exists = false;
/* ADB_BEGIN */
					n->distributeby = $12;
					n->subcluster = $13;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* ADB_END */
					if ($8)
					{
						n->partspec = $8->partitionSpec;
						n->child_rels = $8->children;
					}
					$$ = (Node *)n;
				}		
/* ADB_BEGIN */
		| CREATE OptTemp TABLE qualified_name OF any_name 
		    OptTypedTableElementList OptDistributeByInternal OptSubCluster
			OptPartitionSpec OptWith OnCommitOption OptTableSpace
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $7;
					n->ofTypename = makeTypeNameFromNameList($6);
					n->ofTypename->location = @6;
					n->constraints = NIL;
					n->distributeby = $8;
					n->subcluster = $9;
					n->options = $11;
					n->oncommit = $12;
					n->tablespacename = $13;
					n->if_not_exists = false;
					
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));

					if ($10)
					{
						n->partspec = $10->partitionSpec;
						n->child_rels = $10->children;
					}
					$$ = (Node *)n;
				}
/* ADB_END */			
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name
			OptTypedTableElementList OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $10;
					n->ofTypename = makeTypeNameFromNameList($9);
					n->ofTypename->location = @9;
					n->constraints = NIL;
					n->options = $11;
					n->oncommit = $12;
					n->tablespacename = $13;
					n->if_not_exists = true;
/* ADB_BEGIN */
					n->distributeby = NULL;
					n->subcluster = $14;
/* ADB_END */
					n->partspec = NULL;
					n->child_rels = NIL;
				
					$$ = (Node *)n;
				}	
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name
			OptTypedTableElementList OptPartitionSpecNotEmpty OptWith OnCommitOption OptTableSpace
/* ADB_BEGIN */
			OptDistributeBy OptSubCluster
/* ADB_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $10;
					n->ofTypename = makeTypeNameFromNameList($9);
					n->ofTypename->location = @9;
					n->constraints = NIL;
					n->options = $12;
					n->oncommit = $13;
					n->tablespacename = $14;
					n->if_not_exists = true;
/* ADB_BEGIN */
					n->distributeby = $15;
					n->subcluster = $16;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));
/* ADB_END */
					if ($11)
					{
						n->partspec = $11->partitionSpec;
						n->child_rels = $11->children;
					}
					$$ = (Node *)n;
				}
/* ADB_BEGIN */
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name OptTypedTableElementList
			OptDistributeByInternal OptSubCluster
			OptPartitionSpec OptWith OnCommitOption OptTableSpace
				{
					CreateStmt *n = makeNode(CreateStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $10;
					n->ofTypename = makeTypeNameFromNameList($9);
					n->ofTypename->location = @9;
					n->constraints = NIL;
					n->options = $14;
					n->oncommit = $15;
					n->tablespacename = $16;
					n->if_not_exists = true;
					n->distributeby = $11;
					n->subcluster = $12;
					if (n->inhRelations != NULL && n->distributeby != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("CREATE TABLE cannot contains both an INHERITS and a DISTRIBUTE BY clause"),
								 parser_errposition(exprLocation((Node *) n->distributeby))));

					if ($13)
					{
						n->partspec = $13->partitionSpec;
						n->child_rels = $13->children;
					}
					$$ = (Node *)n;
				}
/* ADB_END */								
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
					/*ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1))); */
					$$ = RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMP
				{
					/* ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1))); */
					$$ = RELPERSISTENCE_TEMP;
				}
			| UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;

OptInherit: INHERITS '(' qualified_name_list ')'	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* ADB_BEGIN */
OptPartitionSpec:
			 OptPartitionSpecNotEmpty
				{
					$$ = $1;
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
        ;
/* ADB_END */

OptPartitionSpecNotEmpty:
			  PartitionSpec
				{
					$$ = $1;
				}
			| PartitionSpec '(' ora_part_child_list ')'
				{
					if (strcmp($1->partitionSpec->strategy, "hash") == 0)
					{
						int			part_count, i;
						ListCell	*cell;
						CreateStmt	*stmt;

						part_count = list_length($3);
						if (part_count > 0)
						{
							cell = list_head($3);
							for (i = 0; i < part_count; i++)
							{
								if (cell && nodeTag(lfirst(cell)) != T_CreateStmt)
									elog(ERROR, "Unknown hash partition table creation clause.");

								stmt = (CreateStmt*) lfirst(cell);
								Assert (stmt->partbound->strategy = PARTITION_STRATEGY_HASH);
								stmt->partbound->modulus = part_count;
								stmt->partbound->remainder = i;

								cell = lnext(cell);
							}
						}
					}

					$$ =  $1;
					$$->children = $3;
				}
		;

PartitionSpec: PARTITION BY part_strategy '(' part_params ')'
				{
					PartitionSpec *n = makeNode(PartitionSpec);

					n->strategy = $3;
					n->partParams = $5;
					n->location = @1;

					$$ = palloc0(sizeof(OraclePartitionSpec));
					$$->partitionSpec = n;
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
			| func_expr_windowless opt_collate opt_class
				{
					PartitionElem *n = makeNode(PartitionElem);

					n->name = NULL;
					n->expr = $1;
					n->collation = $2;
					n->opclass = $3;
					n->location = @1;
					$$ = n;
				}
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

ora_part_child_list:
		  ora_part_child							{ $$ = list_make1($1); }
		| ora_part_child_list ',' ora_part_child	{ $$ = lappend($1, $3); }
		;

ora_part_child:
		PARTITION qualified_name ForValues OptWith OnCommitOption OptTableSpace
			{
				CreateStmt *n = makeNode(CreateStmt);
				n->grammar = PARSE_GRAM_ORACLE;
				n->relation = $2;
				n->partbound = $3;
				n->options = $4;
				n->oncommit = $5;
				n->tablespacename = $6;

				$$ = (Node*)n;
			}
		;

ForValues:
			/* a LIST partition */
			  VALUES '(' expr_list ')'
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_LIST;
					n->listdatums = $3;
					n->location = @2;

					$$ = n;
				}
			| VALUES '(' DEFAULT ')'
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_LIST;
					n->is_default = true;
					n->location = @2;
					$$ = n;
				}
			/* a RANGE partition */
			| VALUES FROM '(' range_datum_list ')' TO '(' range_datum_list ')'
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_RANGE;
					n->lowerdatums = $4;
					n->upperdatums = $8;
					n->location = @2;

					$$ = n;
				}
			| VALUES LESS THAN '(' expr_list ')'
				{
						PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

						n->strategy = PARTITION_STRATEGY_RANGE;
						n->lowerdatums = NIL;
						n->upperdatums = $5;
						n->location = @2;

						$$ = n;
				}
			| VALUES LESS THAN '(' MAXVALUE ')'
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n = makeNode(PartitionBoundSpec);
					n->strategy = PARTITION_STRATEGY_RANGE;
					n->lowerdatums = NIL;
					n->upperdatums = list_make1((Node *) makeColumnRef("maxvalue", NIL, -1, NULL));
					n->location = @2;

					$$ = n;
				}
			| /* empty */
				{
					PartitionBoundSpec *n = makeNode(PartitionBoundSpec);

					n->strategy = PARTITION_STRATEGY_HASH;
					n->location = -1;
					$$ = n;
				}
		;

range_datum_list:
			PartitionRangeDatum					{ $$ = list_make1($1); }
			| range_datum_list ',' PartitionRangeDatum
												{ $$ = lappend($1, $3); }
		;

PartitionRangeDatum: a_expr
				{
					PartitionRangeDatum *n;
					if (!IsA($1, A_Const))
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("syntax error"),
								 parser_errposition(@1)));
					}
					n = makeNode(PartitionRangeDatum);

					n->kind = PARTITION_RANGE_DATUM_VALUE;
					n->value = $1;
					n->location = @1;

					$$ = (Node *) n;
				}
			;

create_generic_options:
	  /* empty */						{ $$ = NIL; }
	;

cte_list:
	  common_table_expr 				{ $$ = list_make1($1); }
	| cte_list ',' common_table_expr	{ $$ = lappend($1, $3); }
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

/* ADB_BEGIN */
OptDistributeBy: OptDistributeByInternal			{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;
/* ADB_END */

OptTypedTableElementList:
			'(' TypedTableElementList ')'		{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TypedTableElementList:
			TypedTableElement
				{
					$$ = list_make1($1);
				}
			| TypedTableElementList ',' TypedTableElement
				{
					$$ = lappend($1, $3);
				}
		;

TypedTableElement:
			columnOptions						{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;

columnOptions:	ColId WITH OPTIONS ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					SplitColQualList($4, &n->constraints, &n->collClause,
									 yyscanner);
					$$ = (Node *)n;
				}
		;


/*
 * The SQL spec defines "contextually typed value expressions" and
 * "contextually typed row value constructors", which for our purposes
 * are the same as "a_expr" and "row" except that DEFAULT can appear at
 * the top level.
 */

ctext_expr:
		a_expr					{ $$ = (Node *) $1; }
		| DEFAULT
			{
				SetToDefault *n = makeNode(SetToDefault);
				n->location = @1;
				$$ = (Node *) n;
			}
	;

ctext_expr_list:
		ctext_expr								{ $$ = list_make1($1); }
		| ctext_expr_list ',' ctext_expr		{ $$ = lappend($1, $3); }
	;

ctext_row: '(' ctext_expr_list ')'					{ $$ = $2; }
	;

DeleteStmt:
	  opt_with_clause DELETE_P opt_from relation_expr
	  opt_alias_clause where_or_current_clause returning_clause
		{
			$$ = makeDeleteStmt($4, $5, $1, $6, $7);
		}
	/*| opt_with_clause  DELETE_P opt_from ONLY '(' relation_expr ')'
	  opt_alias_clause where_or_current_clause returning_clause
		{
			$$ = makeDeleteStmt($6, $8, $1, $9, $10);
		}*/
	/*| DELETE_P opt_from select_with_parens
	  opt_alias_clause where_or_current_clause returning_clause
		{
		}*/
	;

/* variant for UPDATE and DELETE */
where_or_current_clause:
			WHERE a_expr							{ $$ = $2; }
			| WHERE CURRENT_P OF cursor_name
				{
					CurrentOfExpr *n = makeNode(CurrentOfExpr);
					/* cvarno is filled in by parse analysis */
					n->cursor_name = $4;
					n->cursor_param = 0;
					$$ = (Node *) n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


document_or_content: DOCUMENT_P						{ $$ = XMLOPTION_DOCUMENT; }
			| CONTENT_P								{ $$ = XMLOPTION_CONTENT; }
		;
DropProcedureStmt:
			DROP PROCEDURE function_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP PROCEDURE IF_P EXISTS function_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			|	DROP FUNCTION function_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = $3;
					n->behavior = $4;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP FUNCTION IF_P EXISTS function_with_argtypes_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = $5;
					n->behavior = $6;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;
function_with_argtypes_list:
			function_with_argtypes
				{ $$ = list_make1($1); }
			| function_with_argtypes_list ',' function_with_argtypes
				{ $$ = lappend($1, $3); }
		;
function_with_argtypes:
			func_name func_args
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = extractArgTypes($2);
					$$ = n;
				}
			/*
			 * Because of reduce/reduce conflicts, we can t use func_name
			 * below, but we can write it out the long way, which actually
			 * allows more cases.
			 */
			| type_func_name_keyword
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = list_make1(makeString(pstrdup($1)));
					n->args_unspecified = true;
					$$ = n;
				}
			| ColId
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = list_make1(makeString($1));
					n->args_unspecified = true;
					$$ = n;
				}
			| ColId indirection
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = check_func_name(lcons(makeString($1), $2),
												  yyscanner);
					n->args_unspecified = true;
					$$ = n;
				}
		;
func_args:	'(' func_args_list ')'					{ $$ = $2; }
			| '(' ')'								{ $$ = NIL; }
		;
func_args_list:
			func_arg								{ $$ = list_make1($1); }
			| func_args_list ',' func_arg			{ $$ = lappend($1, $3); }
		;

DropStmt:
	DROP drop_type IF_P EXISTS any_name_list opt_drop_behavior DropStmt_opt_purge
		{
			DropStmt *n = makeNode(DropStmt);
			n->removeType = $2;
			n->missing_ok = true;
			n->objects = $5;
			n->behavior = $6;
			n->concurrent = false;
			$$ = (Node *)n;
		}
	| DROP drop_type any_name_list opt_drop_behavior DropStmt_opt_purge
		{
			DropStmt *n = makeNode(DropStmt);
			n->removeType = $2;
			n->missing_ok = false;
			n->objects = $3;
			n->behavior = $4;
			n->concurrent = false;
			$$ = (Node *)n;
		}
	| DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
		{
			DropStmt *n = makeNode(DropStmt);
			n->removeType = OBJECT_INDEX;
			n->missing_ok = false;
			n->objects = $4;
			n->behavior = $5;
			n->concurrent = true;
			$$ = (Node *)n;
		}
	| DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
		{
			DropStmt *n = makeNode(DropStmt);
			n->removeType = OBJECT_INDEX;
			n->missing_ok = true;
			n->objects = $6;
			n->behavior = $7;
			n->concurrent = true;
			$$ = (Node *)n;
		}
		;
	;

drop_type:	TABLE									{ $$ = OBJECT_TABLE; }
			| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW						{ $$ = OBJECT_MATVIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGN_TABLE; }
			| EVENT TRIGGER 						{ $$ = OBJECT_EVENT_TRIGGER; }
			| COLLATION								{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| EXTENSION								{ $$ = OBJECT_EXTENSION; }
			| TEXT_P SEARCH PARSER					{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = OBJECT_TSCONFIGURATION; }
		;

DropStmt_opt_purge:
	  PURGE
	| /* empty */
	;

ExplainStmt:
	EXPLAIN ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $2;
			n->options = NIL;
			$$ = (Node *) n;
		}
	| EXPLAIN analyze_keyword opt_verbose ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $4;
			n->options = list_make1(makeDefElem("analyze", NULL, @2));
			if ($3)
				n->options = lappend(n->options,
									 makeDefElem("verbose", NULL, @3));
			$$ = (Node *) n;
		}
	| EXPLAIN VERBOSE ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $3;
			n->options = list_make1(makeDefElem("verbose", NULL, @2));
			$$ = (Node *) n;
		}
	| EXPLAIN '(' explain_option_list ')' ExplainableStmt
		{
			ExplainStmt *n = makeNode(ExplainStmt);
			n->query = $5;
			n->options = $3;
			$$ = (Node *) n;
		}
	;

ExplainableStmt:
	SelectStmt
	| InsertStmt
	| UpdateStmt
	| DeleteStmt
	| CreateMatViewStmt
	;

explain_option_list:
	explain_option_elem
		{
			$$ = list_make1($1);
		}
	| explain_option_list ',' explain_option_elem
		{
			$$ = lappend($1, $3);
		}
	;

explain_option_elem:
	explain_option_name explain_option_arg
		{
			$$ = makeDefElem($1, $2, @1);
		}
	;

explain_option_name:
	NonReservedWord
	{
		if(strcmp($1, "analyse") == 0)
			$$ = "analyze";
		else
			$$ = $1;
	}
	;

explain_option_arg:
		opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
		| NumericOnly			{ $$ = (Node *) $1; }
		| /* EMPTY */			{ $$ = NULL; }
	;

expr_list: a_expr { $$ = list_make1($1); }
	| expr_list ',' a_expr
		{ $$ = lappend($1, $3); }
	/*
	| DEFAULT
		{
			SetToDefault *n = makeNode(SetToDefault);
			n->location = @1;
			$$ = list_make1((Node *) n);
		}
	*/
	;

trim_list:	a_expr FROM expr_list					{ $$ = lappend($3, $1); }
		| FROM expr_list						{ $$ = $2; }
		| expr_list								{ $$ = $1; }
	;

from_clause: FROM from_list			{ $$ = $2; }
			| /* empty */ { $$ = NIL; }
		;

from_list: table_ref				{ $$ = $1 ? list_make1($1):NIL; }
		| from_list ',' table_ref	{ $$ = $3 ? lappend($1, $3):$1; }
		;

func_arg_expr: a_expr				{ $$ = $1; }
		| TRUE_P					{ $$ = makeBoolAConst(true, @1); }
		| FALSE_P					{ $$ = makeBoolAConst(false, @1); }
		| param_name COLON_EQUALS a_expr
			{
				NamedArgExpr *na = makeNode(NamedArgExpr);
				na->name = $1;
				na->arg = (Expr *) $3;
				na->argnumber = -1;		/* until determined */
				na->location = @1;
				$$ = (Node *) na;
			}
		| param_name EQUALS_GREATER a_expr
			{
				NamedArgExpr *na = makeNode(NamedArgExpr);
				na->name = $1;
				na->arg = (Expr *) $3;
				na->argnumber = -1;		/* until determined */
				na->location = @1;
				$$ = (Node *) na;
			}
		;

func_arg_list: func_arg_expr				{ $$ = list_make1($1); }
		| func_arg_list ',' func_arg_expr	{ $$ = lappend($1, $3); }
		;

extract_list:
			extract_arg FROM a_expr
				{
					$$ = list_make2(makeStringConst($1, @1), $3);
				}
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* Allow delimited string Sconst in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */
extract_arg:
			IDENT									{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| Sconst								{ $$ = $1; }
		;

func_table: func_application
			{
				RangeFunction *n = makeNode(RangeFunction);
				n->lateral = false;
				n->ordinality = false;
				n->is_rowsfrom = false;
				n->functions = list_make1(list_make2($1, NIL));
				/* alias and coldeflist are set by table_ref production */
				$$ = (Node *) n;
			}
		;
/*
 * func_expr is split out from c_expr just so that we have a classification
 * for "everything that is a function call or looks like one".  This isn't
 * very important, but it saves us having to document which variants are
 * legal in the backwards-compatible functional-index syntax for CREATE INDEX.
 * (Note that many of the special SQL functions wouldn't actually make any
 * sense as functional index entries, but we ignore that consideration here.)
 */
func_expr: func_application within_group_clause keep_clause over_clause
			{
				FuncCall *n = (FuncCall *) $1;

				if ($2 != NIL)
				{
					if (IsA(n, FuncCall))
					{
						if (n->agg_order != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP"),
									 parser_errposition(@2)));
						if (n->agg_distinct)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use DISTINCT with WITHIN GROUP"),
									 parser_errposition(@2)));
						if (n->func_variadic)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use VARIADIC with WITHIN GROUP"),
									 parser_errposition(@2)));
						n->agg_order = $2;
						/* oracle "within group (...)" same to postgres func(arg... order by ...) */
						n->agg_within_group = false;

					}else
					{
						/* not FuncCall, not support "within group ..." and "over ..." */
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("syntax error"),
								 parser_errposition($2 != NIL ? @2:($3 != NULL ? @3:($4 ? @4:-1)))));
					}
				}
				if (IsA(n, FuncCall))
				{
					if (strcmp(strVal(llast(n->funcname)), "listagg") == 0 &&
						(list_length(n->funcname) == 1 ||
						 (list_length(n->funcname) == 2 &&
						  strcmp(strVal(linitial(n->funcname)), "oracle") == 0)))
					{
						/* convert listagg(arg1 [,arg2]) to string_agg(arg1, arg2|null) */
						n->funcname = SystemFuncName("string_agg");
						if (list_length(n->args) < 2)
						{
							n->args = lappend(n->args, makeNullAConst(-1));
						}
					}
					n->agg_keep = $3;
					n->over = $4;
				}
				$$ = $1;
			}
		| func_expr_common_subexpr
			{
				$$ = $1;
			}
		;

keep_clause:
		  KEEP '(' DENSE_RANK FIRST_P ORDER BY sortby_list ')'
			{
				KeepClause *n = makeNode(KeepClause);
				n->rank_first = true;
				n->keep_order = $7;
				n->location = @1;

				$$ = n;
			}
		| KEEP '(' DENSE_RANK LAST_P ORDER BY sortby_list ')'
			{
				KeepClause *n = makeNode(KeepClause);
				n->rank_first = false;
				n->keep_order = $7;
				n->location = @1;

				$$ = n;
			}
		| /* EMPTY */
			{
				$$ = NULL;
			}

over_clause: OVER window_specification
				{ $$ = $2; }
			|OVER ColId
				{
					WindowDef *n = makeNode(WindowDef);
					n->name = $2;
					n->refname = NULL;
					n->partitionClause = NIL;
					n->orderClause = NIL;
					n->frameOptions = FRAMEOPTION_DEFAULTS;
					n->startOffset = NULL;
					n->endOffset = NULL;
					n->location = @2;
					$$ = n;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

window_specification: '(' opt_existing_window_name opt_partition_clause
						opt_sort_clause opt_frame_clause ')'
				{
					WindowDef *n = makeNode(WindowDef);
					n->name = NULL;
					n->refname = $2;
					n->partitionClause = $3;
					n->orderClause = $4;
					/* copy relevant fields of opt_frame_clause */
					n->frameOptions = $5->frameOptions;
					n->startOffset = $5->startOffset;
					n->endOffset = $5->endOffset;
					n->location = @1;
					$$ = n;
				}
		;

/*
 * If we see PARTITION, RANGE, or ROWS as the first token after the '('
 * of a window_specification, we want the assumption to be that there is
 * no existing_window_name; but those keywords are unreserved and so could
 * be ColIds.  We fix this by making them have the same precedence as IDENT
 * and giving the empty production here a slightly higher precedence, so
 * that the shift/reduce conflict is resolved in favor of reducing the rule.
 * These keywords are thus precluded from being an existing_window_name but
 * are not reserved for any other purpose.
 */
opt_existing_window_name: ColId						{ $$ = $1; }
			| /*EMPTY*/				%prec Op		{ $$ = NULL; }
		;

opt_partition_clause: PARTITION BY expr_list		{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*
 * For frame clauses, we return a WindowDef, but only some fields are used:
 * frameOptions, startOffset, and endOffset.
 *
 * This is only a subset of the full SQL:2008 frame_clause grammar.
 * We don't support <window frame exclusion> yet.
 */
opt_frame_clause:
			RANGE frame_extent
				{
					WindowDef *n = $2;
					n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_RANGE;
					if (n->frameOptions & (FRAMEOPTION_START_OFFSET_PRECEDING |
										   FRAMEOPTION_END_OFFSET_PRECEDING))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("RANGE PRECEDING is only supported with UNBOUNDED"),
								 parser_errposition(@1)));
					if (n->frameOptions & (FRAMEOPTION_START_OFFSET_FOLLOWING |
										   FRAMEOPTION_END_OFFSET_FOLLOWING))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("RANGE FOLLOWING is only supported with UNBOUNDED"),
								 parser_errposition(@1)));
					$$ = n;
				}
			| ROWS frame_extent
				{
					WindowDef *n = $2;
					n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS;
					$$ = n;
				}
			| /*EMPTY*/
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_DEFAULTS;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
		;

frame_extent: frame_bound
				{
					WindowDef *n = $1;
					/* reject invalid cases */
					if (n->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
								 parser_errposition(@1)));
					if (n->frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from following row cannot end with current row"),
								 parser_errposition(@1)));
					n->frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
					$$ = n;
				}
			| BETWEEN frame_bound AND frame_bound
				{
					WindowDef *n1 = $2;
					WindowDef *n2 = $4;
					/* form merged options */
					int		frameOptions = n1->frameOptions;
					/* shift converts START_ options to END_ options */
					frameOptions |= n2->frameOptions << 1;
					frameOptions |= FRAMEOPTION_BETWEEN;
					/* reject invalid cases */
					if (frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
								 parser_errposition(@2)));
					if (frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame end cannot be UNBOUNDED PRECEDING"),
								 parser_errposition(@4)));
					if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) &&
						(frameOptions & FRAMEOPTION_END_OFFSET_PRECEDING))
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from current row cannot have preceding rows"),
								 parser_errposition(@4)));
					if ((frameOptions & FRAMEOPTION_START_OFFSET_FOLLOWING) &&
						(frameOptions & (FRAMEOPTION_END_OFFSET_PRECEDING |
										 FRAMEOPTION_END_CURRENT_ROW)))
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from following row cannot have preceding rows"),
								 parser_errposition(@4)));
					n1->frameOptions = frameOptions;
					n1->endOffset = n2->startOffset;
					$$ = n1;
				}
		;

/*
 * This is used for both frame start and frame end, with output set up on
 * the assumption it's frame start; the frame_extent productions must reject
 * invalid cases.
 */
frame_bound:
			UNBOUNDED PRECEDING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_UNBOUNDED_PRECEDING;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| UNBOUNDED FOLLOWING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_UNBOUNDED_FOLLOWING;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| CURRENT_P ROW
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_CURRENT_ROW;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| a_expr PRECEDING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_OFFSET_FOLLOWING;
					n->startOffset = $1;
					n->endOffset = NULL;
					$$ = n;
				}
			| a_expr FOLLOWING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_OFFSET_FOLLOWING;
					n->startOffset = $1;
					n->endOffset = NULL;
					$$ = n;
				}
		;

func_application: func_application_normal
			{
				FuncCall *n = (FuncCall*)$1;
				if (IsA(n, FuncCall) &&
					list_length(n->funcname) == 1 &&
					list_length(n->args) == 1 &&
					strcmp(strVal(linitial(n->funcname)), "wm_concat") == 0)
				{
					/* wm_concat(?) -> string_agg(?, ',') */
					n->funcname = SystemFuncName(pstrdup("string_agg"));
					n->args = lappend(n->args, makeStringConst(pstrdup(","), -1));
				}
				$$ = (Node*)n;
			}
		| TABLE '(' a_expr ')'
			{
				if (IsA($3, FuncCall))
				{
					$$ = $3;
				}else
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("unnest");
					n->args = list_make1($3);
					n->location = @1;
					$$ = (Node*)n;
				}
			}
		;

func_application_normal:
		 func_name '(' ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = NIL;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' func_arg_list ')'
			{
				char *nspname = NULL;
				char *objname = NULL;
				$$ = NULL;

				DeconstructQualifiedName($1, &nspname, &objname);
				if (strcasecmp(objname, "decode") == 0 &&
					(nspname == NULL ||
					 strcmp(nspname, "oracle") == 0))
				{
					if (list_length($3) < 3)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("Not engouh parameters for \"decode\" function"),
								parser_errposition(@4)));

					$$ = reparse_decode_func($3, @1);
				}
				if ($$ == NULL)
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = false;
					n->agg_distinct = false;
					n->func_variadic = false;
					n->location = @1;
					$$ = (Node *)n;
				}
			}
		| func_name '(' func_arg_list sort_clause ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = $3;
				n->agg_order = $4;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				/* n->over = $6; */
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' ALL func_arg_list opt_sort_clause ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = $4;
				n->agg_order = $5;
				n->agg_star = false;
				n->agg_distinct = false;
				/* Ideally we'd mark the FuncCall node to indicate
				 * "must be an aggregate", but there's no provision
				 * for that in FuncCall at the moment.
				 */
				n->func_variadic = false;
				/* n->over = $7; */
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' DISTINCT func_arg_list opt_sort_clause ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = $4;
				n->agg_order = $5;
				n->agg_star = false;
				n->agg_distinct = true;
				n->func_variadic = false;
				/* n->over = $7; */
				n->location = @1;
				$$ = (Node *)n;
			}
		| func_name '(' '*' ')'
			{
				/*
				 * We consider AGGREGATE(*) to invoke a parameterless
				 * aggregate.  This does the right thing for COUNT(*),
				 * and there are no other aggregates in SQL that accept
				 * '*' as parameter.
				 *
				 * The FuncCall node is also marked agg_star = true,
				 * so that later processing can detect what the argument
				 * really was.
				 */
				FuncCall *n = makeNode(FuncCall);
				n->funcname = $1;
				n->args = NIL;
				n->agg_order = NIL;
				n->agg_star = true;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->location = @1;
				$$ = (Node *)n;
			}
		;
/*
 * As func_expr but does not accept WINDOW functions directly
 * (but they can still be contained in arguments for functions etc).
 * Use this when window expressions are not allowed, where needed to
 * disambiguate the grammar (e.g. in CREATE INDEX).
 */
func_expr_windowless:
			func_application						{ $$ = $1; }
			| func_expr_common_subexpr				{ $$ = $1; }
		;

func_expr_common_subexpr:
		  CAST '(' a_expr AS Typename ')'
			{ $$ = makeTypeCast($3, $5, @1); }
		| SYSDATE
			{
				/*
				 * Translate as "ora_sys_now()::timestamp(0)".
				 */
				TypeName *tn;
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_sys_now");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = false;
				fc->agg_distinct = false;
				fc->func_variadic = false;
				fc->over = NULL;
				fc->location = -1;

				tn = OracleTypeName("date");
				$$ = makeTypeCast((Node *)fc, tn, @1);
			}
		| SYSTIMESTAMP
			{
				/*
				 * Translate as "ora_sys_now()::timestamp".
				 */
				TypeName *tn;
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_sys_now");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = false;
				fc->agg_distinct = false;
				fc->func_variadic = false;
				fc->over = NULL;
				fc->location = -1;

				tn = SystemTypeName("timestamp");
				$$ = makeTypeCast((Node *)fc, tn, @1);
			}
		| CURRENT_DATE
			{
				/*
				 * Translate as "'now'::text::timestamp(0)".
				 */
				Node *n;
				TypeName *tn;

				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				tn = OracleTypeName("date");
				$$ = makeTypeCast(n, tn, @1);
			}
		| CURRENT_TIMESTAMP
			{
				/*
				 * Translate as "now()", since we have a function that
				 * does exactly what is needed.
				 */
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("now");
				n->args = NIL;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| CURRENT_TIMESTAMP '(' Iconst ')'
			{
				/*
				 * Translate as "'now'::text::timestamptz(n)".
				 * See comments for CURRENT_DATE.
				 */
				Node *n;
				TypeName *d;
				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				d = SystemTypeName("timestamptz");
				d->typmods = list_make1(makeIntConst($3, @3));
				$$ = makeTypeCast(n, d, @1);
			}
		| LOCALTIMESTAMP
			{
				/*
				 * Translate as "'now'::text::timestamp".
				 * See comments for CURRENT_DATE.
				 */
				Node *n;
				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				$$ = makeTypeCast(n, SystemTypeName("timestamp"), @1);
			}
		| LOCALTIMESTAMP '(' Iconst ')'
			{
				/*
				 * Translate as "'now'::text::timestamp(n)".
				 * See comments for CURRENT_DATE.
				 */
				Node *n;
				TypeName *d;
				n = makeStringConstCast("now", -1, SystemTypeName("text"));
				d = SystemTypeName("timestamp");
				d->typmods = list_make1(makeIntConst($3, @3));
				$$ = makeTypeCast(n, d, @1);
			}
		| COALESCE '(' expr_list ')'
			{
				CoalesceExpr *c = makeNode(CoalesceExpr);
				c->args = $3;
				c->location = @1;
				$$ = (Node *)c;
			}
		| NULLIF '(' a_expr ',' a_expr ')'
			{
				$$ = (Node *) makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1);
			}
		| GREATEST '(' expr_list ')'
			{
				MinMaxExpr *v = makeNode(MinMaxExpr);
				v->args = $3;
				v->op = IS_GREATEST;
				v->location = @1;
				$$ = (Node *)v;
			}
		| LEAST '(' expr_list ')'
			{
				MinMaxExpr *v = makeNode(MinMaxExpr);
				v->args = $3;
				v->op = IS_LEAST;
				v->location = @1;
				$$ = (Node *)v;
			}
		| TREAT '(' a_expr AS Typename ')'
			{
				/* TREAT(expr AS target) converts expr of a particular type to target,
				 * which is defined to be a subtype of the original expression.
				 * In SQL99, this is intended for use with structured UDTs,
				 * but let's make this a generally useful form allowing stronger
				 * coercions than are handled by implicit casting.
				 */
				FuncCall *n = makeNode(FuncCall);
				/* Convert SystemTypeName() to SystemFuncName() even though
				 * at the moment they result in the same thing.
				 */
				n->funcname = SystemFuncName(((Value *)llast($5->names))->val.str);
				n->args = list_make1($3);
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' BOTH trim_list ')'
			{
				/* various trim expressions are defined in SQL
				 * - thomas 1997-07-19
				 */
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("btrim");
				n->args = $4;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' LEADING trim_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("ltrim");
				n->args = $4;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' TRAILING trim_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("rtrim");
				n->args = $4;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| TRIM '(' trim_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("btrim");
				n->args = $3;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| DBTIMEZONE_P
			{
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_dbtimezone");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = false;
				fc->agg_distinct = false;
				fc->func_variadic = false;
				fc->over = NULL;
				fc->location = -1;

				$$ = (Node *)fc;
			}
		| SESSIONTIMEZONE
			{
				FuncCall *fc;

				fc = makeNode(FuncCall);
				fc->funcname = OracleFuncName("ora_session_timezone");
				fc->args = NIL;
				fc->agg_order = NIL;
				fc->agg_star = false;
				fc->agg_distinct = false;
				fc->func_variadic = false;
				fc->over = NULL;
				fc->location = -1;

				$$ = (Node *)fc;
			}
		| EXTRACT '(' extract_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("date_part");
				n->args = $3;
				n->agg_order = NIL;
				n->agg_star = false;
				n->agg_distinct = false;
				n->func_variadic = false;
				n->over = NULL;
				n->location = @1;
				$$ = (Node *)n;
			}
		| ROWNUM
			{
				RownumExpr *n = makeNode(RownumExpr);
				n->location = @1;
				$$ = (Node*)n;
			}
		| NEXTVAL '(' func_arg_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("nextval");
				n->args = $3;
				n->location = @1;
				$$ = (Node *)n;
			}
		| CURRVAL '(' func_arg_list ')'
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("currval");
				n->args = $3;
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId '.' NEXTVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("nextval");
				n->args = list_make1(makeStringConst($1, @1));
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId indirection '.' NEXTVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("nextval");
				n->args = check_sequence_name(lcons(makeString($1), $2), yyscanner, @1);
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId '.' CURRVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("currval");
				n->args = list_make1(makeStringConst($1, @1));
				n->location = @1;
				$$ = (Node *)n;
			}
		| ColId  indirection '.' CURRVAL
			{
				FuncCall *n = makeNode(FuncCall);
				n->funcname = SystemFuncName("currval");
				n->args = check_sequence_name(lcons(makeString($1), $2), yyscanner, @1);
				n->location = @1;
				$$ = (Node *)n;
			}
		;

/*
 * The production for a qualified func_name has to exactly match the
 * production for a qualified columnref, because we cannot tell which we
 * are parsing until we see what comes after it ('(' or Sconst for a func_name,
 * anything else for a columnref).  Therefore we allow 'indirection' which
 * may contain subscripts, and reject that case in the C code.  (If we
 * ever implement SQL99-like methods, such syntax may actually become legal!)
 */
func_name:	type_function_name
				{ $$ = list_make1(makeString($1)); }
			| ColId indirection
				{
					$$ = check_func_name(lcons(makeString($1), $2),
										 yyscanner);
				}
			| PUBLIC indirection
				{
					$$ = check_func_name(lcons(makeString(pstrdup($1)), $2),
										 yyscanner);
				}
		;

/*
 * Aggregate decoration clauses
 */
within_group_clause:
			WITHIN GROUP_P '(' sort_clause ')'		{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

group_clause:
		GROUP_P BY group_by_list				{ $$ = $3; }
		| /*EMPTY*/								{ $$ = NIL; }
	;
group_by_list:
			group_by_item							{ $$ = list_make1($1); }
			| group_by_list ',' group_by_item		{ $$ = lappend($1,$3); }
		;
group_by_item:
			a_expr								{ $$ = $1; }
			| empty_grouping_set				{ $$ = $1; }
			| cube_clause						{ $$ = $1; }
			| rollup_clause						{ $$ = $1; }
			| grouping_sets_clause				{ $$ = $1; }
		;

empty_grouping_set:
			'(' ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_EMPTY, NIL, @1);
				}
		;

/*
 * These hacks rely on setting precedence of CUBE and ROLLUP below that of '(',
 * so that they shift in these rules rather than reducing the conflicting
 * unreserved_keyword rule.
 */
rollup_clause:
			ROLLUP '(' expr_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_ROLLUP, $3, @1);
				}
		;
cube_clause:
			CUBE '(' expr_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_CUBE, $3, @1);
				}
		;

grouping_sets_clause:
			GROUPING SETS '(' group_by_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_SETS, $4, @1);
				}
		;

having_clause:
		HAVING a_expr							{ $$ = $2; }
		| /*EMPTY*/								{ $$ = NULL; }
	;

Iconst: ICONST		{ $$ = $1; };

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
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' a_expr ':' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

InsertStmt:
	opt_with_clause INSERT INTO qualified_name insert_rest returning_clause
		{
			$5->relation = $4;
			$5->returningList = $6;
			$5->withClause = $1;
			$$ = (Node *) $5;
		}
	| opt_with_clause INSERT INTO qualified_name opt_as ColId insert_rest_no_cols returning_clause
		{
			$4->alias = makeNode(Alias);
			$4->alias->aliasname = $6;
			$4->alias->colnames = NIL;
			$7->relation = $4;
			$7->returningList = $8;
			$7->withClause = $1;
			$$ = (Node *) $7;
		}
	| opt_with_clause INSERT INTO qualified_name opt_as ColId '(' insert_column_list ')' insert_rest_no_cols returning_clause
		{
			$4->alias = makeNode(Alias);
			$4->alias->aliasname = $6;
			$10->relation = $4;
			$10->cols = $8;
			$10->returningList = $11;
			$10->withClause = $1;
			$$ = (Node *) $10;
		}
	| opt_with_clause INSERT INTO qualified_name opt_as ColId '(' insert_column_list ')' '(' insert_column_list ')' SelectStmt returning_clause
		{
			/*
			 * $8 can not use "name_list", it have conflict.
			 * we need let insert_column_list to name_list
			 */
			InsertStmt *insert;
			ListCell *lc;
			ResTarget *res;
			List *alias_col = NIL;
			foreach(lc, $8)
			{
				res = lfirst(lc);
				if(res->indirection)
				{
					ereport(ERROR,
						(ERRCODE_SYNTAX_ERROR,
						errmsg("%s at or near \"%s\"", _("syntax error"), res->name),
						parser_errposition(res->location)));
				}
				alias_col = lappend(alias_col, makeString(res->name));
			}
			list_free_deep($8);
			insert = makeNode(InsertStmt);
			insert->cols = $11;
			insert->selectStmt = $13;
			$4->alias = makeNode(Alias);
			$4->alias->aliasname = $6;
			$4->alias->colnames = alias_col;
			insert->relation = $4;
			insert->returningList = $14;
			insert->withClause = $1;
			$$ = (Node *)insert;
		}
	;

opt_as:	AS									{}
		| /*EMPTY*/							{}
		;

insert_rest_no_cols:
		SelectStmt
			{
				$$ = makeNode(InsertStmt);
				$$->cols = NIL;
				$$->selectStmt = $1;
			}
		| DEFAULT VALUES
			{
				$$ = makeNode(InsertStmt);
				$$->cols = NIL;
				$$->selectStmt = NULL;
			}
	;

insert_rest:
		 '(' insert_column_list ')' SelectStmt
			{
				$$ = makeNode(InsertStmt);
				$$->cols = $2;
				$$->selectStmt = $4;
			}
		| insert_rest_no_cols
			{
				$$ = $1;
			}
		;

insert_column_list:
		insert_column_item
				{ $$ = list_make1($1); }
		| insert_column_list ',' insert_column_item
				{ $$ = lappend($1, $3); }
	;

insert_column_item:
		ColId opt_indirection
			{
				$$ = makeNode(ResTarget);
				$$->name = $1;
				$$->indirection = check_indirection($2, yyscanner);
				$$->val = NULL;
				$$->location = @1;
				$$->as_location = -1;
			}
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

iso_level:	READ UNCOMMITTED						{ $$ = "read uncommitted"; }
			| READ COMMITTED						{ $$ = "read committed"; }
			| REPEATABLE READ						{ $$ = "repeatable read"; }
			| SERIALIZABLE							{ $$ = "serializable"; }
		;

joined_table: '(' joined_table ')'			{ $$ = $2; }
			| table_ref CROSS JOIN table_ref
				{
					/* CROSS JOIN is same as unqualified
					join */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL;
					n->quals = NULL;
					$$ = n;
				}
			/* | table_ref join_type JOIN table_ref ON a_expr
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $4;
					n->quals = $6;
					$$ = n;
				} */
			| table_ref join_type JOIN table_ref join_qual
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $4;
					if ($5 != NULL && IsA($5, List))
						n->usingClause = (List *) $5; /* USING clause */
					else
						n->quals = $5; /* ON clause */
					$$ = n;
				}
			| table_ref JOIN table_ref ON a_expr
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = false;
					n->larg = $1;
					n->rarg = $3;
					n->quals = $5; /* ON clause */
					$$ = n;
				}
		;

join_qual:	USING '(' name_list ')'					{ $$ = (Node *) $3; }
			| ON a_expr								{ $$ = $2; }
		;

/* OUTER is just noise... */
join_outer: OUTER_P									{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

join_type:	FULL join_outer							{ $$ = JOIN_FULL; }
			| LEFT join_outer						{ $$ = JOIN_LEFT; }
			| RIGHT join_outer						{ $$ = JOIN_RIGHT; }
			| INNER_P								{ $$ = JOIN_INNER; }
		;

implicit_row:	'(' expr_list ',' a_expr ')'		{ $$ = lappend($2, $4); }
		;

MathOp: '+'									{ $$ = "+"; }
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

multiple_set_clause:
			'(' set_target_list ')' '=' set_expr_row
				{
					ListCell *col_cell;
					ListCell *val_cell;

					/*
					 * Break the set_expr_row apart, merge individual expressions
					 * into the destination ResTargets.  XXX this approach
					 * cannot work for general row expressions as sources.
					 */
					if (list_length($2) != list_length($5))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("number of columns does not match number of values"),
								 parser_errposition(@1)));
					forboth(col_cell, $2, val_cell, $5)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						Node *res_val = (Node *) lfirst(val_cell);

						res_col->val = res_val;
					}

					$$ = $2;
				}
			| '(' set_target_list ')' '=' select_with_parens
				{
					int ncolumns = list_length($2);
					int i = 1;
					ListCell *col_cell;

					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->subLinkId = 0;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $5;
					n->location = @5;

					/* Create a MultiAssignRef source for each target */
					foreach(col_cell, $2)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						MultiAssignRef *r = makeNode(MultiAssignRef);

						r->source = (Node *) n;
						r->colno = i;
						r->ncolumns = ncolumns;
						res_col->val = (Node *) r;
						i++;
					}

					$$ = $2;
				}
		;


name: ColId
	;

name_list: name				{ $$ = list_make1(makeString($1)); }
	| name_list ',' name	{ $$ = lappend($1, makeString($3)); }
	;

/* Any not-fully-reserved word --- these names can be, eg, role names.
 */
NonReservedWord:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1; }
			| Sconst								{ $$ = $1; }
		;

Numeric:
	  INT_P						{ $$ = SystemTypeNameLocation("int4", @1); }
	| INTEGER					{ $$ = SystemTypeNameLocation("int4", @1); }
	| SMALLINT					{ $$ = SystemTypeNameLocation("int2", @1); }
	| BIGINT					{ $$ = SystemTypeNameLocation("int8", @1); }
	| LONG_P					{ $$ = SystemTypeNameLocation("text", @1); }
	| BINARY_FLOAT				{ $$ = SystemTypeNameLocation("float4", @1); }
	| REAL						{ $$ = SystemTypeNameLocation("float4", @1); }
	| BINARY_DOUBLE				{ $$ = SystemTypeNameLocation("float8", @1); }
	| FLOAT_P					{ $$ = SystemTypeNameLocation("float8", @1); }
	| FLOAT_P '(' Iconst ')'
		{
			/*
			 * Check FLOAT() precision limits assuming IEEE floating
			 * types - thomas 1997-09-18
			 */
			if ($3 < 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("precision for type float must be at least 1 bit"),
						 parser_errposition(@3)));
			else if ($3 <= 24)
				$$ = SystemTypeNameLocation("float4", @1);
			else if ($3 <= 53)
				$$ = SystemTypeNameLocation("float8", @2);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("precision for type float must be less than 54 bits"),
						 parser_errposition(@3)));
		}
	| DOUBLE_P PRECISION		{ $$ = SystemTypeNameLocation("float8", @1); }
	| DECIMAL_P opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| DEC opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| NUMERIC opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| NUMBER_P opt_type_modifiers
		{
			$$ = SystemTypeNameLocation("numeric", @1);
			$$->typmods = $2;
		}
	| BOOLEAN_P					{ $$ = SystemTypeNameLocation("bool", @1); }
	;

NumericOnly:
			FCONST								{ $$ = makeFloat($1); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;

OptTableElementList:
	  TableElementList	{ $$ = $1; }
	| /* empty */		{ $$ = NIL; }
	;

opt_alias_clause:
		alias_clause  %prec HI_THEN_RETURN
		| /* empty */ %prec RETURN_P		{ $$ = NULL; }
		;

/*
 * func_alias_clause can include both an Alias and a coldeflist, so we make it
 * return a 2-element list that gets disassembled by calling production.
 */
func_alias_clause:
			alias_clause
				{
					$$ = list_make2($1, NIL);
				}
			| AS '(' TableFuncElementList ')'
				{
					$$ = list_make2(NULL, $3);
				}
			| AS ColId '(' TableFuncElementList ')'
				{
					Alias *a = makeNode(Alias);
					a->aliasname = $2;
					$$ = list_make2(a, $4);
				}
			| ColId '(' TableFuncElementList ')'
				{
					Alias *a = makeNode(Alias);
					a->aliasname = $1;
					$$ = list_make2(a, $3);
				}
			| /*EMPTY*/ %prec RETURN_P
				{
					$$ = list_make2(NULL, NIL);
				}
		;

TableFuncElementList:
			TableFuncElement
				{
					$$ = list_make1($1);
				}
			| TableFuncElementList ',' TableFuncElement
				{
					$$ = lappend($1, $3);
				}
		;

TableFuncElement:	ColId Typename opt_collate_clause
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typeName = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->is_from_parent = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collClause = (CollateClause *) $3;
					n->collOid = InvalidOid;
					n->constraints = NIL;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

opt_all:	ALL										{ $$ = true; }
			| DISTINCT								{ $$ = false; }
			| /*EMPTY*/								{ $$ = false; }
		;

opt_asc_desc:
	  ASC				{ $$ = SORTBY_ASC; }
	| DESC				{ $$ = SORTBY_DESC; }
	| /* empty */		{ $$ = SORTBY_DEFAULT; }
	;

opt_boolean_or_string:
			TRUE_P									{ $$ = "true"; }
			| FALSE_P								{ $$ = "false"; }
			| ON									{ $$ = "on"; }
			/*
			 * OFF is also accepted as a boolean value, but is handled by
			 * the NonReservedWord rule.  The action for booleans and strings
			 * is the same, so we don't need to distinguish them here.
			 */
			| NonReservedWord_or_Sconst				{ $$ = $1; }
		;

opt_byte_char:
	  BYTE_P				{ $$ = true; }
	| CHAR_P				{ $$ = true; }
	| /* empty */			{ $$ = false; }
	;

/* We use (NIL) as a placeholder to indicate that all target expressions
 * should be placed in the DISTINCT list during parsetree analysis.
 */
opt_distinct:
			DISTINCT								{ $$ = list_make1(NIL); }
			| ALL									{ $$ = NIL; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| CASCADE CONSTRAINTS		{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| /* EMPTY */				{ $$ = DROP_RESTRICT; /* default */ }
		;

opt_encoding:
			Sconst									{ $$ = $1; }
			| DEFAULT								{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_from:
	  FROM
	| /* empty */
	;

opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
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

opt_name_list:
	  name_list
	| /* empty */ { $$ = NULL; }
	;

opt_restart_seqs:
			CONTINUE_P IDENTITY_P		{ $$ = false; }
			| RESTART IDENTITY_P		{ $$ = true; }
			| /* EMPTY */				{ $$ = false; }
		;

opt_type_mod:
	  '(' Iconst ')'		{ $$ = list_make1(makeIntConst($2, @2)); }
	| /* empty */			{ $$ = NIL; }
	;

opt_type_modifiers:'(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
		;

opt_verbose:
			VERBOSE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

opt_with_clause:
	  with_clause
	| /* empty */ { $$ = NULL; }
	;

qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

qualified_name:
		  ColId { $$ = makeRangeVar(NULL, $1, @1); }
		| ColId indirection
			{
				RangeVar *n;
				check_qualified_name($2, yyscanner);
				n = makeRangeVar(NULL, NULL, @1);
				switch (list_length($2))
				{
					case 1:
						n->catalogname = NULL;
						n->schemaname = $1;
						n->relname = strVal(linitial($2));
						break;
					case 2:
						n->catalogname = $1;
						n->schemaname = strVal(linitial($2));
						n->relname = strVal(lsecond($2));
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("improper qualified name (too many dotted names): %s",
										NameListToString(lcons(makeString($1), $2))),
								 parser_errposition(@1)));
						break;
				}
				$$ = n;
			}
		| PUBLIC indirection
			{
				RangeVar *n;
				check_qualified_name($2, yyscanner);
				n = makeRangeVar(NULL, NULL, @1);
				switch (list_length($2))
				{
					case 1:
						n->catalogname = NULL;
						n->schemaname = pstrdup($1);
						n->relname = strVal(linitial($2));
						break;
					case 2:
						n->catalogname = pstrdup($1);
						n->schemaname = strVal(linitial($2));
						n->relname = strVal(lsecond($2));
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("improper qualified name (too many dotted names): %s",
										NameListToString(lcons(makeString(pstrdup($1)), $2))),
								 parser_errposition(@1)));
						break;
				}
				$$ = n;
			}
		;

qual_Op: Op { $$ = list_make1(makeString($1)); }
	| OPERATOR '(' any_operator ')' { $$ = $3; }
	;

relation_expr:
		qualified_name
		{
				/* default inheritance */
				$$ = $1;
				$$->inh = true;
				$$->alias = NULL;
		}
	|	qualified_name '*'
		{
			/* inheritance query */
			$$ = $1;
			$$->inh = true;
			$$->alias = NULL;
		}
	|	ONLY qualified_name
		{
			/* no inheritance */
			$$ = $2;
			$$->inh = false;
			$$->alias = NULL;
			}
	|	ONLY '(' qualified_name ')'
		{
			/* no inheritance, SQL99-style syntax */
			$$ = $3;
			$$->inh = false;
			$$->alias = NULL;
		}
		;

relation_expr_list:
			relation_expr							{ $$ = list_make1($1); }
			| relation_expr_list ',' relation_expr	{ $$ = lappend($1, $3); }
		;

/*
 * Given "UPDATE foo set set ...", we have to decide without looking any
 * further ahead whether the first "set" is an alias or the UPDATE's SET
 * keyword.  Since "set" is allowed as a column name both interpretations
 * are feasible.  We resolve the shift/reduce conflict by giving the first
 * relation_expr_opt_alias production a higher precedence than the SET token
 * has, causing the parser to prefer to reduce, in effect assuming that the
 * SET is not an alias.
 */
relation_expr_opt_alias: relation_expr					%prec UMINUS
		{
			$$ = $1;
		}
	| relation_expr ColId
		{
			Alias *alias = makeNode(Alias);
			alias->aliasname = $2;
			$1->alias = alias;
			$$ = $1;
		}
	| relation_expr AS ColId
		{
			Alias *alias = makeNode(Alias);
			alias->aliasname = $3;
			$1->alias = alias;
			$$ = $1;
		}
	;

returning_clause:
	  RETURN_P returning_item	{ $$ = $2; }
	| RETURNING returning_item	{ $$ = $2; }
	| /* EMPTY */				{ $$ = NIL; }
	;

returning_item: target_list
	;

Sconst: SCONST		{ $$ = $1; };

select_clause:
		simple_select
		| select_with_parens		%prec LO_THEN_LIMIT
	;

SelectStmt: select_no_parens		%prec UMINUS
		| select_with_parens		%prec UMINUS
		;

select_with_parens:
		'(' select_no_parens ')'		{ $$ = $2; }
		| '(' select_with_parens ')'	{ $$ = $2; }
		;

select_no_parens:
		simple_select					{ $$ = $1; }
		| select_clause select_sort_clause
			{
				oracleInsertSelectOptions((SelectStmt *) $1, $2, NIL,
										  NULL, NULL, NULL,
										  yyscanner);
				$$ = $1;
			}
		| select_clause opt_select_sort_clause for_locking_clause opt_select_limit
			{
				oracleInsertSelectOptions((SelectStmt *) $1, $2, $3,
										  list_nth($4, 0), list_nth($4, 1),
										  NULL,
										  yyscanner);
				$$ = $1;
			}
		| select_clause opt_select_sort_clause select_limit opt_for_locking_clause
			{
				oracleInsertSelectOptions((SelectStmt *) $1, $2, $4,
										  list_nth($3, 0), list_nth($3, 1),
										  NULL,
										  yyscanner);
				$$ = $1;
			}
		| with_clause select_clause
			{
				insertSelectOptions((SelectStmt *) $2, NULL, NIL,
									NULL, NULL,
									$1,
									yyscanner);
				$$ = $2;
			}
		| with_clause select_clause select_sort_clause
			{
				oracleInsertSelectOptions((SelectStmt *) $2, $3, NIL,
										  NULL, NULL,
										  $1,
										  yyscanner);
				$$ = $2;
			}
		| with_clause select_clause opt_select_sort_clause for_locking_clause opt_select_limit
			{
				oracleInsertSelectOptions((SelectStmt *) $2, $3, $4,
										  list_nth($5, 0), list_nth($5, 1),
										  $1,
										  yyscanner);
				$$ = $2;
			}
		| with_clause select_clause opt_select_sort_clause select_limit opt_for_locking_clause
			{
				oracleInsertSelectOptions((SelectStmt *) $2, $3, $5,
										  list_nth($4, 0), list_nth($4, 1),
										  $1,
										  yyscanner);
				$$ = $2;
			}
		;

set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause:
			single_set_clause						{ $$ = list_make1($1); }
			| multiple_set_clause					{ $$ = $1; }
		;

for_locking_clause:
			for_locking_items						{ $$ = $1; }
			| FOR READ ONLY							{ $$ = NIL; }
		;

opt_for_locking_clause:
			for_locking_clause						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

for_locking_items:
			for_locking_item						{ $$ = list_make1($1); }
			| for_locking_items for_locking_item	{ $$ = lappend($1, $2); }
		;

for_locking_item:
			for_locking_strength locked_rels_list opt_nowait_or_skip
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $2;
					n->strength = $1;
					n->waitPolicy = $3;
					$$ = (Node *) n;
				}
		;

for_locking_strength:
			FOR UPDATE 							{ $$ = LCS_FORUPDATE; }
			| FOR NO KEY UPDATE 				{ $$ = LCS_FORNOKEYUPDATE; }
			| FOR SHARE 						{ $$ = LCS_FORSHARE; }
			| FOR KEY SHARE 					{ $$ = LCS_FORKEYSHARE; }
		;

locked_rels_list:
			OF qualified_name_list					{ $$ = $2; }
			| /* EMPTY */							{ $$ = NIL; }
		;

opt_nowait_or_skip:
			NOWAIT							{ $$ = LockWaitError; }
			/*| SKIP LOCKED					{ $$ = LockWaitSkip; }*/
			| /*EMPTY*/						{ $$ = LockWaitBlock; }
		;

opt_nowait:	NOWAIT							{ $$ = true; }
			| /*EMPTY*/						{ $$ = false; }
		;

select_limit:
			limit_clause offset_clause			{ $$ = list_make2($2, $1); }
			| offset_clause limit_clause		{ $$ = list_make2($1, $2); }
			| limit_clause						{ $$ = list_make2(NULL, $1); }
			| offset_clause						{ $$ = list_make2($1, NULL); }
		;

opt_select_limit:
			select_limit						{ $$ = $1; }
			| /* EMPTY */						{ $$ = list_make2(NULL,NULL); }
		;

limit_clause:
			LIMIT select_limit_value
				{ $$ = $2; }
			| LIMIT_LA select_limit_value
				{ $$ = $2; }
			| LIMIT select_limit_value ',' select_offset_value
				{
					/* Disabled because it was too confusing, bjm 2002-02-18 */
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("LIMIT #,# syntax is not supported"),
							 errhint("Use separate LIMIT and OFFSET clauses."),
							 parser_errposition(@1)));
				}
			/* SQL:2008 syntax */
			/*| FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
				{ $$ = $3; }*/
		;

offset_clause:
			OFFSET select_offset_value
				{ $$ = $2; }
			/* SQL:2008 syntax */
			/*| OFFSET select_offset_value2 row_or_rows
				{ $$ = $2; }*/
			| OFFSET c_expr ROW		{ $$ = $2; }
			| OFFSET c_expr ROWS	{ $$ = $2; }
		;

select_limit_value:
			a_expr									{ $$ = $1; }
			| ALL
				{
					/* LIMIT ALL is represented as a NULL constant */
					$$ = makeNullAConst(@1);
				}
		;

select_offset_value:
			a_expr									{ $$ = $1; }
		;

set_expr:
	  a_expr
	| DEFAULT
		{
			SetToDefault *n = makeNode(SetToDefault);
			n->location = @1;
			$$ = (Node *) n;
		}
	;

set_expr_list: set_expr					{ $$ = list_make1($1); }
	| set_expr_list ',' set_expr		{ $$ = lappend($1, $3); }
	;

set_expr_row: '(' set_expr_list ')'		{ $$ = $2; }
	;

set_rest:
			TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION";
					n->args = $2;
					$$ = n;
				}
			| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "SESSION CHARACTERISTICS";
					n->args = $5;
					$$ = n;
				}
			| set_rest_more
			;

set_rest_more:	/* Generic SET syntaxes: */
			var_name TO var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name '=' var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name TO DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name '=' DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name FROM CURRENT_P
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_CURRENT;
					n->name = $1;
					$$ = n;
				}
			/* Special syntaxes mandated by SQL standard: */
			| TIME ZONE zone_value
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| CATALOG_P Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("current database cannot be changed"),
							 parser_errposition(@2)));
					$$ = NULL; /*not reached*/
				}
			| SCHEMA Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "search_path";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| NAMES opt_encoding
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "client_encoding";
					if ($2 != NULL)
						n->args = list_make1(makeStringConst($2, @2));
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| ROLE NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "role";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| SESSION AUTHORIZATION NonReservedWord_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "session_authorization";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
			| SESSION AUTHORIZATION DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "session_authorization";
					$$ = n;
				}
			| XML_P OPTION document_or_content
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "xmloption";
					n->args = list_make1(makeStringConst($3 == XMLOPTION_DOCUMENT ? "DOCUMENT" : "CONTENT", @3));
					$$ = n;
				}
			/* Special syntaxes invented by PostgreSQL: */
			| TRANSACTION SNAPSHOT Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION SNAPSHOT";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
		;

set_target:
	ColId opt_indirection
		{
			$$ = makeNode(ResTarget);
			$$->name = $1;
			$$->indirection = check_indirection($2, yyscanner);
			$$->val = NULL;	/* upper production sets this */
			$$->location = @1;
			$$->as_location = -1;
		}
	;

set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;

SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;

single_set_clause:
			set_target '=' set_expr
				{
					$$ = $1;
					$$->val = (Node *) $3;
				}
		;

SimpleTypename:
	  Numeric
	| Bit
	| Character
	| ConstDatetime
	| ConstInterval
	| INTERVAL YEAR_P opt_type_mod TO MONTH_P
		{
			$$ = SystemTypeNameLocation("interval", @1);
			$$->typmods = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
											 INTERVAL_MASK(MONTH), @1));
			if($3)
			{
				A_Const *n = (A_Const *)linitial($3);
				n->location = @3;
				$$->typmods = lappend($$->typmods, n);
			}
		}
	| INTERVAL DAY_P opt_type_mod TO SECOND_P					%prec '+'
		{
			$$ = SystemTypeNameLocation("interval", @1);
			$$->typmods = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1));
			if($3)
			{
				A_Const *n = (A_Const *)linitial($3);
				n->location = @3;
				$$->typmods = lappend($$->typmods, n);
			}
		}
	| INTERVAL DAY_P opt_type_mod TO SECOND_P '(' Iconst ')'	%prec '/' /* height then no "( n )" operator */
		{
			$$ = SystemTypeNameLocation("interval", @1);
			$$->typmods = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1));
			if($3)
			{
				A_Const *n = (A_Const *)linitial($3);
				n->location = @3;
				$$->typmods = lappend($$->typmods, n);
			}
		}
	| IDENT
		{
			$$ = makeTypeName($1);
			$$->location = @1;
		}
	| IDENT '.' IDENT
		{
			$$ = makeTypeNameFromNameList(list_make2(makeString($1),makeString($3)));
			$$->location = @1;
		}
	| IDENT '(' expr_list ')'
		{
			$$ = makeTypeName($1);
			$$->typmods = $3;
			$$->location = @1;
		}
	| IDENT '.' IDENT '(' expr_list ')'
		{
			$$ = makeTypeNameFromNameList(list_make2(makeString($1),makeString($3)));
			$$->typmods = $5;
			$$->location = @1;
		}
	| type_func_name_keyword
		{
			$$ = makeTypeName(pstrdup($1));
			$$->location = @1;
		}
	| type_func_name_keyword '(' expr_list ')'
		{
			$$ = makeTypeName(pstrdup($1));
			$$->typmods = $3;
			$$->location = @1;
		}
	;

simple_select:
		SELECT opt_distinct target_list into_clause from_clause where_clause
		opt_connect_by_clause group_clause having_clause
			{
				SelectStmt *n = makeNode(SelectStmt);
				ResTarget *rt = llast_node(ResTarget, $3);
				if (rt->expr_len <= 0 && rt->location >= 0)
				{
					if (@4 > rt->location)
						rt->expr_len = @4 - rt->location;
					else if (@5 > rt->location)
						rt->expr_len = @5 - rt->location;
				}
				n->distinctClause = $2;
				n->targetList = $3;
				n->intoClause = $4;
				n->fromClause = $5;
				n->whereClause = $6;
				n->groupClause = $8;
				n->havingClause = $9;
				n->ora_connect_by = $7;
				$$ = (Node*)n;
			}
		| select_clause UNION opt_all select_clause
			{
				$$ = makeSetOp(SETOP_UNION, $3, $1, $4);
			}
		| select_clause MINUS opt_all select_clause
			{
				$$ = makeSetOp(SETOP_EXCEPT, $3, $1, $4);
			}
		| values_clause { $$ = $1; }
		;

into_clause:
			INTO OptTempTableName
				{
					$$ = makeNode(IntoClause);
					$$->rel = $2;
					$$->colNames = NIL;
					$$->options = NIL;
					$$->onCommit = ONCOMMIT_NOOP;
					$$->tableSpaceName = NULL;
					$$->viewQuery = NULL;
					$$->skipData = false;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTemp.
 */
OptTempTableName:
			TABLE qualified_name
				{
					$$ = $2;
					$$->relpersistence = RELPERSISTENCE_PERMANENT;
				}
			| qualified_name
				{
					$$ = $1;
					$$->relpersistence = RELPERSISTENCE_PERMANENT;
				}
		;

sortby: a_expr opt_asc_desc opt_nulls_order
		{
			$$ = makeNode(SortBy);
			$$->node = $1;
			$$->sortby_dir = $2;
			$$->sortby_nulls = $3;
			$$->useOp = NIL;
			$$->location = -1;		/* no operator */
		}
	;

sortby_list: sortby						{ $$ = list_make1($1); }
	| sortby_list ',' sortby			{ $$ = lappend($1, $3); }
	;

sort_clause: ORDER BY sortby_list		{ $$ = $3; }
	;

opt_sort_clause:
	  sort_clause						{ $$ = $1; }
	| /* empty */						{ $$ = NIL; }
	;

select_sort_clause:
		  ORDER BY sortby_list
			{
				SelectSortClause *n = palloc0(sizeof(SelectSortClause));
				n->sortClause = $3;
				n->siblings_loc = -1;
				n->sort_loc = @1;
				$$ = n;
			}
		| ORDER SIBLINGS BY sortby_list
			{
				SelectSortClause *n = palloc0(sizeof(SelectSortClause));
				n->sortClause = $4;
				n->siblings_loc = @2;
				n->sort_loc = @1;
				$$ = n;
			}
		;

opt_select_sort_clause:
		  select_sort_clause			{ $$ = $1; }
		| /* empty */					{ $$ = NULL; }
		;

opt_start_with_clause:
	  start_with_clause					{ $$ = $1; }
	| /* empty */						{ $$ = NULL; }
	;

start_with_clause: START WITH a_expr		{ $$ = $3; }
	;

connect_by_opt_nocycle:
	  CONNECT_BY			{ $$ = false; }
	| CONNECT_BY_NOCYCLE	{ $$ = true; }
	;

opt_connect_by_clause:
		  connect_by_clause					{ $$ = $1; }
		| /* empty */						{ $$ = NULL; }
		;

connect_by_clause:
	  start_with_clause connect_by_opt_nocycle a_expr
		{
			OracleConnectBy *n = makeNode(OracleConnectBy);
			n->no_cycle = $2;
			n->start_with = $1;
			n->connect_by = $3;
			$$ = n;
		}
	| connect_by_opt_nocycle a_expr opt_start_with_clause
		{
			OracleConnectBy *n = makeNode(OracleConnectBy);
			n->no_cycle = $1;
			n->start_with = $3;
			n->connect_by = $2;
			$$ = n;
		}
	;

TableElement:
	  columnDef							{ $$ = $1; }
	| TableLikeClause					{ $$ = $1; }
	| TableConstraint					{ $$ = $1; }
	;

TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					n->options = $3;
					$$ = (Node *)n;
				}
		;

TableLikeOptionList:
				TableLikeOptionList INCLUDING TableLikeOption	{ $$ = $1 | $3; }
				| TableLikeOptionList EXCLUDING TableLikeOption	{ $$ = $1 & ~$3; }
				| /* EMPTY */						{ $$ = 0; }
		;

TableLikeOption:
				DEFAULTS			{ $$ = CREATE_TABLE_LIKE_DEFAULTS; }
				| CONSTRAINTS		{ $$ = CREATE_TABLE_LIKE_CONSTRAINTS; }
				| INDEXES			{ $$ = CREATE_TABLE_LIKE_INDEXES; }
				| STORAGE			{ $$ = CREATE_TABLE_LIKE_STORAGE; }
				| COMMENTS			{ $$ = CREATE_TABLE_LIKE_COMMENTS; }
				| ALL				{ $$ = CREATE_TABLE_LIKE_ALL; }
		;

TableElementList:
	  TableElement 						{ $$ = list_make1($1); }
	| TableElementList ',' TableElement { $$ = lappend($1, $3); }
	;

table_ref:
		  relation_expr opt_alias_clause
			{
				$1->alias = $2;
				$$ = (Node*) $1;
			}
		| select_with_parens opt_alias_clause
			{
				RangeSubselect *n = makeNode(RangeSubselect);
				n->lateral = false;
				n->subquery = $1;
				n->alias = $2;
				if(n->alias == NULL)
					ora_yyget_extra(yyscanner)->has_no_alias_subquery = true;
				$$ = (Node *) n;
			}
		| TABLE select_with_parens opt_alias_clause
			{
				RangeSubselect *n = makeNode(RangeSubselect);
				n->lateral = false;
				n->subquery = $2;
				n->alias = $3;
				if(n->alias == NULL)
					ora_yyget_extra(yyscanner)->has_no_alias_subquery = true;
				$$ = (Node *) n;
				if (IsA($2, SelectStmt) &&
					((SelectStmt*)$2)->targetList != NIL)
				{
					ResTarget *rt = linitial_node(ResTarget, ((SelectStmt*)$2)->targetList);
					if (rt->name == NULL)
					rt->name = pstrdup("column_value");
				}
			}
		| joined_table
			{
				$$ = (Node *) $1;
			}
		| '(' joined_table ')' alias_clause
			{
				$2->alias = $4;
				$$ = (Node *) $2;
			}
		| func_table func_alias_clause
			{
				RangeFunction *n = (RangeFunction *) $1;
				n->alias = linitial($2);
				n->coldeflist = lsecond($2);
				$$ = (Node *) n;
			}
		;

target_item:
		a_expr AS ColLabel
			{
				$$ = makeNode(ResTarget);
				$$->name = $3;
				$$->as_location = @3;
				$$->expr_len = @2 - @1;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}
		| a_expr IDENT
			{
				$$ = makeNode(ResTarget);
				$$->name = $2;
				$$->as_location = @2;
				$$->expr_len = @2 - @1;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
			}
		| a_expr
			{
				$$ = makeNode(ResTarget);
				$$->name = NULL;
				$$->indirection = NIL;
				$$->val = (Node *)$1;
				$$->location = @1;
				$$->as_location = -1;
				$$->expr_len = -1;
			}
		| '*'
			{
				$$ = make_star_target(@1);
				$$->expr_len = 3;
			}
		;

target_list:
		target_item 					{ $$ = list_make1($1); }
		| target_list ',' target_item
			{
				ResTarget *rt = llast_node(ResTarget, $1);
				if (rt->expr_len <= 0 &&
					rt->location >= 0)
					rt->expr_len = @2 - rt->location;
				$$ = lappend($1, $3);
			}
		;

transaction_mode_item:
			ISOLATION LEVEL iso_level
					{ $$ = makeDefElem("transaction_isolation",
									   makeStringConst($3, @3), @1); }
			| READ ONLY
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(true, @1), @1); }
			| READ WRITE
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(false, @1), @1); }
			| DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(true, @1), @1); }
			| NOT DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(false, @1), @1); }
		;

/* Syntax with commas is SQL-spec, without commas is Postgres historical */
transaction_mode_list:
			transaction_mode_item
					{ $$ = list_make1($1); }
			| transaction_mode_list ',' transaction_mode_item
					{ $$ = lappend($1, $3); }
			| transaction_mode_list transaction_mode_item
					{ $$ = lappend($1, $2); }
		;

TruncateStmt:
	TRUNCATE TABLE relation_expr_list opt_restart_seqs opt_drop_behavior
	  TruncateStmt_log_opt TruncateStmt_storage_opt
		{
			TruncateStmt *n = makeNode(TruncateStmt);
			n->relations = $3;
			n->restart_seqs = $4;
			n->behavior = $5;
			/* ignore TruncateStmt_log_opt and TruncateStmt_storage_opt */
			$$ = (Node*)n;
		}
	;

TruncateStmt_log_opt:
	  PRESERVE MATERIALIZED VIEW LOG_P
	| PURGE MATERIALIZED VIEW LOG_P
	| /* empty */
	;

TruncateStmt_storage_opt:
	  DROP ALL STORAGE
	| DROP STORAGE
	| REUSE STORAGE
	| /* empty */
	;

Typename:	SimpleTypename
				{
					$$ = $1;
				}
			| SimpleTypename PIPELINED
				{
					$$ = $1;
					$$->setof = true;
				}
	;

//transaction_mode_list_or_empty:
//			transaction_mode_list
//			| /* EMPTY */
//					{ $$ = NIL; }
//		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

UpdateStmt:
	  UPDATE relation_expr_opt_alias SET set_clause_list
		from_clause
	  where_or_current_clause returning_clause
		{
			UpdateStmt *n = makeNode(UpdateStmt);
			n->relation = $2;
			n->targetList = $4;
			n->fromClause = $5;
			n->whereClause = $6;
			n->returningList = $7;
			$$ = (Node*)n;
		}
	| UPDATE '(' UpdateSelectStmt ')' SET set_clause_list returning_clause
		{
			UpdateStmt *n = (UpdateStmt*)$3;
			n->targetList = $6;
			n->returningList = $7;
			$$ = (Node*)n;
		}
	| UPDATE '(' UpdateSelectStmt ')' ColId SET set_clause_list returning_clause
		{
			UpdateStmt *n = (UpdateStmt*)$3;
			Alias *alias = makeNode(Alias);
			alias->aliasname = $5;
			n->relation->alias = alias;
			n->targetList = $7;
			n->returningList = $8;
			$$ = (Node*)n;
		}
	| UPDATE '(' UpdateSelectStmt ')' AS ColId SET set_clause_list returning_clause
		{
			UpdateStmt *n = (UpdateStmt*)$3;
			Alias *alias = makeNode(Alias);
			alias->aliasname = $6;
			n->relation->alias = alias;
			n->targetList = $8;
			n->returningList = $9;
			$$ = (Node*)n;
		}
	;

UpdateSelectStmt:
		  SELECT target_list FROM relation_expr where_or_current_clause
			{
				UpdateStmt *n = makeNode(UpdateStmt);
				n->relation = $4;
				n->whereClause = $5;
				/* ignore target list */
				$$ = (Node*)n;
			}
		| '(' UpdateSelectStmt ')'
			{
				$$ = $2;
			}
		;


values_clause:
		VALUES ctext_row
			{
				SelectStmt *n = makeNode(SelectStmt);
				n->valuesLists = list_make1($2);
				$$ = (Node *) n;
			}
		| values_clause ',' ctext_row
			{
				SelectStmt *n = (SelectStmt *) $1;
				n->valuesLists = lappend(n->valuesLists, $3);
				$$ = (Node *) n;
			}
	;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
			| PUBLIC
				{ $$ = makeStringConst(pstrdup($1), @1); }
		;

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{
					$$ = palloc(strlen($1) + strlen($3) + 2);
					sprintf($$, "%s.%s", $1, $3);
				}
		;

VariableResetStmt:
			RESET var_name
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $2;
					$$ = (Node *) n;
				}
			| RESET TIME ZONE
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| RESET TRANSACTION ISOLATION LEVEL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| RESET SESSION AUTHORIZATION
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| RESET ALL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			| SET LOCAL set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (Node *) n;
				}
			| SET SESSION set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

VariableShowStmt:
	  SHOW var_name
		{
			VariableShowStmt *n = makeNode(VariableShowStmt);
			n->name = $2;
			$$ = (Node *) n;
		}
         | SHOW TIME ZONE
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "timezone";
                         $$ = (Node *) n;
                 }
         | SHOW TRANSACTION ISOLATION LEVEL
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "transaction_isolation";
                         $$ = (Node *) n;
                 }
         | SHOW SESSION AUTHORIZATION
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "session_authorization";
                         $$ = (Node *) n;
                 }
         | SHOW ALL
                 {
                         VariableShowStmt *n = makeNode(VariableShowStmt);
                         n->name = "all";
                         $$ = (Node *) n;
                 }
	;

/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query>
 *
 *****************************************************************************/

ViewStmt: CREATE OptTemp VIEW qualified_name opt_column_list
				AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $4;
					n->view->relpersistence = $2;
					n->aliases = $5;
					n->query = $7;
					n->replace = false;
					/* n->options = $6; */
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $6;
					n->view->relpersistence = $4;
					n->aliases = $7;
					n->query = $10;
					n->replace = true;
					n->options = $8;
					$$ = (Node *) n;
				}
		| CREATE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')'
				AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $5;
					n->view->relpersistence = $2;
					n->aliases = $7;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $10);
					n->replace = false;
					/* n->options = $9;*/
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp RECURSIVE VIEW qualified_name '(' columnList ')'
				AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->grammar = PARSE_GRAM_ORACLE;
					n->view = $7;
					n->view->relpersistence = $4;
					n->aliases = $9;
					n->query = makeRecursiveViewSelect(n->view->relname, n->aliases, $12);
					n->replace = true;
					/* n->options = $11;*/
					$$ = (Node *) n;
				}
		;

where_clause: WHERE a_expr	{ $$ = $2; }
	| /* empty */ { $$ = NULL; }
	;

with_clause:
	  WITH cte_list
		{
			$$ = makeNode(WithClause);
			$$->ctes = $2;
			$$->recursive = false;
			$$->location = @1;
		}
	| WITH RECURSIVE cte_list
		{
			$$ = makeNode(WithClause);
			$$->ctes = $3;
			$$->recursive = true;
			$$->location = @1;
		}
	;

/*****************************************************************************
 *
 *		Transactions:
 *
 *		BEGIN / COMMIT / ROLLBACK
 *		(also older versions END / ABORT)
 *
 *****************************************************************************/

TransactionStmt:
			ABORT_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| BEGIN_P opt_transaction transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = $3;
					$$ = (Node *)n;
				}
			| START TRANSACTION transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = $3;
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| END_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->savepoint_name = $2;
					$$ = (Node *)n;
				}
			| RELEASE SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->savepoint_name = $3;
					$$ = (Node *)n;
				}
			| RELEASE ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->savepoint_name = $2;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->savepoint_name = $5;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->savepoint_name = $4;
					$$ = (Node *)n;
				}
			| PREPARE TRANSACTION Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_PREPARE;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED IF_P EXISTS Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
#if defined(ADB)
					n->missing_ok = true;
#endif
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $5;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED IF_P EXISTS Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
#if defined(ADB)
					n->missing_ok = true;
#endif
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $5;
					$$ = (Node *)n;
				}
		;

opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

transaction_mode_list_or_empty:
			transaction_mode_list
			| /* EMPTY */
					{ $$ = NIL; }
		;

/* Timezone values can be:
 * - a string such as 'pst8pdt'
 * - an identifier such as "pst8pdt"
 * - an integer or floating point number
 * - a time interval per SQL99
 * ColId gives reduce/reduce errors against ConstInterval and LOCAL,
 * so use IDENT (meaning we reject anything that is a key word).
 */
zone_value:
			Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| IDENT
				{
					$$ = makeStringConst($1, @1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					if ($3 != NIL)
					{
						A_Const *n = (A_Const *) linitial($3);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@3)));
					}
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					TypeName *t = $1;
					if ($6 != NIL)
					{
						A_Const *n = (A_Const *) linitial($6);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@6)));
						if (list_length($6) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						t->typmods = lappend($6, makeIntConst($3, @3));
					}
					else
						t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				close <portalname>
 *
 *****************************************************************************/

ClosePortalStmt:
			CLOSE cursor_name
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = $2;
					$$ = (Node *)n;
				}
			| CLOSE ALL
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = NULL;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CURSOR STATEMENTS
 *
 *****************************************************************************/
DeclareCursorStmt: DECLARE cursor_name cursor_options CURSOR opt_hold FOR SelectStmt
				{
					DeclareCursorStmt *n = makeNode(DeclareCursorStmt);
					n->portalname = $2;
					/* currently we always set FAST_PLAN option */
					n->options = $3 | $5 | CURSOR_OPT_FAST_PLAN;
					n->query = $7;
					$$ = (Node *)n;
				}
		;

BlockCodeStmt: DECLARE Sconst
				{
					DoStmt *n = makeNode(DoStmt);
					n->args = list_make2(makeDefElem("as", (Node *)makeString($2), @2),
										 makeDefElem("language", (Node *)makeString("plorasql"), -1));
					$$ = (Node*)n;
				}
			;


cursor_name:	name						{ $$ = $1; }
		;

cursor_options: /*EMPTY*/					{ $$ = 0; }
			| cursor_options NO SCROLL		{ $$ = $1 | CURSOR_OPT_NO_SCROLL; }
			| cursor_options SCROLL			{ $$ = $1 | CURSOR_OPT_SCROLL; }
			| cursor_options BINARY			{ $$ = $1 | CURSOR_OPT_BINARY; }
			| cursor_options INSENSITIVE	{ $$ = $1 | CURSOR_OPT_INSENSITIVE; }
		;

opt_hold: /* EMPTY */						{ $$ = 0; }
			| WITH HOLD						{ $$ = CURSOR_OPT_HOLD; }
			| WITHOUT HOLD					{ $$ = 0; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *			fetch/move
 *
 *****************************************************************************/

FetchStmt:	FETCH fetch_args
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->ismove = false;
					$$ = (Node *)n;
				}
			| MOVE fetch_args
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->ismove = true;
					$$ = (Node *)n;
				}
		;

fetch_args:	cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $1;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $2;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| NEXT opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| PRIOR opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FIRST_P opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| LAST_P opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = -1;
					$$ = (Node *)n;
				}
			| ABSOLUTE_P SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| RELATIVE_P SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_RELATIVE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = $1;
					$$ = (Node *)n;
				}
			| ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| FORWARD opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FORWARD SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_FORWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| FORWARD ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| BACKWARD opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| BACKWARD SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_BACKWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| BACKWARD ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_BACKWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
		;

from_in:	FROM									{}
			| IN_P									{}
		;

opt_from_in:	from_in								{}
			| /* EMPTY */							{}
		;


/*****************************************************************************
 *
 *	The COMMENT ON statement can take different forms based upon the type of
 *	the object associated with the comment. The form of the statement is:
 *
 *	COMMENT ON [ [ ACCESS METHOD | CONVERSION | COLLATION |
 *                 DATABASE | DOMAIN |
 *                 EXTENSION | EVENT TRIGGER | FOREIGN DATA WRAPPER |
 *                 FOREIGN TABLE | INDEX | [PROCEDURAL] LANGUAGE |
 *                 MATERIALIZED VIEW | POLICY | ROLE | SCHEMA | SEQUENCE |
 *                 SERVER | STATISTICS | TABLE | TABLESPACE |
 *                 TEXT SEARCH CONFIGURATION | TEXT SEARCH DICTIONARY |
 *                 TEXT SEARCH PARSER | TEXT SEARCH TEMPLATE | TYPE |
 *                 VIEW] <objname> |
 *				 AGGREGATE <aggname> (arg1, ...) |
 *				 CAST (<src type> AS <dst type>) |
 *				 COLUMN <relname>.<colname> |
 *				 CONSTRAINT <constraintname> ON <relname> |
 *				 CONSTRAINT <constraintname> ON DOMAIN <domainname> |
 *				 FUNCTION <funcname> (arg1, arg2, ...) |
 *				 LARGE OBJECT <oid> |
 *				 OPERATOR <op> (leftoperand_typ, rightoperand_typ) |
 *				 OPERATOR CLASS <name> USING <access-method> |
 *				 OPERATOR FAMILY <name> USING <access-method> |
 *				 RULE <rulename> ON <relname> |
 *				 TRIGGER <triggername> ON <relname> ]
 *			   IS { 'text' | NULL }
 *
 *****************************************************************************/

CommentStmt:
			COMMENT ON comment_type_any_name any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON comment_type_name name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->object = (Node *) makeString($4);
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON TYPE_P Typename IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TYPE;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON DOMAIN_P Typename IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_DOMAIN;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON AGGREGATE aggregate_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_AGGREGATE;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON FUNCTION function_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_FUNCTION;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR operator_with_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPERATOR;
					n->object = (Node *) $4;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON CONSTRAINT name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TABCONSTRAINT;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON CONSTRAINT name ON DOMAIN_P any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_DOMCONSTRAINT;
					/*
					 * should use Typename not any_name in the production, but
					 * there's a shift/reduce conflict if we do that, so fix it
					 * up here.
					 */
					n->object = (Node *) list_make2(makeTypeNameFromNameList($7), makeString($4));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON POLICY name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_POLICY;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON RULE name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_RULE;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON TRANSFORM FOR Typename LANGUAGE name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TRANSFORM;
					n->object = (Node *) list_make2($5, makeString($7));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON TRIGGER name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TRIGGER;
					n->object = (Node *) lappend($6, makeString($4));
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR CLASS any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPCLASS;
					n->object = (Node *) lcons(makeString($7), $5);
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR FAMILY any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPFAMILY;
					n->object = (Node *) lcons(makeString($7), $5);
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON LARGE_P OBJECT_P NumericOnly IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_LARGEOBJECT;
					n->object = (Node *) $5;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON CAST '(' Typename AS Typename ')' IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_CAST;
					n->object = (Node *) list_make2($5, $7);
					n->comment = $10;
					$$ = (Node *) n;
				}
		;

aggregate_with_argtypes:
			func_name aggr_args
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = extractAggrArgTypes($2);
					$$ = n;
				}
		;

comment_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;

/*****************************************************************************
 *	Oracle Implicit Conversion:
 *		
 *		CREATE [ OR REPLACE ] CONVERT FUNCTION function_name (type1 [,type2 [, ...]]) 
 *				AS new_function_name (new_type1 [,new_type2 [, ...]]);
 *
 *
 *****************************************************************************/

OraImplicitConvertStmt:
			/* implicit convert function or special function */
			CREATE opt_or_replace CONVERT opt_function_or_common convert_functon_name '(' convert_type_list ')' 
						AS convert_functon_name '(' convert_type_list ')'
				{
					OraImplicitConvertStmt *c = makeNode(OraImplicitConvertStmt);
					c->cvtkind = $4;
					c->cvtname = $5;
					c->cvtfrom = $7;
					c->cvtto = $12;
					if ($2)
						c->action = ICONVERT_UPDATE;
					else
						c->action = ICONVERT_CREATE;
					c->if_exists = false;
					c->node_list = NIL;
					$$ = (Node *) c;
				}
			| DROP CONVERT opt_function_or_common convert_functon_name '(' convert_type_list ')'
				{
					OraImplicitConvertStmt *c = makeNode(OraImplicitConvertStmt);
					c->cvtkind = $3;
					c->cvtname = $4;
					c->cvtfrom = $6;
					c->cvtto = NIL;
					c->action = ICONVERT_DELETE;
					c->if_exists = false;
					c->node_list = NIL;
					$$ = (Node *) c;
				}
			| DROP CONVERT opt_function_or_common IF_P EXISTS convert_functon_name '(' convert_type_list ')'
				{
					OraImplicitConvertStmt *c = makeNode(OraImplicitConvertStmt);
					c->cvtkind = $3;
					c->cvtname = $6;
					c->cvtfrom = $8;
					c->cvtto = NIL;
					c->action = ICONVERT_DELETE;
					c->if_exists = true;
					c->node_list = NIL;
					$$ = (Node *) c;
				}
			/* implicit convert operator */
			| CREATE opt_or_replace CONVERT OPERATOR convert_type all_Op convert_type AS convert_type all_Op convert_type
				{
					OraImplicitConvertStmt *c = makeNode(OraImplicitConvertStmt);
					c->cvtkind = ORA_CONVERT_KIND_OPERATOR;
					c->cvtname = $6;
					c->cvtfrom = list_make2($5, $7);
					c->cvtto = list_make2($9, $11);
					if ($2)
						c->action = ICONVERT_UPDATE;
					else
						c->action = ICONVERT_CREATE;
					c->if_exists = false;
					c->node_list = NIL;
					$$ = (Node *) c;
				}
			| DROP CONVERT OPERATOR convert_type all_Op convert_type
				{
					OraImplicitConvertStmt *c = makeNode(OraImplicitConvertStmt);
					c->cvtkind = ORA_CONVERT_KIND_OPERATOR;
					c->cvtname = $5;
					c->cvtfrom = list_make2($4, $6);
					c->cvtto = NIL;
					c->action = ICONVERT_DELETE;
					c->if_exists = false;
					c->node_list = NIL;
					$$ = (Node *) c;
				}
			| DROP CONVERT OPERATOR IF_P EXISTS convert_type all_Op convert_type
				{
					OraImplicitConvertStmt *c = makeNode(OraImplicitConvertStmt);
					c->cvtkind = ORA_CONVERT_KIND_OPERATOR;
					c->cvtname = $7;
					c->cvtfrom = list_make2($6, $8);
					c->cvtto = NIL;
					c->action = ICONVERT_DELETE;
					c->if_exists = true;
					c->node_list = NIL;
					$$ = (Node *) c;
				}
		;
convert_functon_name:
			type_function_name						{ $$ = $1; }
			| /* EMPTY */							{ $$ = ""; }
			;

convert_type_list:
			convert_type							{ $$ = list_make1($1); }
			| convert_type_list ',' convert_type 	{ $$ = lappend($1, $3); }
		;

convert_type:
			ColLabel								{ $$ = (Node*) makeString($1); }
		;

opt_function_or_common:
			FUNCTION								{ $$ = ORA_CONVERT_KIND_FUNCTION; }
			/*
			| SPECIAL FUNCTION						{ $$ = ORA_CONVERT_KIND_SPECIAL_FUN; }
			| COMMON								{ $$ = ORA_CONVERT_KIND_COMMON; }
			*/
		;

/* object types taking any_name */
comment_type_any_name:
			COLUMN								{ $$ = OBJECT_COLUMN; }
			| INDEX								{ $$ = OBJECT_INDEX; }
			| SEQUENCE							{ $$ = OBJECT_SEQUENCE; }
			| STATISTICS						{ $$ = OBJECT_STATISTIC_EXT; }
			| TABLE								{ $$ = OBJECT_TABLE; }
			| VIEW								{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW					{ $$ = OBJECT_MATVIEW; }
			| COLLATION							{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P						{ $$ = OBJECT_CONVERSION; }
			| FOREIGN TABLE						{ $$ = OBJECT_FOREIGN_TABLE; }
			| TEXT_P SEARCH CONFIGURATION		{ $$ = OBJECT_TSCONFIGURATION; }
			| TEXT_P SEARCH DICTIONARY			{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH PARSER				{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH TEMPLATE			{ $$ = OBJECT_TSTEMPLATE; }
		;

/* object types taking name */
comment_type_name:
			ACCESS METHOD						{ $$ = OBJECT_ACCESS_METHOD; }
			| DATABASE							{ $$ = OBJECT_DATABASE; }
			| EVENT TRIGGER						{ $$ = OBJECT_EVENT_TRIGGER; }
			| EXTENSION							{ $$ = OBJECT_EXTENSION; }
			| FOREIGN DATA_P WRAPPER			{ $$ = OBJECT_FDW; }
			| opt_procedural LANGUAGE			{ $$ = OBJECT_LANGUAGE; }
			| PUBLICATION						{ $$ = OBJECT_PUBLICATION; }
			| ROLE								{ $$ = OBJECT_ROLE; }
			| SCHEMA							{ $$ = OBJECT_SCHEMA; }
			| SERVER							{ $$ = OBJECT_FOREIGN_SERVER; }
			| SUBSCRIPTION						{ $$ = OBJECT_SUBSCRIPTION; }
			| TABLESPACE						{ $$ = OBJECT_TABLESPACE; }
	;

operator_with_argtypes:
			any_operator oper_argtypes
				{
					ObjectWithArgs *n = makeNode(ObjectWithArgs);
					n->objname = $1;
					n->objargs = $2;
					$$ = n;
				}
		;
oper_argtypes:
		'(' Typename ')'
				{
				   ereport(ERROR,
						   (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("missing argument"),
							errhint("Use NONE to denote the missing argument of a unary operator."),
							parser_errposition(@3)));
				}
			| '(' Typename ',' Typename ')'
					{ $$ = list_make2($2, $4); }
			| '(' NONE ',' Typename ')'					/* left unary */
				{ $$ = list_make2(NULL, $4); }
			| '(' Typename ',' NONE ')'					/* right unary */
					{ $$ = list_make2($2, NULL); }
		;

/* Aggregate args can be most things that function args can be */
aggr_arg:	func_arg
				{
					if (!($1->mode == FUNC_PARAM_IN ||
						  $1->mode == FUNC_PARAM_VARIADIC))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("aggregates cannot have output arguments"),
								 parser_errposition(@1)));
					$$ = $1;
				}
		;

/*
 * The SQL standard offers no guidance on how to declare aggregate argument
 * lists, since it doesn't have CREATE AGGREGATE etc.  We accept these cases:
 *
 * (*)									- normal agg with no args
 * (aggr_arg,...)						- normal agg with args
 * (ORDER BY aggr_arg,...)				- ordered-set agg with no direct args
 * (aggr_arg,... ORDER BY aggr_arg,...)	- ordered-set agg with direct args
 *
 * The zero-argument case is spelled with '*' for consistency with COUNT(*).
 *
 * An additional restriction is that if the direct-args list ends in a
 * VARIADIC item, the ordered-args list must contain exactly one item that
 * is also VARIADIC with the same type.  This allows us to collapse the two
 * VARIADIC items into one, which is necessary to represent the aggregate in
 * pg_proc.  We check this at the grammar stage so that we can return a list
 * in which the second VARIADIC item is already discarded, avoiding extra work
 * in cases such as DROP AGGREGATE.
 *
 * The return value of this production is a two-element list, in which the
 * first item is a sublist of FunctionParameter nodes (with any duplicate
 * VARIADIC item already dropped, as per above) and the second is an integer
 * Value node, containing -1 if there was no ORDER BY and otherwise the number
 * of argument declarations before the ORDER BY.  (If this number is equal
 * to the first sublist's length, then we dropped a duplicate VARIADIC item.)
 * This representation is passed as-is to CREATE AGGREGATE; for operations
 * on existing aggregates, we can just apply extractArgTypes to the first
 * sublist.
 */
aggr_args:	'(' '*' ')'
				{
					$$ = list_make2(NIL, makeInteger(-1));
				}
			| '(' aggr_args_list ')'
				{
					$$ = list_make2($2, makeInteger(-1));
				}
			| '(' ORDER BY aggr_args_list ')'
				{
					$$ = list_make2($4, makeInteger(0));
				}
			| '(' aggr_args_list ORDER BY aggr_args_list ')'
				{
					/* this is the only case requiring consistency checking */
					$$ = makeOrderedSetArgs($2, $5, yyscanner);
				}
		;
aggr_args_list:
			aggr_arg								{ $$ = list_make1($1); }
			| aggr_args_list ',' aggr_arg			{ $$ = lappend($1, $3); }
		;

opt_procedural:
			PROCEDURAL								{}
			| /*EMPTY*/								{}
		;

/*********************************************************/
col_name_keyword:
	  BIGINT
	| BINARY_DOUBLE
	| BINARY_FLOAT
	| BOOLEAN_P
	| CASE
	| CHAR_P
	| DEC
	| DECIMAL_P
	| DATE_P
	| END_P
	| EXISTS
	| FLOAT_P
	| GROUPING
	| INT_P
	| INOUT
	| INTEGER
	| INTERVAL
	| LONG_P
	| NUMBER_P
	| PIPELINED
	| SETOF
	| SMALLINT
	| NONE
	| NUMERIC
	| OUT_P
	| REAL
	| VARCHAR
	| TIME
	| TIMESTAMP
	| WHEN
	;

reserved_keyword:
	  ACCESS
	| ADD_P
	| ALL
	| AND
	| ANY
	| AS
	| ASC
	| AUDIT
	| BETWEEN
	| BY
	| CHECK
	| CLUSTER
	| COLUMN
	| COMMENT
	| CONSTRAINT
	| COLLATE
	| COMPRESS
	| CONNECT_BY_ROOT
	| CREATE
	| CURRENT_P
	| DEFAULT
	| DEFERRABLE
	| DELETE_P
	| DESC
	| DISTINCT
	| DROP
	| ELSE
	| FALSE_P
	| FILE_P
	| FOR
	| FROM
	| FOREIGN
	| GRANT
	| GROUP_P
	| HAVING
	| IDENTIFIED
	| IMMEDIATE
	| INITIALLY
	| IN_P
	| INCREMENT
	| INITIAL_P
	| INSERT
	| INTERSECT
	| INTO
	| IS
	| LEVEL
	| LIKE
	| LOCK_P
	| MAXEXTENTS
	| MAXVALUE
	| MINUS
	| MLSLABEL
	| MODE
	| MODIFY
	| NOAUDIT
	| NOCOMPRESS
	| NOT
	| NOWAIT
	| NULL_P
	| OF
	| OFFLINE
	| ON
	| ONLINE
	| OPTION
	| OR
	| ONLY
	| ORDER
	| PCTFREE
	| PRIOR
	| PUBLIC
	| RAW
	| REFERENCES
	| RESOURCE
	| REVOKE
	| ROW
	| ROWID
	| ROWNUM
	| ROWS
	| SELECT
	| SESSION
	| SET
	| SIZE
	| START
	| SUCCESSFUL
	| SYNONYM
	| SYSDATE
	| TABLE
	| THEN
	| TO
	| TRIGGER
	| TRUE_P
	| UID
	| UNION
	| UNIQUE
	| UPDATE
	| USER
	| USING
	| VALIDATE
	| VALUES
	| VARCHAR2
	| WHENEVER
	| WHERE
	| WITH
	;

type_func_name_keyword:
	  AUTHORIZATION
	| CROSS
	| COLLATION
	| CONCURRENTLY
	| FULL
	| INNER_P
	| JOIN
	| LEFT
	| OUTER_P
	| OVER
	| RIGHT
	;

unreserved_keyword:
	  ANALYSE
	| ABORT_P
	| ABSOLUTE_P
	| ACTION
	| ADMIN
	| AGGREGATE
	| ANALYZE
	| ALWAYS
	| ALTER
	| AT
	| AUTHID
	| BACKWARD
	| BEGIN_P
	| BFILE
	| BINARY
	| BLOB
	| BYTE_P
	| CASCADE
	| CACHE
	| CAST
	| CLOSE
	| CATALOG_P
	| CONNECTION
	| CHARACTERISTICS
	| CLASS
	| CLOB
	| COMMENTS
	| COMMIT
	| COMMITTED
	| COMMON
	| CONFIGURATION
	| CONTENT_P
	| CONVERSION_P
	| CONTINUE_P
	| CONNECT
	| CONSTRAINTS
	| CONVERT
	/*| CURRVAL*/
	| CUBE
	| CURRENT_USER
	| CURSOR
	| CYCLE
	| DATABASE
	| DAY_P
	| DECLARE
	| DEFERRED
	| DEFINER
	| DENSE_RANK
	| DETERMINISTIC
	| DICTIONARY
	| DOCUMENT_P
	| DOUBLE_P
	| DOMAIN_P
	| DATA_P
	| DEFAULTS
/* ADB_BEGIN */
	| DISTRIBUTE
/* ADB_END */
	| DISABLE_P
	| ESCAPE
	| ENABLE_P
	| EXCLUDE
	| ENCRYPTED
	| EXCLUDING
	| EVENT
	| EXTENSION
	| EXCLUSIVE
	| FETCH
	| FIRST_P
	| FOLLOWING
	| FORWARD
	| FUNCTION
	| GLOBAL
	| HOLD
	| IDENTITY_P
	| IF_P
	| INDEX
	| INHERIT
	| INDEXES
	| INCLUDING
	| INHERITS
	| INSENSITIVE
	| ISOLATION
	| KEEP
	| KEY
	| LANGUAGE
	| LARGE_P
	| LAST_P
	| LESS
	| LIMIT
	| LOCAL
	| LOG_P
	| MATERIALIZED
	| MINUTE_P
	| MATCH
	| METHOD
	| NOMAXVALUE
	| MINVALUE
	| NOMINVALUE
	| MOD
	| MONTH_P
	| MOVE
	| NAMES
	| NCHAR
	| NOCACHE
	| NODE
	| NCLOB
	| NO
	| NOCYCLE
	| NEXT
	| NULLS_P
	/*| NEXTVAL*/
	| NVARCHAR2
	| OBJECT_P
	| OFF
	| OFFSET
	| OIDS
	| OPERATOR
	| OWNER
	| OWNED
	| OPTIONS
	| PARALLEL_ENABLE
	| PARSER
	| PARTIAL
	| PARTITION
	| PASSWORD
	| POLICY
	| PRECISION
	| PRECEDING
	| PREPARE
	| PREPARED
	| PRESERVE
	| PRIMARY
	| PROCEDURE
	| PRIVILEGES
	| PUBLICATION
	| PURGE
	| RANGE
	| READ
	| REFRESH
	| RELATIVE_P
	| RELEASE
	| REPEATABLE
	| REPLACE
	| RESET
	| RESULT_CACHE
	| RULE
	| RESTART
	| RESTRICT
	| RETURNING
	| RETURN_P
	| REUSE
	| RENAME
	| REPLICA
	| ROLE
	| ROLLBACK
	| ROLLUP
	| PROCEDURAL
	| SCHEMA
	| SCROLL
	| SECOND_P
	| SEQUENCE
	| SERVER
	| SETS
	| SHARE
	| STATISTICS
	| SERIALIZABLE
	| SHOW
	| SOME
	| SYSID
	| SAVEPOINT
	| SEARCH
	| SIBLINGS
	| SIMPLE
	| SNAPSHOT
	| SPECIAL
	| STORAGE
	| SUBSCRIPTION
	| TEMP
	| TEMPLATE
	| TEMPORARY
	| THAN
	| TRANSACTION
	| TRANSFORM
	| TRUNCATE
	| TYPE_P
	| TEXT_P
	| TABLESPACE
	| UNBOUNDED
	| UNCOMMITTED
	| UNLOGGED
	| UNTIL
	| UNENCRYPTED
	| VERBOSE
	| VIEW
	| VALID
	| WRAPPER
	| WRITE
	| WORK
	| WITHIN
	| WITHOUT
	| XML_P
	| YEAR_P
	| ZONE
	;

%%

/*
 * Process result of ConstraintAttributeSpec, and set appropriate bool flags
 * in the output command node.  Pass NULL for any flags the particular
 * command doesn't support.
 */
static void
processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner)
{
	/* defaults */
	if (deferrable)
		*deferrable = false;
	if (initdeferred)
		*initdeferred = false;
	if (not_valid)
		*not_valid = false;

	if (cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED))
	{
		if (deferrable)
			*deferrable = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_INITIALLY_DEFERRED)
	{
		if (initdeferred)
			*initdeferred = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NOT_VALID)
	{
		if (not_valid)
			*not_valid = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NOT VALID",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NO_INHERIT)
	{
		if (no_inherit)
			*no_inherit = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NO INHERIT",
							constrType),
					 parser_errposition(location)));
	}
}

void ora_parser_init(ora_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;
	yyext->count_look = 0;
	yyext->has_no_alias_subquery = false;
	yyext->parsing_first_token = true;
	yyext->parsing_code_block = false;
}

static void ora_yyerror(YYLTYPE *yyloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static Node *makeDeleteStmt(RangeVar *range, Alias *alias, WithClause *with
	, Node *where, List *returning)
{
	DeleteStmt *stmt = makeNode(DeleteStmt);
	if(alias)
	{
		Assert(range->alias == NULL);
		range->alias = alias;
	}
	stmt->relation = range;
	stmt->withClause = with;
	stmt->whereClause = where;
	stmt->returningList = returning;
	return (Node*)stmt;
}

#define HAVE_LOOKAHEAD(extra)	(extra->count_look > 0)
#define LEX_LOOKAHEAD(p)		do{											\
	if (HAVE_LOOKAHEAD(yyextra))											\
	{																		\
		(p)->token = pop_lookahead(&((p)->lval),							\
								   &((p)->loc),								\
								   &((p)->length),							\
								   yyextra);								\
	}else																	\
	{																		\
		(p)->token = core_yylex(&((p)->lval), &((p)->loc), yyscanner);		\
		(p)->length = core_yyget_length(yyscanner);							\
	}}while(0)
#define PUSH_LOOKAHEAD(p)	\
	push_lookahead(yyextra, (p)->lval, (p)->loc, (p)->token, (p)->length)

static ora_yy_lookahead_type* push_empty_look(ora_yy_extra_type *extra)
{
	ora_yy_lookahead_type *look;
	if (extra->count_look == ORA_YY_MAX_LOOKAHEAD)
		elog(ERROR, "too many tokens pushed back");

	look = &(extra->lookahead[extra->count_look]);
	++(extra->count_look);
	return look;
}

static ora_yy_lookahead_type* push_lookahead(ora_yy_extra_type *extra, core_YYSTYPE lval,
											 YYLTYPE lloc, int token, size_t length)
{
	ora_yy_lookahead_type *look = push_empty_look(extra);

	look->lval = lval;
	look->length = length;
	look->loc = lloc;
	look->token = token;

	return look;
}

static int pop_lookahead(core_YYSTYPE *lval, YYLTYPE *lloc, size_t *length, ora_yy_extra_type *extra)
{
	ora_yy_lookahead_type *look;
	if (extra->count_look <= 0)
		elog(ERROR, "empty tokens pushed back");

	look = &(extra->lookahead[--(extra->count_look)]);

	*lloc = look->loc;
	*lval = look->lval;
	if (length)
		*length = look->length;

	return look->token;
}

static int ora_yylex(YYSTYPE *lvalp, YYLTYPE *lloc, core_yyscan_t yyscanner)
{
	ora_yy_extra_type *yyextra = ora_yyget_extra(yyscanner);
	ora_yy_lookahead_type look1;
	ora_yy_lookahead_type look2;
	int			cur_token;

	if (HAVE_LOOKAHEAD(yyextra))
		cur_token = pop_lookahead(&(lvalp->core_yystype), lloc, NULL, yyextra);
	else
		cur_token = core_yylex(&(lvalp->core_yystype), lloc, yyscanner);

	if (yyextra->parsing_first_token)
	{
		yyextra->parsing_first_token = false;
		if (cur_token == DECLARE)
		{
			/* find code block */
			LEX_LOOKAHEAD(&look1);

			if (look1.token != SCONST &&
				look1.token != 0)
			{
				LEX_LOOKAHEAD(&look2);

				switch(look2.token)
				{
				case BINARY:
				case INSENSITIVE:
				case NO:
				case SCROLL:
				case CURSOR:
					/* nothing todo */
					break;
				default:
					yyextra->parsing_code_block = true;
					break;
				}

				PUSH_LOOKAHEAD(&look2);
			}

			PUSH_LOOKAHEAD(&look1);
			return cur_token;
		}
	}else if (yyextra->parsing_code_block)
	{
		char   *scanbuf;
		int		wait_end_keyword;
		enum yytokentype last_token;

		yyextra->parsing_code_block = false;
		if (cur_token == SCONST)
			return SCONST;

		/* first find BEGIN keyword */
		if (cur_token != BEGIN_P)
		{
			for (;;)
			{
				LEX_LOOKAHEAD(&look1);
				if (look1.token == 0)
					parser_yyerror("syntax error");
				else if (look1.token == BEGIN_P)
					break;
			}
		}

		/* second found END keyword */
		scanbuf = yyextra->core_yy_extra.scanbuf;
		wait_end_keyword = 1;
		for(;;)
		{
			last_token = look1.token;
			LEX_LOOKAHEAD(&look1);

			switch(look1.token)
			{
			case 0:
				parser_yyerror("syntax error");
				break;
			case BEGIN_P:
			case CASE:
				++wait_end_keyword;
				break;
			case IF_P:
				if (last_token != END_P)
					++wait_end_keyword;
				break;
			case IDENT:
				if (scanbuf[look1.loc] != '"' &&
					strcmp(look1.lval.str, "loop") == 0)
				{
					if (last_token != END_P)
						++wait_end_keyword;
				}
				break;
			case END_P:
				--wait_end_keyword;
				break;
			default:
				break;
			}
			if (wait_end_keyword == 0)
				break;
		}

		/* found optional label */
		LEX_LOOKAHEAD(&look2);
		if (look2.token <= 255)
		{
			lvalp->core_yystype.str = pnstrdup(scanbuf + (*lloc),
											   look1.loc + look1.length - (*lloc));
			PUSH_LOOKAHEAD(&look2);
		}else
		{
			lvalp->core_yystype.str = pnstrdup(scanbuf + (*lloc),
											   look2.loc + look2.length - (*lloc));
		}
		return SCONST;
	}

	switch(cur_token)
	{
	/* find "(+)" token */
	case '(':
		LEX_LOOKAHEAD(&look1);
		if (look1.token == '+')
		{
			LEX_LOOKAHEAD(&look2);
			if (look2.token == ')')
			{
				/* now we have "(+)" token */
				cur_token = ORACLE_JOIN_OP;
			}else
			{
				PUSH_LOOKAHEAD(&look2);
			}
		}else
		{
			PUSH_LOOKAHEAD(&look1);
		}
		break;
	case CONNECT:
		LEX_LOOKAHEAD(&look1);
		if (look1.token == BY)
		{
			/* now we have "connect by" token */
			LEX_LOOKAHEAD(&look2);
			if (look2.token == NOCYCLE)
			{
				cur_token = CONNECT_BY_NOCYCLE;
			}else
			{
				cur_token = CONNECT_BY;
				PUSH_LOOKAHEAD(&look2);
			}
		}else
		{
			PUSH_LOOKAHEAD(&look1);
		}
		break;
	case DROP:
		LEX_LOOKAHEAD(&look1);
		if (look1.token == PARTITION)
		{
			cur_token = DROP_PARTITION;
		}else
		{
			PUSH_LOOKAHEAD(&look1);
		}
		break;
	case NULLS_P:
		LEX_LOOKAHEAD(&look1);
		if (look1.token == FIRST_P || look1.token == LAST_P)
			cur_token = NULLS_LA;
		PUSH_LOOKAHEAD(&look1);
		break;
	case LIMIT:
		LEX_LOOKAHEAD(&look1);
		if (look1.token == ICONST || look1.token == PARAM)
		{
			cur_token = LIMIT_LA;
		}else{
			cur_token = LIMIT;
		}
		PUSH_LOOKAHEAD(&look1);
		break;
	case ';':
		yyextra->parsing_first_token = true;
		break;
	default:
		break;
	}
	return cur_token;
}

static Node *reparse_decode_func(List *args, int location)
{
	ListCell 	*lc = list_head(args);
	Expr		*expr;

	CaseExpr *c = makeNode(CaseExpr);
	c->casetype = InvalidOid; /* not analyzed yet */
	c->isdecode = true;
	c->arg = lfirst(lc);
	lc = lnext(lc);

	while(lc)
	{
		if (lnext(lc))
		{
			CaseWhen *w = makeNode(CaseWhen);
			expr = lfirst(lc);
			if (IsA(expr, A_Const) &&
				((A_Const*)expr)->val.type == T_Null)
			{
				NullTest *n = makeNode(NullTest);
				n->arg = c->arg;
				n->nulltesttype = IS_NULL;
				w->expr = (Expr*)n;
			}else
			{
				w->expr = expr;
			}

			lc = lnext(lc);
			w->result = lfirst(lc);
			w->location = -1;

			c->args = lappend(c->args, w);
		}else
		{
			c->defresult = lfirst(lc);
		}

		lc = lnext(lc);
	}

	c->location = location;

	return (Node *)c;
}

static bool get_all_names(Node *n, List **pplist)
{
	AssertArg(pplist);
	if(n == NULL)
		return false;

	if(IsA(n, Alias))
	{
		//APPEND_ALIAS((Alias*)n);
		Assert(((Alias*)n)->aliasname);
		*pplist = lappend(*pplist, ((Alias*)n)->aliasname);
		return false;
	}/*if(IsA(n, JoinExpr))
	{
		APPEND_ALIAS(((JoinExpr*)n)->alias);
	}else if(IsA(n, RangeSubselect))
	{
		APPEND_ALIAS(((RangeSubselect*)n)->alias)
	}*/else if(IsA(n, RangeVar))
	{
		RangeVar *range = (RangeVar*)n;
		*pplist = lappend(*pplist, range->alias ? range->alias->aliasname : range->relname);
		return false;
	}else if(IsA(n, RangeFunction))
	{
		RangeFunction *range = (RangeFunction*)n;
		if(range->alias)
		{
			*pplist = lappend(*pplist, range->alias->aliasname);
			return false;
		}
	}else if(IsA(n, FuncCall))
	{
		FuncCall *func = (FuncCall*)n;
		Value *value;
		Assert(func->funcname);
		value = linitial(func->funcname);
		Assert(value && IsA(value,String));
		*pplist = lappend(*pplist, strVal(value));
		/* don't need function's argument(s) */
		return false;
	}else if(IsA(n, CommonTableExpr))
	{
		CommonTableExpr *cte = (CommonTableExpr*)n;
		*pplist = lappend(*pplist, cte->ctename);
	}else if(IsA(n, ColumnRef))
	{
		ColumnRef *column = (ColumnRef*)n;
		Value *value;
		int length = list_length(column->fields);
		if(length > 1)
		{
			value = list_nth(column->fields, length - 2);
			Assert(value && IsA(value, String));
			*pplist = lappend(*pplist, strVal(value));
		}
	}
	return node_tree_walker(n, get_all_names, pplist);
}

static unsigned int add_alias_name_idx;
static char add_alias_name[NAMEDATALEN];
static bool add_alias_internal(Node *n, List *names)
{
	if(n && IsA(n, RangeSubselect) && ((RangeSubselect*)n)->alias == NULL)
	{
		RangeSubselect *sub = (RangeSubselect*)n;
		Alias *alias = makeNode(Alias);
		ListCell *lc;
		bool used;
		do
		{
			used = false;
			++add_alias_name_idx;
			snprintf(add_alias_name, lengthof(add_alias_name)
				, "__AUTO_ADD_ALIAS_%d__", add_alias_name_idx);
			foreach(lc, names)
			{
				if(strcmp(add_alias_name, lfirst(lc)) == 0)
				{
					used = true;
					break;
				}
			}
		}while(used);
		alias->aliasname = pstrdup(add_alias_name);
		sub->alias = alias;
	}
	return node_tree_walker(n, add_alias_internal, names);
}

static void add_alias_if_need(List *parseTree)
{
	List *names = NIL;
	get_all_names((Node*)parseTree, &names);
	add_alias_name_idx = 0;
	add_alias_internal((Node*)parseTree, names);
	list_free(names);
}

static Node* make_any_sublink(Node *testexpr, const char *operName, Node *subselect, int location)
{
	SubLink *n = makeNode(SubLink);
	n->subLinkType = ANY_SUBLINK;
	n->testexpr = testexpr;
	n->operName = list_make1(makeString(pstrdup(operName)));
	n->subselect = subselect;
	n->location = location;
	return (Node*)n;
}

static void oracleInsertSelectOptions(SelectStmt *stmt,
									  SelectSortClause *sortClause, List *lockingClause,
									  Node *limitOffset, Node *limitCount,
									  WithClause *withClause,
									  core_yyscan_t yyscanner)
{
	List *sort_clause = NIL;
	if (sortClause)
	{
		if(sortClause->siblings_loc == -1)
		{
			/* no siblings */
			sort_clause = sortClause->sortClause;
		}else
		{
			Assert(sortClause->siblings_loc >= 0);
			if (stmt->ora_connect_by == NULL)
			{
				char *scanbuf = ora_yyget_extra(yyscanner)->core_yy_extra.scanbuf;
				/* no connect by */
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						  /* translator: first %s is typically the translation of "syntax error" */
						  errmsg("%s at or near \"%s\"", _("syntax error"), scanbuf + sortClause->siblings_loc),
						  parser_errposition(sortClause->siblings_loc)));
			}else if(stmt->ora_connect_by->sortClause)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple ORDER BY clauses not allowed"),
						 parser_errposition(sortClause->sort_loc)));
			}else
			{
				stmt->ora_connect_by->sortClause = sortClause->sortClause;
			}
		}
	}
	insertSelectOptions(stmt, sort_clause,
						lockingClause, limitOffset, limitCount, withClause, yyscanner);
}

static A_Indirection* listToIndirection(A_Indirection *in, ListCell *lc)
{	
	if(lc != NULL)
	{	
		A_Indirection *sub_in = makeNode(A_Indirection);
		sub_in->arg = (Node *)in;
		sub_in->indirection = list_make1(lfirst(lc));
		return listToIndirection(sub_in, lnext(lc));
	}
	return in;
}
