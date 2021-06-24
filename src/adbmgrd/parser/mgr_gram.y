%{

#include "postgres.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "nodes/pg_list.h"
#include "parser/mgr_node.h"
#include "parser/parser.h"
#include "parser/scanner.h"
#include "catalog/mgr_node.h"
#include "catalog/mgr_parm.h"
#include "catalog/mgr_updateparm.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"    /* For F_NAMEEQ	*/
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/monitor_job.h"
#include "catalog/monitor_jobitem.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "postmaster/bgworker.h" 

/*
 * The YY_EXTRA data that a flex scanner allows us to pass around.  Private
 * state needed for raw parsing/lexing goes here.
 */
typedef struct mgr_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	core_yy_extra_type core_yy_extra;

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */
} mgr_yy_extra_type;

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define mgr_yyget_extra(yyscanner) (*((mgr_yy_extra_type **) (yyscanner)))

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

#define YYMALLOC palloc
#define YYFREE   pfree

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)

#define GTMCOORD_TYPE      'G'
#define COORDINATOR_TYPE  'C'
#define DATANODE_TYPE     'D'

union YYSTYPE;					/* need forward reference for tok_is_keyword */

bool adbmonitor_start_daemon;

static void mgr_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner,
						 const char *msg);
static int mgr_yylex(union YYSTYPE *lvalp, YYLTYPE *llocp,
		   core_yyscan_t yyscanner);
List *mgr_parse_query(const char *query_string);
static Node* make_column_in(const char *col_name, List *values);
static Node* makeNode_RangeFunction(const char *func_name, List *func_args);
static Node* make_func_call(const char *func_name, List *func_args);
/* static List* make_start_agent_args(List *options); */
extern char *defGetString(DefElem *def);
static Node* make_ColumnRef(const char *col_name);
static Node* make_whereClause_for_datanode(char* node_type_str, List* node_name_list, char* like_expr);
static Node* make_whereClause_for_coord(char * node_type_str, List* node_name_list, char* like_expr);
static Node* make_whereClause_for_gtm(char * node_type_str, List* node_name_list, char* like_expr);
static void check_node_name_isvaild(char node_type, List* node_name_list);
static void check__name_isvaild(List *node_name_list);
static void check_host_name_isvaild(List *node_name_list);
static void check_job_name_isvaild(List *node_name_list);
static void check_jobitem_name_isvaild(List *node_name_list);
static void check_job_status_intbl(void);
extern char *mgr_get_mastername_by_nodename_type(char* nodename, char nodetype);
%}

%pure-parser
%expect 0
%name-prefix="mgr_yy"
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
	DefElem				*defelt;
	bool				boolean;
	List				*list;
	Node				*node;
	VariableSetStmt		*vsetstmt;
	Value				*value;
	RoleSpec			*rolespec;
}

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

%type <list>	stmtblock stmtmulti
%type <node>	stmt
%type <node>	AddHostStmt DropHostStmt ListHostStmt AlterHostStmt
				ListParmStmt StartAgentStmt AddNodeStmt StopAgentStmt
				DropNodeStmt AlterNodeStmt ListNodeStmt InitNodeStmt
				VariableSetStmt StartNodeMasterStmt StopNodeMasterStmt
				MonitorStmt FailoverStmt /* ConfigAllStmt */ DeploryStmt
				Gethostparm ListMonitor Gettopologyparm Update_host_config_value
				Get_host_threshold Get_alarm_info AppendNodeStmt
				AddUpdataparmStmt CleanAllStmt ResetUpdataparmStmt ShowStmt FlushStmt
				AddHbaStmt DropHbaStmt ListHbaStmt ListAclStmt
				CreateUserStmt DropUserStmt GrantStmt privilege username hostname
				AlterUserStmt AddJobitemStmt AlterJobitemStmt DropJobitemStmt ListJobStmt
				AddExtensionStmt DropExtensionStmt RemoveNodeStmt FailoverManualStmt SwitchoverStmt
				ZoneStmt GetBoottimeStmt

				ExpandNodeStmt CheckNodeStmt
				StartDoctorStmt StopDoctorStmt SetDoctorParamStmt ListDoctorParamStmt
%type <node>	ListNodeSize
%type <node>	opt_nodesize_with_list  opt_nodesize_with_list_items
// %type <boolean>	opt_pretty
%type <list>	dataNameList

%type <list>	general_options opt_general_options general_option_list HbaParaList
				AConstList targetList ObjList var_list NodeConstList set_parm_general_options
				OptRoleList name_list privilege_list username_list hostname_list
				AlterOptRoleList

%type <node>	general_option_item general_option_arg target_el
%type <node> 	var_value

%type <defelt>	CreateOptRoleElem AlterOptRoleElem

%type <ival>	Iconst SignedIconst opt_gtm_inner_type opt_dn_inner_type opt_general_force opt_cn_inner_type
		opt_slave_inner_type
%type <vsetstmt> set_rest set_rest_more
%type <value>	NumericOnly

%type <keyword>	unreserved_keyword reserved_keyword
%type <str>		Ident SConst ColLabel var_name opt_boolean_or_string
				NonReservedWord NonReservedWord_or_Sconst set_ident
				opt_password opt_stop_mode
				opt_general_all var_dotparam var_showparam
				sub_like_expr RoleId name ColId
%type <rolespec> RoleSpec
%type <chr>		node_type cluster_type

%token<keyword>	ADD_P DEPLOY DROP ALTER LIST CREATE ACL CLUSTER
%token<keyword>	IF_P EXISTS NOT FOR IN_P MINVALUE MAXVALUE
%token<keyword>	FALSE_P TRUE_P
%token<keyword>	HOST MONITOR PARAM_P HBA HA BOOTTIME READONLY
%token<keyword>	INIT MASTER SLAVE ALL NODE COORDINATOR DATANODE GTMCOORD
%token<keyword>	PRETTY SIZE WITH SLINK
%token<keyword> PASSWORD CLEAN RESET WHERE ROW_ID
%token<keyword> START AGENT STOP FAILOVER
%token<keyword> SET TO ON OFF
%token<keyword> APPEND CONFIG MODE FAST SMART IMMEDIATE S I F FORCE SHOW FLUSH
%token<keyword> GRANT REVOKE FROM ITEM JOB EXTENSION REMOVE DATA_CHECKSUMS
%token<keyword> EXPAND ACTIVATE CHECKOUT STATUS RECOVER BASEBACKUP FAIL SUCCESS DOPROMOTE SLOT DOCHECK END SLEEP
%token<keyword> ZONE CLEAR
%token<keyword> PROMOTE ADBMGR REWIND SWITCHOVER

/* for ADB monitor*/
%token<keyword> GET_HOST_LIST_ALL GET_HOST_LIST_SPEC
				GET_HOST_HISTORY_USAGE
				GET_HOST_HISTORY_USAGE_BY_TIME_PERIOD
				GET_ALL_NODENAME_IN_SPEC_HOST
				GET_AGTM_NODE_TOPOLOGY GET_COORDINATOR_NODE_TOPOLOGY GET_DATANODE_NODE_TOPOLOGY
				GET_CLUSTER_SUMMARY GET_DATABASE_TPS_QPS GET_CLUSTER_HEADPAGE_LINE
				GET_DATABASE_TPS_QPS_INTERVAL_TIME MONITOR_DATABASETPS_FUNC_BY_TIME_PERIOD
				GET_DATABASE_SUMMARY GET_SLOWLOG GET_USER_INFO UPDATE_USER GET_SLOWLOG_COUNT
				UPDATE_THRESHOLD_VALUE UPDATE_PASSWORD CHECK_USER USER
				GET_THRESHOLD_TYPE GET_THRESHOLD_ALL_TYPE CHECK_PASSWORD GET_DB_THRESHOLD_ALL_TYPE
				GET_ALARM_INFO_ASC GET_ALARM_INFO_DESC RESOLVE_ALARM GET_ALARM_INFO_COUNT
				GET_CLUSTER_TPS_QPS GET_CLUSTER_CONNECT_DBSIZE_INDEXSIZE
/* ADB_DOCTOR_BEGIN */
%token<keyword>	DOCTOR
/* ADB_DOCTOR_END */

%%
/*
 *	The target production for the whole parse.
 */
stmtblock:	stmtmulti
			{
				mgr_yyget_extra(yyscanner)->parsetree = $1;
			}
		;

/* the thrashing around here is to discard "empty" statements... */
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

stmt :
	  AddHostStmt
	| AlterUserStmt
	| CreateUserStmt
	| DropUserStmt
	| DropHostStmt
	| ListHostStmt
	| AlterHostStmt
	| StartAgentStmt
	| StopAgentStmt
	| ListAclStmt
	| ListMonitor
	| ListParmStmt
	| AddNodeStmt
	| AlterNodeStmt
	| DropNodeStmt
	| ListNodeStmt
	| ListNodeSize
	| MonitorStmt
	| VariableSetStmt
	| InitNodeStmt
	| StartNodeMasterStmt
	| StopNodeMasterStmt
	| FailoverStmt
/*	| ConfigAllStmt */
	| DeploryStmt
	| Gethostparm     /* for ADB monitor host page */
	| Gettopologyparm /* for ADB monitor home page */
	| Update_host_config_value
	| Get_host_threshold
	| GrantStmt
	| Get_alarm_info
	| AppendNodeStmt
	| AddUpdataparmStmt
	| ResetUpdataparmStmt
	| CleanAllStmt
	| ShowStmt
	| FlushStmt
	| AddHbaStmt
	| DropHbaStmt
	| ListHbaStmt
	| AddJobitemStmt
	| AlterJobitemStmt
	| DropJobitemStmt
	| ListJobStmt
	| AddExtensionStmt
	| DropExtensionStmt
	| RemoveNodeStmt
	| ExpandNodeStmt
	| CheckNodeStmt
	| FailoverManualStmt
	| SwitchoverStmt
	| ZoneStmt
	| GetBoottimeStmt
	/* DOCTOR BEGIN */
	| SetDoctorParamStmt
	| ListDoctorParamStmt
	| StartDoctorStmt
	| StopDoctorStmt
/* ADB END */
	| /* empty */
		{ $$ = NULL; }
	;

AlterUserStmt:
			ALTER USER RoleSpec AlterOptRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1; /* add, if there are members */
					n->options = $4;
					$$ = (Node *)n;
				 }
			;

GrantStmt:
		GRANT privilege_list TO username_list
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *command_type = makeIntConst(PRIV_GRANT, -1);
			Node *privs = makeAArrayExpr($2, @2);
			Node *names = makeAArrayExpr($4, @4);
			List *args = list_make3(command_type, privs, names);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_priv_manage", args));
			$$ = (Node*)stmt;
		}
		| GRANT ALL TO username_list
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *command_type = makeIntConst(PRIV_GRANT, -1);
			Node *names = makeAArrayExpr($4, @4);
			List *args = list_make2(command_type, names);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_priv_all_to_username", args));
			$$ = (Node*)stmt;
		}
		| GRANT privilege_list TO ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *command_type = makeIntConst(PRIV_GRANT, -1);
			Node *privs = makeAArrayExpr($2, @2);
			List *args = list_make2(command_type, privs);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_priv_list_to_all", args));
			$$ = (Node*)stmt;
		}
		| REVOKE privilege_list FROM username_list
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *command_type = makeIntConst(PRIV_REVOKE, -1);
			Node *privs = makeAArrayExpr($2, @2);
			Node *names = makeAArrayExpr($4, @4);
			List *args = list_make3(command_type, privs, names);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_priv_manage", args));
			$$ = (Node*)stmt;
		}
		| REVOKE ALL FROM username_list
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *command_type = makeIntConst(PRIV_REVOKE, -1);
			Node *names = makeAArrayExpr($4, @4);
			List *args = list_make2(command_type, names);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_priv_all_to_username", args));
			$$ = (Node*)stmt;
		}
		| REVOKE privilege_list FROM ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *command_type = makeIntConst(PRIV_REVOKE, -1);
			Node *privs = makeAArrayExpr($2, @2);
			List *args = list_make2(command_type, privs);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_priv_list_to_all", args));
			$$ = (Node*)stmt;
		}
		;

privilege_list:
			privilege
				{ $$ = list_make1($1); }
			| privilege_list ',' privilege
				{ $$ = lappend($1, $3); }
			;

privilege : IDENT { $$ = makeStringConst(pstrdup($1), @1); };
			| unreserved_keyword { $$ = makeStringConst(pstrdup($1), @1); }
			;

username_list:
			username
				{ $$ = list_make1($1); }
			| username_list ',' username
				{ $$ = lappend($1, $3); }
			;

username : IDENT { $$ = makeStringConst(pstrdup($1), @1); };

DropUserStmt:
			DROP USER name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = false;
					n->roles = $3;
					$$ = (Node *)n;
				}
			;

name_list:
		name { $$ = list_make1(makeString($1)); }
		| name_list ',' name
			{ $$ = lappend($1, makeString($3)); }
		;

name: ColId     { $$ = $1; };

ColId: IDENT     { $$ = $1; };

CreateUserStmt:
			CREATE USER RoleId OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_USER;
					n->role = $3;
					n->options = $4;
					$$ = (Node *)n;
				}
			;

OptRoleList:
			OptRoleList CreateOptRoleElem     { $$ = lappend($1, $2); }
			| /* EMPTY */                     { $$ = NIL; }
			;

AlterOptRoleList:
			AlterOptRoleElem  { $$ = list_make1($1); }
			;

CreateOptRoleElem:
			AlterOptRoleElem  { $$ = $1; }
			;

AlterOptRoleElem:
			PASSWORD SConst
				{
					$$ = makeDefElem("password", (Node *)makeString($2), @1);
				}
				;

/*			| IDENT
 *				{
 *					if (strcmp($1, "superuser") == 0)
 *						$$ = makeDefElem("superuser", (Node *)makeInteger(true));
 *					else if (strcmp($1, "nosuperuser") == 0)
 *						$$ = makeDefElem("superuser", (Node *)makeInteger(false));
 *					else
 *						ereport(ERROR,
 *								(errcode(ERRCODE_SYNTAX_ERROR),
 *								 errmsg("unrecognized role option \"%s\"", $1),
 *									parser_errposition(@1)));
 *				}
 */

ExpandNodeStmt:
		EXPAND DATANODE MASTER Ident TO Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			args = lappend(args,makeStringConst($6, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_dnmaster", args));
			$$ = (Node*)stmt;
		}
		|
		EXPAND ACTIVATE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_activate_dnmaster", NULL));
			$$ = (Node*)stmt;
		}
		|
		EXPAND RECOVER BASEBACKUP FAIL Ident TO Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = lappend(args,makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_recover_backup_fail", args));
			$$ = (Node*)stmt;
		}
		|
		EXPAND RECOVER BASEBACKUP SUCCESS Ident TO Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = lappend(args,makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_recover_backup_suc", args));
			$$ = (Node*)stmt;
		}
		|
		EXPAND ACTIVATE RECOVER DOPROMOTE SUCCESS Ident
		{

			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($6, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_activate_recover_promote_suc", args));
			$$ = (Node*)stmt;
		}
		|
		EXPAND DOCHECK STATUS
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_check_status", NULL));
			$$ = (Node*)stmt;
		}
		|
		EXPAND SHOW STATUS
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_show_status", NULL));
			$$ = (Node*)stmt;
		}
		|
		EXPAND CLEAN
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_expand_clean", NULL));
			$$ = (Node*)stmt;
		}
		|
		EXPAND SLEEP ICONST
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("pg_sleep", args));
			$$ = (Node*)stmt;
		};

CheckNodeStmt:
		CHECKOUT DATANODE SLAVE STATUS
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("checkout_dnslave_status"), -1));
			$$ = (Node*)stmt;
		}

AppendNodeStmt:
		APPEND DATANODE MASTER Ident opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_dnmaster", args));
			with_data_checksums = false;
			g_initall_options = $5;
			$$ = (Node*)stmt;
		}
		|	APPEND DATANODE MASTER Ident DATA_CHECKSUMS opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_dnmaster", args));
			with_data_checksums = true;
			g_initall_options = $6;
			$$ = (Node*)stmt;
		}
		| APPEND DATANODE SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_dnslave", args));
			$$ = (Node*)stmt;
		}
		| APPEND COORDINATOR MASTER Ident opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_coordmaster", args));
			with_data_checksums = false;
			g_initall_options = $5;
			$$ = (Node*)stmt;
		}
		| APPEND COORDINATOR MASTER Ident DATA_CHECKSUMS opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_coordmaster", args));
			with_data_checksums = true;
			g_initall_options = $6;
			$$ = (Node*)stmt;
		}
		| APPEND COORDINATOR SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			char *mastername = mgr_get_mastername_by_nodename_type($4, CNDN_TYPE_COORDINATOR_SLAVE);
			List *args = list_make1(makeStringConst(mastername, -1));
			args = lappend(args, makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_coord_to_coord", args));
			$$ = (Node*)stmt;
		}
		| APPEND GTMCOORD SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_agtmslave", args));
			$$ = (Node*)stmt;
		}
		|APPEND COORDINATOR Ident FOR Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = lappend(args, makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_coord_to_coord", args));
			$$ = (Node*)stmt;
		}
		|APPEND ACTIVATE COORDINATOR Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_append_activate_coord", args));
			$$ = (Node*)stmt;
		}
		;

Get_alarm_info:
		GET_ALARM_INFO_ASC '(' Ident ',' Ident ',' SConst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			args = lappend(args, makeIntConst($13, -1));
			args = lappend(args, makeIntConst($15, -1));
			args = lappend(args, makeIntConst($17, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_alarm_info_asc", args));
			$$ = (Node*)stmt;
		}
		| GET_ALARM_INFO_DESC '(' Ident ',' Ident ',' SConst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			args = lappend(args, makeIntConst($13, -1));
			args = lappend(args, makeIntConst($15, -1));
			args = lappend(args, makeIntConst($17, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_alarm_info_desc", args));
			$$ = (Node*)stmt;
		}
		| GET_ALARM_INFO_COUNT '(' Ident ',' Ident ',' SConst ',' SignedIconst ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			args = lappend(args, makeIntConst($13, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_alarm_info_count", args));
			$$ = (Node*)stmt;
		}
		|RESOLVE_ALARM '(' SignedIconst ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args,makeStringConst($5, -1));
			args = lappend(args,makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("resolve_alarm", args));
			$$ = (Node*)stmt;
		};

Gettopologyparm:
        GET_AGTM_NODE_TOPOLOGY
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_agtm_node_topology"), -1));
            $$ = (Node*)stmt;
        }
        | GET_COORDINATOR_NODE_TOPOLOGY
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_coordinator_node_topology"), -1));
            $$ = (Node*)stmt;
        }
        | GET_DATANODE_NODE_TOPOLOGY
        {
            SelectStmt *stmt = makeNode(SelectStmt);
            stmt->targetList = list_make1(make_star_target(-1));
            stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_datanode_node_topology"), -1));
            $$ = (Node*)stmt;
        };

Gethostparm:
		GET_HOST_LIST_ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_all_host_parm"), -1));
			$$ = (Node*)stmt;
		}
		| GET_HOST_LIST_SPEC '(' AConstList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_spec_host_parm"), -1));
			stmt->whereClause = make_column_in("hostname", $3);
			$$ = (Node*)stmt;
		}
		| GET_HOST_HISTORY_USAGE '(' Ident ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeIntConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_host_history_usage", args));
			$$ = (Node*)stmt;
		}
		| GET_HOST_HISTORY_USAGE_BY_TIME_PERIOD '(' Ident ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_host_history_usage_by_time_period", args));
			$$ = (Node*)stmt;
		}
        | GET_ALL_NODENAME_IN_SPEC_HOST '(' Ident ')'
        {
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_all_nodename_in_spec_host", args));
			$$ = (Node*)stmt;
        };

Update_host_config_value:
		UPDATE_THRESHOLD_VALUE '(' SignedIconst ',' SignedIconst ',' SignedIconst ',' SignedIconst')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args, makeIntConst($5, -1));
			args = lappend(args, makeIntConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("update_threshold_value", args));
			$$ = (Node*)stmt;
		};

Get_host_threshold:
		GET_THRESHOLD_TYPE '(' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("get_threshold_type", args));
			$$ = (Node*)stmt;
		}
		| GET_THRESHOLD_ALL_TYPE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_threshold_all_type"), -1));
			$$ = (Node*)stmt;
		}
		|	GET_DB_THRESHOLD_ALL_TYPE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("get_db_threshold_all_type"), -1));
			$$ = (Node*)stmt;
		}
		;

/*ConfigAllStmt:
 *	CONFIG ALL
 *	{
 *		SelectStmt *stmt = makeNode(SelectStmt);
 *		stmt->targetList = list_make1(make_star_target(-1));
 *		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_configure_nodes_all", NULL));
 *		$$ = (Node*)stmt;
 *	};
 */

MonitorStmt:
		MONITOR opt_general_all
		{
			SelectStmt *stmt;
			stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_all"), -1));
			$$ = (Node*)stmt;
		}
		| MONITOR GTMCOORD opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_gtmcoord_all", NULL));
			$$ = (Node*)stmt;
		}
		| MONITOR DATANODE opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_datanode_all", NULL));
			$$ = (Node*)stmt;
		}
		| MONITOR node_type NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = lcons(makeIntConst($2, @2), $3);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_nodetype_namelist", args));
			$$ = (Node*)stmt;
		}
		| MONITOR node_type opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *arg = list_make1(makeIntConst($2,-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_nodetype_all", arg));
			$$ = (Node*)stmt;
		}
		| MONITOR AGENT opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_agent_all", NULL));
			$$ = (Node*)stmt;
		}
		| MONITOR AGENT hostname_list
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *hostnames = makeAArrayExpr($3, @3);
			List *arg = list_make1(hostnames);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_agent_hostlist", arg));
			$$ = (Node*)stmt;
		}
		| MONITOR HA
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("ha"), -1));
			$$ = (Node*)stmt;
		}
		| MONITOR HA '(' targetList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("ha"), -1));
			$$ = (Node*)stmt;
		}
	| MONITOR HA AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("ha"), -1));
			stmt->whereClause = make_column_in("nodename", $3);
			$$ = (Node*)stmt;
			check__name_isvaild($3);
		}
	| MONITOR HA'(' targetList ')' AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("ha"), -1));
			stmt->whereClause = make_column_in("nodename", $6);
			$$ = (Node*)stmt;
			check__name_isvaild($6);
		}
		| MONITOR HA ZONE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *arg = list_make1(makeStringConst($4, @4));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_ha_zone", arg));
			$$ = (Node*)stmt;
		}
		|
		MONITOR ZONE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *arg = list_make1(makeStringConst($3, @3));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_monitor_zone_all", arg));
			$$ = (Node*)stmt;
		}
		;

hostname_list:
			hostname
				{ $$ = list_make1($1); }
			| hostname_list ',' hostname
				{ $$ = lappend($1, $3); }
			;

hostname : IDENT { $$ = makeStringConst(pstrdup($1), @1); };

node_type:
		DATANODE MASTER			{$$ = CNDN_TYPE_DATANODE_MASTER;}
		| DATANODE SLAVE		{$$ = CNDN_TYPE_DATANODE_SLAVE;}
		| COORDINATOR	MASTER		{$$ = CNDN_TYPE_COORDINATOR_MASTER;}
		| COORDINATOR	SLAVE		{$$ = CNDN_TYPE_COORDINATOR_SLAVE;}
		| GTMCOORD MASTER			{$$ = CNDN_TYPE_GTM_COOR_MASTER;}
		| GTMCOORD SLAVE				{$$ = CNDN_TYPE_GTM_COOR_SLAVE;}
		;

opt_general_all:
		ALL 		{ $$ = pstrdup("all"); }
		| /*empty */{ $$ = pstrdup("all"); }
		;

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			;

set_rest: set_rest_more { $$ = $1; };

set_rest_more:
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
			;

var_name:	IDENT									{ $$ = $1; }
			| var_name '.' IDENT
				{
					$$ = palloc(strlen($1) + strlen($3) + 2);
					sprintf($$, "%s.%s", $1, $3);
				}
			;
var_dotparam:
			Ident '.' Ident
				{
					$$ = palloc(strlen($1) + strlen($3) + 2);
					sprintf($$, "%s.%s", $1, $3);
				}
			;
var_showparam:
			Ident							{ $$ = $1; }
			| var_dotparam					{ $$ = $1; }
			;
var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
			;

var_value:	opt_boolean_or_string  	{ $$ = makeStringConst($1, @1); }
			| NumericOnly    		{ $$ = makeAConst($1, @1); }
			;
opt_boolean_or_string:
			TRUE_P									{ $$ = "true"; }
			| FALSE_P								{ $$ = "false"; }
			| ON									{ $$ = "on"; }
			/*
			 * OFF is also accepted as a boolean value, but is handled by
			 * the NonReservedWord rule.  The action for booleans and strings
			 * is the same, so we don''t need to distinguish them here.
			 */
			| NonReservedWord_or_Sconst				{ $$ = $1; }
			;

NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1; }
			| SConst								{ $$ = $1; }
			;

RoleId:		NonReservedWord							{ $$ = $1; };

RoleSpec:	NonReservedWord
			{
				/*
					* "public" and "none" are not keywords, but they must
					* be treated specially here.
					*/
				RoleSpec *n;
				if (strcmp($1, "public") == 0)
				{
					n = (RoleSpec *) makeRoleSpec(ROLESPEC_PUBLIC, @1);
					n->roletype = ROLESPEC_PUBLIC;
				}
				else if (strcmp($1, "none") == 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_RESERVED_NAME),
								errmsg("role name \"%s\" is reserved",
									"none"),
								parser_errposition(@1)));
				}
				else if (strcmp($1, "current_user") == 0)
				{
					n = makeRoleSpec(ROLESPEC_CURRENT_USER, @1);
				}
				else if (strcmp($1, "session_user") == 0)
				{
					n = makeRoleSpec(ROLESPEC_SESSION_USER, @1);
				}
				else
				{
					n = makeRoleSpec(ROLESPEC_CSTRING, @1);
					n->rolename = pstrdup($1);
				}
				$$ = n;
			}
		;

NonReservedWord:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
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

AddHostStmt:
	  ADD_P HOST Ident opt_general_options
		{
			MGRAddHost *node = makeNode(MGRAddHost);
			node->if_not_exists = false;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
		}
	| ADD_P HOST IF_P NOT EXISTS Ident opt_general_options
		{
			MGRAddHost *node = makeNode(MGRAddHost);
			node->if_not_exists = true;
			node->name = $6;
			node->options = $7;
			$$ = (Node*)node;
		}
	;

opt_general_options:
	  general_options	{ $$ = $1; }
	| /* empty */		{ $$ = NIL; }
	;

set_parm_general_options:
	  general_options	{ $$ = $1; }
	;

general_options: '(' general_option_list ')'
		{
			$$ = $2;
		}
	;

general_option_list:
	  general_option_item
		{
			$$ = list_make1($1);
		}
	| general_option_list ',' general_option_item
		{
			$$ = lappend($1, $3);
		}
	;

general_option_item:
	  ColLabel general_option_arg		{ $$ = (Node*)makeDefElem($1, $2, @1); }
	| ColLabel '=' general_option_arg	{ $$ = (Node*)makeDefElem($1, $3, @1); }
	| ColLabel 							{ $$ = (Node*)makeDefElem($1, NULL, @1); }
	| var_dotparam						{ $$ = (Node*)makeDefElem($1, NULL, @1); }
	| var_dotparam '=' general_option_arg { $$ = (Node*)makeDefElem($1, $3, @1); }

	;
/*conntype database role addr auth_method*/

general_option_arg:
	  Ident								{ $$ = (Node*)makeString($1); }
	| SConst							{ $$ = (Node*)makeString($1); }
	| SignedIconst						{ $$ = (Node*)makeInteger($1); }
	| FCONST							{ $$ = (Node*)makeFloat($1); }
	| reserved_keyword					{ $$ = (Node*)makeString(pstrdup($1)); }
	;

DropHostStmt:
	  DROP HOST ObjList
		{
			MGRDropHost *node = makeNode(MGRDropHost);
			node->if_exists = false;
			node->hosts = $3;
			$$ = (Node*)node;
		}
	| DROP HOST IF_P EXISTS ObjList
		{
			MGRDropHost *node = makeNode(MGRDropHost);
			node->if_exists = true;
			node->hosts = $5;
			$$ = (Node*)node;
		}
	;

ObjList:
	  ObjList ',' Ident
		{
			$$ = lappend($1, makeString($3));
		}
	| Ident
		{
			$$ = list_make1(makeString($1));
		}
	;

ListHostStmt:
	  LIST HOST
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			$$ = (Node*)stmt;
		}
	| LIST HOST '(' targetList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			$$ = (Node*)stmt;
		}
	| LIST HOST AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			stmt->whereClause = make_column_in("name", $3);
			$$ = (Node*)stmt;

			check_host_name_isvaild($3);
		}
	| LIST HOST '(' targetList ')' AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("host"), -1));
			stmt->whereClause = make_column_in("name", $6);
			$$ = (Node*)stmt;

			check_host_name_isvaild($6);
		}
	;

AConstList:
	  AConstList ',' Ident	{ $$ = lappend($1, makeAConst(makeString($3), @3)); }
	| Ident						{ $$ = list_make1(makeAConst(makeString($1), @1)); }
	;
NodeConstList:
	  NodeConstList ',' Ident	{ $$ = lappend($1, makeStringConst($3, @3)); }
	| Ident						{ $$ = list_make1(makeStringConst($1, @1)); }
	;
targetList:
	  targetList ',' target_el	{ $$ = lappend($1, $3); }
	| target_el					{ $$ = list_make1($1); }
	;

target_el:
	  Ident
		{
			ResTarget *target = makeNode(ResTarget);
			ColumnRef *col = makeNode(ColumnRef);
			col->fields = list_make1(makeString($1));
			col->location = @1;
			target->val = (Node*)col;
			target->location = @1;
			$$ = (Node*)target;
		}
	| '*'
		{
			$$ = (Node*)make_star_target(@1);
		}
	;

Ident:
	  IDENT					{ $$ = $1; }
	| unreserved_keyword	{ $$ = pstrdup($1); }
	;
set_ident:
	 Ident					{ $$ = $1; }
	|	ALL					{ $$ = pstrdup("*"); }
	;
SConst: SCONST				{ $$ = $1; }
Iconst: ICONST				{ $$ = $1; }

SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;

ColLabel:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| reserved_keyword						{ $$ = pstrdup($1); }
		;

AlterHostStmt:
        ALTER HOST Ident opt_general_options
		{
			MGRAlterHost *node = makeNode(MGRAlterHost);
			node->if_not_exists = false;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
		}
	;

StartAgentStmt:
		START AGENT ALL opt_password
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_agent_all", args));
			$$ = (Node*)stmt;
		}
		| START AGENT hostname_list opt_password
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *password = makeStringConst($4, -1);
			Node *hostnames = makeAArrayExpr($3, @3);
			List *args = list_make2(password, hostnames);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_agent_hostnamelist", args));
			$$ = (Node*)stmt;
		}
		;

StopAgentStmt:
		STOP AGENT ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_agent_all", NULL));
			$$ = (Node*)stmt;
		}
		| STOP AGENT hostname_list
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *hostnames = makeAArrayExpr($3, @3);
			List *arg = list_make1(hostnames);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_agent_hostnamelist", arg));
			$$ = (Node*)stmt;
		}
		;

/* parm start*/
AddUpdataparmStmt:
		SET GTMCOORD opt_gtm_inner_type Ident set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	SET GTMCOORD opt_gtm_inner_type Ident set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force= true;
				$$ = (Node*)node;
		}
	|	SET GTMCOORD opt_gtm_inner_type ALL set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	SET GTMCOORD opt_gtm_inner_type ALL set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $5;
				node->is_force= true;
				$$ = (Node*)node;
		}
	|	SET GTMCOORD ALL set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = CNDN_TYPE_GTMCOOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	SET GTMCOORD ALL set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = CNDN_TYPE_GTMCOOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force= true;
				$$ = (Node*)node;
		}
	| SET DATANODE opt_dn_inner_type set_ident set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| SET DATANODE opt_dn_inner_type set_ident set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| SET DATANODE ALL set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = CNDN_TYPE_DATANODE;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| SET DATANODE ALL set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = CNDN_TYPE_DATANODE;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| SET COORDINATOR opt_cn_inner_type set_ident set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| SET COORDINATOR opt_cn_inner_type set_ident set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| SET COORDINATOR ALL set_parm_general_options
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| SET COORDINATOR ALL set_parm_general_options FORCE
		{
				MGRUpdateparm *node = makeNode(MGRUpdateparm);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| SET CLUSTER INIT
		{
			MGRSetClusterInit *node = makeNode(MGRSetClusterInit);
			$$ = (Node*)node;
		}
		;
ResetUpdataparmStmt:
		RESET GTMCOORD opt_gtm_inner_type Ident set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	RESET GTMCOORD opt_gtm_inner_type Ident set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	|	RESET GTMCOORD opt_gtm_inner_type ALL set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	|	RESET GTMCOORD opt_gtm_inner_type ALL set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = $3;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET GTMCOORD ALL set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = CNDN_TYPE_GTMCOOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET GTMCOORD ALL set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_GTMCOOR;
				node->nodetype = CNDN_TYPE_GTMCOOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET DATANODE opt_dn_inner_type set_ident set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET DATANODE opt_dn_inner_type set_ident set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET DATANODE ALL set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = CNDN_TYPE_DATANODE;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET DATANODE ALL set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_DATANODE;
				node->nodetype = CNDN_TYPE_DATANODE;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET COORDINATOR opt_cn_inner_type set_ident set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET COORDINATOR opt_cn_inner_type set_ident set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = $3;
				node->nodename = $4;
				node->options = $5;
				node->is_force = true;
				$$ = (Node*)node;
		}
	| RESET COORDINATOR ALL set_parm_general_options
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = false;
				$$ = (Node*)node;
		}
	| RESET COORDINATOR ALL set_parm_general_options FORCE
		{
				MGRUpdateparmReset *node = makeNode(MGRUpdateparmReset);
				node->parmtype = PARM_TYPE_COORDINATOR;
				node->nodetype = CNDN_TYPE_COORDINATOR;
				node->nodename = MACRO_STAND_FOR_ALL_NODENAME;
				node->options = $4;
				node->is_force = true;
				$$ = (Node*)node;
		}
		;

ListParmStmt:
	  LIST PARAM_P
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));
			$$ = (Node*)stmt;
		}
	| LIST PARAM_P node_type Ident sub_like_expr
		{
			StringInfoData like_expr;
			List* node_name;
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));

			node_name = (List*)list_make1(makeStringConst($4, -1));
			check_node_name_isvaild($3, node_name);

			initStringInfo(&like_expr);
			if (strcmp($5, "NULL") == 0)
				appendStringInfo(&like_expr, "%%%%");
			else
				appendStringInfo(&like_expr, "%%%s%%", $5);
			switch ($3)
			{
				case CNDN_TYPE_DATANODE_MASTER:
						stmt->whereClause = make_whereClause_for_datanode("datanode master", node_name, like_expr.data);
						break;
				case CNDN_TYPE_DATANODE_SLAVE:
						stmt->whereClause = make_whereClause_for_datanode("datanode slave", node_name, like_expr.data);
						break;
				case CNDN_TYPE_COORDINATOR_MASTER:
						stmt->whereClause = make_whereClause_for_coord("coordinator master", node_name, like_expr.data);
						break;
				case CNDN_TYPE_COORDINATOR_SLAVE:
						stmt->whereClause = make_whereClause_for_coord("coordinator slave", node_name, like_expr.data);
						break;
				case CNDN_TYPE_GTM_COOR_MASTER:
						stmt->whereClause = make_whereClause_for_gtm("gtmcoord master", node_name, like_expr.data);
						break;
				case CNDN_TYPE_GTM_COOR_SLAVE:
						stmt->whereClause = make_whereClause_for_gtm("gtmcoord slave", node_name, like_expr.data);
						break;
				default:
						break;
			}

			$$ = (Node*)stmt;
		}
	| LIST PARAM_P cluster_type ALL sub_like_expr
	{
			StringInfoData like_expr;
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("updateparm"), -1));

			initStringInfo(&like_expr);

			if (strcmp($5, "NULL") == 0)
				appendStringInfo(&like_expr, "%%%%");
			else
				appendStringInfo(&like_expr, "%%%s%%", $5);

			switch ($3)
			{
				case GTMCOORD_TYPE:
					stmt->whereClause =
						(Node *)(Node *)makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
									make_ColumnRef("nodetype"),
									makeStringConst(pstrdup("gtmcoord"), -1), -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~~",
									make_ColumnRef("key"),
									makeStringConst(pstrdup(like_expr.data), -1), -1),
									-1);
					break;
				case COORDINATOR_TYPE:
					stmt->whereClause =
						(Node *)(Node *)makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
									make_ColumnRef("nodetype"),
									makeStringConst(pstrdup("coordinator"), -1), -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~~",
									make_ColumnRef("key"),
									makeStringConst(pstrdup(like_expr.data), -1), -1),
									-1);
					break;
				case DATANODE_TYPE:
					stmt->whereClause =
						(Node *)(Node *)makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
									make_ColumnRef("nodetype"),
									makeStringConst(pstrdup("datanode"), -1), -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~~",
									make_ColumnRef("key"),
									makeStringConst(pstrdup(like_expr.data), -1), -1),
									-1);
					break;
				case CNDN_TYPE_COORDINATOR_MASTER:
					stmt->whereClause =
					(Node *)makeAndExpr(
						(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
								make_ColumnRef("nodetype"),
								makeStringConst(pstrdup("coordinator master"), -1),-1),
						(Node *) makeSimpleA_Expr(AEXPR_OP, "~~",
										make_ColumnRef("key"),
										makeStringConst(pstrdup(like_expr.data), -1), -1),
										-1);
					break;
				case CNDN_TYPE_COORDINATOR_SLAVE:
					stmt->whereClause =
					(Node *)makeAndExpr(
						(Node *)makeOrExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
										make_ColumnRef("nodetype"),
										makeStringConst(pstrdup("coordinator slave"), -1),-1),
							(Node *) makeAndExpr(
								(Node *) makeSimpleA_Expr(AEXPR_OP, "=",
											make_ColumnRef("nodename"),
											makeStringConst(pstrdup("*"), -1), -1),
								(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
											make_ColumnRef("nodetype"),
											makeStringConst(pstrdup("coordinator master"), -1), -1),
											-1),-1),
						(Node *)makeSimpleA_Expr(AEXPR_OP, "~~",
								make_ColumnRef("key"),
								makeStringConst(pstrdup(like_expr.data), -1), -1),
								-1);
					break;
				case CNDN_TYPE_DATANODE_MASTER:
					stmt->whereClause =
					(Node *)makeAndExpr(
						(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
								make_ColumnRef("nodetype"),
								makeStringConst(pstrdup("datanode master"), -1),-1),
						(Node *) makeSimpleA_Expr(AEXPR_OP, "~~",
										make_ColumnRef("key"),
										makeStringConst(pstrdup(like_expr.data), -1), -1),
										-1);
					break;
				case CNDN_TYPE_DATANODE_SLAVE:
					stmt->whereClause =
					(Node *)makeAndExpr(
						(Node *)makeOrExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
										make_ColumnRef("nodetype"),
										makeStringConst(pstrdup("datanode slave"), -1),-1),
							(Node *) makeAndExpr(
								(Node *) makeSimpleA_Expr(AEXPR_OP, "=",
											make_ColumnRef("nodename"),
											makeStringConst(pstrdup("*"), -1), -1),
								(Node *) makeSimpleA_Expr(AEXPR_OP, "~",
											make_ColumnRef("nodetype"),
											makeStringConst(pstrdup("datanode master"), -1), -1),
											-1),-1),
						(Node *)makeSimpleA_Expr(AEXPR_OP, "~~",
								make_ColumnRef("key"),
								makeStringConst(pstrdup(like_expr.data), -1), -1),
								-1);
					break;
				default:
					break;
			}

			$$ = (Node*)stmt;
		}
	;

/* parm end*/

CleanAllStmt:
		CLEAN ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_clean_all", NULL));
			$$ = (Node*)stmt;
		}
	| CLEAN ZONE Ident
		{
			List *arg = list_make1(makeStringConst($3, @3));
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_clean_zone_all", arg));
			$$ = (Node*)stmt;
		}	
	| CLEAN GTMCOORD opt_gtm_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args,makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_clean_node", args));
			$$ = (Node*)stmt;
		}
	| CLEAN COORDINATOR opt_cn_inner_type NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = list_concat(args, $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_clean_node", args));
			$$ = (Node*)stmt;
		}
	| CLEAN DATANODE opt_dn_inner_type NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = list_concat(args, $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_clean_node", args));
			$$ = (Node*)stmt;
		}
	| CLEAN MONITOR ICONST
		{
			MonitorDeleteData *node = makeNode(MonitorDeleteData);
			node->days = $3;
			$$ = (Node*)node;
		}
	;
/*hba start*/

AddHbaStmt:
	ADD_P HBA GTMCOORD set_ident '(' NodeConstList ')'
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		List *args = lappend($6, makeStringConst($4,@4));
		args = lappend(args, makeIntConst(CNDN_TYPE_GTMCOOR,@3));
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_add_hba", args));
		$$ = (Node*)stmt;
	}
	|	ADD_P HBA COORDINATOR set_ident '(' NodeConstList ')'
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		List *args = lappend($6, makeStringConst($4,@4));
		args = lappend(args, makeIntConst(CNDN_TYPE_COORDINATOR,@3));
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_add_hba", args));
		$$ = (Node*)stmt;
	}
	|	ADD_P HBA DATANODE set_ident '(' NodeConstList ')'
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		List *args = lappend($6, makeStringConst($4,@4));
		args = lappend(args, makeIntConst(CNDN_TYPE_DATANODE,@3));
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_add_hba", args));
		$$ = (Node*)stmt;
	}
	;

DropHbaStmt:
	DROP HBA set_ident HbaParaList
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		List *args = lappend($4, makeStringConst($3,@3));
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_drop_hba", args));
		$$ = (Node*)stmt;
	}
	;

ListHbaStmt:
	LIST HBA
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("hba"), -1));
		$$ = (Node*)stmt;
	}
	| LIST HBA NodeConstList
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_list_hba_by_name", $3));
		$$ = (Node*)stmt;
	}

HbaParaList:
	'(' NodeConstList ')' 	{$$ = $2;}
	| /*empty*/             {$$ = NIL;}
	;

/*hba end*/
/* gtmcoord/coordinator/datanode
*/

AddNodeStmt:
	  ADD_P GTMCOORD MASTER Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->nodetype = CNDN_TYPE_GTM_COOR_MASTER;
			node->mastername = $4;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ADD_P GTMCOORD SLAVE Ident FOR Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->nodetype = CNDN_TYPE_GTM_COOR_SLAVE;
			node->mastername = $6;
			node->name = $4;
			node->options = $7;
			$$ = (Node*)node;
		}
	| ADD_P COORDINATOR MASTER Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->nodetype = CNDN_TYPE_COORDINATOR_MASTER;
			node->mastername = $4;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ADD_P COORDINATOR SLAVE Ident FOR Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->nodetype = CNDN_TYPE_COORDINATOR_SLAVE;
			node->mastername = $6;
			node->name = $4;
			node->options = $7;
			$$ = (Node*)node;
		}
	| ADD_P DATANODE MASTER Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->nodetype = CNDN_TYPE_DATANODE_MASTER;
			node->mastername = $4;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ADD_P DATANODE SLAVE Ident FOR Ident opt_general_options
		{
			MGRAddNode *node = makeNode(MGRAddNode);
			node->nodetype = CNDN_TYPE_DATANODE_SLAVE;
			node->mastername = $6;
			node->name = $4;
			node->options = $7;
			$$ = (Node*)node;
		}
	;

AlterNodeStmt:
		ALTER GTMCOORD opt_gtm_inner_type Ident opt_general_options
		{
			MGRAlterNode *node = makeNode(MGRAlterNode);
			node->nodetype = $3;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ALTER COORDINATOR opt_cn_inner_type Ident opt_general_options
		{
			MGRAlterNode *node = makeNode(MGRAlterNode);
			node->nodetype = $3;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	| ALTER DATANODE opt_dn_inner_type Ident opt_general_options
		{
			MGRAlterNode *node = makeNode(MGRAlterNode);
			node->nodetype = $3;
			node->name = $4;
			node->options = $5;
			$$ = (Node*)node;
		}
	;
DropNodeStmt:
	  DROP GTMCOORD opt_gtm_inner_type Ident
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->nodetype = $3;
			node->name = $4;
			$$ = (Node*)node;
		}
	|	DROP COORDINATOR opt_cn_inner_type Ident
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->nodetype = $3;
			node->name = $4;
			$$ = (Node*)node;
		}
	|	DROP DATANODE opt_dn_inner_type Ident
		{
			MGRDropNode *node = makeNode(MGRDropNode);
			node->nodetype = $3;
			node->name = $4;
			$$ = (Node*)node;
		}
	| LIST NODE ZONE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("zone", args);
			$$ = (Node*)stmt;
		}
	;
ListAclStmt:
		LIST ACL opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_list_acl_all", NULL));
			$$ = (Node*)stmt;
		}
		;
ListNodeSize:
	LIST NODE SIZE dataNameList opt_nodesize_with_list
		{
			SelectStmt 	*stmt = makeNode(SelectStmt);
			List 		*args = $4;

			if($4 == NIL)
			{
				args = list_make1($5);
			}
			else
			{
				args = lappend(args, $5);
			}

			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_list_nodesize_all", args));
			//stmt->whereClause = make_column_in("nodename", $4);
			$$ = (Node*)stmt;
		}
	| LIST NODE PRETTY SIZE dataNameList opt_nodesize_with_list
		{
			SelectStmt 	*stmt = makeNode(SelectStmt);
			ResTarget 	*resTarget1 = makeNode(ResTarget);
			ResTarget 	*resTarget2 = makeNode(ResTarget);
			ResTarget 	*resTarget3 = makeNode(ResTarget);
			ResTarget 	*resTarget4 = makeNode(ResTarget);
			ResTarget 	*resTarget5 = makeNode(ResTarget);
			List 		*args = $5;

			if($5 == NIL)
			{
				args = list_make1($6);
			}
			else
			{
				args = lappend(args, $6);
			}

			//1
			resTarget1->name = NULL;
			resTarget1->indirection = NIL;
			resTarget1->val = makeColumnRef("nodename", NIL, -1, 0);
			resTarget1->location = -1;
			//2
			resTarget2->name = NULL;
			resTarget2->indirection = NIL;
			resTarget2->val = makeColumnRef("type", NIL, -1, 0);
			resTarget2->location = -1;
			//3
			resTarget3->name = NULL;
			resTarget3->indirection = NIL;
			resTarget3->val = makeColumnRef("port", NIL, -1, 0);
			resTarget3->location = -1;
			//4
			resTarget4->name = NULL;
			resTarget4->indirection = NIL;
			resTarget4->val = makeColumnRef("nodepath", NIL, -1, 0);
			resTarget4->location = -1;
			//5
			resTarget5->name = "nodesize";
			resTarget5->indirection = NIL;
			resTarget5->val = (Node *)makeFuncCall(list_make1(makeString("pg_size_pretty")),
												   list_make1(makeColumnRef("nodesize", NIL, -1, 0)),
												   COERCE_EXPLICIT_CALL,
												   -1);
			resTarget5->location = -1;

			stmt->targetList = list_make5(resTarget1, resTarget2, resTarget3, resTarget4, resTarget5);
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_list_nodesize_all", args));
			//stmt->whereClause = make_column_in("nodename", $5);
			$$ = (Node*)stmt;
		}
		;
opt_nodesize_with_list:
	WITH opt_nodesize_with_list_items
		{ $$ = $2; }
	| /* empty */
		{ $$ = makeBoolAConst(false, -1); }
	;
opt_nodesize_with_list_items:
	SLINK
		{$$ = makeBoolAConst(true, @1); }
	| /* empty */
		{ $$ = makeBoolAConst(false, -1); }
	;

dataNameList:
	dataNameList ',' Ident
	  	{ $$ = lappend($1, makeAConst(makeString($3), @3)); }
	| Ident
		{ $$ = list_make1(makeAConst(makeString($1), @1)); }
	| ALL
		{ $$ = NIL; }
	;

ListNodeStmt:
	  LIST NODE
		{
			SelectStmt *stmt;
			check_node_incluster();
			stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			$$ = (Node*)stmt;
		}
	| LIST NODE '(' targetList ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			$$ = (Node*)stmt;
		}
	| LIST NODE AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("name", $3);
			$$ = (Node*)stmt;

			check__name_isvaild($3);
		}
	| LIST NODE '(' targetList ')' AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = $4;
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("name", $6);
			$$ = (Node*)stmt;

			check__name_isvaild($6);
		}
	| LIST NODE COORDINATOR
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("coordinator master", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			args = lappend(args,makeStringConst("coordinator slave", -1));
			stmt->whereClause = make_column_in("type", args);
			$$ = (Node*)stmt;
		}
	|	LIST NODE COORDINATOR MASTER
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("coordinator master", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("type", args);
			$$ = (Node*)stmt;
		}
	|	LIST NODE COORDINATOR SLAVE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("coordinator slave", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("type", args);
			$$ = (Node*)stmt;
		}
	|	LIST NODE DATANODE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("datanode master", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			args = lappend(args,makeStringConst("datanode slave", -1));
			stmt->whereClause = make_column_in("type", args);
			$$ = (Node*)stmt;
		}
	|	LIST NODE DATANODE MASTER
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("datanode master", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("type", args);
			$$ = (Node*)stmt;
		}
	|	LIST NODE DATANODE MASTER Ident
		{
			List* node_name;
			SelectStmt *stmt = makeNode(SelectStmt);
			node_name = (List*)list_make1(makeStringConst($5, -1));
			check_node_name_isvaild(CNDN_TYPE_DATANODE_MASTER, node_name);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = (Node *) makeOrExpr((Node *) makeSimpleA_Expr(AEXPR_OP, "=",
															make_ColumnRef("name")
															, makeStringConst($5, -1), -1),
													(Node *) makeSimpleA_Expr(AEXPR_OP, "=",
															make_ColumnRef("mastername")
															, makeStringConst($5, -1), -1)
													,-1);
			$$ = (Node*)stmt;
		}
	|	LIST NODE DATANODE SLAVE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst("datanode slave", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("type", args);
			$$ = (Node*)stmt;
		}
	| LIST NODE HOST AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("node"), -1));
			stmt->whereClause = make_column_in("host", $4);
			$$ = (Node*)stmt;

			check_host_name_isvaild($4);
		}
	;

InitNodeStmt:
INIT ALL opt_general_options
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("initall"), -1));
			with_data_checksums = false;
			g_initall_options = $3;
			$$ = (Node*)stmt;
	}
| INIT ALL DATA_CHECKSUMS opt_general_options
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("initall"), -1));
			with_data_checksums = true;
			g_initall_options = $4;
			$$ = (Node*)stmt;
	}
	;
StartNodeMasterStmt:
		START GTMCOORD MASTER Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_gtmcoord_master", args));
			$$ = (Node*)stmt;
		}
	|	START GTMCOORD SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_gtmcoord_slave", args));
			$$ = (Node*)stmt;
		}
	| START GTMCOORD ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("start_gtmcoord_all"), -1));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR MASTER NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_cn_master", $4));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR SLAVE NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_cn_slave", $4));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR MASTER ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_cn_master", args));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR SLAVE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_cn_slave", args));
			$$ = (Node*)stmt;
		}
	|	START COORDINATOR ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("start_coordinator_all"), -1));
			$$ = (Node*)stmt;
		}
	|	START DATANODE MASTER NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_master", $4));
			$$ = (Node*)stmt;
		}
	| START DATANODE MASTER ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_master", args));
			$$ = (Node*)stmt;
		}
	|	START DATANODE SLAVE NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_slave", $4));
			$$ = (Node*)stmt;
		}
	|	START DATANODE SLAVE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
		 	List *args = list_make1(makeNullAConst(-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_dn_slave", args));
			$$ = (Node*)stmt;
		}
	|	START DATANODE ALL
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("start_datanode_all"), -1));
			$$ = (Node*)stmt;
		}
	|	START ALL
		{
			SelectStmt *stmt;
			mgr_check_job_in_updateparam("monitor_handle_gtm");
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			mgr_check_job_in_updateparam("monitor_handle_datanode");
			stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("startall"), -1));
			$$ = (Node*)stmt;
		}
	|	START ZONE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_start_zone", args));
			$$ = (Node*)stmt;
		}		
	;
StopNodeMasterStmt:
		STOP GTMCOORD MASTER Ident opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = lappend(args,makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtmcoord_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP GTMCOORD SLAVE Ident opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = lappend(args,makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_gtmcoord_slave", args));
			$$ = (Node*)stmt;
		}
	| STOP GTMCOORD ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			if (strcmp($4, SHUTDOWN_S) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_gtmcoord_all"), -1));
			else if (strcmp($4, SHUTDOWN_F) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_gtmcoord_all_f"), -1));
			else
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_gtmcoord_all_i"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR MASTER NodeConstList opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = list_concat(args, $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR SLAVE NodeConstList opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = list_concat(args, $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_cn_slave", args));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			if (strcmp($4, SHUTDOWN_S) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_all"), -1));
			else if (strcmp($4, SHUTDOWN_F) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_all_f"), -1));
			else
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_all_i"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR MASTER ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			if (strcmp($5, SHUTDOWN_S) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_master_all"), -1));
			else if (strcmp($5, SHUTDOWN_F) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_master_all_f"), -1));
			else
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_master_all_i"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP COORDINATOR SLAVE ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			if (strcmp($5, SHUTDOWN_S) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_slave_all"), -1));
			else if (strcmp($5, SHUTDOWN_F) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_slave_all_f"), -1));
			else
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_coordinator_slave_all_i"), -1));
			$$ = (Node*)stmt;
		}	

	|	STOP DATANODE MASTER NodeConstList opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = list_concat(args, $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE MASTER ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = list_concat(args, list_make1(makeNullAConst(-1)));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_master", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE NodeConstList opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = list_concat(args, $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE SLAVE ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($5, -1));
			args = list_concat(args, list_make1(makeNullAConst(-1)));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_dn_slave", args));
			$$ = (Node*)stmt;
		}
	|	STOP DATANODE ALL opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			if (strcmp($4, SHUTDOWN_S) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_datanode_all"), -1));
			else if (strcmp($4, SHUTDOWN_F) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_datanode_all_f"), -1));
			else
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stop_datanode_all_i"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP ALL opt_stop_mode
		{
			SelectStmt *stmt;

			mgr_check_job_in_updateparam("monitor_handle_gtm");
			mgr_check_job_in_updateparam("monitor_handle_coordinator");
			mgr_check_job_in_updateparam("monitor_handle_datanode");
			stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			if (strcmp($3, SHUTDOWN_S) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stopall"), -1));
			else if (strcmp($3, SHUTDOWN_F) == 0)
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stopall_f"), -1));
			else
				stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("stopall_i"), -1));
			$$ = (Node*)stmt;
		}
	|	STOP ZONE Ident opt_stop_mode
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			if (strcmp($4, SHUTDOWN_S) == 0)
				args = lappend(args, makeStringConst("smart", -1));
			else if (strcmp($4, SHUTDOWN_F) == 0)
				args = lappend(args, makeStringConst("fast", -1));
			else
				args = lappend(args, makeStringConst("immediate", -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_stop_zone", args));
			$$ = (Node*)stmt;
		}	
	;
FailoverStmt:
		FAILOVER DATANODE Ident opt_general_force
			{
				SelectStmt *stmt = makeNode(SelectStmt);
				List *args = list_make1(makeStringConst($3, -1));
				mgr_check_job_in_updateparam("monitor_handle_datanode");
				args = lappend(args, makeBoolAConst($4, -1));
				args = lappend(args, makeNullAConst(-1));
				stmt->targetList = list_make1(make_star_target(-1));
				stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_one_dn", args));
				$$ = (Node*)stmt;
			}
		| FAILOVER GTMCOORD Ident opt_general_force
			{
				SelectStmt *stmt = makeNode(SelectStmt);
				List *args = list_make1(makeStringConst($3, -1));
				args = lappend(args, makeBoolAConst($4, -1));
				args = lappend(args, makeNullAConst(-1));
				mgr_check_job_in_updateparam("monitor_handle_gtm");
				stmt->targetList = list_make1(make_star_target(-1));
				stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_gtm", args));
				$$ = (Node*)stmt;
			}
		| FAILOVER DATANODE Ident TO Ident opt_general_force
			{
					SelectStmt *stmt = makeNode(SelectStmt);
					List *args = list_make1(makeStringConst($3, -1));
					mgr_check_job_in_updateparam("monitor_handle_datanode");
					args = lappend(args, makeBoolAConst($6, -1));
					args = lappend(args, makeStringConst($5, -1));
					stmt->targetList = list_make1(make_star_target(-1));
					stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_one_dn", args));
					$$ = (Node*)stmt;
			}
		| FAILOVER GTMCOORD  Ident TO Ident opt_general_force
			{
				SelectStmt *stmt = makeNode(SelectStmt);
				List *args = list_make1(makeStringConst($3, -1));
				args = lappend(args, makeBoolAConst($6, -1));
				args = lappend(args, makeStringConst($5, -1));
				mgr_check_job_in_updateparam("monitor_handle_gtm");
				stmt->targetList = list_make1(make_star_target(-1));
				stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_gtm", args));
				$$ = (Node*)stmt;
			}
		;
opt_general_force:
	FORCE		{$$ = true;}
	|/*empty*/	{$$ = false;}
	;
/* cndn end*/

DeploryStmt:
	  DEPLOY ALL opt_password
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_deploy_all", args));
			$$ = (Node*)stmt;
		}
	| DEPLOY hostname_list opt_password
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			Node *password = makeStringConst($3, -1);
			Node *hostnames = makeAArrayExpr($2, @2);
			List *args = list_make2(password, hostnames);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_deploy_hostnamelist", args));
			$$ = (Node*)stmt;
		}
	;

opt_password:
	  PASSWORD SConst		{ $$ = $2; }
	| PASSWORD ColLabel		{ $$ = $2; }
	| /* empty */			{ $$ = NULL; }
	;
opt_stop_mode:
	MODE SMART			{ $$ = pstrdup(SHUTDOWN_S); }
	| MODE S			{ $$ = pstrdup(SHUTDOWN_S); }
	| /* empty */		{ $$ = pstrdup(SHUTDOWN_S); }
	| MODE FAST	{ $$ = pstrdup(SHUTDOWN_F); }
	| MODE F	{ $$ = pstrdup(SHUTDOWN_F); }
	|	MODE IMMEDIATE		{ $$ = pstrdup(SHUTDOWN_I); }
	| MODE I			{ $$ = pstrdup(SHUTDOWN_I); }
	;

opt_gtm_inner_type:
	  MASTER { $$ = CNDN_TYPE_GTM_COOR_MASTER; }
	| SLAVE { $$ = CNDN_TYPE_GTM_COOR_SLAVE; }
	;
opt_dn_inner_type:
	 MASTER { $$ = CNDN_TYPE_DATANODE_MASTER; }
	|SLAVE { $$ = CNDN_TYPE_DATANODE_SLAVE; }
	;
opt_cn_inner_type:
	 MASTER { $$ = CNDN_TYPE_COORDINATOR_MASTER; }
	|SLAVE { $$ = CNDN_TYPE_COORDINATOR_SLAVE; }
	;
opt_slave_inner_type:
		GTMCOORD SLAVE { $$ = CNDN_TYPE_GTM_COOR_SLAVE; }
	|	COORDINATOR SLAVE { $$ = CNDN_TYPE_COORDINATOR_SLAVE; }
	|	DATANODE SLAVE { $$ = CNDN_TYPE_DATANODE_SLAVE; }
	;

cluster_type:
	GTMCOORD             {$$ = GTMCOORD_TYPE;}
	| GTMCOORD MASTER    {$$ = CNDN_TYPE_GTM_COOR_MASTER;}
	| GTMCOORD SLAVE    {$$ = CNDN_TYPE_GTM_COOR_SLAVE;}
	| COORDINATOR    {$$ = COORDINATOR_TYPE;}
	| COORDINATOR MASTER   {$$ = CNDN_TYPE_COORDINATOR_MASTER;}
	| COORDINATOR SLAVE   {$$ = CNDN_TYPE_COORDINATOR_SLAVE;}
	| DATANODE        {$$ = DATANODE_TYPE;}
	| DATANODE MASTER {$$ = CNDN_TYPE_DATANODE_MASTER;}
	| DATANODE SLAVE  {$$ = CNDN_TYPE_DATANODE_SLAVE;}
	;

sub_like_expr:
	Ident             { $$ = $1;}
	| /* empty */     { $$ = pstrdup("NULL");}
	;

ListMonitor:
	GET_CLUSTER_HEADPAGE_LINE
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_cluster_firstline_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_CLUSTER_TPS_QPS  /*monitor first page, tps,qps, the data in current 12hours*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_12hours_tpsqps_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_CLUSTER_CONNECT_DBSIZE_INDEXSIZE  /*monitor first page, connect,dbsize,indexsize, the data in current 12hours*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_12hours_connect_dbsize_indexsize_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_CLUSTER_SUMMARY  /*monitor cluster summary, the data in current time*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_cluster_summary_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_DATABASE_TPS_QPS /*monitor all database tps,qps, runtime at current time*/
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("monitor_all_dbname_tps_qps_runtime_v"), -1));
			$$ = (Node*)stmt;
	}
	| GET_DATABASE_TPS_QPS_INTERVAL_TIME '(' Ident ',' Ident ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeIntConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_databasetps_func", args));
			$$ = (Node*)stmt;
		}
	| MONITOR_DATABASETPS_FUNC_BY_TIME_PERIOD '(' Ident ',' Ident ',' Ident ')'
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_databasetps_func_by_time_period", args));
			$$ = (Node*)stmt;
	}
	| GET_DATABASE_SUMMARY '(' Ident')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_databasesummary_func", args));
			$$ = (Node*)stmt;
		}
	| GET_SLOWLOG '(' Ident ',' Ident ',' Ident ',' SignedIconst ',' SignedIconst ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			args = lappend(args, makeIntConst($9, -1));
			args = lappend(args, makeIntConst($11, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_slowlog_func_page", args));
			$$ = (Node*)stmt;
		}
	| GET_SLOWLOG_COUNT '(' Ident ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			args = lappend(args, makeStringConst($7, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_slowlog_count_func", args));
			$$ = (Node*)stmt;
		}
	| CHECK_USER '(' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_checkuser_func", args));
			$$ = (Node*)stmt;
		}
	| GET_USER_INFO  SignedIconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_getuserinfo_func", args));
			$$ = (Node*)stmt;
		}
	| UPDATE_USER SignedIconst '(' Ident ',' Ident ',' Ident ',' Ident ',' Ident ',' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, -1));
			args = lappend(args, makeStringConst($4, -1));
			args = lappend(args, makeStringConst($6, -1));
			args = lappend(args, makeStringConst($8, -1));
			args = lappend(args, makeStringConst($10, -1));
			args = lappend(args, makeStringConst($12, -1));
			args = lappend(args, makeStringConst($14, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_updateuserinfo_func", args));
			$$ = (Node*)stmt;
		}
	| CHECK_PASSWORD '(' SignedIconst ',' Ident ')'
	{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, -1));
			args = lappend(args, makeStringConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_checkuserpassword_func", args));
			$$ = (Node*)stmt;
	}
	| UPDATE_PASSWORD SignedIconst '(' Ident ')'
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, -1));
			args = lappend(args, makeStringConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("monitor_updateuserpassword_func", args));
			$$ = (Node*)stmt;
		}
	;

ShowStmt:
	SHOW PARAM_P Ident var_showparam
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		List *args = list_make1(makeStringConst($3, @3));
		args = lappend(args, makeStringConst($4, @4));
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_show_var_param", args));
		$$ = (Node*)stmt;
	}
	|
	SHOW HBA NodeConstList
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_show_hba_all", $3));
		$$ = (Node*)stmt;
	}
	| SHOW HBA opt_general_all
	{
		SelectStmt *stmt = makeNode(SelectStmt);
		List *args = list_make1(makeNullAConst(-1));
		stmt->targetList = list_make1(make_star_target(-1));
		stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_show_hba_all", args));
		$$ = (Node*)stmt;
	}
	;

FlushStmt:
	FLUSH HOST
	{
		MGRFlushHost *node = makeNode(MGRFlushHost);
		$$ = (Node*)node;
	}
	| FLUSH PARAM_P
	{
		MGRFlushParam *node = makeNode(MGRFlushParam);
		$$ = (Node*)node;
	}
	| FLUSH READONLY SLAVE
	{
		MGRFlushReadonlySlave *node = makeNode(MGRFlushReadonlySlave);
		$$ = (Node*)node;
	}
	;
AddJobitemStmt:
	ADD_P ITEM Ident opt_general_options
	{
			MonitorJobitemAdd *node = makeNode(MonitorJobitemAdd);
			node->if_not_exists = false;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
	}
	| ADD_P ITEM IF_P NOT EXISTS Ident opt_general_options
	{
			MonitorJobitemAdd *node = makeNode(MonitorJobitemAdd);
			node->if_not_exists = true;
			node->name = $6;
			node->options = $7;
			$$ = (Node*)node;
	}
	| ADD_P JOB Ident opt_general_options
	{
			MonitorJobAdd *node = makeNode(MonitorJobAdd);
			node->if_not_exists = false;
			node->name = $3;
			node->options = $4;
			$$ = (Node*)node;
	}
	| ADD_P JOB IF_P NOT EXISTS Ident opt_general_options
	{
			MonitorJobAdd *node = makeNode(MonitorJobAdd);
			node->if_not_exists = true;
			node->name = $6;
			node->options = $7;
			$$ = (Node*)node;
	}
	;

AlterJobitemStmt:
	ALTER ITEM Ident opt_general_options
	{
		MonitorJobitemAlter *node = makeNode(MonitorJobitemAlter);
		node->name = $3;
		node->options = $4;
		$$ = (Node*)node;
	}
	| ALTER JOB Ident opt_general_options
	{
		MonitorJobAlter *node = makeNode(MonitorJobAlter);
		node->name = $3;
		node->options = $4;
		$$ = (Node*)node;
	}
	| ALTER JOB ALL opt_general_options
	{
		MonitorJobAlter *node = makeNode(MonitorJobAlter);
		node->name = MACRO_STAND_FOR_ALL_JOB;
		node->options = $4;
		$$ = (Node*)node;
	}
	;

DropJobitemStmt:
	DROP ITEM ObjList
	{
		MonitorJobitemDrop *node = makeNode(MonitorJobitemDrop);
		node->if_exists = false;
		node->namelist = $3;
		$$ = (Node*)node;
	}
	|	DROP ITEM IF_P EXISTS ObjList
	{
		MonitorJobitemDrop *node = makeNode(MonitorJobitemDrop);
		node->if_exists = true;
		node->namelist = $5;
		$$ = (Node*)node;
	}
	| DROP JOB ObjList
	{
		MonitorJobDrop *node = makeNode(MonitorJobDrop);
		node->if_exists = false;
		node->namelist = $3;
		$$ = (Node*)node;
	}
	| DROP JOB IF_P EXISTS ObjList
	{
		MonitorJobDrop *node = makeNode(MonitorJobDrop);
		node->if_exists = true;
		node->namelist = $5;
		$$ = (Node*)node;
	}
	;

ListJobStmt:
	  LIST JOB
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			check_job_status_intbl();
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("job"), -1));
			$$ = (Node*)stmt;
		}
	|	LIST JOB AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			check_job_status_intbl();
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("job"), -1));
			stmt->whereClause = make_column_in("name", $3);
			$$ = (Node*)stmt;

			check_job_name_isvaild($3);
		}
	| LIST ITEM
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("jobitem"), -1));
			$$ = (Node*)stmt;
		}
	|	LIST ITEM AConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("jobitem"), -1));
			stmt->whereClause = make_column_in("item", $3);
			$$ = (Node*)stmt;

			check_jobitem_name_isvaild($3);
		}
		;

AddExtensionStmt:
		ADD_P EXTENSION Ident
		{
			MgrExtensionAdd *node = makeNode(MgrExtensionAdd);
			node->cmdtype = EXTENSION_CREATE;
			node->name = $3;
			$$ = (Node*)node;
		}
		;
DropExtensionStmt:
		DROP EXTENSION Ident
		{
			MgrExtensionDrop *node = makeNode(MgrExtensionDrop);
			node->cmdtype = EXTENSION_DROP;
			node->name = $3;
			$$ = (Node*)node;
		}
		;
RemoveNodeStmt:
		REMOVE GTMCOORD opt_gtm_inner_type ObjList
		{
			MgrRemoveNode *node = makeNode(MgrRemoveNode);
			node->nodetype = $3;
			node->names = $4;
			$$ = (Node*)node;
		}
	| REMOVE DATANODE opt_dn_inner_type ObjList
		{
			MgrRemoveNode *node = makeNode(MgrRemoveNode);
			node->nodetype = $3;
			node->names = $4;
			$$ = (Node*)node;
		}
	|	REMOVE COORDINATOR opt_cn_inner_type ObjList
		{
			MgrRemoveNode *node = makeNode(MgrRemoveNode);
			node->nodetype = $3;
			node->names = $4;
			$$ = (Node*)node;
		}
	;

FailoverManualStmt:
		ADBMGR PROMOTE opt_slave_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			args = lappend(args, makeStringConst($4, @4));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_manual_adbmgr_func", args));
			$$ = (Node*)stmt;
		}
	|	PROMOTE GTMCOORD opt_gtm_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			args = lappend(args, makeStringConst($4, @4));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_manual_promote_func", args));
			$$ = (Node*)stmt;
		}
	| PROMOTE DATANODE opt_dn_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			args = lappend(args, makeStringConst($4, @4));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_manual_promote_func", args));
			$$ = (Node*)stmt;
		}
	|	CONFIG DATANODE SLAVE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst(CNDN_TYPE_DATANODE_SLAVE, @3));
			args = lappend(args, makeStringConst($4, @4));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_manual_pgxcnode_func", args));
			$$ = (Node*)stmt;
		}
	| REWIND opt_slave_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($2, @2));
			args = lappend(args, makeStringConst($3, @3));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_failover_manual_rewind_func", args));
			$$ = (Node*)stmt;
		}
	;

SwitchoverStmt:
	SWITCHOVER GTMCOORD opt_gtm_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_gtm");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(0, -1));
			args = lappend(args, makeIntConst(10, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER GTMCOORD opt_gtm_inner_type Ident Iconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_gtm");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(0, -1));
			args = lappend(args, makeIntConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER GTMCOORD opt_gtm_inner_type Ident FORCE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_gtm");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(1, -1));
			args = lappend(args, makeIntConst(10, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER GTMCOORD opt_gtm_inner_type Ident FORCE Iconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_gtm");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(1, -1));
			args = lappend(args, makeIntConst($6, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER DATANODE opt_dn_inner_type Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_datanode");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(0, -1));
			args = lappend(args, makeIntConst(10, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER DATANODE opt_dn_inner_type Ident Iconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_datanode");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(0, -1));
			args = lappend(args, makeIntConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER DATANODE opt_dn_inner_type Ident FORCE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_datanode");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(1, -1));
			args = lappend(args, makeIntConst(10, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
	| SWITCHOVER DATANODE opt_dn_inner_type Ident FORCE Iconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeIntConst($3, @3));
			mgr_check_job_in_updateparam("monitor_handle_datanode");
			args = lappend(args, makeStringConst($4, @4));
			args = lappend(args, makeIntConst(1, -1));
			args = lappend(args, makeIntConst($6, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_switchover_func", args));
			$$ = (Node*)stmt;
		}
		;

ZoneStmt:
		ZONE FAILOVER Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			args = lappend(args, makeIntConst(0, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_failover", args));
			$$ = (Node*)stmt;
		}
	|		ZONE FAILOVER Ident FORCE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			args = lappend(args, makeIntConst(1, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_failover", args));
			$$ = (Node*)stmt;
		}
	|	ZONE SWITCHOVER Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			args = lappend(args, makeIntConst(0, -1));
			args = lappend(args, makeIntConst(10, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_switchover", args));
			$$ = (Node*)stmt;
		}
	|	ZONE SWITCHOVER Ident Iconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			args = lappend(args, makeIntConst(0, -1));
			args = lappend(args, makeIntConst($4, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_switchover", args));
			$$ = (Node*)stmt;
		}	
	|	ZONE SWITCHOVER Ident FORCE
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			args = lappend(args, makeIntConst(1, -1));
			args = lappend(args, makeIntConst(10, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_switchover", args));
			$$ = (Node*)stmt;
		}		
	|	ZONE SWITCHOVER Ident FORCE Iconst
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			args = lappend(args, makeIntConst(1, -1));
			args = lappend(args, makeIntConst($5, -1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_switchover", args));
			$$ = (Node*)stmt;
		}			
	|	DROP ZONE Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_clear", args));
			$$ = (Node*)stmt;
		}
	|	ZONE INIT Ident
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = list_make1(makeStringConst($3, @3));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_zone_init", args));
			$$ = (Node*)stmt;
		}	
	;
/* ADB DOCTOR BEGIN */
StartDoctorStmt: 
		START DOCTOR 
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_doctor_start", NULL));
			$$ = (Node*)stmt;
		}
	;

StopDoctorStmt:
		STOP DOCTOR
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_doctor_stop", NULL));
			$$ = (Node*)stmt;
		}
	;

SetDoctorParamStmt:
		SET DOCTOR opt_general_options
		{
			MGRDoctorSet *node = makeNode(MGRDoctorSet);
			node -> options = $3;
			$$ = (Node*)node;
		}
	|	SET DOCTOR NODE Ident ON
		{
			MGRDoctorSet *node = makeNode(MGRDoctorSet);
			node -> nodename = $4;
			node -> enable = true;
			$$ = (Node*)node;
		}
	|	SET DOCTOR NODE Ident OFF
		{
			MGRDoctorSet *node = makeNode(MGRDoctorSet);
			node -> nodename = $4;
			node -> enable = false;
			$$ = (Node*)node;
		}
	|	SET DOCTOR HOST Ident ON
		{
			MGRDoctorSet *node = makeNode(MGRDoctorSet);
			node -> hostname = $4;
			node -> enable = true;
			$$ = (Node*)node;
		}
	|	SET DOCTOR HOST Ident OFF
		{
			MGRDoctorSet *node = makeNode(MGRDoctorSet);
			node -> hostname = $4;
			node -> enable = false;
			$$ = (Node*)node;
		}
	;

ListDoctorParamStmt:
		LIST DOCTOR opt_general_options
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_doctor_list", NULL));
			$$ = (Node*)stmt;
		}
	;
/* ADB DOCTOR END */

GetBoottimeStmt:
		SHOW BOOTTIME opt_general_all
		{
			SelectStmt *stmt;
			stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeRangeVar(pstrdup("adbmgr"), pstrdup("boottime_all"), -1));
			$$ = (Node*)stmt;
		}
		| 	SHOW BOOTTIME GTMCOORD opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_boottime_gtmcoord_all", NULL));
			$$ = (Node*)stmt;
		}
		| 	SHOW BOOTTIME DATANODE opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_boottime_datanode_all", NULL));
			$$ = (Node*)stmt;
		}
		| 	SHOW BOOTTIME COORDINATOR opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_boottime_coordinator_all", NULL));
			$$ = (Node*)stmt;
		}
		| 	SHOW BOOTTIME node_type NodeConstList
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *args = lcons(makeIntConst($3, @3), $4);
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_boottime_nodetype_namelist", args));
			$$ = (Node*)stmt;
		}
		|   SHOW BOOTTIME node_type opt_general_all
		{
			SelectStmt *stmt = makeNode(SelectStmt);
			List *arg = list_make1(makeIntConst($3,-1));
			stmt->targetList = list_make1(make_star_target(-1));
			stmt->fromClause = list_make1(makeNode_RangeFunction("mgr_boottime_nodetype_all", arg));
			$$ = (Node*)stmt;
		};

unreserved_keyword:
	  ACL
	| ACTIVATE
	| ADBMGR
	| ADD_P
	| AGENT
	| ALTER
	| APPEND
	| BOOTTIME
	| CHECK_PASSWORD
	| CHECK_USER
	| CLEAN
	| CLEAR
	| CONFIG
	| CLUSTER
	| DATA_CHECKSUMS
	| DEPLOY
	| DOCTOR
	| DROP
	| EXISTS
	| EXTENSION
	| F
	| FAILOVER
	| FAST
	| FLUSH
	| FOR
	| FROM
	| GET_AGTM_NODE_TOPOLOGY
	| GET_ALARM_INFO_ASC
	| GET_ALARM_INFO_COUNT
	| GET_ALARM_INFO_DESC
	| GET_ALL_NODENAME_IN_SPEC_HOST
	| GET_CLUSTER_TPS_QPS
	| GET_CLUSTER_CONNECT_DBSIZE_INDEXSIZE
	| GET_CLUSTER_HEADPAGE_LINE
	| GET_CLUSTER_SUMMARY
	| GET_COORDINATOR_NODE_TOPOLOGY
	| GET_DATABASE_SUMMARY
	| GET_DATABASE_TPS_QPS
	| GET_DATABASE_TPS_QPS_INTERVAL_TIME
	| GET_DATANODE_NODE_TOPOLOGY
	| GET_DB_THRESHOLD_ALL_TYPE
	| GET_HOST_HISTORY_USAGE
	| GET_HOST_HISTORY_USAGE_BY_TIME_PERIOD
	| GET_HOST_LIST_ALL
	| GET_HOST_LIST_SPEC
	| GET_SLOWLOG
	| GET_SLOWLOG_COUNT
	| GET_THRESHOLD_ALL_TYPE
	| GET_THRESHOLD_TYPE
	| GET_USER_INFO
	| GTMCOORD
	| HBA
	| HOST
	| JOB
	| I
	| IF_P
	| IMMEDIATE
	| INIT
	| ITEM
	| LIST
	| MAXVALUE
	| MINVALUE
	| MODE
	| MONITOR
	| NODE
	| OFF
	| PARAM_P
	| PASSWORD
	| PROMOTE
	| PRETTY
	| READONLY
	| REMOVE
	| RESET
	| REVOKE
	| RESOLVE_ALARM
	| REWIND
	| S
	| SET
	| SHOW
	| SIZE
	| SLINK
	| SMART
	| START
	| STOP
	| SWITCHOVER
	| TO
	| UPDATE_PASSWORD
	| UPDATE_THRESHOLD_VALUE
	| UPDATE_USER
	| USER
	| WITH
	| ZONE
	;

reserved_keyword:
	  ALL
	| FALSE_P
	| FORCE
	| IN_P
	| MASTER
	| SLAVE
	| NOT
	| TRUE_P
	| ON
	| CREATE
	| GRANT
	| COORDINATOR
	| DATANODE
	| STATUS
	;

%%
#define PG_KEYWORD(kwname, value, category) value,
static const uint16 MgrScanKeywordTokens[] = {
#include "parser/mgr_kwlist.h"
};

/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
mgr_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static int mgr_yylex(union YYSTYPE *lvalp, YYLTYPE *llocp,
		   core_yyscan_t yyscanner)
{
	return core_yylex(&(lvalp->core_yystype), llocp, yyscanner);
}

List *mgr_parse_query(const char *query_string)
{
	core_yyscan_t yyscanner;
	mgr_yy_extra_type yyextra;
	int			yyresult;

	Assert(lengthof(MgrScanKeywordTokens) == ManagerKeywords.num_keywords);
	/* initialize the flex scanner */
	yyscanner = scanner_init(query_string, &yyextra.core_yy_extra,
							 &ManagerKeywords, MgrScanKeywordTokens);

	yyextra.parsetree = NIL;

	/* Parse! */
	yyresult = mgr_yyparse(yyscanner);

	/* Clean up (release memory) */
	scanner_finish(yyscanner);

	if (yyresult)				/* error */
		return NIL;

	return yyextra.parsetree;
}

static Node* make_column_in(const char *col_name, List *values)
{
	A_Expr *expr;
	ColumnRef *col = makeNode(ColumnRef);
	col->fields = list_make1(makeString(pstrdup(col_name)));
	col->location = -1;
	expr = makeA_Expr(AEXPR_IN
			, list_make1(makeString(pstrdup("=")))
			, (Node*)col
			, (Node*)values
			, -1);
	return (Node*)expr;
}

static Node* makeNode_RangeFunction(const char *func_name, List *func_args)
{
	RangeFunction *n = makeNode(RangeFunction);
	n->lateral = false;
	n->coldeflist = NIL;
	n->ordinality = false;
	n->is_rowsfrom = false;
	n->functions = list_make1(list_make2(make_func_call(func_name, func_args), NIL));
	return (Node *) n;
}

static Node* make_func_call(const char *func_name, List *func_args)
{
	FuncCall *n = makeNode(FuncCall);
	n->funcname = list_make1(makeString(pstrdup(func_name)));
	n->args = func_args;
	n->agg_order = NIL;
	n->agg_star = false;
	n->agg_distinct = false;
	n->func_variadic = false;
	n->over = NULL;
	n->location = -1;
	return (Node *) n;
}

#if 0
static List* make_start_agent_args(List *options)
{
	List *result;
	char *password = NULL;
	ListCell *lc;
	DefElem *def;

	foreach(lc,options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "password") == 0)
			password = defGetString(def);
		else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is password.")));
		}
	}

	if(password == NULL)
		result = list_make1(makeNullAConst(-1));
	else
		result = list_make1(makeStringConst(password, -1));

	return result;
}
#endif

static Node* make_ColumnRef(const char *col_name)
{
	ColumnRef *col = makeNode(ColumnRef);
	col->fields = list_make1(makeString(pstrdup(col_name)));
	col->location = -1;
	return (Node*)col;
}

static Node* make_whereClause_for_datanode(char* node_type_str, List* node_name_list, char* like_expr)
{
	Node* whereClause = NULL;

	whereClause =
		(Node *) makeAndExpr(
			(Node *) makeOrExpr(
					(Node *) makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~", make_ColumnRef("nodetype"), makeStringConst(pstrdup("^datanode master"), -1), -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "=", make_ColumnRef("nodename"), makeStringConst(pstrdup("*"), -1), -1),
							-1),
					(Node *) makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_IN, "=", make_ColumnRef("nodename"), (Node*)node_name_list, -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "=", make_ColumnRef("nodetype"), makeStringConst(pstrdup(node_type_str), -1), -1),
							-1),
					-1),
			(Node *)makeSimpleA_Expr(AEXPR_OP, "~~",
									make_ColumnRef("key"),
									makeStringConst(pstrdup(like_expr), -1), -1),
			-1);
		return  (Node *)whereClause;
}

static Node* make_whereClause_for_coord(char* node_type_str, List* node_name_list, char* like_expr)
{
	Node* whereClause = NULL;

	whereClause =
		(Node *) makeAndExpr(
			(Node *) makeOrExpr(
					(Node *) makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~", make_ColumnRef("nodetype"), makeStringConst(pstrdup("^coordinator"), -1), -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "=", make_ColumnRef("nodename"), makeStringConst(pstrdup("*"), -1), -1),
							-1),
					(Node *) makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_IN, "=", make_ColumnRef("nodename"), (Node*)node_name_list, -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "=", make_ColumnRef("nodetype"), makeStringConst(pstrdup(node_type_str), -1), -1),
							-1),
					-1),
			(Node *)makeSimpleA_Expr(AEXPR_OP, "~~",
									make_ColumnRef("key"),
									makeStringConst(pstrdup(like_expr), -1), -1),
			-1);

	return  (Node *)whereClause;
}

static Node* make_whereClause_for_gtm(char* node_type_str, List* node_name_list, char* like_expr)
{
	Node * whereClause = NULL;

	whereClause =
		(Node *) makeAndExpr(
			(Node *) makeOrExpr(
					(Node *) makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_OP, "~", make_ColumnRef("nodetype"), makeStringConst(pstrdup("^gtmcoord"), -1), -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "=", make_ColumnRef("nodename"), makeStringConst(pstrdup("*"), -1), -1),
							-1),
					(Node *) makeAndExpr(
							(Node *) makeSimpleA_Expr(AEXPR_IN, "=", make_ColumnRef("nodename"), (Node*)node_name_list, -1),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "=", make_ColumnRef("nodetype"), makeStringConst(pstrdup(node_type_str), -1), -1),
							-1),
					-1),
			(Node *)makeSimpleA_Expr(AEXPR_OP, "~~",
										make_ColumnRef("key"),
										makeStringConst(pstrdup(like_expr), -1), -1),
			-1);

		return  (Node *)whereClause;

}

static void check_node_name_isvaild(char node_type, List* node_name_list)
{
	ListCell *lc;
	A_Const *node_name;
	NameData name;
	Relation rel_node;
	TableScanDesc scan;
	ScanKeyData key[2];
	TupleTableSlot *slot;

	if (node_name_list == NIL)
		return;

	rel_node = table_open(NodeRelationId, AccessShareLock);
	slot = table_slot_create(rel_node, NULL);
	foreach(lc, node_name_list)
	{
		node_name = (A_Const *) lfirst(lc);
		Assert(node_name && IsA(&(node_name->val), String));

		namestrcpy(&name, strVal(&(node_name->val)));
		ScanKeyInit(&key[0],
					Anum_mgr_node_nodename,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					NameGetDatum(&name));

		ScanKeyInit(&key[1],
					Anum_mgr_node_nodetype,
					BTEqualStrategyNumber,
					F_CHAREQ,
					CharGetDatum(node_type));

		scan = table_beginscan_catalog(rel_node, lengthof(key), key);

		if (table_scan_getnextslot(scan, ForwardScanDirection, slot) == false)
		{
			switch (node_type)
			{
				case CNDN_TYPE_COORDINATOR_MASTER:
					ereport(ERROR, (errmsg("coordinator master \"%s\" does not exist", NameStr(name))));
					break;
				case CNDN_TYPE_COORDINATOR_SLAVE:
					ereport(ERROR, (errmsg("coordinator slave \"%s\" does not exist", NameStr(name))));
					break;
				case CNDN_TYPE_DATANODE_MASTER:
					ereport(ERROR, (errmsg("datanode master \"%s\" does not exist", NameStr(name))));
					break;
				case CNDN_TYPE_DATANODE_SLAVE:
					ereport(ERROR, (errmsg("datanode slave \"%s\" does not exist", NameStr(name))));
					break;
				case CNDN_TYPE_GTM_COOR_MASTER:
					ereport(ERROR, (errmsg("gtmcoord master \"%s\" does not exist", NameStr(name))));
					break;
				case CNDN_TYPE_GTM_COOR_SLAVE:
					ereport(ERROR, (errmsg("gtmcoord slave \"%s\" does not exist", NameStr(name))));
					break;
				default:
					ereport(ERROR, (errmsg("node type \"%c\" does not exist", node_type)));
					break;
			}
		}

		ExecClearTuple(slot);
		table_endscan(scan);
	}
	ExecDropSingleTupleTableSlot(slot);
	table_close(rel_node, AccessShareLock);

	return;
}

static void check_host_name_isvaild(List *node_name_list)
{
	ListCell *lc;
	A_Const *host_name;
	NameData name;
	Relation rel_node;
	TableScanDesc scan;
	ScanKeyData key[1];
	TupleTableSlot *slot;

	if (node_name_list == NIL)
		return;

	rel_node = table_open(HostRelationId, AccessShareLock);
	slot = table_slot_create(rel_node, NULL);

	foreach(lc, node_name_list)
	{
		host_name = (A_Const *) lfirst(lc);
		Assert(host_name && IsA(&(host_name->val), String));
		namestrcpy(&name, strVal(&(host_name->val)));

		ScanKeyInit(&key[0],
					Anum_mgr_node_nodename,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					NameGetDatum(&name));

		scan = table_beginscan_catalog(rel_node, 1, key);

		if (table_scan_getnextslot(scan, ForwardScanDirection, slot) == false)
		{
			ereport(ERROR, (errmsg("host name \"%s\" does not exist", NameStr(name))));
		}

		ExecClearTuple(slot);
		table_endscan(scan);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_close(rel_node, AccessShareLock);
}

static void check__name_isvaild(List *node_name_list)
{
	ListCell *lc = NULL;
	A_Const *host_name  = NULL;
	NameData name;
	Relation rel_node;
	TableScanDesc scan;
	ScanKeyData key[1];
	TupleTableSlot *slot;

	if (node_name_list == NIL)
		return;

	rel_node = table_open(NodeRelationId, AccessShareLock);
	slot = table_slot_create(rel_node, NULL);

	foreach(lc, node_name_list)
	{
		host_name = (A_Const *) lfirst(lc);
		Assert(host_name && IsA(&(host_name->val), String));
		namestrcpy(&name, strVal(&(host_name->val)));

		ScanKeyInit(&key[0],
					Anum_mgr_node_nodename,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					NameGetDatum(&name));

		scan = table_beginscan_catalog(rel_node, 1, key);

		if (table_scan_getnextslot(scan, ForwardScanDirection, slot) == false)
		{
			ereport(ERROR, (errmsg("node name \"%s\" does not exist", NameStr(name))));
		}

		ExecClearTuple(slot);
		table_endscan(scan);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_close(rel_node, AccessShareLock);
}

static void check_job_name_isvaild(List *node_name_list)
{
	ListCell *lc = NULL;
	A_Const *job_name  = NULL;
	NameData name;
	Relation rel_job;
	TableScanDesc scan;
	ScanKeyData key[1];
	TupleTableSlot *slot;

	if (node_name_list == NIL)
		return;

	rel_job = table_open(MjobRelationId, AccessShareLock);
	slot = table_slot_create(rel_job, NULL);

	foreach(lc, node_name_list)
	{
		job_name = (A_Const *) lfirst(lc);
		Assert(job_name && IsA(&(job_name->val), String));
		namestrcpy(&name, strVal(&(job_name->val)));

		ScanKeyInit(&key[0],
					Anum_monitor_job_name,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					NameGetDatum(&name));

		scan = table_beginscan_catalog(rel_job, 1, key);

		if (table_scan_getnextslot(scan, ForwardScanDirection, slot) == false)
		{
			ereport(ERROR, (errmsg("job name \"%s\" does not exist", NameStr(name))));
		}

		ExecClearTuple(slot);
		table_endscan(scan);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_close(rel_job, AccessShareLock);
}

static void check_jobitem_name_isvaild(List *node_name_list)
{
	ListCell *lc = NULL;
	A_Const *jobitem_name  = NULL;
	NameData name;
	Relation rel_jobitem;
	TableScanDesc scan;
	ScanKeyData key[1];
	TupleTableSlot *slot;

	if (node_name_list == NIL)
		return;

	rel_jobitem = table_open(MjobitemRelationId, AccessShareLock);
	slot = table_slot_create(rel_jobitem, NULL);

	foreach(lc, node_name_list)
	{
		jobitem_name = (A_Const *) lfirst(lc);
		Assert(jobitem_name && IsA(&(jobitem_name->val), String));
		namestrcpy(&name, strVal(&(jobitem_name->val)));

		ScanKeyInit(&key[0],
					Anum_monitor_jobitem_jobitem_itemname,
					BTEqualStrategyNumber,
					F_NAMEEQ,
					NameGetDatum(&name));

		scan = table_beginscan_catalog(rel_jobitem, 1, key);

		if (table_scan_getnextslot(scan, ForwardScanDirection, slot) == false)
		{
			ereport(ERROR, (errmsg("job item name \"%s\" does not exist", NameStr(name))));
		}

		ExecClearTuple(slot);
		table_endscan(scan);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_close(rel_jobitem, AccessShareLock);
}

static void check_job_status_intbl(void)
{
	Relation rel_job;
	TableScanDesc scan;
	ScanKeyData key[1];
	TupleTableSlot *slot;
	bool bget = false;

	ScanKeyInit(&key[0],
				Anum_monitor_job_status,
				BTEqualStrategyNumber,
				F_BOOLEQ,
				BoolGetDatum(true));
	rel_job = table_open(MjobRelationId, AccessShareLock);
	slot = table_slot_create(rel_job, NULL);
	scan = table_beginscan_catalog(rel_job, 1, key);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bget = true;
		ExecClearTuple(slot);
		break;
	}
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
	table_close(rel_job, AccessShareLock);

	if (bget && (adbmonitor_start_daemon==false))
		ereport(WARNING, (errmsg("in postgresql.conf of ADBMGR adbmonitor=off and all jobs cannot be running, you should change adbmonitor=on which can be made effect by mgr_ctl reload ")));

	return;
}
