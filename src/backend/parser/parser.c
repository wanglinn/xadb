/*-------------------------------------------------------------------------
 *
 * parser.c
 *		Main entry point/driver for PostgreSQL grammar
 *
 * Note that the grammar is not allowed to perform any table access
 * (since we need to be able to do basic parsing even while inside an
 * aborted transaction).  Therefore, the data structures returned by
 * the grammar are "raw" parsetrees that still need to be analyzed by
 * analyze.c and related files.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/parser.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/gramparse.h"
#include "parser/parser.h"
#include "parser/scansup.h"

static bool check_uescapechar(unsigned char escape);
static char *str_udeescape(const char *str, char escape,
						   int position, core_yyscan_t yyscanner);

#ifdef ADB
#include "lib/stringinfo.h"
#include "miscadmin.h" /* check_stack_depth */
#endif /* ADB */

#ifdef ADB_GRAM_ORA
#include "catalog/heap.h" /* SystemAttributeByName */
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/ora_gramparse.h"
#include "parser/parse_target.h"
#endif /* ADB_GRAM_ORA */

#ifdef ADB_GRAM_DB2
#include "parser/db2_gramparse.h"
#endif /* ADB_GRAM_DB2 */

/*
 * raw_parser
 *		Given a query in string form, do lexical and grammatical analysis.
 *
 * Returns a list of raw (un-analyzed) parse trees.  The contents of the
 * list have the form required by the specified RawParseMode.
 */
List *
raw_parser(const char *str, RawParseMode mode)
{
	core_yyscan_t yyscanner;
	base_yy_extra_type yyextra;
	int			yyresult;

	/* initialize the flex scanner */
	yyscanner = scanner_init(str, &yyextra.core_yy_extra,
							 &ScanKeywords, ScanKeywordTokens);

	/* base_yylex() only needs us to initialize the lookahead token, if any */
	if (mode == RAW_PARSE_DEFAULT)
		yyextra.have_lookahead = false;
	else
	{
		/* this array is indexed by RawParseMode enum */
		static const int mode_token[] = {
			0,					/* RAW_PARSE_DEFAULT */
			MODE_TYPE_NAME,		/* RAW_PARSE_TYPE_NAME */
			MODE_PLPGSQL_EXPR,	/* RAW_PARSE_PLPGSQL_EXPR */
			MODE_PLPGSQL_ASSIGN1,	/* RAW_PARSE_PLPGSQL_ASSIGN1 */
			MODE_PLPGSQL_ASSIGN2,	/* RAW_PARSE_PLPGSQL_ASSIGN2 */
			MODE_PLPGSQL_ASSIGN3	/* RAW_PARSE_PLPGSQL_ASSIGN3 */
		};

		yyextra.have_lookahead = true;
		yyextra.lookahead_token = mode_token[mode];
		yyextra.lookahead_yylloc = 0;
		yyextra.lookahead_end = NULL;
	}

	/* initialize the bison parser */
	parser_init(&yyextra);

	/* Parse! */
	yyresult = base_yyparse(yyscanner);

	/* Clean up (release memory) */
	scanner_finish(yyscanner);

	if (yyresult)				/* error */
		return NIL;

	return yyextra.parsetree;
}

#ifdef ADB_MULTI_GRAM
List *
raw_parser_for_gram(const char *str, RawParseMode mode, ParseGrammar grammar)
{
	switch(grammar)
	{
	case PARSE_GRAM_POSTGRES:
		return raw_parser(str, mode);
#ifdef ADB_GRAM_ORA
	case PARSE_GRAM_ORACLE:
		return ora_raw_parser(str, mode);
#endif /* ADB_GRAM_ORA */
#ifdef ADB_GRAM_DB2
	case PARSE_GRAM_DB2:
		return db2_raw_parser(str, mode);
#endif /* ADB_GRAM_DB2 */
	default:
		ereport(ERROR,
				errmsg("Unknown grammar %d", grammar),
				errcode(ERRCODE_INTERNAL_ERROR),
				errhint("Use SQL:\"set grammar=postgres|oracle|db2\" to change grammar"));
		return NIL;	/* keep compiler quite */
	}
}
#endif

/*
 * Intermediate filter between parser and core lexer (core_yylex in scan.l).
 *
 * This filter is needed because in some cases the standard SQL grammar
 * requires more than one token lookahead.  We reduce these cases to one-token
 * lookahead by replacing tokens here, in order to keep the grammar LALR(1).
 *
 * Using a filter is simpler than trying to recognize multiword tokens
 * directly in scan.l, because we'd have to allow for comments between the
 * words.  Furthermore it's not clear how to do that without re-introducing
 * scanner backtrack, which would cost more performance than this filter
 * layer does.
 *
 * We also use this filter to convert UIDENT and USCONST sequences into
 * plain IDENT and SCONST tokens.  While that could be handled by additional
 * productions in the main grammar, it's more efficient to do it like this.
 *
 * The filter also provides a convenient place to translate between
 * the core_YYSTYPE and YYSTYPE representations (which are really the
 * same thing anyway, but notationally they're different).
 */
int
base_yylex(YYSTYPE *lvalp, YYLTYPE *llocp, core_yyscan_t yyscanner)
{
	base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);
	int			cur_token;
	int			next_token;
	int			cur_token_length;
	YYLTYPE		cur_yylloc;

	/* Get next token --- we might already have it */
	if (yyextra->have_lookahead)
	{
		cur_token = yyextra->lookahead_token;
		lvalp->core_yystype = yyextra->lookahead_yylval;
		*llocp = yyextra->lookahead_yylloc;
		if (yyextra->lookahead_end)
			*(yyextra->lookahead_end) = yyextra->lookahead_hold_char;
		yyextra->have_lookahead = false;
	}
	else
		cur_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner);

	/*
	 * If this token isn't one that requires lookahead, just return it.  If it
	 * does, determine the token length.  (We could get that via strlen(), but
	 * since we have such a small set of possibilities, hardwiring seems
	 * feasible and more efficient --- at least for the fixed-length cases.)
	 */
	switch (cur_token)
	{
		case NOT:
			cur_token_length = 3;
			break;
		case NULLS_P:
			cur_token_length = 5;
			break;
		case WITH:
			cur_token_length = 4;
			break;
		case UIDENT:
		case USCONST:
			cur_token_length = strlen(yyextra->core_yy_extra.scanbuf + *llocp);
			break;
		default:
			return cur_token;
	}

	/*
	 * Identify end+1 of current token.  core_yylex() has temporarily stored a
	 * '\0' here, and will undo that when we call it again.  We need to redo
	 * it to fully revert the lookahead call for error reporting purposes.
	 */
	yyextra->lookahead_end = yyextra->core_yy_extra.scanbuf +
		*llocp + cur_token_length;
	Assert(*(yyextra->lookahead_end) == '\0');

	/*
	 * Save and restore *llocp around the call.  It might look like we could
	 * avoid this by just passing &lookahead_yylloc to core_yylex(), but that
	 * does not work because flex actually holds onto the last-passed pointer
	 * internally, and will use that for error reporting.  We need any error
	 * reports to point to the current token, not the next one.
	 */
	cur_yylloc = *llocp;

	/* Get next token, saving outputs into lookahead variables */
	next_token = core_yylex(&(yyextra->lookahead_yylval), llocp, yyscanner);
	yyextra->lookahead_token = next_token;
	yyextra->lookahead_yylloc = *llocp;

	*llocp = cur_yylloc;

	/* Now revert the un-truncation of the current token */
	yyextra->lookahead_hold_char = *(yyextra->lookahead_end);
	*(yyextra->lookahead_end) = '\0';

	yyextra->have_lookahead = true;

	/* Replace cur_token if needed, based on lookahead */
	switch (cur_token)
	{
		case NOT:
			/* Replace NOT by NOT_LA if it's followed by BETWEEN, IN, etc */
			switch (next_token)
			{
				case BETWEEN:
				case IN_P:
				case LIKE:
				case ILIKE:
				case SIMILAR:
					cur_token = NOT_LA;
					break;
			}
			break;

		case NULLS_P:
			/* Replace NULLS_P by NULLS_LA if it's followed by FIRST or LAST */
			switch (next_token)
			{
				case FIRST_P:
				case LAST_P:
					cur_token = NULLS_LA;
					break;
			}
			break;

		case WITH:
			/* Replace WITH by WITH_LA if it's followed by TIME or ORDINALITY */
			switch (next_token)
			{
				case TIME:
				case ORDINALITY:
					cur_token = WITH_LA;
					break;
			}
			break;

		case UIDENT:
		case USCONST:
			/* Look ahead for UESCAPE */
			if (next_token == UESCAPE)
			{
				/* Yup, so get third token, which had better be SCONST */
				const char *escstr;

				/* Again save and restore *llocp */
				cur_yylloc = *llocp;

				/* Un-truncate current token so errors point to third token */
				*(yyextra->lookahead_end) = yyextra->lookahead_hold_char;

				/* Get third token */
				next_token = core_yylex(&(yyextra->lookahead_yylval),
										llocp, yyscanner);

				/* If we throw error here, it will point to third token */
				if (next_token != SCONST)
					scanner_yyerror("UESCAPE must be followed by a simple string literal",
									yyscanner);

				escstr = yyextra->lookahead_yylval.str;
				if (strlen(escstr) != 1 || !check_uescapechar(escstr[0]))
					scanner_yyerror("invalid Unicode escape character",
									yyscanner);

				/* Now restore *llocp; errors will point to first token */
				*llocp = cur_yylloc;

				/* Apply Unicode conversion */
				lvalp->core_yystype.str =
					str_udeescape(lvalp->core_yystype.str,
								  escstr[0],
								  *llocp,
								  yyscanner);

				/*
				 * We don't need to revert the un-truncation of UESCAPE.  What
				 * we do want to do is clear have_lookahead, thereby consuming
				 * all three tokens.
				 */
				yyextra->have_lookahead = false;
			}
			else
			{
				/* No UESCAPE, so convert using default escape character */
				lvalp->core_yystype.str =
					str_udeescape(lvalp->core_yystype.str,
								  '\\',
								  *llocp,
								  yyscanner);
			}

			if (cur_token == UIDENT)
			{
				/* It's an identifier, so truncate as appropriate */
				truncate_identifier(lvalp->core_yystype.str,
									strlen(lvalp->core_yystype.str),
									true);
				cur_token = IDENT;
			}
			else if (cur_token == USCONST)
			{
				cur_token = SCONST;
			}
			break;
	}

	return cur_token;
}

/* convert hex digit (caller should have verified that) to value */
static unsigned int
hexval(unsigned char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 0xA;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 0xA;
	elog(ERROR, "invalid hexadecimal digit");
	return 0;					/* not reached */
}

/* is Unicode code point acceptable? */
static void
check_unicode_value(pg_wchar c)
{
	if (!is_valid_unicode_codepoint(c))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid Unicode escape value")));
}

/* is 'escape' acceptable as Unicode escape character (UESCAPE syntax) ? */
static bool
check_uescapechar(unsigned char escape)
{
	if (isxdigit(escape)
		|| escape == '+'
		|| escape == '\''
		|| escape == '"'
		|| scanner_isspace(escape))
		return false;
	else
		return true;
}

/*
 * Process Unicode escapes in "str", producing a palloc'd plain string
 *
 * escape: the escape character to use
 * position: start position of U&'' or U&"" string token
 * yyscanner: context information needed for error reports
 */
static char *
str_udeescape(const char *str, char escape,
			  int position, core_yyscan_t yyscanner)
{
	const char *in;
	char	   *new,
			   *out;
	size_t		new_len;
	pg_wchar	pair_first = 0;
	ScannerCallbackState scbstate;

	/*
	 * Guesstimate that result will be no longer than input, but allow enough
	 * padding for Unicode conversion.
	 */
	new_len = strlen(str) + MAX_UNICODE_EQUIVALENT_STRING + 1;
	new = palloc(new_len);

	in = str;
	out = new;
	while (*in)
	{
		/* Enlarge string if needed */
		size_t		out_dist = out - new;

		if (out_dist > new_len - (MAX_UNICODE_EQUIVALENT_STRING + 1))
		{
			new_len *= 2;
			new = repalloc(new, new_len);
			out = new + out_dist;
		}

		if (in[0] == escape)
		{
			/*
			 * Any errors reported while processing this escape sequence will
			 * have an error cursor pointing at the escape.
			 */
			setup_scanner_errposition_callback(&scbstate, yyscanner,
											   in - str + position + 3);	/* 3 for U&" */
			if (in[1] == escape)
			{
				if (pair_first)
					goto invalid_pair;
				*out++ = escape;
				in += 2;
			}
			else if (isxdigit((unsigned char) in[1]) &&
					 isxdigit((unsigned char) in[2]) &&
					 isxdigit((unsigned char) in[3]) &&
					 isxdigit((unsigned char) in[4]))
			{
				pg_wchar	unicode;

				unicode = (hexval(in[1]) << 12) +
					(hexval(in[2]) << 8) +
					(hexval(in[3]) << 4) +
					hexval(in[4]);
				check_unicode_value(unicode);
				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) out);
					out += strlen(out);
				}
				in += 5;
			}
			else if (in[1] == '+' &&
					 isxdigit((unsigned char) in[2]) &&
					 isxdigit((unsigned char) in[3]) &&
					 isxdigit((unsigned char) in[4]) &&
					 isxdigit((unsigned char) in[5]) &&
					 isxdigit((unsigned char) in[6]) &&
					 isxdigit((unsigned char) in[7]))
			{
				pg_wchar	unicode;

				unicode = (hexval(in[2]) << 20) +
					(hexval(in[3]) << 16) +
					(hexval(in[4]) << 12) +
					(hexval(in[5]) << 8) +
					(hexval(in[6]) << 4) +
					hexval(in[7]);
				check_unicode_value(unicode);
				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
						goto invalid_pair;
				}
				else if (is_utf16_surrogate_second(unicode))
					goto invalid_pair;

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					pg_unicode_to_server(unicode, (unsigned char *) out);
					out += strlen(out);
				}
				in += 8;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid Unicode escape"),
						 errhint("Unicode escapes must be \\XXXX or \\+XXXXXX.")));

			cancel_scanner_errposition_callback(&scbstate);
		}
		else
		{
			if (pair_first)
				goto invalid_pair;

			*out++ = *in++;
		}
	}

	/* unfinished surrogate pair? */
	if (pair_first)
		goto invalid_pair;

	*out = '\0';
	return new;

	/*
	 * We might get here with the error callback active, or not.  Call
	 * scanner_errposition to make sure an error cursor appears; if the
	 * callback is active, this is duplicative but harmless.
	 */
invalid_pair:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid Unicode surrogate pair"),
			 scanner_errposition(in - str + position + 3,	/* 3 for U&" */
								 yyscanner)));
	return NULL;				/* keep compiler quiet */
}

/* ADB move from gram.y */
#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)
RawStmt *
makeRawStmt(Node *stmt, int stmt_location)
{
	RawStmt    *rs = makeNode(RawStmt);

	rs->stmt = stmt;
	rs->stmt_location = stmt_location;
	rs->stmt_len = 0;			/* might get changed later */
	return rs;
}

/* Adjust a RawStmt to reflect that it doesn't run to the end of the string */
void
updateRawStmtEnd(RawStmt *rs, int end_location)
{
	/*
	 * If we already set the length, don't change it.  This is for situations
	 * like "select foo ;; select bar" where the same statement will be last
	 * in the string for more than one semicolon.
	 */
	if (rs->stmt_len > 0)
		return;

	/* OK, update length of RawStmt */
	rs->stmt_len = end_location - rs->stmt_location;
}

Node *
makeColumnRef(char *colname, List *indirection,
			  int location, core_yyscan_t yyscanner)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection,
																  nfields),
												   yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		else if (IsA(lfirst(l), A_Star))
		{
			/* We only allow '*' at the end of a ColumnRef */
			if (lnext(indirection, l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}

Node *
makeTypeCast(Node *arg, TypeName *typename, int location)
{
	TypeCast *n = makeNode(TypeCast);
	n->arg = arg;
	n->typeName = typename;
	n->location = location;
	return (Node *) n;
}

Node *
makeStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeStringConstCast(char *str, int location, TypeName *typename)
{
	Node *s = makeStringConst(str, location);

	return makeTypeCast(s, typename, -1);
}

Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;

	return (Node *)n;
}

Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeBitStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_BitString;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeNullAConst(int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Null;
	n->location = location;

	return (Node *)n;
}

Node *
makeAConst(Value *v, int location)
{
	Node *n;

	switch (v->type)
	{
		case T_Float:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_Integer:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_String:
		default:
			n = makeStringConst(v->val.str, location);
			break;
	}

	return n;
}

/* makeBoolAConst()
 * Create an A_Const string node and put it inside a boolean cast.
 */
Node *
makeBoolAConst(bool state, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = (state ? "t" : "f");
	n->location = location;

	return makeTypeCast((Node *)n, SystemTypeName("bool"), -1);
}

/* makeRoleSpec
 * Create a RoleSpec with the given type
 */
RoleSpec *
makeRoleSpec(RoleSpecType type, int location)
{
	RoleSpec *spec = makeNode(RoleSpec);

	spec->roletype = type;
	spec->location = location;

	return spec;
}

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
void
check_qualified_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
}

/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
List *
check_func_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
	return names;
}

/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
List *
check_indirection(List *indirection, core_yyscan_t yyscanner)
{
	ListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Star))
		{
			if (lnext(indirection, l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/* extractArgTypes()
 * Given a list of FunctionParameter nodes, extract a list of just the
 * argument types (TypeNames) for input parameters only.  This is what
 * is needed to look up an existing function, which is what is wanted by
 * the productions that use this call.
 */
List *
extractArgTypes(List *parameters)
{
	List	   *result = NIL;
	ListCell   *i;

	foreach(i, parameters)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(i);

		if (p->mode != FUNC_PARAM_OUT && p->mode != FUNC_PARAM_TABLE)
			result = lappend(result, p->argType);
	}
	return result;
}

/* extractAggrArgTypes()
 * As above, but work from the output of the aggr_args production.
 */
List *
extractAggrArgTypes(List *aggrargs)
{
	Assert(list_length(aggrargs) == 2);
	return extractArgTypes((List *) linitial(aggrargs));
}

/* makeOrderedSetArgs()
 * Build the result of the aggr_args production (which see the comments for).
 * This handles only the case where both given lists are nonempty, so that
 * we have to deal with multiple VARIADIC arguments.
 */
List *
makeOrderedSetArgs(List *directargs, List *orderedargs,
				   core_yyscan_t yyscanner)
{
	FunctionParameter *lastd = (FunctionParameter *) llast(directargs);
	Value	   *ndirectargs;

	/* No restriction unless last direct arg is VARIADIC */
	if (lastd->mode == FUNC_PARAM_VARIADIC)
	{
		FunctionParameter *firsto = (FunctionParameter *) linitial(orderedargs);

		/*
		 * We ignore the names, though the aggr_arg production allows them;
		 * it doesn't allow default values, so those need not be checked.
		 */
		if (list_length(orderedargs) != 1 ||
			firsto->mode != FUNC_PARAM_VARIADIC ||
			!equal(lastd->argType, firsto->argType))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("an ordered-set aggregate with a VARIADIC direct argument must have one VARIADIC aggregated argument of the same data type"),
					 parser_errposition(exprLocation((Node *) firsto))));

		/* OK, drop the duplicate VARIADIC argument from the internal form */
		orderedargs = NIL;
	}

	/* don't merge into the next line, as list_concat changes directargs */
	ndirectargs = makeInteger(list_length(directargs));

	return list_make2(list_concat(directargs, orderedargs),
					  ndirectargs);
}

/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in SelectStmt productions.
 */
void
insertSelectOptions(SelectStmt *stmt,
					List *sortClause, List *lockingClause,
					SelectLimit *limitClause,
					WithClause *withClause,
					core_yyscan_t yyscanner)
{
	Assert(IsA(stmt, SelectStmt));

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple ORDER BY clauses not allowed"),
					 parser_errposition(exprLocation((Node *) sortClause))));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitClause && limitClause->limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple OFFSET clauses not allowed"),
					 parser_errposition(exprLocation(limitClause->limitOffset))));
		stmt->limitOffset = limitClause->limitOffset;
	}
	if (limitClause && limitClause->limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple LIMIT clauses not allowed"),
					 parser_errposition(exprLocation(limitClause->limitCount))));
		stmt->limitCount = limitClause->limitCount;
	}
	if (limitClause && limitClause->limitOption != LIMIT_OPTION_DEFAULT)
	{
		if (stmt->limitOption)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple limit options not allowed")));
		if (!stmt->sortClause && limitClause->limitOption == LIMIT_OPTION_WITH_TIES)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("WITH TIES cannot be specified without ORDER BY clause")));
		stmt->limitOption = limitClause->limitOption;
	}
	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple WITH clauses not allowed"),
					 parser_errposition(exprLocation((Node *) withClause))));
		stmt->withClause = withClause;
	}
}

Node *
makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg)
{
	SelectStmt *n = makeNode(SelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (SelectStmt *) larg;
	n->rarg = (SelectStmt *) rarg;
	return (Node *) n;
}

/* SystemFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
List *
SystemFuncName(char *name)
{
	return list_make2(makeString("pg_catalog"), makeString(name));
}

/* SystemTypeName()
 * Build a properly-qualified reference to a built-in type.
 *
 * typmod is defaulted, but may be changed afterwards by caller.
 * Likewise for the location.
 */
TypeName *
SystemTypeName(char *name)
{
	return makeTypeNameFromNameList(list_make2(makeString("pg_catalog"),
											   makeString(name)));
}

#ifdef ADB_MULTI_GRAM
TypeName *SystemTypeNameLocation(char *name, int location)
{
	TypeName *typ = makeTypeNameFromNameList(list_make2(makeString("pg_catalog"),
											   makeString(name)));
	typ->location = location;
	return typ;
}
#endif /* ADB_MULTI_GRAM */

/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
Node *
doNegate(Node *n, int location)
{
	if (IsA(n, A_Const))
	{
		A_Const *con = (A_Const *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_Integer)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_Float)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (Node *) makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;

	Assert(IsA(v, Float));
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
		v->val.str = psprintf("-%s", oldval);
}

Node *
makeAndExpr(Node *lexpr, Node *rexpr, int location)
{
	/* Flatten "a AND b AND c ..." to a single BoolExpr on sight */
	if (IsA(lexpr, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexpr;

		if (blexpr->boolop == AND_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(AND_EXPR, list_make2(lexpr, rexpr), location);
}

Node *
makeOrExpr(Node *lexpr, Node *rexpr, int location)
{
	/* Flatten "a OR b OR c ..." to a single BoolExpr on sight */
	if (IsA(lexpr, BoolExpr))
	{
		BoolExpr *blexpr = (BoolExpr *) lexpr;

		if (blexpr->boolop == OR_EXPR)
		{
			blexpr->args = lappend(blexpr->args, rexpr);
			return (Node *) blexpr;
		}
	}
	return (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), location);
}

Node *
makeNotExpr(Node *expr, int location)
{
	return (Node *) makeBoolExpr(NOT_EXPR, list_make1(expr), location);
}

Node *
makeAArrayExpr(List *elements, int location)
{
	A_ArrayExpr *n = makeNode(A_ArrayExpr);

	n->elements = elements;
	n->location = location;
	return (Node *) n;
}

Node *
makeSQLValueFunction(SQLValueFunctionOp op, int32 typmod, int location)
{
	SQLValueFunction *svf = makeNode(SQLValueFunction);

	svf->op = op;
	/* svf->type will be filled during parse analysis */
	svf->typmod = typmod;
	svf->location = location;
	return (Node *) svf;
}

Node *
makeXmlExpr(XmlExprOp op, char *name, List *named_args, List *args,
			int location)
{
	XmlExpr		*x = makeNode(XmlExpr);

	x->op = op;
	x->name = name;
	/*
	 * named_args is a list of ResTarget; it'll be split apart into separate
	 * expression and name lists in transformXmlExpr().
	 */
	x->named_args = named_args;
	x->arg_names = NIL;
	x->args = args;
	/* xmloption, if relevant, must be filled in by caller */
	/* type and typmod will be filled in during parse analysis */
	x->type = InvalidOid;			/* marks the node as not analyzed */
	x->location = location;
	return (Node *) x;
}

/*
 * Merge the input and output parameters of a table function.
 */
List *
mergeTableFuncParameters(List *func_args, List *columns)
{
	ListCell   *lc;

	/* Explicit OUT and INOUT parameters shouldn't be used in this syntax */
	foreach(lc, func_args)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(lc);

		if (p->mode != FUNC_PARAM_DEFAULT &&
			p->mode != FUNC_PARAM_IN &&
			p->mode != FUNC_PARAM_VARIADIC)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("OUT and INOUT arguments aren't allowed in TABLE functions")));
	}

	return list_concat(func_args, columns);
}

/*
 * Determine return type of a TABLE function.  A single result column
 * returns setof that column's type; otherwise return setof record.
 */
TypeName *
TableFuncTypeName(List *columns)
{
	TypeName *result;

	if (list_length(columns) == 1)
	{
		FunctionParameter *p = (FunctionParameter *) linitial(columns);

		result = copyObject(p->argType);
	}
	else
		result = SystemTypeName("record");

	result->setof = true;

	return result;
}

/*
 * Convert a list of (dotted) names to a RangeVar (like
 * makeRangeVarFromNameList, but with position support).  The
 * "AnyName" refers to the any_name production in the grammar.
 */
RangeVar *
makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner)
{
	RangeVar *r = makeNode(RangeVar);

	switch (list_length(names))
	{
		case 1:
			r->catalogname = NULL;
			r->schemaname = NULL;
			r->relname = strVal(linitial(names));
			break;
		case 2:
			r->catalogname = NULL;
			r->schemaname = strVal(linitial(names));
			r->relname = strVal(lsecond(names));
			break;
		case 3:
			r->catalogname = strVal(linitial(names));
			r->schemaname = strVal(lsecond(names));
			r->relname = strVal(lthird(names));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("improper qualified name (too many dotted names): %s",
							NameListToString(names)),
					 parser_errposition(position)));
			break;
	}

	r->relpersistence = RELPERSISTENCE_PERMANENT;
	r->location = position;

	return r;
}

/* Separate Constraint nodes from COLLATE clauses in a ColQualList */
void
SplitColQualList(List *qualList,
				 List **constraintList, CollateClause **collClause,
				 core_yyscan_t yyscanner)
{
	ListCell   *cell;

	*collClause = NULL;
	foreach(cell, qualList)
	{
		Node   *n = (Node *) lfirst(cell);

		if (IsA(n, Constraint))
		{
			/* keep it in list */
			continue;
		}
		if (IsA(n, CollateClause))
		{
			CollateClause *c = (CollateClause *) n;

			if (*collClause)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple COLLATE clauses not allowed"),
						 parser_errposition(c->location)));
			*collClause = c;
		}
		else
			elog(ERROR, "unexpected node type %d", (int) n->type);
		/* remove non-Constraint nodes from qualList */
		qualList = foreach_delete_current(qualList, cell);
	}
	*constraintList = qualList;
}

/*----------
 * Recursive view transformation
 *
 * Convert
 *
 *     CREATE RECURSIVE VIEW relname (aliases) AS query
 *
 * to
 *
 *     CREATE VIEW relname (aliases) AS
 *         WITH RECURSIVE relname (aliases) AS (query)
 *         SELECT aliases FROM relname
 *
 * Actually, just the WITH ... part, which is then inserted into the original
 * view definition as the query.
 * ----------
 */
Node *
makeRecursiveViewSelect(char *relname, List *aliases, Node *query)
{
	SelectStmt *s = makeNode(SelectStmt);
	WithClause *w = makeNode(WithClause);
	CommonTableExpr *cte = makeNode(CommonTableExpr);
	List	   *tl = NIL;
	ListCell   *lc;

	/* create common table expression */
	cte->ctename = relname;
	cte->aliascolnames = aliases;
	cte->ctematerialized = CTEMaterializeDefault;
	cte->ctequery = query;
	cte->location = -1;

	/* create WITH clause and attach CTE */
	w->recursive = true;
	w->ctes = list_make1(cte);
	w->location = -1;

	/* create target list for the new SELECT from the alias list of the
	 * recursive view specification */
	foreach (lc, aliases)
	{
		ResTarget *rt = makeNode(ResTarget);

		rt->name = NULL;
		rt->indirection = NIL;
		rt->val = makeColumnRef(strVal(lfirst(lc)), NIL, -1, 0);
		rt->location = -1;

		tl = lappend(tl, rt);
	}

	/* create new SELECT combining WITH clause, target list, and fake FROM
	 * clause */
	s->withClause = w;
	s->targetList = tl;
	s->fromClause = list_make1(makeRangeVar(NULL, relname, -1));

	return (Node *) s;
}
/* ADB end from gram.y */

ResTarget* make_star_target(int location)
{
	ResTarget *target;
	ColumnRef *n = makeNode(ColumnRef);
	n->fields = list_make1(makeNode(A_Star));
	n->location = location;

	target = makeNode(ResTarget);
	target->name = NULL;
	target->indirection = NIL;
	target->val = (Node *)n;
	target->location = location;
#ifdef ADB_GRAM_ORA
	target->as_location = -1;
	target->expr_len = -1;
#endif

	return target;
}

#ifdef ADB_GRAM_ORA
List *
check_sequence_name(List *names, core_yyscan_t yyscanner, int location)
{
	ListCell   *i;
	StringInfoData buf;

	initStringInfo(&buf);
	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");

		appendStringInfo(&buf, "%s.", strVal((Value*)lfirst(i)));
	}
	buf.data[buf.len - 1] = '\0';

	return list_make1(makeStringConst(buf.data, location));
}

List *OracleFuncName(char *name)
{
	return list_make2(makeString("oracle"), makeString(name));
}

TypeName *OracleTypeName(char *name)
{
	return makeTypeNameFromNameList(list_make2(makeString("oracle"),
											   makeString(name)));
}

TypeName *OracleTypeNameLocation(char *name, int location)
{
	TypeName *typ = makeTypeNameFromNameList(list_make2(makeString("oracle"),
												makeString(name)));
	typ->location = location;
	return typ;
}
#endif /* ADB_GRAM_ORA */
