#ifndef ORA_GRAMPARSE_H
#define ORA_GRAMPARSE_H

#include "nodes/pg_list.h"
#include "parser/scanner.h"

#define ORA_YY_MAX_LOOKAHEAD 2

typedef struct ora_yy_lookahead_type
{
	core_YYSTYPE	lval;
	size_t			length;
	int 			token;
	YYLTYPE			loc;
}ora_yy_lookahead_type;

typedef struct ora_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	core_yy_extra_type core_yy_extra;

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */

	/* lookahead */
	ora_yy_lookahead_type lookahead[ORA_YY_MAX_LOOKAHEAD];
	int			count_look;

	bool has_no_alias_subquery;
	bool parsing_first_token;
	bool parsing_code_block;
}ora_yy_extra_type;

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define ora_yyget_extra(yyscanner) (*((ora_yy_extra_type **) (yyscanner)))

/* from ora_gram.y */
extern void ora_parser_init(ora_yy_extra_type *yyext);
extern int	ora_yyparse(core_yyscan_t yyscanner);

/* from ora_keywords.c */
struct ScanKeywordList;
extern const struct ScanKeywordList OraScanKeywords;
/* from ora_gram.y */
extern const uint16 OraScanKeywordTokens[];

#endif /* ORA_GRAMPARSE_H */
