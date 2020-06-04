#ifndef DB2_GRAMPARSE_H
#define DB2_GRAMPARSE_H

#include "nodes/pg_list.h"
#include "parser/scanner.h"

typedef struct db2_yy_extra_type
{
	/*
	 * Fields used by the core scanner.
	 */
	core_yy_extra_type core_yy_extra;

	/*
	 * State variables that belong to the grammar.
	 */
	List	   *parsetree;		/* final parse result is delivered here */
}db2_yy_extra_type;

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define db2_yyget_extra(yyscanner) (*((db2_yy_extra_type **) (yyscanner)))

/* from db2_gram.y */
extern void db2_parser_init(db2_yy_extra_type *yyext);
extern int	db2_yyparse(core_yyscan_t yyscanner);

/* from db2_keywords.c */
struct ScanKeywordList;
extern const struct ScanKeywordList db2ScanKeywords;
/* from db2_gram.y */
extern const uint16 db2ScanKeywordTokens[];

#endif /* ORA_GRAMPARSE_H */
