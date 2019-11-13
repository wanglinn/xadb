#include "postgres.h"

#include "common/keywords.h"
#include "parser/ora_gramparse.h"
/* we don't need YYSTYPE */
#define YYSTYPE_IS_DECLARED
#include "parser/ora_gram.h"

#define PG_KEYWORD(a,b,c) {a,b,c},

const ScanKeyword OraScanKeywords[]={
#include "parser/ora_kwlist.h"
};

const int OraNumScanKeywords = lengthof(OraScanKeywords);
