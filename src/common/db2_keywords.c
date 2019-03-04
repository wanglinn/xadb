#include "postgres.h"

#include "common/keywords.h"
#include "nodes/parsenodes.h"
#include "parser/db2_gramparse.h"
/* we don't need YYSTYPE */
#define YYSTYPE_IS_DECLARED
#include "parser/db2_gram.h"

#define PG_KEYWORD(a,b,c) {a,b,c},

const ScanKeyword db2ScanKeywords[]={
#include "parser/db2_kwlist.h"
};

const int db2NumScanKeywords = lengthof(db2ScanKeywords);
