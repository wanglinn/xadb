#include "postgres.h"

#include "parser/scanner.h"
#include "plora_kwlist.h"
#include "plpgsql.h"

#include "plora_gram.h"

#include "plora_reserved_kwlist_d.h"
#include "plora_unreserved_kwlist_d.h"

#define PG_KEYWORD(a,b) b,

const uint16 ReservedPLORAKeywordTokens[] = {
#include "plora_reserved_kwlist.h"
};

const uint16 UnreservedPLORAKeywordTokens[] = {
#include "plora_unreserved_kwlist.h"
};

bool plorasql_token_is_unreserved_keyword(int token)
{
	int			i;

	StaticAssertStmt(lengthof(ReservedPLORAKeywordTokens) == RESERVEDPLORAKEYWORDS_NUM_KEYWORDS,
					 "reserved keyword count not equal");
	StaticAssertStmt(lengthof(UnreservedPLORAKeywordTokens) == UNRESERVEDPLORAKEYWORDS_NUM_KEYWORDS,
					 "unreserved keyword count not equal");

	for (i = 0; i < lengthof(UnreservedPLORAKeywordTokens); i++)
	{
		if (UnreservedPLORAKeywordTokens[i] == token)
			return true;
	}

	return false;
}