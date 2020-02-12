#include "postgres.h"

#include "lib/stringinfo.h"
#include "parser/parse_node.h"
#include "pgxc/nodemgr.h"

void AlterNodeExpansion(AlterNodeStmt *stmt, ParseState *pstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("not support yet")));
}