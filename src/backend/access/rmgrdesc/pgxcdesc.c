/*-------------------------------------------------------------------------
 *
 * pgxcdesc.c
 *	  rmgr descriptor routines for XC special
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2014 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgxc/cluster_barrier.h"

void
cluster_barrier_redo(XLogReaderState *record)
{
	/* Nothing to do */
	return;
}

void
cluster_barrier_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		xl_info PG_USED_FOR_ASSERTS_ONLY = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	Assert(xl_info == XLOG_CLUSTER_BARRIER_CREATE);
	appendStringInfo(buf, "CLUSTER BARRIER \"%s\"", rec);
}

const char*
cluster_barrier_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_CLUSTER_BARRIER_CREATE:
			id = "CREATE";
			break;
		default:
			break;
	}

	return id;
}
