/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"


extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
										ParamListInfo params, QueryCompletion *qc);

extern DestReceiver *CreateTransientRelDestReceiver(Oid oid);

extern bool MatViewIncrementalMaintenanceIsEnabled(void);

#ifdef ADB
extern void ClusterRefreshMatView(StringInfo mem_toc);
extern void pgxc_send_matview_data(RangeVar *matview_rv, const char *query_string);
extern void pgxc_fill_matview_by_copy(DestReceiver *mv_dest, bool skipdata,
										int operation, TupleDesc tupdesc);
#endif

#endif							/* MATVIEW_H */
