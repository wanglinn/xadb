/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the PITR barrier handling
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2017-2018 ADB Development Group
 *
 * src/include/pgxc/cluster_barrier.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CLUSTER_BARRIER_H
#define CLUSTER_BARRIER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "tcop/dest.h"

/* internal cluster barrier command type */
#define CLUSTER_BARRIER_PREPARE		'P'
#define CLUSTER_BARRIER_EXECUTE		'X'
#define CLUSTER_BARRIER_END			'E'

#define XLOG_CLUSTER_BARRIER_CREATE	0x00

/* internal cluster barrier command */
extern void InterCreateClusterBarrier(char cmd_type, const char *barrierID, CommandDest dest);

/* extern create cluster barrier command */
extern void ExecCreateClusterBarrier(const char *barrierID, QueryCompletion *qc);

/* xlog recovery */
extern void cluster_barrier_redo(XLogReaderState *record);
extern void cluster_barrier_desc(StringInfo buf, XLogReaderState *record);
extern const char* cluster_barrier_identify(uint8 info);

#endif /* CLUSTER_BARRIER_H */
