/*-------------------------------------------------------------------------
 *
 * streamutil.h
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  src/bin/pg_basebackup/streamutil.h
 *-------------------------------------------------------------------------
 */

#ifndef STREAMUTIL_H
#define STREAMUTIL_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "libpq-fe.h"

extern const char *progname;
extern char *connection_string;
extern char *dbhost;
extern char *dbuser;
extern char *dbport;
extern char *dbname;
extern int	dbgetpassword;
extern uint32 WalSegSz;

#ifdef WITH_RDMA
extern bool is_rs;
#endif

/* Connection kept global so we can disconnect easily */
extern PGconn *conn;

extern PGconn *GetConnection(void);

/* Replication commands */
extern bool CreateReplicationSlot(PGconn *conn, const char *slot_name,
								  const char *plugin, bool is_temporary,
								  bool is_physical, bool reserve_wal,
								  bool slot_exists_ok);
extern bool DropReplicationSlot(PGconn *conn, const char *slot_name);
extern bool RunIdentifySystem(PGconn *conn, char **sysid,
							  TimeLineID *starttli,
							  XLogRecPtr *startpos,
							  char **db_name);
extern bool RetrieveWalSegSize(PGconn *conn);
extern TimestampTz feGetCurrentTimestamp(void);
extern void feTimestampDifference(TimestampTz start_time, TimestampTz stop_time,
								  long *secs, int *microsecs);

extern bool feTimestampDifferenceExceeds(TimestampTz start_time, TimestampTz stop_time,
										 int msec);
extern void fe_sendint64(int64 i, char *buf);
extern int64 fe_recvint64(char *buf);

#endif							/* STREAMUTIL_H */
