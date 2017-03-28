/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *		Postgres-XC flags and connection control information
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * IDENTIFICATION
 * 		src/include/pgxc/pgxc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_H
#define PGXC_H

#include "storage/lwlock.h"
#include "postgres.h"

extern bool isPGXCCoordinator;
extern bool isPGXCDataNode;
extern bool isRestoreMode;
extern bool isADBLoader;

typedef enum
{
	REMOTE_CONN_APP,
	REMOTE_CONN_COORD,
	REMOTE_CONN_RXACTMGR,
	REMOTE_CONN_DATANODE
} RemoteConnTypes;

/* Determine remote connection type for a PGXC backend */
extern int		remoteConnType;

/* Local node name and numer */
extern char	*PGXCNodeName;
extern int PGXCNodeId;
extern Oid PGXCNodeOid;
extern uint32 PGXCNodeIdentifier;

extern Datum xc_lockForBackupKey1;
extern Datum xc_lockForBackupKey2;

#define IS_PGXC_COORDINATOR isPGXCCoordinator
#define IS_PGXC_DATANODE isPGXCDataNode
#define REMOTE_CONN_TYPE remoteConnType
#define IS_ADBLOADER isADBLoader

#define IsConnFromApp()         (remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord()       (remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode()    (remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromRxactMgr()    (remoteConnType == REMOTE_CONN_RXACTMGR)

/* key pair to be used as object id while using advisory lock for backup */
#define XC_LOCK_FOR_BACKUP_KEY_1	0xFFFF
#define XC_LOCK_FOR_BACKUP_KEY_2	0xFFFF

#endif   /* PGXC_H */
