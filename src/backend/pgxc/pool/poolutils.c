/*-------------------------------------------------------------------------
 *
 * poolutils.c
 *
 * Utilities for Postgres-XC pooler
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"

#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "pgxc/pgxcnode.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/syscache.h"

#ifdef ADB
#include "access/rxact_mgr.h"
#include "catalog/namespace.h"
#include "intercomm/inter-comm.h"
#include "pgxc/execRemote.h"
#endif /* ADB */

volatile bool need_reload_pooler = false;
static List* get_all_xcnode_oid(void);

/*
 * pgxc_pool_reload
 *
 * Reload data cached in pooler and reload node connection
 * information in all the server sessions. This aborts all
 * the existing transactions on this node and reinitializes pooler.
 * First a lock is taken on Pooler to keep consistency of node information
 * being updated. If connection information cached is already consistent
 * in pooler, reload is not executed.
 * Reload itself is made in 2 phases:
 * 1) Update database pools with new connection information based on catalog
 *    pgxc_node. Remote node pools are changed as follows:
 *	  - cluster nodes dropped in new cluster configuration are deleted and all
 *      their remote connections are dropped.
 *    - cluster nodes whose port or host value is modified are dropped the same
 *      way, as connection information has changed.
 *    - cluster nodes whose port or host has not changed are kept as is, but
 *      reorganized respecting the new cluster configuration.
 *    - new cluster nodes are added.
 * 2) Reload information in all the sessions of the local node.
 *    All the sessions in server are signaled to reconnect to pooler to get
 *    newest connection information and update connection information related
 *    to remote nodes. This results in losing prepared and temporary objects
 *    in all the sessions of server. All the existing transactions are aborted
 *    and a WARNING message is sent back to client.
 *    Session that invocated the reload does the same process, but no WARNING
 *    message is sent back to client.
 */
Datum
pgxc_pool_reload(PG_FUNCTION_ARGS)
{
	MemoryContext old_context;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to manage pooler"))));

	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("pgxc_pool_reload cannot run inside a transaction block")));

	/* A Datanode has no pooler active, so do not bother about that */
	if (IS_PGXC_DATANODE)
		PG_RETURN_BOOL(true);

	RemoteXactReloadNode();

	/* Take a lock on pooler to forbid any action during reload */
	PoolManagerLock(true);

	/* No need to reload, node information is consistent */
	if (PoolManagerCheckConnectionInfo())
	{
		/* Release the lock on pooler */
		PoolManagerLock(false);
#ifdef ADB
		/* Sync cluster nextXid with AGTM */
		ClusterSyncXid();
#endif
		PG_RETURN_BOOL(true);
	}

	/* Reload connection information in pooler */
	PoolManagerReloadConnectionInfo();

	/* Be sure it is done consistently */
	if (!PoolManagerCheckConnectionInfo())
	{
		/* Release the lock on pooler */
		PoolManagerLock(false);
#ifdef ADB
		/* Sync cluster nextXid with AGTM */
		ClusterSyncXid();
#endif
		PG_RETURN_BOOL(false);
	}

	/* Now release the lock on pooler */
	PoolManagerLock(false);

	/* Signal other sessions to reconnect to pooler */
	ReloadConnInfoOnBackends();

	/* Session is being reloaded, drop prepared and temporary objects */
	DropAllPreparedStatements();

	/* Now session information is reset in correct memory context */
	old_context = MemoryContextSwitchTo(TopMemoryContext);

	/* Reinitialize session, while old pooler connection is active */
	InitMultinodeExecutor(true);

	/* And reconnect to pool manager */
	PoolManagerReconnect();

	MemoryContextSwitchTo(old_context);

#ifdef ADB
	/* Sync cluster nextXid with AGTM */
	ClusterSyncXid();
#endif

	PG_RETURN_BOOL(true);
}

/*
 * CleanConnection()
 *
 * Utility to clean up Postgres-XC Pooler connections.
 * This utility is launched to all the Coordinators of the cluster
 *
 * Use of CLEAN CONNECTION is limited to a super user.
 * It is advised to clean connections before shutting down a Node or drop a Database.
 *
 * SQL query synopsis is as follows:
 * CLEAN CONNECTION TO
 *		(COORDINATOR num | DATANODE num | ALL {FORCE})
 *		[ FOR DATABASE dbname ]
 *		[ TO USER username ]
 *
 * Connection cleaning can be made on a chosen database called dbname
 * or/and a chosen user.
 * Cleaning is done for all the users of a given database
 * if no user name is specified.
 * Cleaning is done for all the databases for one user
 * if no database name is specified.
 *
 * It is also possible to clean connections of several Coordinators or Datanodes
 * Ex:	CLEAN CONNECTION TO DATANODE dn1,dn2,dn3 FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR co2,co4,co3 FOR DATABASE template1
 *		CLEAN CONNECTION TO DATANODE dn2,dn5 TO USER postgres
 *		CLEAN CONNECTION TO COORDINATOR co6,co1 FOR DATABASE template1 TO USER postgres
 *
 * Or even to all Coordinators/Datanodes at the same time
 * Ex:	CLEAN CONNECTION TO DATANODE * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1
 *		CLEAN CONNECTION TO COORDINATOR * TO USER postgres
 *		CLEAN CONNECTION TO COORDINATOR * FOR DATABASE template1 TO USER postgres
 *
 * When FORCE is used, all the transactions using pooler connections are aborted,
 * and pooler connections are cleaned up.
 * Ex:	CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1;
 *		CLEAN CONNECTION TO ALL FORCE TO USER postgres;
 *		CLEAN CONNECTION TO ALL FORCE FOR DATABASE template1 TO USER postgres;
 *
 * FORCE can only be used with TO ALL, as it takes a lock on pooler to stop requests
 * asking for connections, aborts all the connections in the cluster, and cleans up
 * pool connections associated to the given user and/or database.
 */
void
CleanConnection(CleanConnStmt *stmt)
{
	ListCell   *nodelist_item;
	List	   *stmt_nodes = NIL;
	char	   *dbname = stmt->dbname;
	char	   *username = stmt->username;
	bool		is_coord = stmt->is_coord;
	bool		is_force = stmt->is_force;

	/* Only a DB administrator can clean pooler connections */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to clean pool connections")));

	/* Database name or user name is mandatory */
	if (!dbname && !username)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("must define Database name or user name")));

	/* Check if the Database exists by getting its Oid */
	if (dbname &&
		!OidIsValid(get_database_oid(dbname, true)))
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database \"%s\" does not exist", dbname)));
		return;
	}

	/* Check if role exists */
	if (username &&
		!OidIsValid(get_role_oid(username, false)))
	{
		ereport(WARNING,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role \"%s\" does not exist", username)));
		return;
	}

	if (!IsPoolHandle())
	{
		PoolManagerReconnect();
	}

	/*
	 * FORCE is activated,
	 * Send a SIGTERM signal to all the processes and take a lock on Pooler
	 * to avoid backends to take new connections when cleaning.
	 * Only Disconnect is allowed.
	 */
	if (is_force)
	{
		int loop = 0;
		int *proc_pids = NULL;
		int num_proc_pids, count;

		num_proc_pids = PoolManagerAbortTransactions(dbname, username, &proc_pids);

		/*
		 * Watch the processes that received a SIGTERM.
		 * At the end of the timestamp loop, processes are considered as not finished
		 * and force the connection cleaning has failed
		 */

		while (num_proc_pids > 0 && loop < TIMEOUT_CLEAN_LOOP)
		{
			for (count = num_proc_pids - 1; count >= 0; count--)
			{
				switch(kill(proc_pids[count],0))
				{
					case 0: /* Termination not done yet */
						break;

					default:
						/* Move tail pid in free space */
						proc_pids[count] = proc_pids[num_proc_pids - 1];
						num_proc_pids--;
						break;
				}
			}
			pg_usleep(1000000);
			loop++;
		}

		if (proc_pids)
			pfree(proc_pids);

		if (loop >= TIMEOUT_CLEAN_LOOP)
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("All Transactions have not been aborted")));
	}

	foreach(nodelist_item, stmt->nodes)
	{
		Value *value = lfirst(nodelist_item);
		Form_pgxc_node xcnode;
		HeapTuple tuple = SearchSysCache1(PGXCNODENAME, PointerGetDatum(strVal(value)));
		if (!HeapTupleIsValid(tuple))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("Node \"%s\" not exist", strVal(value))));
		}

		xcnode = (Form_pgxc_node)GETSTRUCT(tuple);
		if ((is_coord && xcnode->node_type != PGXC_NODE_COORDINATOR) ||
			(!is_coord && xcnode->node_type != PGXC_NODE_DATANODE))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("%s node \"%s\" not exist", is_coord ? "coordinator":"datanode", strVal(value))));
		}

		stmt_nodes = lappend_oid(stmt_nodes, HeapTupleGetOid(tuple));
		ReleaseSysCache(tuple);
	}

	if (stmt->nodes == NIL)
		stmt_nodes = get_all_xcnode_oid();

	/*
	 * If force is launched, send a signal to all the processes
	 * that are in transaction and take a lock.
	 * Get back their process number and watch them locally here.
	 * Process are checked as alive or not with pg_usleep and when all processes are down
	 * go out of the control loop.
	 * If at the end of the loop processes are not down send an error to client.
	 * Then Make a clean with normal pool cleaner.
	 * Always release the lock when calling CLEAN CONNECTION.
	 */

	/* Finish by contacting Pooler Manager */
	PoolManagerCleanConnectionOid(stmt_nodes, dbname, username);

	/* Clean up memory */
	list_free(stmt_nodes);
}

/*
 * DropDBCleanConnection
 *
 * Clean Connection for given database before dropping it
 * FORCE is not used here
 */
void
DropDBCleanConnection(char *dbname)
{
	List	*list;

	/* Check permissions for this database */
	if (!pg_database_ownercheck(get_database_oid(dbname, true), GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
					   dbname);
	if (!IsPoolHandle())
	{
		PoolManagerReconnect();
	}

	list = get_all_xcnode_oid();
	PoolManagerCleanConnectionOid(list, dbname, NULL);

	/* Clean up memory */
	list_free(list);
}

/*
 * HandlePoolerReload
 *
 * This is called when PROCSIG_PGXCPOOL_RELOAD is activated.
 * Abort the current transaction if any, then reconnect to pooler.
 * and reinitialize session connection information.
 */
void
HandlePoolerReload(void)
{
	MemoryContext old_context;
	ResourceOwner volatile reload_ro;

	/* A Datanode has no pooler active, so do not bother about that */
	if (IS_PGXC_DATANODE || !IsNormalProcessingMode() || MessageContext == NULL)
		return;

	/* Abort existing xact if any */
	AbortCurrentTransaction();

	/* Drop temp objects */
	StartTransactionCommand();
	ResetTempTableNamespace();
	CommitTransactionCommand();

	/* Session is being reloaded, drop prepared and temporary objects */
	DropAllPreparedStatements();

	/* Now session information is reset in correct memory context */
	old_context = MemoryContextSwitchTo(TopMemoryContext);

	/* Need to be able to look into catalogs */
	CurrentResourceOwner = reload_ro = ResourceOwnerCreate(CurrentResourceOwner, "ForPoolerReload");

	PG_TRY();
	{
		/* Reinitialize session, while old pooler connection is active */
		InitMultinodeExecutor(true);

		/* And reconnect to pool manager */
		PoolManagerReconnect();
	}PG_CATCH();
	{
		CurrentResourceOwner = ResourceOwnerGetParent(reload_ro);
		PG_RE_THROW();
	}PG_END_TRY();

	/* Send a message back to client regarding session being reloaded */
	ereport(WARNING,
			(errcode(ERRCODE_OPERATOR_INTERVENTION),
			 errmsg("session has been reloaded due to a cluster configuration modification"),
			 errdetail("Temporary and prepared objects hold by session have been"
					   " dropped and current transaction has been aborted.")));

	/* Release everything */
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	CurrentResourceOwner = ResourceOwnerGetParent(reload_ro);
	ResourceOwnerDelete(reload_ro);

	MemoryContextSwitchTo(old_context);
}

static List* get_all_xcnode_oid(void)
{
	HeapTuple tuple;
	Relation rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	HeapScanDesc scan = heap_beginscan_catalog(rel, 0, NULL);
	List *list = NIL;

	while ((tuple=heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		list = lappend_oid(list, HeapTupleGetOid(tuple));
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	return list;
}
