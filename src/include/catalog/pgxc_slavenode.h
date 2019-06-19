/*-------------------------------------------------------------------------
 *
 * pgxc_slavenode.h
 *	  definition of the system "PGXC slavenode" relation (pgxc_slavenode)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * src/include/catalog/pgxc_slavenode.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_SLAVENODE_H
#define PGXC_SLAVENODE_H

#include "catalog/genbki.h"
#include "catalog/pgxc_slavenode_d.h"

CATALOG(pgxc_slavenode,4993,PgxcSlaveNodeRelationId) BKI_SHARED_RELATION
{
	NameData	node_name;

	/*
	 * Possible node types are defined as follows
	 * Types are defined below PGXC_NODES_XXXX
	 */
	char		node_type;

	/*
	 * Port number of the node to connect to
	 */
	int32 		node_port;

	/*
	 * Host name of IP address of the node to connect to
	 */
	NameData	node_host;

	/*
	 * Is this node primary
	 */
	bool		nodeis_primary;

	/*
	 * Is this node preferred
	 */
	bool		nodeis_preferred;
	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	int32		node_id;
	/*
	 * the node's master name
	 */
	NameData	node_mastername;
} FormData_pgxc_slavenode;

typedef FormData_pgxc_slavenode *Form_pgxc_slavenode;

#ifdef EXPOSE_TO_CLIENT_CODE

/* Possible types of nodes */
#define PGXC_SLAVE_NODE_COORDINATOR		'c'
#define PGXC_SLAVE_NODE_DATANODE		'd'
#define PGXC_NODE_NONE					'N'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PGXC_SLAVENODE_H */
