/*-------------------------------------------------------------------------
 *
 * pgxc_node.h
 *	  definition of the system "PGXC node" relation (pgxc_node)
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * src/include/catalog/pgxc_node.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGXC_NODE_H
#define PGXC_NODE_H

#include "catalog/genbki.h"
#include "catalog/pgxc_node_d.h"

CATALOG(pgxc_node,9015,PgxcNodeRelationId) BKI_SHARED_RELATION
{
	Oid			oid;			/* oid */

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
	bool		nodeis_gtm;

	/*
	 * Node identifier to be used at places where a fixed length node identification is required
	 */
	int32		node_id;

	/*
	 * the master name of the node
	 */
	Oid			node_master_oid;
} FormData_pgxc_node;

typedef FormData_pgxc_node *Form_pgxc_node;

#ifdef EXPOSE_TO_CLIENT_CODE

/* Possible types of nodes */
#define PGXC_NODE_COORDINATOR		'C'
#define PGXC_NODE_DATANODE			'D'
#define PGXC_NODE_DATANODESLAVE		'E'
#define PGXC_NODE_NONE				'N'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PGXC_NODE_H */
