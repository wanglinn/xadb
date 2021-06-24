/*-----------------------------------------------------------
 *
 * Portions Copyright (c) 2010-2013, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 *-----------------------------------------------------------
 */
#ifndef PGXC_CLASS_H
#define PGXC_CLASS_H

#include "catalog/genbki.h"
#include "catalog/pgxc_class_d.h"

CATALOG(pgxc_class,9020,PgxcClassRelationId)
{
	/* Table Oid */
	Oid			pcrelid;

	/* Type of distribution */
	char		pclocatortype;

	/* List of nodes used by table */
	oidvector	nodeoids;

#ifdef CATALOG_VARLEN
	int2vector	pcattrs BKI_FORCE_NULL;		/* each member of the array is the attribute
											 * number of a distribute key column, or 0 if
											 * the column is actually an expression */

	oidvector	pcclass BKI_FORCE_NULL;		/* operator class to compare keys */

	pg_node_tree pcexprs BKI_FORCE_NULL;	/* list of expression in the distribute key;
											 * one item for each zero entry in pcattrs[] */

	pg_node_tree pcvalues BKI_FORCE_NULL;	/* list of each node's hash(...) values */
#endif
} FormData_pgxc_class;

typedef FormData_pgxc_class *Form_pgxc_class;

DECLARE_TOAST(pgxc_class, 9126, 9127);
#define PgxcClassToastTable 9126
#define PgxcClassToastIndex 9127

DECLARE_UNIQUE_INDEX(pgxc_class_pcrelid_index, 9021, on pgxc_class using btree(pcrelid oid_ops));
#define PgxcClassPgxcRelIdIndexId 	9021

#ifdef EXPOSE_TO_CLIENT_CODE

typedef enum PgxcClassAlterType
{
	PGXC_CLASS_ALTER_DISTRIBUTION = 1,
	PGXC_CLASS_ALTER_NODES = 2,
	PGXC_CLASS_ALTER_ALL = PGXC_CLASS_ALTER_DISTRIBUTION|PGXC_CLASS_ALTER_NODES
} PgxcClassAlterType;

#define LOCATOR_TYPE_INVALID		'\0'
#define LOCATOR_TYPE_REPLICATED		'R'
#define LOCATOR_TYPE_HASH			'H'
#define LOCATOR_TYPE_MODULO			'M'
#define LOCATOR_TYPE_RANDOM			'N'
#define LOCATOR_TYPE_NONE			'O'
#define LOCATOR_TYPE_DISTRIBUTED	'D'	/* for distributed table without specific
										 * scheme, e.g. result of JOIN of
										 * replicated and distributed table */


/* Maximum number of preferred Datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsLocatorNone(x)						((x) == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) 					((x) == LOCATOR_TYPE_REPLICATED)
#define IsLocatorColumnDistributed(x) 			((x) == LOCATOR_TYPE_HASH || \
												 (x) == LOCATOR_TYPE_RANDOM || \
												 (x) == LOCATOR_TYPE_MODULO || \
												 (x) == LOCATOR_TYPE_DISTRIBUTED)
#define IsLocatorDistributedByValue(x)			((x) == LOCATOR_TYPE_HASH || \
												 (x) == LOCATOR_TYPE_MODULO)

#endif							/* EXPOSE_TO_CLIENT_CODE */

typedef struct DistributeNameType
{
	char	loc_type;
	char	name[15];
}DistributeNameType;

extern const DistributeNameType all_distribute_name_type[];
extern const uint32				cnt_distribute_name_type;
extern int default_distribute_by;
extern char	*default_user_group;

extern void PgxcClassCreate(Oid pcrelid,
							char pclocatortype,
							List *keys,
							List *values,
							int numnodes,
							Oid *nodes);
extern void PgxcClassAlter(Oid pcrelid,
						   char pclocatortype,
						   List *keys,
						   List *values,
						   int numnodes,
						   Oid *nodes,
						   PgxcClassAlterType type);
extern void RemovePgxcClass(Oid pcrelid);

extern void CreatePgxcRelationAttrDepend(Oid relid, AttrNumber attnum);

extern uint32 MakeHashModuloNodesAndValues(Oid *remainder_node, uint32 modulus, Oid **nodeoids, List **values);

#endif   /* PGXC_CLASS_H */
