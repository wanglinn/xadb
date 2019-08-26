/*-------------------------------------------------------------------------
 *
 * locator.h
 *		Externally declared locator functions
 *
 *
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * src/include/pgxc/locator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCATOR_H
#define LOCATOR_H

#include "catalog/pgxc_class_d.h"
#include "nodes/primnodes.h"
#include "utils/relcache.h"

/*
 * How relation is accessed in the query
 */
typedef enum RelationAccessType
{
	RELATION_ACCESS_READ,				/* SELECT */
	RELATION_ACCESS_READ_FOR_UPDATE,	/* SELECT FOR UPDATE */
	RELATION_ACCESS_UPDATE,				/* UPDATE OR DELETE */
	RELATION_ACCESS_INSERT				/* INSERT */
} RelationAccessType;

typedef struct LocatorKeyInfo
{
	Expr	   *key;				/* not NULL when attno is 0, or NULL when attno is not 0 */
	Oid			opclass;			/* operator class for key compare */
	Oid			opfamily;			/* operator family from operator class */
	Oid			collation;			/* user-specified collation */
	AttrNumber	attno;				/* attribute number if key is column, or 0 when key is a exprssion */
}LocatorKeyInfo;

typedef struct RelationLocInfo
{
	Oid			relid;					/* OID of relation */
	char		locatorType;			/* locator type, see above */
	List	   *keys;					/* Distribution key(s) attribute, list of LocatorKeyInfo */
	List	   *nodeids;				/* Node Oid(s) where data is located */
	List	   *values;					/* each nodes values for distribute by list and range */
	List	   *masternodeids;
	List	   *slavenodeids;
} RelationLocInfo;

#define IsRelationReplicated(rel_loc)				IsLocatorReplicated((rel_loc)->locatorType)
#define IsRelationDistributedByValue(rel_loc)		IsLocatorDistributedByValue((rel_loc)->locatorType)
#define FirstLocKeyInfo(rel_loc)					((LocatorKeyInfo*)linitial(rel_loc->keys))

/*
 * Nodes to execute on
 * primarynodelist is for replicated table writes, where to execute first.
 * If it succeeds, only then should it be executed on nodelist.
 * primarynodelist should be set to NULL if not doing replicated write operations
 * Note on dist_vars:
 * dist_vars is a list of Var nodes indicating the columns by which the
 * relations (result of query) are distributed. The result of equi-joins between
 * distributed relations, can be considered to be distributed by distribution
 * columns of either of relation. Hence a list. dist_vars is ignored in case of
 * distribution types other than HASH or MODULO.
 */
typedef struct ExecNodes
{
	NodeTag			type;
	RelationAccessType	accesstype;		/* Access type to determine execution
										 * nodes */
	char			baselocatortype;	/* Locator type, see above */
	Oid				en_relid;			/* Relation to determine execution nodes */
	Oid				en_funcid;			/* User-defined function OID */
	List		   *en_expr;			/* Expression to evaluate at execution time
										 * if planner can not determine execution
										 * nodes */
	List		   *en_dist_vars;		/* See above for details */
	List		   *nodeids;			/* Node ids list */
} ExecNodes;

#define IsExecNodesReplicated(en)				IsLocatorReplicated((en)->baselocatortype)
#define IsExecNodesColumnDistributed(en) 		IsLocatorColumnDistributed((en)->baselocatortype)
#define IsExecNodesDistributedByValue(en)		IsLocatorDistributedByValue((en)->baselocatortype)

/* Function for RelationLocInfo building and management */
#define RelationIdHasLocator(relid)	\
	HasRelationLocator(relid)

#define RelationHasLocator(relation) \
	HasRelationLocator(RelationGetRelid(relation))

extern bool HasRelationLocator(Oid relid);
extern RelationLocInfo *RelationIdBuildLocator(Oid relid);
extern void RelationBuildLocator(Relation rel);
extern RelationLocInfo *GetRelationLocInfo(Oid relid);
extern RelationLocInfo *CopyRelationLocInfo(RelationLocInfo *srcInfo);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);
extern char *GetRelationDistribColumn(RelationLocInfo *locInfo);
extern char GetLocatorType(Oid relid);
extern List *GetPreferredRepNodeIds(List *nodeids);
extern bool IsTableDistOnPrimary(RelationLocInfo *locInfo);
extern bool IsLocatorInfoEqual(const RelationLocInfo *a, const RelationLocInfo *b);
extern Oid GetRandomRelNodeId(Oid relid);
extern bool IsTypeDistributable(Oid colType);
extern bool IsDistribOnlyOneColumn(Oid relid, AttrNumber attNum);
extern bool LocatorIncludeColumn(RelationLocInfo *loc, AttrNumber attno, bool include_expr);
extern bool LocatorKeyIncludeColumn(List *keys, AttrNumber attno, bool include_expr);

extern ExecNodes *GetRelationNodes(RelationLocInfo *rel_loc_info,
								   int nelems,
								   Datum* valueForDistCol,
								   bool* isValueNull,
								   Oid* typeOfValueForDistCol,
								   RelationAccessType accessType);
extern ExecNodes *GetRelationNodesByQuals(Oid reloid,
										  Index varno,
										  Node *quals,
										  RelationAccessType relaccess);
extern ExecNodes *MakeExecNodesByOids(RelationLocInfo *loc_info, List *oids, RelationAccessType accesstype);
extern AttrNumber GetFirstLocAttNumIfOnlyOne(RelationLocInfo *loc);

/* Global locator data */
extern void FreeExecNodes(ExecNodes **exec_nodes);

extern List *GetInvolvedNodes(RelationLocInfo *rel_loc, int nelems, Datum* dist_values, bool* dist_nulls,
							  Oid* dist_types, RelationAccessType accessType);
extern List *adbUseDnSlaveNodeids(List *nodeids);
extern List *adbGetRelationNodeids(Oid relid);
#endif   /* LOCATOR_H */
