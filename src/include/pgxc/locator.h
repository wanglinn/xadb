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

#define LOCATOR_TYPE_REPLICATED			'R'
#define LOCATOR_TYPE_HASH			'H'
#define LOCATOR_TYPE_RANGE			'G'
#define LOCATOR_TYPE_RANDOM			'N'
#define LOCATOR_TYPE_CUSTOM			'C'
#define LOCATOR_TYPE_MODULO			'M'
#define LOCATOR_TYPE_NONE			'O'
#define LOCATOR_TYPE_DISTRIBUTED		'D'	/* for distributed table without specific
										 * scheme, e.g. result of JOIN of
										 * replicated and distributed table */
#define LOCATOR_TYPE_USER_DEFINED		'U'
#define LOCATOR_TYPE_HASHMAP			'B'


/* Maximum number of preferred Datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

#define HASH_SIZE 4096
#define HASH_MASK 0x00000FFF;

#define IsLocatorNone(x)						((x) == LOCATOR_TYPE_NONE)
#define IsLocatorReplicated(x) 					((x) == LOCATOR_TYPE_REPLICATED)
#define IsLocatorColumnDistributed(x) 			((x) == LOCATOR_TYPE_HASH || \
												 (x) == LOCATOR_TYPE_RANDOM || \
												 (x) == LOCATOR_TYPE_HASHMAP || \
												 (x) == LOCATOR_TYPE_MODULO || \
												 (x) == LOCATOR_TYPE_DISTRIBUTED || \
												 (x) == LOCATOR_TYPE_USER_DEFINED)
#define IsLocatorDistributedByValue(x)			((x) == LOCATOR_TYPE_HASH || \
												 (x) == LOCATOR_TYPE_MODULO || \
												 (x) == LOCATOR_TYPE_HASHMAP || \
												 (x) == LOCATOR_TYPE_RANGE)
#define IsLocatorDistributedByUserDefined(x)	((x) == LOCATOR_TYPE_USER_DEFINED)

#define IsLocatorHashmap(x) 					(x == LOCATOR_TYPE_HASHMAP)


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

typedef struct RelationLocInfo
{
	Oid			relid;					/* OID of relation */
	char		locatorType;			/* locator type, see above */
	AttrNumber	partAttrNum;			/* Distribution column attribute */
	List	   *nodeids;				/* Node ids where data is located */
	List	   *masternodeids;
	List	   *slavenodeids;
	Oid			funcid;					/* Oid of user-defined distribution function */
	List	   *funcAttrNums;			/* Attributes indices used for user-defined function  */
} RelationLocInfo;

#define IsRelationReplicated(rel_loc)				IsLocatorReplicated((rel_loc)->locatorType)
#define IsRelationColumnDistributed(rel_loc)		IsLocatorColumnDistributed((rel_loc)->locatorType)
#define IsRelationDistributedByValue(rel_loc)		IsLocatorDistributedByValue((rel_loc)->locatorType)
#define IsRelationDistributedByUserDefined(rel_loc)	IsLocatorDistributedByUserDefined((rel_loc)->locatorType)

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
#define IsExecNodesDistributedByUserDefined(en)	IsLocatorDistributedByUserDefined((en)->baselocatortype)

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
extern bool EqualRelationLocInfo(const RelationLocInfo *a, const RelationLocInfo *b);
extern void FreeRelationLocInfo(RelationLocInfo *relationLocInfo);
extern char *GetRelationDistribColumn(RelationLocInfo *locInfo);
extern List *GetRelationDistribColumnList(RelationLocInfo *locInfo);
extern Oid GetRelationDistribFunc(Oid relid);
extern char GetLocatorType(Oid relid);
extern List *GetPreferredRepNodeIds(List *nodeids);
extern bool IsTableDistOnPrimary(RelationLocInfo *locInfo);
extern bool IsLocatorInfoEqual(RelationLocInfo *locInfo1,
							   RelationLocInfo *locInfo2);
extern Oid GetRandomRelNodeId(Oid relid);
extern bool IsTypeDistributable(Oid colType);
extern bool IsDistribColumn(Oid relid, AttrNumber attNum);

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
extern ExecNodes *GetRelationNodesByMultQuals(RelationLocInfo *rel_loc_info,
											  Oid reloid,
											  Index varno,
											  Node *quals,
											  RelationAccessType relaccess);
extern void CoerceUserDefinedFuncArgs(Oid funcid,
									  int nargs,
									  Datum *values,
									  bool *nulls,
									  Oid *types);

/* Global locator data */
extern void FreeExecNodes(ExecNodes **exec_nodes);

extern List *GetInvolvedNodes(RelationLocInfo *rel_loc, int nelems, Datum* dist_values, bool* dist_nulls,
							  Oid* dist_types, RelationAccessType accessType);
extern List *GetInvolvedNodesByQuals(Oid reloid, Index varno, Node *quals, RelationAccessType relaccess);
extern List *GetInvolvedNodesByMultQuals(RelationLocInfo *rel_loc, Index varno, Node *quals, RelationAccessType relaccess);
extern List *adbUseDnSlaveNodeids(List *nodeids);
extern List *adbGetRelationNodeids(Oid relid);
extern void adbUpdateListNodeids(List *destList, List *sourceList);
#endif   /* LOCATOR_H */
