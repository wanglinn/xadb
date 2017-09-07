#ifndef INTER_NODE_H
#define INTER_NODE_H

#include "nodes/pg_list.h"

typedef enum
{
	IBLOCK_DEFAULT		= 0,		/* default state */
	IBLOCK_BEGIN		= 1 << 0,	/* starting inter transaction block OK */
	IBLOCK_INPROGRESS	= 1 << 1,	/* live inter transaction */
	IBLOCK_PREPARE		= 1 << 2,	/* live inter transaction, PREPARE OK */
	IBLOCK_END			= 1 << 3,	/* COMMIT/COMMIT PREPARED OK */
	IBLOCK_ABORT_END	= 1 << 4,	/* ROLLBACK/ROLLBACK PREPARED OK */
	IBLOCK_ABORT		= 1 << 5,	/* failed inter transaction, awaiting ROLLBACK  */
} IBlockState;

typedef enum
{
	TYPE_CN_NODE	=	1,
	TYPE_DN_NODE	=	2,
} NodeType;

typedef struct NodeHandle
{
	Oid					node_oid;
	NameData			node_name;
	NodeType			node_type;
	bool				node_primary;

	/* set by caller */
	struct pg_conn	   *node_conn;
	void			   *node_context;	/* it is set by caller for callback */
} NodeHandle;

typedef struct NodeMixHandle
{
	NodeHandle	   *pr_handle;	/* primary NodeHandle */
	List		   *handles;	/* list of NodeHandle */
} NodeMixHandle;

#if NOT_USED
typedef struct NodeMixHandle
{
	int				cn_count;
	int				dn_count;
	NodeHandle	  **cn_handles;
	NodeHandle	  **dn_handles;
	NodeHandle	   *pr_handle;
} NodeMixHandle;
#endif

extern void ResetNodeExecutor(void);
extern void ReleaseNodeExecutor(void);
extern void InitNodeExecutor(bool force);
extern NodeHandle *GetNodeHandle(Oid node_oid, bool attatch, void *context);
extern NodeHandle *GetCnHandle(Oid node_oid, bool attatch, void *context);
extern NodeHandle *GetDnHandle(Oid node_oid, bool attatch, void *context);
extern void HandleAttatchPGconn(NodeHandle *handle);
extern void HandleDetachPGconn(NodeHandle *handle);
extern void HandleReAttatchPGconn(NodeHandle *handle);
extern NodeMixHandle *GetMixedHandles(const List *oid_list, void *context);
#if NOT_USED
extern NodeMixHandle *GetMixedHandles(List *cnlist, List *dnlist);
#endif
extern NodeMixHandle *GetAllHandles(void);
extern NodeMixHandle *CopyMixhandle(NodeMixHandle *src);
extern NodeMixHandle *ConcatMixHandle(NodeMixHandle *mix1, NodeMixHandle *mix2);
extern void FreeMixHandle(NodeMixHandle *mix_handle);

extern List *GetAllCnOids(bool include_self);
extern List *GetAllDnOids(bool include_self);
extern List *GetAllNodeOids(bool include_self);

#endif /* INTER_NODE_H */
