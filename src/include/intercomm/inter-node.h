/*-------------------------------------------------------------------------
 *
 * inter-node.h
 *
 *	  Internode struct and routines definition
 *
 *
 * Portions Copyright (c) 2016-2017, ADB Development Group
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/intercomm/inter-node.h
 *
 *-------------------------------------------------------------------------
 */
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
	TYPE_CN_NODE		= 1 << 0,	/* coordinator node */
	TYPE_DN_NODE		= 1 << 1,	/* datanode node */
} NodeType;

typedef struct NodeHandle
{
	Oid					node_id;
	NameData			node_name;
	NodeType			node_type;
	bool				node_primary;

	/* set by caller */
	struct pg_conn	   *node_conn;
	void			   *node_context;	/* InterXactState, it is set by caller for callback */
	void			   *node_owner;		/* RemoteQueryState, it is set by caller for cache data */
} NodeHandle;

typedef struct NodeMixHandle
{
	NodeType			mix_types;	/* current mixed NodeType(s) */
	NodeHandle		   *pr_handle;	/* primary NodeHandle */
	List			   *handles;	/* list of NodeHandle */
} NodeMixHandle;

typedef struct CustomOption
{
	void			   *cumstom;
	const struct PGcustumFuns*cumstom_funcs;
} CustomOption;

extern void ResetNodeExecutor(void);
extern void ReleaseNodeExecutor(void);
extern void InitNodeExecutor(bool force);
extern NodeHandle *GetNodeHandle(Oid node_id, bool attatch, void *context);
extern NodeHandle *GetCnHandle(Oid cn_id, bool attatch, void *context);
extern NodeHandle *GetDnHandle(Oid dn_id, bool attatch, void *context);
extern void HandleAttatchPGconn(NodeHandle *handle);
extern void HandleDetachPGconn(NodeHandle *handle);
extern void HandleReAttatchPGconn(NodeHandle *handle);
extern CustomOption *HandleSetCustomOption(NodeHandle *handle, void *custom, struct PGcustumFuns *custom_funcs);
extern void HandleResetCustomOption(NodeHandle *handle, CustomOption *opt);
extern List *HandleListSetCustomOption(List *handle_list, struct PGcustumFuns *custom_funcs);
extern void HandleListResetCustomOption(List *handle_list, List *opt_list);
extern NodeMixHandle *GetMixedHandles(const List *node_list, void *context);
extern NodeMixHandle *GetAllHandles(void *context);
extern NodeMixHandle *CopyMixhandle(NodeMixHandle *src);
extern NodeMixHandle *ConcatMixHandle(NodeMixHandle *mix1, NodeMixHandle *mix2);
extern void FreeMixHandle(NodeMixHandle *mix_handle);

extern List *GetAllCnIds(bool include_self);
extern List *GetAllDnIds(bool include_self);
extern List *GetAllNodeIds(bool include_self);

#endif /* INTER_NODE_H */
