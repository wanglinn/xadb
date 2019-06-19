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
	TYPE_CN_NODE		= 1 << 0,	/* coordinator node */
	TYPE_DN_NODE		= 1 << 1,	/* datanode node */
	TYPE_DN_SLAVENODE	= 1 << 2,	/* datanode slave node */
} NodeType;

typedef struct NodeHandle
{
	Oid					node_id;
	NameData			node_name;
	NodeType			node_type;
	bool				node_primary;
	bool				node_preferred;

	/* used for hash table */
	bool				isvalid;		/* Used to mark whether or not it is valid in the hash table */
	uint32				hashvalue;		/* The hash value is obtained by calculating the node_id  */

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

extern Oid SelfNodeID;

extern void ResetNodeExecutor(void);
extern void InitializeNodeExecutor(void);
extern void AtStart_NodeExecutor(void);
extern NodeHandle *GetNodeHandle(Oid node_id, bool attatch, void *context);
extern List *GetNodeHandleList(const Oid *nodes, int nnodes,
						   bool include_self, bool noerror,
						   bool attatch, void *context);
extern struct pg_conn *HandleGetPGconn(void *handle);
extern CustomOption *PGconnSetCustomOption(struct pg_conn *conn, void *custom, struct PGcustumFuns *custom_funcs);
extern void PGconnResetCustomOption(struct pg_conn *conn, CustomOption *opt);
extern NodeMixHandle *GetMixedHandles(const List *node_list, void *context);
extern NodeMixHandle *CopyMixhandle(NodeMixHandle *src);
extern NodeMixHandle *ConcatMixHandle(NodeMixHandle *mix1, NodeMixHandle *mix2);
extern void FreeMixHandle(NodeMixHandle *cur_handle);

extern List *GetAllCnIDL(bool include_self);
extern List *GetAllDnIDL(bool include_self);
extern List *GetAllNodeIDL(bool include_self);

extern Oid *GetAllCnIDA(bool include_self, int *cn_num);
extern Oid *GetAllDnIDA(bool include_self, int *dn_num);
extern Oid *GetAllNodeIDA(bool include_self, int *node_num);

extern const char *GetNodeName(const Oid node_id);
extern const char *GetPrimaryNodeName(void);
extern NodeHandle *GetPrimaryNodeHandle(void);
extern Oid GetPrimaryNodeID(void);
extern bool IsPrimaryNode(Oid node_id);
extern bool HasPrimaryNode(const List *node_list);

extern List *GetPreferredRepNodes(const List *src_nodes);

extern Size EstimateNodeInfoSpace(void);
extern void SerializeNodeInfo(Size maxsize, char *ptr);
extern void RestoreNodeInfo(char *start_addr);

#ifdef DEBUG_ADB
extern void DebugPrintPGconn(const char *desc, Oid node, struct pg_conn *conn);
extern void DebugPrintHandle(const char *desc, NodeHandle *handle);
extern void DebugPrintPGconnList(const char *desc, List *conn_list);
extern void DebugPrintHandleList(const char *desc, List *handle_list);
#endif

#endif /* INTER_NODE_H */
