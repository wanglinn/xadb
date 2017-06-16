#ifndef LIBPQ_NODE_H
#define LIBPQ_NODE_H

struct pg_result;
struct pg_conn;

typedef enum PQNHookFuncType
{
	PQNHFT_ERROR = 1,		/* system error, see errno */
	PQNHFT_COPY_OUT_DATA,	/* got copy out data */
	PQNHFT_COPY_IN_ONLY,	/* copy in only */
	PQNHFT_RESULT			/* got PQresult */
}PQNHookFuncType;

#define PQN_COORD_VALUE		0x40000000
#define PQN_DATANODE_VALUE	0x80000000
/*#define PQN_AGTM_MARK		0xC0000000*/
#define PQN_NODE_VALUE_MARK	0x3FFFFFFF
#define PQN_NODE_TYPE_MARK	0xC0000000

typedef bool (*PQNExecFinishHook_function)(void *context, struct pg_conn *conn, PQNHookFuncType type,  ...);

extern List *PQNGetConnUseOidList(List *oid_list);
extern Oid PQNNodeGetNodeOid(int node_index);
extern bool PQNOneExecFinish(struct pg_conn *conn, PQNExecFinishHook_function hook, const void *context, bool blocking);
extern bool PQNListExecFinish(List *conn_list, PQNExecFinishHook_function hook, const void *context, bool blocking);
extern bool PQNEFHNormal(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);
extern void PQNReleaseAllConnect(void);
extern void PQNReportResultError(struct pg_result *result, struct pg_conn *conn, int elevel, bool free_result);
extern const char *PQNConnectName(struct pg_conn *conn);
extern Oid PQNConnectOid(struct pg_conn *conn);

#endif /* LIBPQ_NODE_H */
