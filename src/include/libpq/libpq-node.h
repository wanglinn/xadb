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

typedef struct PQNHookFunctions PQNHookFunctions;

typedef bool (*PQNHookError_function)(PQNHookFunctions *pub);
typedef bool (*PQNHookCopyOut_function)(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);
typedef bool (*PQNHookCopyInOnly_function)(PQNHookFunctions *pub, struct pg_conn *conn);
typedef bool (*PQNHookResult_function)(PQNHookFunctions *pub, struct pg_conn *conn, struct pg_result *res);

struct PQNHookFunctions
{
	PQNHookError_function HookError;
	PQNHookCopyOut_function HookCopyOut;
	PQNHookCopyInOnly_function HookCopyInOnly;
	PQNHookResult_function HookResult;
};

typedef struct pg_conn* (*GetPGconnHook)(void *arg);

PGDLLIMPORT const PQNHookFunctions PQNDefaultHookFunctions;
PGDLLIMPORT const PQNHookFunctions PQNFalseHookFunctions;

extern void PQNForceReleaseWhenTransactionFinish(void);
extern List *PQNGetConnUseOidList(List *oid_list);
extern struct pg_conn* PQNFindConnUseOid(Oid oid);
extern List* PQNGetAllConns(void);
extern bool PQNOneExecFinish(struct pg_conn *conn, const PQNHookFunctions *hook, bool blocking);
extern bool PQNListExecFinish(List *conn_list, GetPGconnHook get_pgconn_hook, const PQNHookFunctions *hook, bool blocking);
extern bool PQNEFHNormal(void *context, struct pg_conn *conn, PQNHookFuncType type, ...);
extern void PQNExecFinish_trouble(struct pg_conn *conn);
extern void PQNReleaseAllConnect(bool request_cancel);
extern void PQNRequestCancelAllconnect(void);
extern void PQNReportResultError(struct pg_result *result, struct pg_conn *conn, int elevel, bool free_result);
extern const char *PQNConnectName(struct pg_conn *conn);
extern Oid PQNConnectOid(struct pg_conn *conn);
extern int PQNFlush(List *conn_list, bool blocking);

extern void PQNputCopyData(List *conn_list, const char *buffer, int nbytes);
extern void PQNPutCopyEnd(List *conn_list);

extern void* PQNMakeDefHookFunctions(Size size);

extern bool PQNDefHookError(PQNHookFunctions *pub);
extern bool PQNDefHookCopyOut(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);
extern bool PQNFalseHookCopyOut(PQNHookFunctions *pub, struct pg_conn *conn, const char *buf, int len);
extern bool PQNDefHookCopyInOnly(PQNHookFunctions *pub, struct pg_conn *conn);
extern bool PQNDefHookResult(PQNHookFunctions *pub, struct pg_conn *conn, struct pg_result *res);

#endif /* LIBPQ_NODE_H */
