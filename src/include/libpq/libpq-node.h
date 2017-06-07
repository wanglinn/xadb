#ifndef LIBPQ_NODE_H
#define LIBPQ_NODE_H

struct pg_result;
struct pg_conn;

extern List *PQNGetConnUseOidList(List *oid_list);
extern int PQNWaitResult(List *conn_list, struct pg_conn **ppconn, bool noError);
extern int PQNListGetReslut(List *conn_list, struct pg_result **ppres, bool noError);
extern void PQNListExecFinish(List *conn_list, bool report_error);
extern void PQNReleaseAllConnect(void);
extern void PQNReportResultError(struct pg_result *result, struct pg_conn *conn, int elevel, bool free_result);
extern const char *PQNConnectName(struct pg_conn *conn);
#endif /* LIBPQ_NODE_H */
