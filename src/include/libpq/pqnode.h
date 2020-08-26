/*--------------------------------------------------------------------
 * pqnode.h
 *
 *修改完成agtm_ReadCommand函数(替换了原来的SocketBackend函数),
 *可以同时accept客户端消息以及接受新的连接
 *(随机端口通过agtm_port GUC参数已发送到客户端，还未实现accept新连接的代码)
 *
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * src/agtm/include/pqnode.h
 *--------------------------------------------------------------------
 */
#ifndef PQ_NODE_H
#define PQ_NODE_H

typedef struct pq_comm_node pq_comm_node;

extern pgsocket socket_pq_node(pq_comm_node *node);
extern bool pq_node_send_pending(pq_comm_node *node);
extern bool pq_node_is_write_only(pq_comm_node *node);
extern int	pq_node_flush_sock(pq_comm_node *node);
extern pq_comm_node* pq_node_new(pgsocket sock, bool check_proc_port);
extern int	pq_node_recvbuf(pq_comm_node *node);
extern void pq_node_close(pq_comm_node *node);
extern int	pq_node_get_msg(StringInfo s, pq_comm_node *node);
extern void pq_node_switch_to(pq_comm_node *node);
extern int	pq_node_get_id_socket(pq_comm_node *node);
extern int	pq_node_flush_if_writable_sock(pq_comm_node *node);
extern int	pq_node_putmessage_sock(pq_comm_node *node, char msgtype, const char *s, size_t len);
extern void pq_node_putmessage_noblock_sock(pq_comm_node *node, char msgtype, const char *s, size_t len);
extern pgsocket pq_get_socket(pq_comm_node *node);
#endif /* PQ_NODE_H */
