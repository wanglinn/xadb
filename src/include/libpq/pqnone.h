/*--------------------------------------------------------------------
 * pqnone.h
 *
 *修改完成agtm_ReadCommand函数(替换了原来的SocketBackend函数),
 *可以同时accept客户端消息以及接受新的连接
 *(随机端口通过agtm_port GUC参数已发送到客户端，还未实现accept新连接的代码)
 *
 * Portions Copyright (c) 2014-2017, ADB Development Group
 *
 * src/agtm/include/pqnone.h
 *--------------------------------------------------------------------
 */
#ifndef PQ_NONE_H
#define PQ_NONE_H

extern void pq_switch_to_none(void);

#endif /* PQ_NONE_H */
