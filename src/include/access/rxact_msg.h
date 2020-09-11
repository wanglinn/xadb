
#ifndef RXACT_MSG_H_
#define RXACT_MSG_H_

/* same msg need save to xlog, so it must less or equal 0xf */
#define RXACT_MSG_CONNECT		0x10
#define RXACT_MSG_DO			0x20
#define RXACT_MSG_SUCCESS		0x30
#define RXACT_MSG_FAILED		0x40
#define RXACT_MSG_AUTO			0x50
#define RXACT_MSG_CHECKPOINT	0x60

/* send to client */
#define RXACT_MSG_OK			0x01
#define RXACT_MSG_ERROR			0x02
#define RXACT_MSG_RUNNING		0x03
#define RXACT_MSG_WAIT_GID		0x04

#endif /* RXACT_MSG_H_ */
