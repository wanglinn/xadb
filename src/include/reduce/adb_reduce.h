/*-------------------------------------------------------------------------
 *
 * adb_reduce.h
 *	  header file for integrated adb reduce daemon for backend called.
 *
 * Portions Copyright (c) 2010-2017 ADB Development Group
 *
 * IDENTIFICATION
 * 			src/include/reduce/adb_reduce.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ADB_REDUCE_H
#define ADB_REDUCE_H

#include "reduce/rdc_comm.h"
#include "reduce/rdc_msg.h"

extern void EndSelfReduce(int code, Datum arg);

extern RdcPort *ConnectSelfReduce(RdcPortType self_type, RdcPortId self_id);

extern int StartSelfReduceLauncher(RdcPortId rid);

extern void StartSelfReduceGroup(RdcListenMask *rdc_masks, int num);

extern void EndSelfReduceGroup(void);

extern int SendTupleToSelfReduce(RdcPort *port, const Oid nodeIds[], int num,
							 const char *data, int len);

extern void* GetTupleFromSelfReduce(RdcPort *port);

#endif /* ADB_BROKER_H */
