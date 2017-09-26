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

#include "executor/tuptable.h"
#include "reduce/rdc_comm.h"
#include "reduce/rdc_msg.h"

extern void AtEOXact_Reduce(void);

extern void EndSelfReduce(int code, Datum arg);

extern RdcPort *ConnectSelfReduce(RdcPortType self_type, RdcPortId self_id,
								  RdcPortPID self_pid, RdcExtra self_extra);

extern int StartSelfReduceLauncher(RdcPortId rid);

extern void StartSelfReduceGroup(RdcMask *rdc_masks, int num);

extern void EndSelfReduceGroup(void);

extern List *GetReduceGroup(void);

extern void SendRejectToRemote(RdcPort *port, List *dest_nodes);

extern void SendCloseToRemote(RdcPort *port, List *dest_nodes);

extern void SendEofToRemote(RdcPort *port, List *dest_nodes);

extern void SendSlotToRemote(RdcPort *port, List *dest_nodes, TupleTableSlot *slot);

extern TupleTableSlot* GetSlotFromRemote(RdcPort *port, TupleTableSlot *slot,
										 Oid *slot_oid, Oid *eof_oid,
										 List **closed_remote);

extern Size EstimateReduceInfoSpace(void);
extern void SerializeReduceInfo(Size maxsize, char *ptr);
extern void RestoreReduceInfo(char *start_addr);

#endif /* ADB_BROKER_H */
