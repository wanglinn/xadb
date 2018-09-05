/*-------------------------------------------------------------------------
 *
 * Slot.h
 *  Routines for Slot management
 *
 * src/include/pgxc/slot.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef Slot_H
#define Slot_H

#include "nodes/parsenodes.h"

extern bool adb_slot_enable_mvcc;
extern bool adb_slot_enable_clean;
extern bool hash_distribute_by_hashmap_default;

#define SLOTSIZE	1024
#define SLOTBEGIN	0
#define SLOTEND		(SLOTSIZE-1)
#define GetNodeIdFromHashValue 9121

#define UNINIT_SLOT_VALUE	  	-2
#define INVALID_SLOT_VALUE		InvalidOid

#define SlotStatusOnlineInDB		1
#define SlotStatusMoveInDB		2
#define SlotStatusCleanInDB		3

#define SLOT_ERR_MVCC 		elog(ERROR, "%s:fatal error. clean slot when mvcc is off.", PGXCNodeName);
#define SLOT_ERR_DN 		elog(ERROR, "%s:clean slot cmd only deletes data on datanode.", PGXCNodeName);
#define SLOT_ERR_HTABLE 	elog(ERROR, "%s:clean slot cmd only deletes hash table or toast table.", PGXCNodeName);

extern char*	PGXCNodeName;
extern bool		DatanodeInClusterPlan;

extern void SlotShmemInit(void);
extern Size SlotShmemSize(void);

extern void SlotGetInfo(int slotid, int* pnodeindex, int* pstatus);

extern void SlotAlter(AlterSlotStmt *stmt);
extern void SlotCreate(CreateSlotStmt *stmt);
extern void SlotRemove(DropSlotStmt *stmt);

extern void SlotFlush(FlushSlotStmt* stmt);
extern void SlotClean(CleanSlotStmt* stmt);

/*
When adb_slot_enable_clean is off,the function returns true if this tuple belongs to current node.
When adb_slot_enable_clean is on, the function returns true if this tuple doesn't belong to current node.
*/
extern bool HeapTupleSatisfiesSlot(Relation rel, HeapTuple tuple);
extern int GetHeapTupleSlotId(Relation rel, HeapTuple tuple);
extern int GetValueSlotId(Relation rel, Datum value, AttrNumber	attrNum);
extern Oid get_nodeid_from_hashvalue(void);
extern void InitSLOTPGXCNodeOid(void);

#endif
