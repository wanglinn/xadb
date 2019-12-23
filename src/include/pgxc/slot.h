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

#define HASHMAP_SLOTBEGIN	0
#define HASHMAP_SLOTEND		(HASHMAP_SLOTSIZE-1)

#define UNINIT_SLOT_VALUE	  	-2
#define INVALID_SLOT_VALUE		InvalidOid

#define SlotStatusOnlineInDB		1
#define SlotStatusMoveInDB		2
#define SlotStatusCleanInDB		3

extern char*	PGXCNodeName;
extern bool		DatanodeInClusterPlan;

extern void SlotShmemInit(void);
extern Size SlotShmemSize(void);

extern void SlotGetInfo(int slotid, int* pnodeindex, int* pstatus);
extern List *GetSlotNodeOids(void);
extern bool IsSlotNodeOidsEqualOidList(const List *list);
extern Expr* CreateNodeOidFromSlotIndexExpr(Expr *index);

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
extern void InitSLOTPGXCNodeOid(void);

#endif
