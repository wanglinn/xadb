#ifndef ORA_ROWID_H
#define ORA_ROWID_H

#define ROWID_NODE_BITS_LENGTH	10
#if (ROWID_NODE_BITS_LENGTH==10)
#define ROWID_NODE_MAX_VALUE	1024
#define ROWID_NODE_BITS_MASK	UINT64CONST(0xffc0000000000000)
#define ROWID_NODE_VALUE_MASK	UINT64CONST(0x003fffffffffffff)
#elif (ROWID_NODE_BITS_LENGTH==9)
#define ROWID_NODE_MAX_VALUE	512
#define ROWID_NODE_BITS_MASK	UINT64CONST(0xff80000000000000)
#define ROWID_NODE_VALUE_MASK	UINT64CONST(0x007fffffffffffff)
#elif (ROWID_NODE_BITS_LENGTH==8)
#define ROWID_NODE_MAX_VALUE	256
#define ROWID_NODE_BITS_MASK	UINT64CONST(0xff00000000000000)
#define ROWID_NODE_VALUE_MASK	UINT64CONST(0x00ffffffffffffff)
#elif (ROWID_NODE_BITS_LENGTH==7)
#define ROWID_NODE_MAX_VALUE	128
#define ROWID_NODE_BITS_MASK	UINT64CONST(0xfe00000000000000)
#define ROWID_NODE_VALUE_MASK	UINT64CONST(0x01ffffffffffffff)
#else
#error todo define ROWID_NODE_BITS_MASK and ROWID_NODE_VALUE_MASK
#endif

#define RowidIsLocalValid(id)	(((id) & ROWID_NODE_BITS_MASK) == compare_with_rowid_id)
#define RowidIsLocalInvalid(id)	(((id) & ROWID_NODE_BITS_MASK) != compare_with_rowid_id)
#define RowidGetNodeID(id)		(((id) & ROWID_NODE_BITS_MASK) >> (64 - ROWID_NODE_BITS_LENGTH))

extern uint64 compare_with_rowid_id;
extern int default_with_rowid_id;
extern bool default_with_rowids;

extern void GucAssignRowidNodeId(int newval, void *extra);
extern void InitRowidShmem(void);
extern Size SizeRowidShmem(void);
extern void RedoNextRowid(void *ptr);
extern void SetCheckpointRowid(int64 rowid);
extern int64 GetCheckpointRowid(void);

#endif /* ORA_ROWID_H */
