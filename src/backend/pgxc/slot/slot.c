/*-------------------------------------------------------------------------
 *
 * Slot.c
 *	  Routines to support manipulation of the slot meta table
 *	  Support concerns CREATE/ALTER/DROP on slot node object.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/adb_slot.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pgxc_node.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "nodes/execnodes.h"	/* before execCluster.h */
#include "executor/execCluster.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/slot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Shared memory tables of slot definitions */
static int *slotnode = NULL;
static int *slotstatus = NULL;
static Oid *unique_slot_oids = NULL;	/* terminal by InvalidOid, order by slotid */

static HeapTuple search_adb_slot_tuple_by_slotid(Relation rel, int slotid);
static void SlotUploadFromCurrentDB(void);
static void LockSlotsForRead(void);
static void check_Slot_options(List *options, char **pnodename, char *pnodestatus);
static int32 DatumGetHashAndModulo(Datum datum, Oid typid, bool isnull);

Datum nodeid_from_slotindex(PG_FUNCTION_ARGS);

#define SLOT_STATUS_ONLINE	"online"
#define SLOT_STATUS_MOVE	"move"
#define SLOT_STATUS_CLEAN	"clean"

bool	adb_slot_enable_clean;
bool	DatanodeInClusterPlan;

static Oid	SLOTPGXCNodeOid = InvalidOid;

/*
 * SlotShmemInit
 *	Initializes shared memory tables of Coordinators and Datanodes.
 */
void
SlotShmemInit(void)
{
	Size i;
	bool found;

	slotnode = ShmemInitStruct("node in adb slot table",
							   SlotShmemSize(),
							   &found);
	slotstatus = (int*)(((char*)slotnode) + MAXALIGN(mul_size(sizeof(*slotnode), HASHMAP_SLOTSIZE)));
	unique_slot_oids = (Oid*)(((char*)slotstatus) + MAXALIGN(mul_size(sizeof(*slotstatus), HASHMAP_SLOTSIZE)));

	if (!found)
	{
		i = HASHMAP_SLOTSIZE;
		do
		{
			--i;
			slotnode[i] = UNINIT_SLOT_VALUE;
			slotstatus[i] = UNINIT_SLOT_VALUE;
			unique_slot_oids[i] = InvalidOid;
		}while(i > 0);
	}
}


/*
 * SlotShmemSize
 *	Get the size of shared memory dedicated to Slot definitions
 */
Size
SlotShmemSize(void)
{
	Size size;

	size = MAXALIGN(mul_size(sizeof(*slotnode), HASHMAP_SLOTSIZE));
	size = add_size(size, MAXALIGN(mul_size(sizeof(*slotstatus), HASHMAP_SLOTSIZE)));
	size = add_size(size, MAXALIGN(mul_size(sizeof(*unique_slot_oids), HASHMAP_SLOTSIZE)));

	return size;
}


/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void
check_Slot_options(List *options, char **pnodename, char *pnodestatus)
{
	ListCell   *option;

	if (!options)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("No options specified")));

	/* Filter options */
	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "nodename") == 0)
		{
			*pnodename = defGetString(defel);
		}
		else if (strcmp(defel->defname, "status") == 0)
		{
			char *status;

			status = defGetString(defel);

			if (strcmp(status, SLOT_STATUS_ONLINE) == 0)
				*pnodestatus = SlotStatusOnlineInDB;
			else if (strcmp(status, SLOT_STATUS_MOVE) == 0)
				*pnodestatus = SlotStatusMoveInDB;
			else if (strcmp(status, SLOT_STATUS_CLEAN) == 0)
				*pnodestatus = SlotStatusCleanInDB;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("status value is incorrect, specify online, move, switch or clean")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("incorrect option: %s", defel->defname)));
		}
	}
}

void SlotGetInfo(int slotid, int* pnodeindex, int* pstatus)
{
	int nodeindex;
	int status;

	if (slotid >= HASHMAP_SLOTSIZE ||
		slotid < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Invalid slotid %d", slotid)));
	}

	LWLockAcquire(SlotTableLock, LW_SHARED);
	nodeindex = slotnode[slotid];
	status = slotstatus[slotid];
	LWLockRelease(SlotTableLock);

	/*
	* if this is the firt time slot info is used after db starts,
	* flush slot info to memory.
	*/
	if (nodeindex==UNINIT_SLOT_VALUE ||
		status==UNINIT_SLOT_VALUE)
	{
		SlotUploadFromCurrentDB();

		LWLockAcquire(SlotTableLock, LW_SHARED);
		nodeindex = slotnode[slotid];
		status = slotstatus[slotid];
		LWLockRelease(SlotTableLock);
	}

	if (nodeindex==INVALID_SLOT_VALUE ||
		status==INVALID_SLOT_VALUE)
		elog(ERROR, "slot is invalid. slot %d can not be used. nodeindex=%d status=%d",
					slotid, nodeindex, status);

	if (DatanodeInClusterPlan &&
		IS_PGXC_DATANODE)
	{
		int newindex = GetCurrentCnRdcID(get_pgxc_nodename(nodeindex));
		if (newindex == InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Can not found node OID %u in datanode", nodeindex)));
		else
			nodeindex = newindex;
	}

	*pnodeindex = nodeindex;
	*pstatus = status;
}

/*
 * result all slot using datanode's object id list
 * it order by slot id
 */
List *GetSlotNodeOids(void)
{
	List *list = NIL;
	Size i;

	LockSlotsForRead();
	for(i=0;i<HASHMAP_SLOTSIZE && OidIsValid(unique_slot_oids[i]);++i)
		list = lappend_oid(list, unique_slot_oids[i]);
	LWLockRelease(SlotTableLock);

	return list;
}

bool IsSlotNodeOidsEqualOidList(const List *list)
{
	Size i;
	int count_found;

	if (list == NIL)
		return false;	/* quick quit */

	count_found = 0;

	LockSlotsForRead();

	for(i=0;i<HASHMAP_SLOTSIZE && OidIsValid(unique_slot_oids[i]);++i)
	{
		if (list_member_oid(list, unique_slot_oids[i]))
		{
			count_found++;
		}else
		{
			LWLockRelease(SlotTableLock);
			return false;
		}
	}
	LWLockRelease(SlotTableLock);

	return (list_length(list) == count_found);
}

/*
* select adb slot table by slotid
*/
static HeapTuple search_adb_slot_tuple_by_slotid(Relation rel, int slotid)
{
	ScanKeyData key[1];
	SysScanDesc scandesc;
	HeapTuple tuple =NULL;
	HeapTuple tupleret = NULL;
	Snapshot snapshot;

	ScanKeyInit(&key[0],
		Anum_adb_slot_slotid
		,BTEqualStrategyNumber
		,F_INT4EQ
		,Int32GetDatum((int32)slotid));

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scandesc = systable_beginscan(rel, AdbSlotSlotidIndexId, true, snapshot, lengthof(key), key);
	tuple = systable_getnext(scandesc);
	tupleret = heap_copytuple(tuple);
	systable_endscan(scandesc);
	UnregisterSnapshot(snapshot);

	return tupleret;
}


void
SlotCreate(CreateSlotStmt *stmt)
{
	Relation 	adbslotsrel;
	HeapTuple	htup;
	bool 		nulls[Natts_adb_slot];
	Datum 		values[Natts_adb_slot];
	int 		slotid = stmt->slotid;
	char* 		nodename = NULL;
	char 		slotstatus = 0;
	Oid 		nodeid = 0;
	int 		i = 0;

	/* Only a DB administrator can add slots */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create slot")));

	if ((slotid<0)||(slotid>=HASHMAP_SLOTSIZE))
		elog(ERROR, "slotid must be between %d and %d", 0, HASHMAP_SLOTSIZE-1);

	/* Filter options */
	check_Slot_options(stmt->options, &nodename, &slotstatus);

	nodeid = get_pgxc_nodeoid(nodename);
	if (!OidIsValid(nodeid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("%s: does not exist",
						nodename)));

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_adb_slot; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	adbslotsrel = table_open(AdbSlotRelationId, RowExclusiveLock);
	if (!RelationIsValid(adbslotsrel))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 err_generic_string(PG_DIAG_TABLE_NAME, "adb_slot"),
				 errmsg("relation \"adb_slot\" does not exist")));

	htup = search_adb_slot_tuple_by_slotid(adbslotsrel, slotid);
	if (HeapTupleIsValid(htup))
		elog(ERROR, "the slotid %u has already existed", slotid);


	/* Build entry tuple */
	values[Anum_adb_slot_slotid - 1] = Int32GetDatum(slotid);
	values[Anum_adb_slot_slotnodename - 1] = DirectFunctionCall1(namein, CStringGetDatum(nodename));
	values[Anum_adb_slot_slotstatus - 1] = Int32GetDatum(slotstatus);

	htup = heap_form_tuple(adbslotsrel->rd_att, values, nulls);

	CatalogTupleInsert(adbslotsrel, htup);

	/* lock relation until transaction end */
	table_close(adbslotsrel, NoLock);
}


void
SlotAlter(AlterSlotStmt *stmt)
{
	int 	slotid = stmt->slotid;
	char* 	nodename = NULL;
	char 	slotstatus = 0;
	HeapTuple	oldtup, newtup;
	Relation	rel;
	Form_adb_slot  slotForm;
	Datum		new_record[Natts_adb_slot];
	bool		new_record_nulls[Natts_adb_slot];
	bool		new_record_repl[Natts_adb_slot];
	Oid 		nodeid = 0;


	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change slot")));

	/* Look at the node tuple, and take exclusive lock on it */
	rel = table_open(AdbSlotRelationId, RowExclusiveLock);
	if (!RelationIsValid(rel))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 err_generic_string(PG_DIAG_TABLE_NAME, "adb_slot"),
				 errmsg("relation \"adb_slot\" does not exist")));
	oldtup = search_adb_slot_tuple_by_slotid(rel, slotid);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for slotid %u", slotid);

	slotForm = (Form_adb_slot) GETSTRUCT(oldtup);

	nodename = NameStr(slotForm->slotnodename);
	slotstatus = slotForm->slotstatus;

	check_Slot_options(stmt->options, &nodename, &slotstatus);

	nodeid = get_pgxc_nodeoid(nodename);
	if (!OidIsValid(nodeid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("%s: does not exist",
						nodename)));

	/* Update values for catalog entry */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	new_record[Anum_adb_slot_slotid - 1] = Int32GetDatum(slotid);
	new_record_repl[Anum_adb_slot_slotid - 1] = true;
	new_record[Anum_adb_slot_slotnodename - 1] = DirectFunctionCall1(namein, CStringGetDatum(nodename));
	new_record_repl[Anum_adb_slot_slotnodename - 1] = true;
	new_record[Anum_adb_slot_slotstatus - 1] = Int32GetDatum(slotstatus);
	new_record_repl[Anum_adb_slot_slotstatus - 1] = true;

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* lock relation until transaction end */
	table_close(rel, NoLock);
}


void
SlotRemove(DropSlotStmt *stmt)
{
	int 		slotid = stmt->slotid;
	HeapTuple	tup;
	Relation	rel;

	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change slot")));

	/* Look at the node tuple, and take exclusive lock on it */
	rel = table_open(AdbSlotRelationId, RowExclusiveLock);
	if (!RelationIsValid(rel))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"adb_slot\" does not exist")));
	tup = search_adb_slot_tuple_by_slotid(rel, slotid);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for slotid %u", slotid);

	CatalogTupleDelete(rel, &tup->t_self);

	/* lock relation until transaction end */
	table_close(rel, NoLock);
}

void SlotFlush(FlushSlotStmt* stmt)
{
	SlotUploadFromCurrentDB();
}

void SlotClean(CleanSlotStmt* stmt)
{
	int nodeindex, status, ret;

	StringInfoData qstr;
	initStringInfo(&qstr);

	//adb_slot_enable_mvcc is only set on expand and expanded node, others nodes just return.
	if(!adb_slot_enable_mvcc)
		return;

	//avoid access adb_slot error when execute slot clean which is the first cmd after server starts.
	SlotGetInfo(0, &nodeindex, &status);


	adb_slot_enable_clean = true;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	appendStringInfo(&qstr,"delete from %s.%s", stmt->schema_name, stmt->table_name);

	ret = SPI_execute(qstr.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR, (errmsg("clean slot error %s result is %d", qstr.data, ret)));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	adb_slot_enable_clean = false;

	return;
}

bool HeapTupleSatisfiesSlot(Relation rel, HeapTuple tuple)
{
	int		modulo;
	int		nodeIndex;
	int		slotstatus;
	bool ret;

	if(InvalidOid==SLOTPGXCNodeOid)
	{
		InitPGXCNodeIdentifier();
		if((InvalidOid==SLOTPGXCNodeOid)||(InvalidOid==PGXCNodeOid))
			elog(ERROR, "%s:SLOTPGXCNodeOid=%d, PGXCNodeOid=%d, this may happen when node is expanded. try again.", PGXCNodeName, SLOTPGXCNodeOid, PGXCNodeOid);
	}

	ret = false;
	//1.get modulo
	modulo = GetHeapTupleSlotId(rel, tuple);

	//2.check if the tuple belongs to this datanode
	SlotGetInfo(modulo, &nodeIndex, &slotstatus);

	if((DatanodeInClusterPlan)&&IS_PGXC_DATANODE)
		ret = (PGXCNodeOid==nodeIndex);
	else
		ret = (SLOTPGXCNodeOid==nodeIndex);

	ereport(DEBUG1,
			(errmsg("PGXCNodeOid=%d-SLOTPGXCNodeOid=%d-nodeIndex=%d-adb_slot_enable_mvcc=%d-adb_slot_enable_clean=%d-ret=%d",
			 PGXCNodeOid,
			 SLOTPGXCNodeOid,
			 nodeIndex,
			 adb_slot_enable_mvcc,
			 adb_slot_enable_clean,
			 ret)));

	if(adb_slot_enable_clean)
		return !ret;
	else
		return ret;
}

int GetHeapTupleSlotId(Relation rel, HeapTuple tuple)
{
	TupleDesc			tupDesc;
	Datum				value;
	bool				isnull;

	int		modulo;
	AttrNumber attrNum;

	tupDesc = RelationGetDescr(rel);
	attrNum = GetFirstLocAttNumIfOnlyOne(rel->rd_locator_info);

	if (attrNum <= 0 ||
		attrNum > tupDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 err_generic_string(PG_DIAG_TABLE_NAME, RelationGetRelationName(rel)),
				 errmsg("invalid distribute attribute number %d", attrNum)));

	value = fastgetattr(tuple, attrNum, tupDesc, &isnull);

	//2.check if the tuple belongs to this datanode
	modulo = DatumGetHashAndModulo(value,
								   TupleDescAttr(tupDesc, attrNum-1)->atttypid,
								   isnull);

	return modulo;
}

int GetValueSlotId(Relation rel, Datum value, AttrNumber	attrNum)
{
	TupleDesc	tupDesc;
	Form_pg_attribute attr;
	int			slotid;

	tupDesc = RelationGetDescr(rel);
	if (attrNum <= 0 ||
		attrNum > tupDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
				 errmsg("Invalid distribute attribute number %d", attrNum)));
	attr = TupleDescAttr(tupDesc, attrNum-1);

	slotid = DatumGetHashAndModulo(value, attr->atttypid, false);

	return slotid;
}

static int32 DatumGetHashAndModulo(Datum datum, Oid typid, bool isnull)
{
	int32 hashvalue;

	if (isnull)
		return 0;

	hashvalue = execHashValue(datum, typid, InvalidOid);
	datum = DirectFunctionCall2(hash_combin_mod,
								UInt32GetDatum(HASHMAP_SLOTSIZE),
								UInt32GetDatum(hashvalue));

	return DatumGetInt32(datum);
}

static void
SlotUploadFromCurrentDB(void)
{
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	Form_adb_slot	slotForm;
	List		   *oids;
	ListCell	   *lc;
	int			   *load_slotnode;
	int			   *load_slotstatus;
	int				last_slotid;
	Oid				oid;
	Size			i;

	load_slotnode = palloc(sizeof(*load_slotnode)*HASHMAP_SLOTSIZE);
	load_slotstatus = palloc(sizeof(*load_slotstatus)*HASHMAP_SLOTSIZE);

	rel = table_open(AdbSlotRelationId, AccessShareLock);
	if (!RelationIsValid(rel))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 err_generic_string(PG_DIAG_TABLE_NAME, "adb_slot"),
				 errmsg("load adb_slot failed. relation adb_slot doesn't exist.")));

	/* order by slotid */
	scan = systable_beginscan(rel, AdbSlotSlotidIndexId, true, GetActiveSnapshot(), 0, NULL);

	last_slotid = 0;
	oids = NIL;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		slotForm = (Form_adb_slot) GETSTRUCT(tuple);
		if (slotForm->slotid >= HASHMAP_SLOTSIZE ||
			slotForm->slotid < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Invalid slot id \"%d\" from adb_slot", slotForm->slotid)));
		}
		if (slotForm->slotid != last_slotid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("slot id is not continuous"),
					 errdetail("between %d and %d", last_slotid, slotForm->slotid)));
		}

		oid = get_pgxc_nodeoid(NameStr(slotForm->slotnodename));
		if(!OidIsValid(oid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("%s load adb_slot failed. node name %s in adb_slot table does not exist in pgxc_node",
					 		PGXCNodeName, NameStr(slotForm->slotnodename))));
		}

		load_slotnode[slotForm->slotid] = oid;
		oids = list_append_unique_oid(oids, oid);
		load_slotstatus[slotForm->slotid] = slotForm->slotstatus;
		++last_slotid;
	}
	systable_endscan(scan);

	/* lock relation until transaction end */
	table_close(rel, NoLock);

	if (last_slotid != HASHMAP_SLOTSIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("load adb_slot failed. the total num of slot in adb_slot table is not %d", HASHMAP_SLOTSIZE),
				 errdetail("total number is %d", last_slotid)));
	}

	/* update share memory */
	LWLockAcquire(SlotTableLock, LW_EXCLUSIVE);
	memcpy(slotnode, load_slotnode, sizeof(*slotnode)*HASHMAP_SLOTSIZE);
	memcpy(slotstatus, load_slotstatus, sizeof(*slotstatus)*HASHMAP_SLOTSIZE);
	i = 0;
	foreach(lc, oids)
		unique_slot_oids[i++] = lfirst_oid(lc);
	if (i<HASHMAP_SLOTSIZE)
		unique_slot_oids[i++] = InvalidOid;
	LWLockRelease(SlotTableLock);

	/* cleanup */
	pfree(load_slotnode);
	pfree(load_slotstatus);
	list_free(oids);
}

static void LockSlotsForRead(void)
{
	LWLockAcquire(SlotTableLock, LW_SHARED);
	if (!OidIsValid(unique_slot_oids[0]))
	{
		LWLockRelease(SlotTableLock);
		SlotUploadFromCurrentDB();
		LWLockAcquire(SlotTableLock, LW_SHARED);
	}
}

Datum
nodeoid_from_slotindex(PG_FUNCTION_ARGS)
{
	int32 pnodeindex;
	struct
	{
		ArrayType  *arr_range;
		ArrayType  *arr_node;
		int			varsize_range;
		int			varsize_node;
		int32		count_oid;
		Oid	   *oids;
	}*extra = fcinfo->flinfo->fn_extra;
	ArrayType	  *arr_range;
	ArrayType	  *arr_node;

	if (PG_ARGISNULL(0) ||
		PG_ARGISNULL(1) ||
		PG_ARGISNULL(2))
	{
		if (PG_ARGISNULL(3))
			PG_RETURN_NULL();
		PG_RETURN_DATUM(PG_GETARG_DATUM(3));
	}

	arr_range = PG_GETARG_ARRAYTYPE_P(0);
	arr_node = PG_GETARG_ARRAYTYPE_P(1);

	if (extra == NULL ||
		VARSIZE_ANY(arr_range) != extra->varsize_range ||
		VARSIZE_ANY(arr_node) != extra->varsize_node ||
		memcmp(arr_range, extra->arr_range, extra->varsize_range) != 0 ||
		memcmp(arr_node, extra->arr_node, extra->varsize_node) != 0)
	{
		int				count_range;
		int				count_oids;
		int32		   *ranges;
		Oid			   *oids;
		MemoryContext	oldcontext;
		int				i;

		if (ARR_NDIM(arr_range) != 1 ||
			ARR_HASNULL(arr_range))
			elog(ERROR, "argument range_list is not a 1-D not null array");
		if (ARR_NDIM(arr_node) != 1 ||
			ARR_HASNULL(arr_node))
			elog(ERROR, "argument nodes is not a 1-D not null array");
		ranges = (int32*)ARR_DATA_PTR(arr_range);
		count_range = ARR_DIMS(arr_range)[0];
		oids = (Oid*)ARR_DATA_PTR(arr_node);
		count_oids = ARR_DIMS(arr_node)[0];
		if (count_range != count_oids ||
			count_oids <= 0 ||
			ARR_LBOUND(arr_range)[0] != ARR_LBOUND(arr_node)[0])
			elog(ERROR, "bad argument for range_list or nodes");

		oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		if (extra == NULL)
		{
			extra = palloc0(sizeof(*extra));
			fcinfo->flinfo->fn_extra = extra;
		}else
		{
			pfree(extra->arr_range);
			pfree(extra->arr_node);
		}
		extra->arr_range = (ArrayType*)datumCopy(PointerGetDatum(arr_range), false, -1);
		extra->varsize_range = VARSIZE_ANY(arr_range);
		extra->arr_node = (ArrayType*)datumCopy(PointerGetDatum(arr_node), false, -1);
		extra->varsize_node = VARSIZE_ANY(arr_node);
		extra->count_oid = ranges[count_range-1];
		if (extra->oids == NULL)
			extra->oids = palloc(sizeof(Oid) * extra->count_oid);
		else
			extra->oids = repalloc(extra->oids,
								   sizeof(Oid) * extra->count_oid);
		MemoryContextSwitchTo(oldcontext);

		for (i=0;i<count_range;++i)
		{
			int j = (i==0 ? 0:ranges[i-1]);
			int count=ranges[i];
			for (;j<count;++j)
				extra->oids[j] = oids[i];
		}
	}
	PG_FREE_IF_COPY(arr_range, 0);
	PG_FREE_IF_COPY(arr_node, 1);

	pnodeindex = PG_GETARG_INT32(2);
	if (pnodeindex >= extra->count_oid ||
		pnodeindex < 0)
	{
		if (PG_ARGISNULL(3))
			PG_RETURN_NULL();
		PG_RETURN_DATUM(PG_GETARG_DATUM(3));
	}
	PG_RETURN_OID(extra->oids[pnodeindex]);
}

Expr* CreateNodeOidFromSlotIndexExpr(Expr *index)
{
	List	   *args = NIL;
	Datum	   *ranges;
	Datum	   *oids;
	ArrayType  *array;
	Const	   *c;
	int			count;
	int			i;
	Assert(exprType((Node*)index) == INT4OID);

	ranges = palloc0(sizeof(Datum) * HASHMAP_SLOTSIZE);
	count = 0;

	LockSlotsForRead();
	for (i=1;i<=HASHMAP_SLOTSIZE;++i)
	{
		if (i == HASHMAP_SLOTSIZE ||
			slotnode[i] != slotnode[i-1])
		{
			ranges[count] = Int32GetDatum(i);
			++count;
		}
	}

	array = construct_array(ranges, count, INT4OID, sizeof(int32), true, 'i');
	c = makeConst(INT4ARRAYOID, -1, InvalidOid, -1, PointerGetDatum(array), false, false);
	args = list_make1(c);

	oids = palloc(sizeof(Datum) * count);
	for (i=0;i<count;++i)
		oids[i] = slotnode[DatumGetInt32(ranges[i])-1];
	LWLockRelease(SlotTableLock);
	array = construct_array(oids, count, OIDOID, sizeof(Oid), true, 'i');
	c = makeConst(OIDARRAYOID, -1, InvalidOid, -1, PointerGetDatum(array), false, false);
	args = lappend(args, c);

	pfree(oids);
	pfree(ranges);

	args = lappend(args, index);

	/* when not found */
	c = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid), ObjectIdGetDatum(slotnode[0]), false, true);
	args = lappend(args, c);

	return (Expr*)makeFuncExpr(F_NODEOID_FROM_SLOTINDEX,
							   OIDOID,
							   args,
							   InvalidOid,
							   InvalidOid,
							   COERCE_EXPLICIT_CALL);
}

void InitSLOTPGXCNodeOid(void)
{
	SLOTPGXCNodeOid = get_pgxc_nodeoid(PGXCNodeName);
	ereport(DEBUG1, (errmsg("NodeName=%s-SLOTPGXCNodeOid=%d", PGXCNodeName, SLOTPGXCNodeOid)));
}

/*
* invalidate all cache of relations
*/
Datum
adb_invalidate_relcache_all(PG_FUNCTION_ARGS)
{
	CacheInvalidateRelcacheAll();

	PG_RETURN_BOOL(true);
}
