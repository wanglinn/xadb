/*-------------------------------------------------------------------------
 *
 * Slot.c
 *	  Routines to support manipulation of the slot meta table
 *	  Support concerns CREATE/ALTER/DROP on slot node object.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "access/htup_details.h"
#include "pg_config.h"

#include "catalog/namespace.h"
#include "pgxc/slot.h"

#include <unistd.h>
#include <fcntl.h>

#include "utils/fmgroids.h"

#include "access/hash.h"
#include "executor/spi.h"
#include "commands/dbcommands.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"

#include "executor/execCluster.h"
#include "parser/parse_func.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "pgxc/nodemgr.h"
#include "utils/inval.h"


typedef struct FormData_adb_slot
{
	int32 		slotid;
	NameData	nodename;
	int32		status;
} FormData_adb_slot;
typedef FormData_adb_slot *Form_adb_slot;

static char* adb_slot_schema_name = "adb";
static char* adb_slot_table_name = "adb_slot";
#define Natts_adb_slot			3

#define Anum_adb_slotid			1
#define Anum_adb_nodename		2
#define Anum_adb_status			3


/* Shared memory tables of slot definitions */
int*	slotnode;
int*	slotstatus;

static HeapTuple search_adb_slot_tuple_by_slotid(Relation rel, int slotid);
static Oid GetAdbSlotRelId(void);
static void InitSlotArrary(int value);
static void SlotUploadFromCurrentDB(void);
static void SlotUploadFromRemoteDB(void);
static void SlotUploadFlush(void);
static void check_Slot_options(List *options, char **pnodename, char *pnodestatus);

Datum nodeid_from_hashvalue(PG_FUNCTION_ARGS);
Datum nodeid_from_hashvalue(PG_FUNCTION_ARGS);



#define SLOT_STATUS_ONLINE	"online"
#define SLOT_STATUS_MOVE	"move"
#define SLOT_STATUS_CLEAN	"clean"

bool	adb_slot_enable_clean;
bool	DatanodeInClusterPlan;


extern char* 	SlotDatabaseName;
extern int 		PostPortNumber;

#define SELECT_CHECK_DBLINK				"select count(*) from pg_extension where extname='dblink';"
#define SELECT_CHECK_CONNECT			"select count(*) from dblink_get_connections() where dblink_get_connections = '{slotlink}';"
#define CREATE_DBLINK_CONNECT			"select dblink_connect('slotlink','dbname=%s host=localhost port=%d user=adbslotuser password=asiainfonj connect_timeout=5');"
#define DROP_DBLINK_CONNECT				"select dblink_disconnect('slotlink');"
#define FLUSH_SLOT_BY_DBLINK 			"select * from dblink('slotlink','flush slot') as t1(result varchar(64));"

Oid	SLOTPGXCNodeOid = InvalidOid;

static Oid GetAdbSlotRelId(void)
{
	Oid np_oid;
	np_oid = LookupExplicitNamespace(adb_slot_schema_name, true);
	return get_relname_relid(adb_slot_table_name, np_oid);
}

static void InitSlotArrary(int value)
{
	int i=0;
	for(i=0; i<SLOTSIZE; i++)
	{
		slotnode[i] = value;
		slotstatus[i] = value;
	}
}

/*
 * SlotShmemInit
 *	Initializes shared memory tables of Coordinators and Datanodes.
 */
void
SlotShmemInit(void)
{
	bool found;
	Size size;

	size = mul_size(sizeof(*slotnode), SLOTSIZE);
	size = MAXALIGN(size);
	slotnode = ShmemInitStruct("node in adb slot table",
								size,
								&found);
	//TODO handle found

	size = mul_size(sizeof(*slotstatus), SLOTSIZE);
	size = MAXALIGN(size);
	slotstatus = ShmemInitStruct("status in adb slot table",
								size,
								&found);

	InitSlotArrary(UNINIT_SLOT_VALUE);
}


/*
 * SlotShmemSize
 *	Get the size of shared memory dedicated to Slot definitions
 */
Size
SlotShmemSize(void)
{
	return add_size(MAXALIGN(mul_size(sizeof(*slotnode), SLOTSIZE)),
					MAXALIGN(mul_size(sizeof(*slotstatus), SLOTSIZE)));
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

			if (strcmp(status, SLOT_STATUS_ONLINE) != 0 &&
				strcmp(status, SLOT_STATUS_MOVE) != 0 &&
				strcmp(status, SLOT_STATUS_CLEAN)  != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("status value is incorrect, specify online, move, switch or clean")));

			if (strcmp(status, SLOT_STATUS_ONLINE) == 0)
				*pnodestatus = SlotStatusOnlineInDB;
			else if (strcmp(status, SLOT_STATUS_MOVE) == 0)
				*pnodestatus = SlotStatusMoveInDB;
			else if (strcmp(status, SLOT_STATUS_CLEAN) == 0)
				*pnodestatus = SlotStatusCleanInDB;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("incorrect option: %s", defel->defname)));
		}
	}


}


static void
SlotUploadFlush(void)
{
	Name db = (Name) palloc(NAMEDATALEN);
	namestrcpy(db, get_database_name(MyDatabaseId));

	InitSLOTPGXCNodeOid();
	if(0==namestrcmp(db, SlotDatabaseName))
		SlotUploadFromCurrentDB();
	else
		SlotUploadFromRemoteDB();
}

void SlotGetInfo(int slotid, int* pnodeindex, int* pstatus)
{
	LWLockAcquire(SlotTableLock, LW_SHARED);
	*pnodeindex = slotnode[slotid];
	*pstatus = slotstatus[slotid];
	LWLockRelease(SlotTableLock);

	/*
	* if this is the firt time slot info is used after db starts,
	* flush slot info to memory.
	*/
	if(((*pnodeindex)==UNINIT_SLOT_VALUE)
		|| ((*pstatus)==UNINIT_SLOT_VALUE))
		SlotUploadFlush();

	LWLockAcquire(SlotTableLock, LW_SHARED);
	*pnodeindex = slotnode[slotid];
	*pstatus = slotstatus[slotid];
	LWLockRelease(SlotTableLock);

	/*if slot table is invalid, flush slot cmd will set UNINIT_SLOT_VALUE*/
	Assert((*pnodeindex)!=UNINIT_SLOT_VALUE);
	Assert((*pstatus)!=UNINIT_SLOT_VALUE);

	if(((*pnodeindex)==INVALID_SLOT_VALUE)
		||((*pstatus)==INVALID_SLOT_VALUE))
		elog(ERROR, "slot is invalid.slot %d can not be used. nodeindex=%d status=%d",
		slotid,*pnodeindex, *pstatus);

	if((DatanodeInClusterPlan)&&IS_PGXC_DATANODE)
	{
		*pnodeindex = GetCurrentCnRdcID(get_pgxc_nodename(*pnodeindex));
		Assert((*pnodeindex)!=InvalidOid);
	}

}


/*
* select adb slot table by slotid
*/
static HeapTuple search_adb_slot_tuple_by_slotid(Relation rel, int slotid)
{
	ScanKeyData key[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple =NULL;
	HeapTuple tupleret = NULL;
	Snapshot snapshot;

	ScanKeyInit(&key[0],
		Anum_adb_slotid
		,BTEqualStrategyNumber
		,F_INT4EQ
		,Int32GetDatum((int32)slotid));

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	rel_scan = heap_beginscan(rel, snapshot, 1, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		break;
	}
	tupleret = heap_copytuple(tuple);
	heap_endscan(rel_scan);
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
	Oid			slotrelOid =0;
	int 		i = 0;

	/* Only a DB administrator can add slots */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create slot")));

	if ((slotid<SLOTBEGIN)||(slotid>SLOTEND))
		elog(ERROR, "slotid must be between %d and %d", SLOTBEGIN, SLOTEND);

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

	slotrelOid = GetAdbSlotRelId();
	if (!OidIsValid(slotrelOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s.%s\" does not exist", adb_slot_schema_name, adb_slot_table_name)));

	adbslotsrel = heap_open(slotrelOid, RowExclusiveLock);

	htup = search_adb_slot_tuple_by_slotid(adbslotsrel, slotid);
	if (HeapTupleIsValid(htup))
		elog(ERROR, "this slot has already existed");


	/* Build entry tuple */
	values[Anum_adb_slotid - 1] = Int32GetDatum(slotid);
	values[Anum_adb_nodename - 1] = DirectFunctionCall1(namein, CStringGetDatum(nodename));
	values[Anum_adb_status - 1] = Int32GetDatum(slotstatus);

	htup = heap_form_tuple(adbslotsrel->rd_att, values, nulls);

	simple_heap_insert(adbslotsrel, htup);

	heap_close(adbslotsrel, RowExclusiveLock);
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
	Oid			slotrelOid =0;
	Oid 		nodeid = 0;


	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change slot")));


	slotrelOid = GetAdbSlotRelId();
	if (!OidIsValid(slotrelOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s%s\" does not exist", adb_slot_schema_name, adb_slot_table_name)));


	/* Look at the node tuple, and take exclusive lock on it */
	rel = heap_open(slotrelOid, RowExclusiveLock);
	oldtup = search_adb_slot_tuple_by_slotid(rel, slotid);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for slotid %u", slotid);

	slotForm = (Form_adb_slot) GETSTRUCT(oldtup);

	nodename = slotForm->nodename.data;
	slotstatus = slotForm->status;

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

	new_record[Anum_adb_slotid - 1] = Int32GetDatum(slotid);
	new_record_repl[Anum_adb_slotid - 1] = true;
	new_record[Anum_adb_nodename - 1] = DirectFunctionCall1(namein, CStringGetDatum(nodename));
	new_record_repl[Anum_adb_nodename - 1] = true;
	new_record[Anum_adb_status - 1] = Int32GetDatum(slotstatus);
	new_record_repl[Anum_adb_status - 1] = true;

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	simple_heap_update(rel, &oldtup->t_self, newtup);

	heap_close(rel, RowExclusiveLock);
}


void
SlotRemove(DropSlotStmt *stmt)
{
	int 		slotid = stmt->slotid;
	HeapTuple	tup;
	Relation	rel;
	Oid			slotrelOid =0;


	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change slot")));


	slotrelOid = GetAdbSlotRelId();
	if (!OidIsValid(slotrelOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s%s\" does not exist", adb_slot_schema_name, adb_slot_table_name)));


	/* Look at the node tuple, and take exclusive lock on it */
	rel = heap_open(slotrelOid, RowExclusiveLock);
	tup = search_adb_slot_tuple_by_slotid(rel, slotid);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for slotid %u", slotid);

	simple_heap_delete(rel, &tup->t_self);

	heap_close(rel, RowExclusiveLock);
}

void SlotFlush(FlushSlotStmt* stmt)
{
	SlotUploadFlush();
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
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	Datum	   *values;
	bool	   *nulls;
	Form_pg_attribute attr;

	long	hashValue;
	int		modulo;
	int		nodeIndex;
	int		slotstatus;
	AttrNumber attrNum;
	bool ret;

	if(InvalidOid==SLOTPGXCNodeOid)
	{
		InitPGXCNodeIdentifier();
		if((InvalidOid==SLOTPGXCNodeOid)||(InvalidOid==PGXCNodeOid))
			elog(ERROR, "%s:SLOTPGXCNodeOid=%d, PGXCNodeOid=%d, this may happen when node is expanded. try again.", PGXCNodeName, SLOTPGXCNodeOid, PGXCNodeOid);
	}

	ret = false;
	//1.get value
	attrNum = rel->rd_locator_info->partAttrNum;
	tupDesc = RelationGetDescr(rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	values = (Datum *) palloc(num_phys_attrs * sizeof(Datum));
	nulls = (bool *) palloc(num_phys_attrs * sizeof(bool));
	heap_deform_tuple(tuple, tupDesc, values, nulls);

	//2.check if the tuple belongs to this datanode
	if(!nulls[attrNum - 1])
	{
		hashValue = execHashValue(values[attrNum - 1],
								  attr[attrNum - 1].atttypid,
								  InvalidOid);
		modulo = execModuloValue(Int32GetDatum(hashValue),
									 INT4OID,
									 SLOTSIZE);

		SlotGetInfo(modulo, &nodeIndex, &slotstatus);

		pfree(values);
		pfree(nulls);

		if((DatanodeInClusterPlan)&&IS_PGXC_DATANODE)
			ret = (PGXCNodeOid==nodeIndex);
		else
			ret = (SLOTPGXCNodeOid==nodeIndex);
	}
	else
	{
		pfree(values);
		pfree(nulls);

		//rows which's distribution key is null is only stored in slot 0.
		SlotGetInfo(0, &nodeIndex, &slotstatus);

		if((DatanodeInClusterPlan)&&IS_PGXC_DATANODE)
			ret =  (PGXCNodeOid==nodeIndex);
		else
			ret =  (SLOTPGXCNodeOid==nodeIndex);
	}

	elog(DEBUG1,
		"PGXCNodeOid=%d-SLOTPGXCNodeOid=%d-data=%d-nodeIndex=%d-adb_slot_enable_mvcc=%d-adb_slot_enable_clean=%d-ret=%d",
		PGXCNodeOid,
		SLOTPGXCNodeOid,
		DatumGetInt32(values[attrNum - 1]),
		nodeIndex,
		adb_slot_enable_mvcc,
		adb_slot_enable_clean,
		ret);

	if(adb_slot_enable_clean)
		return !ret;
	else
		return ret;
}

int GetHeapTupleSlotId(Relation rel, HeapTuple tuple)
{
	TupleDesc	tupDesc;
	int			num_phys_attrs;
	Datum	   *values;
	bool	   *nulls;
	Form_pg_attribute attr;

	long	hashValue;
	int		modulo;
	AttrNumber attrNum;

	//1.get value
	attrNum = rel->rd_locator_info->partAttrNum;
	tupDesc = RelationGetDescr(rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	values = (Datum *) palloc(num_phys_attrs * sizeof(Datum));
	nulls = (bool *) palloc(num_phys_attrs * sizeof(bool));
	heap_deform_tuple(tuple, tupDesc, values, nulls);

	//2.check if the tuple belongs to this datanode
	if(!nulls[attrNum - 1])
	{
		hashValue = execHashValue(values[attrNum - 1],
								  attr[attrNum - 1].atttypid,
								  InvalidOid);
		modulo = execModuloValue(Int32GetDatum(hashValue),
									 INT4OID,
									 SLOTSIZE);

		pfree(values);
		pfree(nulls);
	}
	else
	{
		pfree(values);
		pfree(nulls);

		modulo = 0;
	}

	return modulo;
}

int GetValueSlotId(Relation rel, Datum value, AttrNumber	attrNum)
{
	TupleDesc	tupDesc;
	Form_pg_attribute attr;
	long		hashValue;
	int			slotid;

	//1.get value
	tupDesc = RelationGetDescr(rel);
	attr = tupDesc->attrs;

	//2.check if the tuple belongs to this datanode
	//hashValue = locator_compute_hash(attr[attrNum - 1]->atttypid, value, LOCATOR_TYPE_HASH);
	//slotid = locator_compute_modulo(labs(hashValue), SLOTSIZE);

	hashValue = execHashValue(value,
			attr[attrNum - 1].atttypid, InvalidOid);
	slotid = execModuloValue(Int32GetDatum(hashValue),
			INT4OID, SLOTSIZE);

	return slotid;
}

static void
SlotUploadFromCurrentDB(void)
{
	Relation 	rel;
	HeapScanDesc scan;
	HeapTuple	tuple;
	Oid			slotrelOid;
	int 		i;
	Form_adb_slot  slotForm;
	char		msg[200];
	Snapshot snapshot;

	strcpy(msg,"");


	i = 0;
	slotForm = NULL;
	slotrelOid = GetAdbSlotRelId();

	LWLockAcquire(SlotTableLock, LW_EXCLUSIVE);

	if (!OidIsValid(slotrelOid))
	{
		sprintf(msg, "%s", "load adb_slot failed.adb_slot doesn't exist.");
		goto slot_handler_finish;
	}

	rel = heap_open(slotrelOid, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		slotForm = (Form_adb_slot) GETSTRUCT(tuple);
		//slotnode[slotForm->slotid] = PGXCNodeGetNodeIdFromName(NameStr(slotForm->nodename), PGXC_NODE_DATANODE);
		slotnode[slotForm->slotid] = get_pgxc_nodeoid(NameStr(slotForm->nodename));

		if(INVALID_SLOT_VALUE == slotnode[slotForm->slotid])
		{
			sprintf(msg, "%s load adb_slot failed.node name %s in adb_slot table does not exist in pgxc_node", PGXCNodeName, NameStr(slotForm->nodename));
			break;
		}
		slotstatus[slotForm->slotid] = slotForm->status;
		i++;
	}
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);

	heap_close(rel, AccessShareLock);


slot_handler_finish:

	if(i!=SLOTSIZE)
	{
		/* if adb_slot is empty no need init slot array */
		if (i != 0)
			InitSlotArrary(INVALID_SLOT_VALUE);

		LWLockRelease(SlotTableLock);

		if(0==strcmp("",msg))
			sprintf(msg, "load adb_slot failed. the total num of slot in adb_slot table is not %d", SLOTSIZE);

		elog(ERROR, "%s", msg);
	}
	else
	{
		LWLockRelease(SlotTableLock);

		elog(LOG, "load adb_slot success.");
		for(i=0; i<SLOTSIZE; i++)
		{
			elog(DEBUG1, "slotid=%d-nodeindex=%d-status=%d", i, slotnode[i], slotstatus[i]);
		}
	}


}

static void
SlotUploadFromRemoteDB(void)
{
	int			ret;
	char		msg[200];
	char		sql[200];
	char*		pextent_count;
	char*		ptable_count;
	char*		pconn_count;


	strcpy(msg,"");
	pextent_count = ptable_count = pconn_count = NULL;

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		strcpy(msg,"SPI_connect failed");
		goto slot_handler_finish;
	}

	/*1.1check extension exists*/
	ret = SPI_execute(SELECT_CHECK_DBLINK, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		sprintf(msg, "load adb_slot failed.%s result is %d",SELECT_CHECK_DBLINK, ret);
		goto slot_handler_finish;
	}
	if(0==SPI_processed)
	{
		sprintf(msg, "load adb_slot failed.%s result is null.",SELECT_CHECK_DBLINK);
		goto slot_handler_finish;
	}
	pextent_count = SPI_getvalue(SPI_tuptable->vals[0],SPI_tuptable->tupdesc, 1);
	if(1!=atoi(pextent_count))
	{
		sprintf(msg, "load adb_slot failed.%s result is %d",SELECT_CHECK_DBLINK, atoi(pextent_count));
		goto slot_handler_finish;
	}

	/*1.2check connect exists*/
	ret = SPI_execute(SELECT_CHECK_CONNECT, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		sprintf(msg, "load adb_slot failed.%s result is %d",SELECT_CHECK_CONNECT, ret);
		goto slot_handler_finish;
	}
	if(0==SPI_processed)
	{
		sprintf(msg, "load adb_slot failed.%s result is null.",SELECT_CHECK_CONNECT);
		goto slot_handler_finish;
	}

	pconn_count = SPI_getvalue(SPI_tuptable->vals[0],SPI_tuptable->tupdesc, 1);

	/*2. create connect*/
	sprintf(sql, CREATE_DBLINK_CONNECT, SlotDatabaseName, PostPortNumber);
	if(0==atoi(pconn_count))
	{
		ret = SPI_execute(sql, false, 0);
		if (ret != SPI_OK_SELECT)
		{
			sprintf(msg, "load adb_slot failed.%s result is %d",sql, ret);
			goto slot_handler_finish;
		}
	}

	/*3. flush slot*/
	ret = SPI_execute(FLUSH_SLOT_BY_DBLINK, false, 0);
	if (ret != SPI_OK_SELECT)
	{
		sprintf(msg, "load adb_slot failed.%s result is %d",FLUSH_SLOT_BY_DBLINK, ret);
		goto slot_handler_finish;
	}


	/*4. drop connection*/
	ret = SPI_execute(DROP_DBLINK_CONNECT, false, 0);
	if (ret != SPI_OK_SELECT)
	{
		sprintf(msg, "load adb_slot failed.%s result is %d",DROP_DBLINK_CONNECT, ret);
		goto slot_handler_finish;
	}

	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	return;

slot_handler_finish:
	elog(ERROR, "%s", msg);
	return;

}

Datum
nodeid_from_hashvalue(PG_FUNCTION_ARGS)
{
	int pnodeindex;
	int pstatus;
	int	slotid;

	if(PG_ARGISNULL(0))
		slotid = 0;
	else
		slotid = PG_GETARG_INT32(0);

	SlotGetInfo(slotid, &pnodeindex, &pstatus);

	PG_RETURN_INT32(pnodeindex);
}


Oid get_nodeid_from_hashvalue(void)
{
	Oid			typeId[1];
	typeId[0] = INT4OID;
	return LookupFuncName(list_make1(makeString("nodeid_from_hashvalue")), 1, typeId, false);
}

void InitSLOTPGXCNodeOid(void)
{
	SLOTPGXCNodeOid = get_pgxc_nodeoid(PGXCNodeName);
	elog(DEBUG1, "NodeName=%s-SLOTPGXCNodeOid=%d", PGXCNodeName,SLOTPGXCNodeOid);
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
