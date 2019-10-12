/*
 * commands of parm
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_node.h"
#include "catalog/mgr_parm.h"
#include "catalog/mgr_updateparm.h"
#include "commands/defrem.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "utils/tqual.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "parser/scansup.h"
#include "executor/spi.h"
#include "utils/elog.h"

#include <stdlib.h>
#include <string.h>

#if (Natts_mgr_updateparm != 4)
#error "need change code"
#endif

#define NAMEDATALEN_LOCAL 512

typedef struct nameDataLocal
{
	char		data[NAMEDATALEN_LOCAL];
} NameDataLocal;
typedef NameDataLocal *NameLocal;


static void mgr_send_show_parameters(char cmdtype, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst);
static bool mgr_recv_showparam_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst);
static void mgr_add_givenname_updateparm(MGRUpdateparm *node, Name nodename, char nodetype, Relation rel_node, Relation rel_updateparm, Relation rel_parm, bool bneednotice);
static int mgr_get_character_num(char *str, char character);
static int namestrcpylocal(NameLocal name, const char *str);
static char *mgr_get_value_in_updateparm(Relation rel_node, HeapTuple tuple);
static bool mgr_check_set_value_format(char *keyname, char *keyvalue, int level);
static int mgr_find_char_num(char *str, char checkValue);

/*if the parmeter in gtm or coordinator or datanode pg_settins, the nodetype in mgr_parm is '*'
 , if the parmeter in coordinator or datanode pg_settings, the nodetype in mgr_parm is '#'
*/
#define PARM_IN_GTM_CN_DN '*'
#define PARM_IN_CN_DN '#'
/*the string used to stand for value which be reset by force and the param table has '*' for this parameter*/
#define DEFAULT_VALUE "--"

typedef enum CheckInsertParmStatus
{
	PARM_NEED_INSERT=1,
	PARM_NEED_UPDATE,
	PARM_NEED_NONE
}CheckInsertParmStatus;
/*
typedef struct InitNodeInfo
{
	Relation rel_node;
	HeapScanDesc rel_scan;
	ListCell  **lcp;
}InitNodeInfo;
*/
/*
 * Displayable names for context types (enum GucContext)
 *
 * Note: these strings are deliberately not localized.
 */
const char *const GucContext_Parmnames[] =
{
	 /* PGC_INTERNAL */ "internal",
	 /* PGC_POSTMASTER */ "postmaster",
	 /* PGC_SIGHUP */ "sighup",
	/* PGC_SU_BACKEND */ "superuser-backend",
	 /* PGC_BACKEND */ "backend",
	 /* PGC_SUSET */ "superuser",
	 /* PGC_USERSET */ "user"
};

static bool mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, NameLocal value, int *vartype, Name parmunit, Name parmmin, Name parmmax, int *effectparmstatus, StringInfo enumvalue, bool bneednotice, int elevel);
static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value);
static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, StringInfo paramstrdata, int effectparmstatus, bool bforce);
static void mgr_updateparm_send_parm(GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, StringInfo paramstrdata, int effectparmstatus, bool bforce);
static int mgr_delete_tuple_not_all(Relation noderel, char nodetype, Name key);
static int mgr_check_parm_value(char *name, char *value, int vartype, char *parmunit, char *parmmin, char *parmmax, StringInfo enumvalue);
static int mgr_get_parm_unit_type(char *nodename, char *parmunit);
static bool mgr_parm_enum_lookup_by_name(char *value, StringInfo valuelist);
static void mgr_string_add_single_quota(NameLocal value);
/*
* for command: set {datanode|coordinaotr}  {master|slave} {nodename|ALL} {key1=value1,key2=value2...} ,
* set datanode all {key1=value1,key2=value2...},set gtm all {key1=value1,key2=value2...}, set gtm master|slave
* gtmx {key1=value1,key2=value2...} to record the parameter in mgr_updateparm
*/
void mgr_add_updateparm(MGRUpdateparm *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_set())
	{
		DirectFunctionCall5(mgr_add_updateparm_func,
							CharGetDatum(node->parmtype),
							CStringGetDatum(node->nodename),
							CharGetDatum(node->nodetype),
							BoolGetDatum(node->is_force),
							PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_add_updateparm_func(PG_FUNCTION_ARGS)
{
	Relation rel_updateparm;
	Relation rel_parm;
	Relation rel_node;
	NameData nodename;
	ScanKeyData scankey[1];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	HeapTuple checktuple;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodemaster;
	bool bneednotice = true;
	char nodetype; /*master/slave*/

	MGRUpdateparm *parm_node;
	parm_node = makeNode(MGRUpdateparm);
	parm_node->parmtype = PG_GETARG_CHAR(0);
	parm_node->nodename = PG_GETARG_CSTRING(1);
	parm_node->nodetype = PG_GETARG_CHAR(2);
	parm_node->is_force = PG_GETARG_BOOL(3);
	parm_node->options = (List *)PG_GETARG_POINTER(4);

	Assert(parm_node && parm_node->nodename && parm_node->nodetype && parm_node->parmtype);
	nodetype = parm_node->nodetype;
	/*nodename*/
	namestrcpy(&nodename, parm_node->nodename);

	/*open systbl: mgr_parm*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	/* check node */
	if (strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) != 0)
	{
		checktuple = mgr_get_tuple_node_from_name_type(rel_node, NameStr(nodename));
		if (!HeapTupleIsValid(checktuple))
		{
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("the node \"%s\" does not exist", NameStr(nodename))));
		}
		/* check node type */
		mgr_nodemaster = (Form_mgr_node)GETSTRUCT(checktuple);
		if (parm_node->nodetype != mgr_nodemaster->nodetype)
		{
			heap_freetuple(checktuple);
			heap_close(rel_node, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("the type of node \"%s\" is not %s", NameStr(nodename), mgr_nodetype_str(parm_node->nodetype))));
		}
		heap_freetuple(checktuple);
	}

	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);

	/*set datanode master/slave all (key=value,...)*/
	PG_TRY();
	{
		if (strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) == 0 && (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype || CNDN_TYPE_COORDINATOR_MASTER == nodetype
		|| CNDN_TYPE_COORDINATOR_SLAVE == nodetype || CNDN_TYPE_GTM_COOR_MASTER == nodetype
		|| CNDN_TYPE_GTM_COOR_SLAVE == nodetype))
		{
			bneednotice = true;
			ScanKeyInit(&scankey[0]
					,Anum_mgr_node_nodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetype));
			rel_scan = heap_beginscan_catalog(rel_node, 1, scankey);
			while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
			{
				if(!HeapTupleIsValid(tuple))
					break;
				mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
				Assert(mgr_node);
				mgr_add_givenname_updateparm(parm_node, &(mgr_node->nodename), mgr_node->nodetype, rel_node, rel_updateparm, rel_parm, bneednotice);
				bneednotice = false;
			}
			heap_endscan(rel_scan);
		}
		/*set datanode/gtmcoord all (key=value,...), set nodetype nodname (key=value,...)*/
		else
		{
			bneednotice = true;
			mgr_add_givenname_updateparm(parm_node, &nodename, nodetype, rel_node, rel_updateparm, rel_parm, bneednotice);
		}
	}PG_CATCH();
	{
		heap_close(rel_updateparm, RowExclusiveLock);
		heap_close(rel_parm, RowExclusiveLock);
		heap_close(rel_node, RowExclusiveLock);
		pfree(parm_node);
		PG_RE_THROW();
	}PG_END_TRY();
	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
	pfree(parm_node);
	PG_RETURN_BOOL(true);
}

static void mgr_add_givenname_updateparm(MGRUpdateparm *parm_node, Name nodename, char nodetype, Relation rel_node, Relation rel_updateparm, Relation rel_parm, bool bneednotice)
{
	HeapTuple newtuple;
	Datum datum[Natts_mgr_updateparm];
	List *param_keyvules_list = NIL;
	ListCell *cell;
	ListCell *lc;
	DefElem *def;
	NameData key;
	NameDataLocal value;
	NameDataLocal defaultvalue;
	NameData parmunit;
	NameData parmmin;
	NameData parmmax;
	NameData valuetmp;
	StringInfoData enumvalue;
	StringInfoData paramstrdata;
	bool isnull[Natts_mgr_updateparm];
	bool bsighup = false;
	char parmtype; /*coordinator or datanode or gtm */
	char *pvalue;
	int insertparmstatus;
	int effectparmstatus;
	int vartype;  /*the parm value type: bool, string, enum, int*/
	int ipoint = 0;
	const int namemaxlen = NAMEDATALEN_LOCAL;
	struct keyvalue
	{
		char key[namemaxlen];
		char value[namemaxlen];
	};
	struct keyvalue *key_value = NULL;
	Assert(parm_node && parm_node->parmtype);
	parmtype =  parm_node->parmtype;
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	initStringInfo(&enumvalue);

	/*check the key and value*/
	foreach(lc, parm_node->options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		namestrcpy(&key, def->defname);
		namestrcpylocal(&value, defGetString(def));
		if (strcmp(key.data, "port") == 0 || strcmp(key.data, "synchronous_standby_names") == 0)
		{
			pfree(enumvalue.data);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				, errmsg("permission denied: \"%s\" shoule be modified in \"node\" table before init all, \nuse \"list node\" to get information", key.data)));
		}
		/* Check the read and write separation settings */
		if ((strcmp(key.data, "enable_readsql_on_slave") == 0) || strcmp(key.data, "enable_readsql_on_slave_async") == 0)
		{
			bool isSlaveSync = strcmp(value.data, "on") == 0 ? true : false;
			if (!mgr_update_cn_pgxcnode_readonlysql_slave(key.data, isSlaveSync, (Node *)parm_node))
			{
				pfree(enumvalue.data);
				ereport(WARNING, (errmsg("Adding read-only standby node information to coord failed.")));
			}
		}

		if (!parm_node->is_force)
		{
			/*check the parameter is right for the type node of postgresql.conf*/
			resetStringInfo(&enumvalue);
			mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, &defaultvalue, &vartype, &parmunit
				, &parmmin, &parmmax, &effectparmstatus, &enumvalue, bneednotice, ERROR);
			if(PGC_SIGHUP == effectparmstatus)
				bsighup = true;

			if (PGC_STRING == vartype || (PGC_ENUM == vartype && strstr(value.data, " ") != NULL))
			{
				/*if the value of key is string or the type is enum and value include space, it need use signle quota*/
				mgr_string_add_single_quota(&value);
			}
			/* allow whitespace between integer and unit */
			if (PGC_INT == vartype)
			{
				/*check the value not include empty space*/
				pvalue=strchr(value.data, ' ');
				if (pvalue != NULL)
				{
					pvalue = value.data;
					ipoint = 0;
					/*skip head space*/
					while (isspace((unsigned char) *pvalue))
					{
						pvalue++;
					}
					while(*pvalue != '\0' && *pvalue != ' ')
					{
						valuetmp.data[ipoint] = *pvalue;
						ipoint++;
						pvalue++;
					}
					/*skip the space between value and unit*/
					while (isspace((unsigned char) *pvalue))
					{
						pvalue++;
					}
					if (*pvalue < '0' || *pvalue > '9')
					{
						/*get the unit*/
						while(*pvalue != '\0' && *pvalue != ' ')
						{
							valuetmp.data[ipoint] = *pvalue;
							ipoint++;
							pvalue++;
						}
						/*skip the space after unit*/
						while (isspace((unsigned char) *pvalue))
						{
							pvalue++;
						}
						if ('\0' == *pvalue)
						{
							valuetmp.data[ipoint]='\0';
							namestrcpylocal(&value, valuetmp.data);
						}
					}
				}
			}

			/*check the key's value*/
			if (mgr_check_parm_value(key.data, value.data, vartype, parmunit.data, parmmin.data, parmmax.data, &enumvalue) != 1)
			{
				pfree(enumvalue.data);
				return;
			}
		}
		else
		{
			/*add single quota for it if it not using single quota*/
			if (strcasecmp(value.data, "on") != 0 && strcasecmp(value.data, "off") != 0
			&& strcasecmp(value.data, "true") != 0 && strcasecmp(value.data, "false") != 0
			&& (!(strspn(value.data, "0123456789.") == strlen(value.data) && mgr_get_character_num(value.data, '.')<2)))
				mgr_string_add_single_quota(&value);
		}
		key_value = palloc(sizeof(struct keyvalue));
		strncpy(key_value->key, key.data, namemaxlen-1);
		/*get key*/
		if (strlen(key.data) < namemaxlen)
			key_value->key[strlen(key.data)] = '\0';
		else
			key_value->key[namemaxlen-1] = '\0';
		/*get value*/
		strncpy(key_value->value, value.data, namemaxlen-1);
		if (strlen(value.data) < namemaxlen)
			key_value->value[strlen(value.data)] = '\0';
		else
			key_value->value[namemaxlen-1] = '\0';
		if (!mgr_check_set_value_format(key.data, value.data, WARNING))
		{
			pfree(enumvalue.data);
			return;
		}

		param_keyvules_list = lappend(param_keyvules_list,key_value);
	}
	pfree(enumvalue.data);

	initStringInfo(&paramstrdata);
	/*refresh the param table*/
	foreach(cell, param_keyvules_list)
	{
		key_value = (struct keyvalue *)(lfirst(cell));
		namestrcpy(&key, key_value->key);
		namestrcpylocal(&value, key_value->value);
		/*add key, value to send string*/
		mgr_append_pgconf_paras_str_str(key.data, value.data, &paramstrdata);
		/*check the parm exists already in mgr_updateparm systbl*/
		insertparmstatus = mgr_check_parm_in_updatetbl(rel_updateparm, nodetype, nodename, &key, value.data);
		if (PARM_NEED_NONE == insertparmstatus)
			continue;
		else if (PARM_NEED_UPDATE == insertparmstatus)
		{
			continue;
		}
		datum[Anum_mgr_updateparm_updateparmnodename-1] = NameGetDatum(nodename);
		datum[Anum_mgr_updateparm_updateparmnodetype-1] = CharGetDatum(nodetype);
		datum[Anum_mgr_updateparm_updateparmkey-1] = NameGetDatum(&key);
		datum[Anum_mgr_updateparm_updateparmvalue-1] = CStringGetTextDatum(value.data);
		/* now, we can insert record */
		newtuple = heap_form_tuple(RelationGetDescr(rel_updateparm), datum, isnull);
		CatalogTupleInsert(rel_updateparm, newtuple);
		heap_freetuple(newtuple);
	}
	list_free(param_keyvules_list);
	/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
	if (bsighup)
		effectparmstatus = PGC_SIGHUP;
	mgr_reload_parm(rel_node, nodename->data, nodetype, &paramstrdata, effectparmstatus, false);
	pfree(paramstrdata.data);
}

/*
*check the given parameter nodetype, key,value in mgr_parm, if not in, shows the parameter is not right in postgresql.conf
*/
static bool mgr_check_parm_in_pgconf(Relation noderel, char parmtype, Name key, NameLocal value, int *vartype, Name parmunit, Name parmmin, Name parmmax, int *effectparmstatus, StringInfo enumvalue, bool bneednotice, int elevel)
{
	HeapTuple tuple;
	char *gucconntent;
	Form_mgr_parm mgr_parm;
	Datum datumparmunit;
	Datum datumparmmin;
	Datum datumparmmax;
	Datum datumenumvalues;
	bool isNull = false;

	/*check the name of key exist in mgr_parm system table, if the key only in gtm/coordinator/datanode, the parmtype in
	* mgr_parm is PARM_TYPE_GTMCOOR/PARM_TYPE_COORDINATOR/PARM_TYPE_DATANODE; if the key only in cn or dn, the parmtype in
	* mgr_parm is '#'; if the key in gtm or cn or dn, the parmtype in mgr_parm is '*';
	* first: check the parmtype the input parameter given; second: check the parmtype '#'; third check the parmtype '*'
	*/
	/*check the parm in mgr_parm, type is parmtype*/
	tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(parmtype), NameGetDatum(key));
	if(!HeapTupleIsValid(tuple))
	{
		if (PARM_TYPE_COORDINATOR == parmtype || PARM_TYPE_DATANODE ==parmtype || PARM_TYPE_GTMCOOR == parmtype)
		{
			/*check the parm in mgr_parm, type is '#'*/
			tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(PARM_IN_CN_DN), NameGetDatum(key));
			if(!HeapTupleIsValid(tuple))
			{
				/*check the parm in mgr_parm, type is '*'*/
				tuple = SearchSysCache2(PARMTYPENAME, CharGetDatum(PARM_IN_GTM_CN_DN), NameGetDatum(key));
				if(!HeapTupleIsValid(tuple))
				{
					ereport(elevel, (errcode(ERRCODE_UNDEFINED_OBJECT)
						, errmsg("unrecognized configuration parameter \"%s\"", key->data)));
					return false;
				}
				mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
			}
			else
				mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
		}
		else
		{
			ereport(elevel, (errcode(ERRCODE_UNDEFINED_OBJECT)
				, errmsg("the parm type \"%c\" does not exist", parmtype)));
			return false;
		}
	}
	else
	{
		mgr_parm = (Form_mgr_parm)GETSTRUCT(tuple);
	}

	Assert(mgr_parm);
	gucconntent = NameStr(mgr_parm->parmcontext);
	if (strcmp(NameStr(mgr_parm->parmvartype), "string") == 0)
	{
		*vartype = PGC_STRING;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "real") == 0)
	{
		*vartype = PGC_REAL;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "enum") == 0)
	{
		*vartype = PGC_ENUM;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "bool") == 0)
	{
		*vartype = PGC_BOOL;
	}
	else if (strcmp(NameStr(mgr_parm->parmvartype), "integer") == 0)
	{
		*vartype = PGC_INT;
	}
	else
	{
		ReleaseSysCache(tuple);
		ereport(elevel, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("the value type \"%s\" does not exist", NameStr(mgr_parm->parmvartype))));
		return false;
	}

	/*get the default value*/
	namestrcpylocal(value, NameStr(mgr_parm->parmvalue));
	if (PGC_STRING == *vartype || (PGC_ENUM == *vartype && strstr(value->data, " ") != NULL))
	{
		/*if the value of key is string or the type is enum and value includes space, it need use signle quota*/
		mgr_string_add_single_quota(value);
	}
	/*get parm unit*/
	datumparmunit = heap_getattr(tuple, Anum_mgr_parm_parmunit, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		namestrcpy(parmunit, "");
	}
	else
	{
		namestrcpy(parmunit,TextDatumGetCString(datumparmunit));
	}
	/*get parm min*/
	datumparmmin = heap_getattr(tuple, Anum_mgr_parm_parmminval, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		namestrcpy(parmmin, "0");
	}
	else
	{
		namestrcpy(parmmin,TextDatumGetCString(datumparmmin));
	}
	/*get parm max*/
	datumparmmax = heap_getattr(tuple, Anum_mgr_parm_parmmaxval, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		namestrcpy(parmmax, "0");
	}
	else
	{
		namestrcpy(parmmax,TextDatumGetCString(datumparmmax));
	}
	/*get enumvalues*/
	datumenumvalues = heap_getattr(tuple, Anum_mgr_parm_parmenumval, RelationGetDescr(noderel), &isNull);
	if(isNull)
	{
		/*never come here*/
		appendStringInfo(enumvalue, "%s", "{0}");
	}
	else
	{
		appendStringInfo(enumvalue, "%s", TextDatumGetCString(datumenumvalues));
	}

	if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_USERSET]) == 0 || strcasecmp(gucconntent, GucContext_Parmnames[PGC_SUSET]) == 0 || strcasecmp(gucconntent, GucContext_Parmnames[PGC_SIGHUP]) == 0)
	{
		*effectparmstatus = PGC_SIGHUP;
	}
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_POSTMASTER]) == 0)
	{
		*effectparmstatus = PGC_POSTMASTER;
		if (bneednotice)
			ereport(NOTICE, (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
				, errmsg("parameter \"%s\" cannot be changed without restarting the server", key->data)));
	}
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_INTERNAL]) == 0)
	{
		*effectparmstatus = PGC_INTERNAL;
		if (bneednotice)
			ereport(NOTICE, (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
				, errmsg("parameter \"%s\" cannot be changed", key->data)));
	}
	else if (strcasecmp(gucconntent, GucContext_Parmnames[PGC_BACKEND]) == 0 || strcasecmp(gucconntent, GucContext_Parmnames[PGC_SU_BACKEND]) == 0)
	{
		*effectparmstatus = PGC_BACKEND;
		if (bneednotice)
			ereport(NOTICE, (errcode(ERRCODE_CANT_CHANGE_RUNTIME_PARAM)
				, errmsg("parameter \"%s\" cannot be set after connection start", key->data)));
	}
	else
	{
		ReleaseSysCache(tuple);
		ereport(elevel, (errcode(ERRCODE_UNDEFINED_OBJECT)
			, errmsg("unkown the content of this parameter \"%s\"", key->data)));
		return false;
	}
	ReleaseSysCache(tuple);

	return true;
}


/*
* check the parmeter exist in mgr_updateparm systbl or not.
* 1. nodename is MACRO_STAND_FOR_ALL_NODENAME, does not in mgr_updateparm, clean the gtm or datanode param which name is not
*	MACRO_STAND_FOR_ALL_NODENAME and has the same key, return PARM_NEED_INSERT
* 2. nodename is MACRO_STAND_FOR_ALL_NODENAME, exists in mgr_updateparm, clean the gtm or datanode param which name is not
*	MACRO_STAND_FOR_ALL_NODENAME and has the same key, return PARM_NEED_UPDATE if the value need refresh or delnum != 0
*	(refresh value in this function if it need)
* 3. nodename is MACRO_STAND_FOR_ALL_NODENAME, exists in mgr_updateparm, clean the gtm or datanode param which name is not
*	MACRO_STAND_FOR_ALL_NODENAME and has the same key, return PARM_NEED_DONE if the value does not need refresh and delnum == 0
* 4. nodename is not MACRO_STAND_FOR_ALL_NODENAME, MACRO_STAND_FOR_ALL_NODENAME its type include the nodename type-key, has the
*	same value, if  nodename-type-key exists and has the same value, delete the tuple nodename-type-key return PARM_NEED_NONE
*	if  nodename-type-key exists and has not the same value, delete the tuple nodename-type-key return PARM_NEED_UPDATE,
*	if  nodename-type-key does not exists, return PARM_NEED_NONE
* 5. nodename is not MACRO_STAND_FOR_ALL_NODENAME, MACRO_STAND_FOR_ALL_NODENAME its type includes the nodename type-key, has not
*	 the same value or not find the tuple for MACRO_STAND_FOR_ALL_NODENAME, then check the nodename-key-type exists in
*	mgr_updateparm, if has same value, return PARM_NEED_NONE else
*	PARM_NEED_UPDATE (refresh its value in this function)
* 6. nodename is not MACRO_STAND_FOR_ALL_NODENAME, MACRO_STAND_FOR_ALL_NODENAME its type includes the nodename type-key, has not
*		 the same value or not find the tuple for MACRO_STAND_FOR_ALL_NODENAME, then check the nodename-key-type does not exists in
*	mgr_updateparm, return PARM_NEED_INSERT
*/

static int mgr_check_parm_in_updatetbl(Relation noderel, char nodetype, Name nodename, Name key, char *value)
{
	HeapTuple tuple;
	HeapTuple newtuple;
	HeapTuple alltype_tuple;
	Form_mgr_updateparm mgr_updateparm;
	Form_mgr_updateparm mgr_updateparm_alltype;
	NameData name_standall;
	NameDataLocal valuedata;
	HeapScanDesc rel_scan;
	HeapScanDesc rel_scanall;
	ScanKeyData scankey[3];
	TupleDesc tupledsc;
	Datum datum[Natts_mgr_updateparm];
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	char allnodetype;
	char *kValue;
	int delnum = 0;
	int ret;

	if (CNDN_TYPE_GTM_COOR_MASTER == nodetype || CNDN_TYPE_GTM_COOR_SLAVE == nodetype)
		allnodetype = CNDN_TYPE_GTMCOOR;
	else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
		allnodetype = CNDN_TYPE_DATANODE;
	else if (CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_COORDINATOR_SLAVE == nodetype)
		allnodetype = CNDN_TYPE_COORDINATOR;
	else
		allnodetype = nodetype;

	/*nodename is MACRO_STAND_FOR_ALL_NODENAME*/
	if (namestrcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) == 0)
	{
		ScanKeyInit(&scankey[0]
			,Anum_mgr_updateparm_updateparmnodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&scankey[1]
			,Anum_mgr_updateparm_updateparmnodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(nodename));
		ScanKeyInit(&scankey[2]
			,Anum_mgr_updateparm_updateparmkey
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(key));
		rel_scan = heap_beginscan_catalog(noderel, 3, scankey);
		tuple = heap_getnext(rel_scan, ForwardScanDirection);
		/*1.does not exist in mgr_updateparm*/
		if (!HeapTupleIsValid(tuple))
		{
			mgr_delete_tuple_not_all(noderel, nodetype, key);
			heap_endscan(rel_scan);
			return PARM_NEED_INSERT;
		}
		else
		{
			/*2,3. check need update*/
			mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
			Assert(mgr_updateparm);
			kValue = mgr_get_value_in_updateparm(noderel, tuple);
			if (strcmp(value, kValue) == 0)
			{
				pfree(kValue);
				delnum += mgr_delete_tuple_not_all(noderel, nodetype, key);
				heap_endscan(rel_scan);
				if (delnum > 0)
					return PARM_NEED_UPDATE;
				else
					return PARM_NEED_NONE;
			}
			else
			{
				pfree(kValue);
				mgr_delete_tuple_not_all(noderel, nodetype, key);
				/*update parm's value*/
				memset(datum, 0, sizeof(datum));
				memset(isnull, 0, sizeof(isnull));
				memset(got, 0, sizeof(got));
				namestrcpylocal(&valuedata, value);
				datum[Anum_mgr_updateparm_updateparmvalue-1] = CStringGetTextDatum(valuedata.data);
				got[Anum_mgr_updateparm_updateparmvalue-1] = true;
				tupledsc = RelationGetDescr(noderel);
				newtuple = heap_modify_tuple(tuple, tupledsc, datum,isnull, got);
				CatalogTupleUpdate(noderel, &tuple->t_self, newtuple);
				heap_endscan(rel_scan);
				return PARM_NEED_UPDATE;
			}
		}
	}
	else
	{
		namestrcpy(&name_standall, MACRO_STAND_FOR_ALL_NODENAME);
		ScanKeyInit(&scankey[0]
			,Anum_mgr_updateparm_updateparmnodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(allnodetype));
		ScanKeyInit(&scankey[1]
			,Anum_mgr_updateparm_updateparmnodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(&name_standall));
		ScanKeyInit(&scankey[2]
			,Anum_mgr_updateparm_updateparmkey
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(key));
		rel_scanall = heap_beginscan_catalog(noderel, 3, scankey);
		alltype_tuple = heap_getnext(rel_scanall, ForwardScanDirection);
		ScanKeyInit(&scankey[0]
			,Anum_mgr_updateparm_updateparmnodetype
			,BTEqualStrategyNumber
			,F_CHAREQ
			,CharGetDatum(nodetype));
		ScanKeyInit(&scankey[1]
			,Anum_mgr_updateparm_updateparmnodename
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(nodename));
		ScanKeyInit(&scankey[2]
			,Anum_mgr_updateparm_updateparmkey
			,BTEqualStrategyNumber
			,F_NAMEEQ
			,NameGetDatum(key));
		rel_scan = heap_beginscan_catalog(noderel, 3, scankey);
		tuple = heap_getnext(rel_scan, ForwardScanDirection);
		if (HeapTupleIsValid(alltype_tuple))
		{
			mgr_updateparm_alltype = (Form_mgr_updateparm)GETSTRUCT(alltype_tuple);
			Assert(mgr_updateparm_alltype);
			/*4. MACRO_STAND_FOR_ALL_NODENAME has the same type-key-value */
			kValue = mgr_get_value_in_updateparm(noderel, alltype_tuple);
			if (strcmp(kValue, value) == 0)
			{
				pfree(kValue);
				if (HeapTupleIsValid(tuple))
				{
					mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
					Assert(mgr_updateparm_alltype);
					kValue = mgr_get_value_in_updateparm(noderel, tuple);
					if (strcmp(kValue, value) != 0)
						ret = PARM_NEED_UPDATE;
					else
						ret = PARM_NEED_NONE;
					pfree(kValue);
					CatalogTupleDelete(noderel, &tuple->t_self);
					heap_endscan(rel_scan);
					heap_endscan(rel_scanall);
					return ret;
				}
				else
				{
					heap_endscan(rel_scanall);
					heap_endscan(rel_scan);
					return PARM_NEED_NONE;
				}
			}
			else
			{
				pfree(kValue);
			}
		}
		/*5,6*/
		if (HeapTupleIsValid(tuple))
		{
			mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
			Assert(mgr_updateparm);
			kValue = mgr_get_value_in_updateparm(noderel, tuple);
			if (strcmp(kValue, value) == 0)
			{
				pfree(kValue);
				heap_endscan(rel_scan);
				heap_endscan(rel_scanall);
				return PARM_NEED_NONE;
			}
			else
			{
				pfree(kValue);
				/*update parm's value*/
				memset(datum, 0, sizeof(datum));
				memset(isnull, 0, sizeof(isnull));
				memset(got, 0, sizeof(got));
				namestrcpylocal(&valuedata, value);
				datum[Anum_mgr_updateparm_updateparmvalue-1] = CStringGetTextDatum(valuedata.data);
				got[Anum_mgr_updateparm_updateparmvalue-1] = true;
				tupledsc = RelationGetDescr(noderel);
				newtuple = heap_modify_tuple(tuple, tupledsc, datum,isnull, got);
				CatalogTupleUpdate(noderel, &tuple->t_self, newtuple);
				heap_endscan(rel_scan);
				heap_endscan(rel_scanall);
				return PARM_NEED_UPDATE;
			}
		}
		else
		{
			heap_endscan(rel_scan);
			heap_endscan(rel_scanall);
			return PARM_NEED_INSERT;
		}
	}
}

/*
*get the parameters from mgr_updateparm, then add them to infosendparamsg,  used for initdb
*first, add the parameter which the nodename is '*' with given nodetype; second, add the parameter for given name with given nodetype
*/
void mgr_add_parm(char *nodename, char nodetype, StringInfo infosendparamsg)
{
	Relation rel_updateparm;
	Form_mgr_updateparm mgr_updateparm;
	Form_mgr_updateparm mgr_updateparm_check;
	ScanKeyData key[2];
	HeapScanDesc rel_scan;
	HeapTuple tuple;
	HeapTuple checktuple;
	NameDataLocal parmvalue;
	char *parmkey;
	char *kValue;
	char allnodetype;
	NameData nodenamedata;
	NameData nodenamedatacheck;

	if (CNDN_TYPE_GTM_COOR_MASTER == nodetype || CNDN_TYPE_GTM_COOR_SLAVE == nodetype)
		allnodetype = CNDN_TYPE_GTMCOOR;
	else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
		allnodetype = CNDN_TYPE_DATANODE;
	else
		allnodetype = CNDN_TYPE_COORDINATOR;
	/*first: add the parameter which the nodename is '*' with allnodetype*/
	namestrcpy(&nodenamedata, MACRO_STAND_FOR_ALL_NODENAME);
	namestrcpy(&nodenamedatacheck, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_updateparm_updateparmnodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(allnodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_updateparm_updateparmnodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenamedata));
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_scan = heap_beginscan_catalog(rel_updateparm, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
		Assert(mgr_updateparm);
		/*get key, value*/
		checktuple = SearchSysCache3(MGRUPDATAPARMNODENAMENODETYPEKEY, NameGetDatum(&nodenamedatacheck), CharGetDatum(nodetype), NameGetDatum(&(mgr_updateparm->updateparmkey)));
		if(HeapTupleIsValid(checktuple))
		{
			mgr_updateparm_check = (Form_mgr_updateparm)GETSTRUCT(checktuple);
			Assert(mgr_updateparm_check);
			kValue = mgr_get_value_in_updateparm(rel_updateparm, checktuple);
			if (strcmp(kValue, DEFAULT_VALUE) == 0)
			{
				pfree(kValue);
				ReleaseSysCache(checktuple);
				continue;
			}
			pfree(kValue);
			ReleaseSysCache(checktuple);
		}
		parmkey = NameStr(mgr_updateparm->updateparmkey);
		kValue = mgr_get_value_in_updateparm(rel_updateparm, tuple);
		namestrcpylocal(&parmvalue, kValue);
		pfree(kValue);
		mgr_append_pgconf_paras_str_str(parmkey, parmvalue.data, infosendparamsg);
	}
	heap_endscan(rel_scan);
	/*second: add the parameter for given name with given nodetype*/
	namestrcpy(&nodenamedata, nodename);
	ScanKeyInit(&key[0],
		Anum_mgr_updateparm_updateparmnodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	ScanKeyInit(&key[1],
		Anum_mgr_updateparm_updateparmnodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(&nodenamedata));
	rel_scan = heap_beginscan_catalog(rel_updateparm, 2, key);
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
		Assert(mgr_updateparm);
		kValue = mgr_get_value_in_updateparm(rel_updateparm, tuple);
		if (strcmp(kValue, DEFAULT_VALUE) == 0)
		{
			pfree(kValue);
			continue;
		}
		pfree(kValue);

		/*get key, value*/
		parmkey = NameStr(mgr_updateparm->updateparmkey);
		kValue = mgr_get_value_in_updateparm(rel_updateparm, tuple);
		namestrcpylocal(&parmvalue, kValue);
		pfree(kValue);
		mgr_append_pgconf_paras_str_str(parmkey, parmvalue.data, infosendparamsg);
	}
	heap_endscan(rel_scan);
	heap_close(rel_updateparm, RowExclusiveLock);
}

/*
* according to "set datanode|coordinator|gtm master|slave nodename(key1=value1,...)" , get the nodename, key and value,
* then from node systbl to get ip and path, then reload the key for the node(datanode or coordinator or gtm) when
* the type of the key does not need restart to make effective
*/

static void mgr_reload_parm(Relation noderel, char *nodename, char nodetype, StringInfo paramstrdata, int effectparmstatus, bool bforce)
{
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	GetAgentCmdRst getAgentCmdRst;
	Datum datumpath;
	HeapScanDesc rel_scan;
	char *nodepath;
	char *nodetypestr;
	bool isNull;

	initStringInfo(&(getAgentCmdRst.description));
	/*nodename is MACRO_STAND_FOR_ALL_NODENAME*/
	if (strcmp(nodename, MACRO_STAND_FOR_ALL_NODENAME) == 0)
	{
		rel_scan = heap_beginscan_catalog(noderel, 0, NULL);
		while((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
			Assert(mgr_node);
			if(!mgr_node->nodeincluster)
				continue;
			/*all gtm type: master/slave*/
			if (CNDN_TYPE_GTMCOOR == nodetype)
			{
				if (mgr_node->nodetype != CNDN_TYPE_GTM_COOR_MASTER && mgr_node->nodetype != CNDN_TYPE_GTM_COOR_SLAVE)
					continue;
			}
			/*all datanode type: master/slave*/
			else if (CNDN_TYPE_DATANODE == nodetype)
			{
				if (mgr_node->nodetype != CNDN_TYPE_DATANODE_MASTER && mgr_node->nodetype != CNDN_TYPE_DATANODE_SLAVE)
					continue;
			}
			/*for coordinator all*/
			else if (CNDN_TYPE_COORDINATOR == nodetype)
			{
				if (mgr_node->nodetype != CNDN_TYPE_COORDINATOR_MASTER
						&& mgr_node->nodetype != CNDN_TYPE_COORDINATOR_SLAVE)
					continue;
			}
			else
				continue;

			datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column cndnpath is null")));
			}
			nodepath = TextDatumGetCString(datumpath);
			ereport(LOG,
				(errmsg("send parameter %s ... to %s", paramstrdata->data, nodepath)));
			mgr_updateparm_send_parm(&getAgentCmdRst, mgr_node->nodehost, nodepath, paramstrdata, effectparmstatus, bforce);
		}
		heap_endscan(rel_scan);
	}
	else	/*for given nodename*/
	{
		tuple = mgr_get_tuple_node_from_name_type(noderel, nodename);
		if(!(HeapTupleIsValid(tuple)))
		{
			nodetypestr = mgr_nodetype_str(nodetype);
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				 ,errmsg("%s \"%s\" does not exist", nodetypestr, nodename)));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		if(!mgr_node->nodeincluster)
		{
			pfree(getAgentCmdRst.description.data);
			heap_freetuple(tuple);
			return;
		}
		/*get path*/
		datumpath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(noderel), &isNull);
		if(isNull)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		nodepath = TextDatumGetCString(datumpath);
		/*send the parameter to node path, then reload it*/
		ereport(LOG,
			(errmsg("send parameter %s ... to %s", paramstrdata->data, nodepath)));
		mgr_updateparm_send_parm(&getAgentCmdRst, mgr_node->nodehost, nodepath, paramstrdata, effectparmstatus, bforce);
		heap_freetuple(tuple);
	}
	pfree(getAgentCmdRst.description.data);
}

/*
* send parameter to node, refresh its postgresql.conf, if the guccontent of parameter is superuser/user/sighup, will reload the parameter
*/
static void mgr_updateparm_send_parm(GetAgentCmdRst *getAgentCmdRst, Oid hostoid, char *nodepath, StringInfo paramstrdata, int effectparmstatus, bool bforce)
{
	/*send the parameter to node path, then reload it*/
	resetStringInfo(&(getAgentCmdRst->description));
	if(effectparmstatus == PGC_SIGHUP)
	{
		if (!bforce)
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, nodepath, paramstrdata, hostoid, getAgentCmdRst);
		else
			mgr_send_conf_parameters(AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE, nodepath, paramstrdata, hostoid, getAgentCmdRst);
	}
	else
	{
		if (!bforce)
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF, nodepath, paramstrdata, hostoid, getAgentCmdRst);
		else
			mgr_send_conf_parameters(AGT_CMD_CNDN_DELPARAM_PGSQLCONF_FORCE, nodepath, paramstrdata, hostoid, getAgentCmdRst);
	}

	if (getAgentCmdRst->ret != true)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE)
			 ,errmsg("reload parameter fail: %s", (getAgentCmdRst->description).data)));
	}
}

static int mgr_delete_tuple_not_all(Relation noderel, char nodetype, Name key)
{
	HeapTuple looptuple;
	Form_mgr_updateparm mgr_updateparm;
	HeapScanDesc rel_scan;
	int delnum = 0;

	/*check the nodename in mgr_updateparm nodetype and key are not the same with MACRO_STAND_FOR_ALL_NODENAME*/
	rel_scan = heap_beginscan_catalog(noderel, 0, NULL);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
		Assert(mgr_updateparm);
		if (strcmp(NameStr(mgr_updateparm->updateparmkey), key->data) != 0)
			continue;
		if (strcmp(NameStr(mgr_updateparm->updateparmnodename), MACRO_STAND_FOR_ALL_NODENAME) == 0)
			continue;
		/*all gtm type: master/slave*/
		if (CNDN_TYPE_GTMCOOR == nodetype)
		{
			if (mgr_updateparm->updateparmnodetype != CNDN_TYPE_GTM_COOR_MASTER && mgr_updateparm->updateparmnodetype != CNDN_TYPE_GTM_COOR_SLAVE)
				continue;
		}
		/*all datanode type: master/slave*/
		else if (CNDN_TYPE_DATANODE == nodetype)
		{
			if (mgr_updateparm->updateparmnodetype != CNDN_TYPE_DATANODE_MASTER && mgr_updateparm->updateparmnodetype != CNDN_TYPE_DATANODE_SLAVE)
				continue;
		}
		/*for coordinator all*/
		else if (CNDN_TYPE_COORDINATOR == nodetype)
		{
			if (mgr_updateparm->updateparmnodetype != CNDN_TYPE_COORDINATOR_MASTER
					&& mgr_updateparm->updateparmnodetype != CNDN_TYPE_COORDINATOR_SLAVE)
				continue;
		}
		else
			continue;
		/*delete the tuple which nodename is not MACRO_STAND_FOR_ALL_NODENAME and has the same nodetype and key*/
		delnum++;
		CatalogTupleDelete(noderel, &looptuple->t_self);
	}
	heap_endscan(rel_scan);
	return delnum;
}

/*
* for command: reset {datanode|coordinaotr} {master|slave} {nodename | all}{key1,key2...} , reset datanode
* all {key1,key2...}, reset gtm all{key1,key2...}, reset gtm master|slave gtmx {key1,key2...}, to remove the
* parameter in mgr_updateparm; if the reset parameters not in mgr_updateparm, report error; otherwise use the values
* which come from mgr_parm to replace the old values;
*/
void mgr_reset_updateparm(MGRUpdateparmReset *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_reset())
	{
		DirectFunctionCall5(mgr_reset_updateparm_func,
							CharGetDatum(node->parmtype),
							CStringGetDatum(node->nodename),
							CharGetDatum(node->nodetype),
							BoolGetDatum(node->is_force),
							PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_reset_updateparm_func(PG_FUNCTION_ARGS)
{
	Relation rel_updateparm;
	Relation rel_parm;
	Relation rel_node;
	HeapTuple newtuple;
	HeapTuple looptuple;
	HeapTuple tuple;
	HeapTuple checktuple;
	NameData nodename;
	NameData nodenametmp;
	NameData parmmin;
	NameData parmmax;
	Datum datum[Natts_mgr_updateparm];
	ListCell *lc;
	DefElem *def;
	NameData key;
	NameDataLocal defaultvalue;
	NameData allnodevalue;
	NameData parmunit;
	ScanKeyData scankey[3];
	HeapScanDesc rel_scan;
	Form_mgr_node mgr_node;
	Form_mgr_node mgr_nodemaster;
	StringInfoData enumvalue;
	StringInfoData paramstrdata;
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];
	bool bneedinsert = false;
	bool bneednotice = true;
	bool bsighup = false;
	char parmtype;			/*coordinator or datanode or gtm */
	char nodetype;			/*master/slave*/
	char nodetypetmp;
	char allnodetype;
	char *kValue;
	int effectparmstatus;
	int vartype; /*the parm value type: bool, string, enum, int*/
	Form_mgr_updateparm mgr_updateparm;

	MGRUpdateparmReset *parm_node;
	parm_node = makeNode(MGRUpdateparmReset);
	parm_node->parmtype = PG_GETARG_CHAR(0);
	parm_node->nodename = PG_GETARG_CSTRING(1);
	parm_node->nodetype = PG_GETARG_CHAR(2);
	parm_node->is_force = PG_GETARG_BOOL(3);
	parm_node->options = (List *)PG_GETARG_POINTER(4);

	initStringInfo(&enumvalue);
	initStringInfo(&paramstrdata);
	Assert(parm_node && parm_node->nodename && parm_node->nodetype && parm_node->parmtype);
	nodetype = parm_node->nodetype;
	parmtype =  parm_node->parmtype;
	/*nodename*/
	namestrcpy(&nodename, parm_node->nodename);

	/*open systbl: mgr_parm*/
	rel_node = heap_open(NodeRelationId, RowExclusiveLock);
	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	rel_parm = heap_open(ParmRelationId, RowExclusiveLock);

	PG_TRY();
	{
		/* check node */
		if (strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) != 0)
		{
			checktuple = mgr_get_tuple_node_from_name_type(rel_node, NameStr(nodename));
			if (!HeapTupleIsValid(checktuple))
			{
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the node \"%s\" does not exist", NameStr(nodename))));
			}
			/* check node type */
			mgr_nodemaster = (Form_mgr_node)GETSTRUCT(checktuple);
			if (parm_node->nodetype != mgr_nodemaster->nodetype)
			{
				heap_freetuple(checktuple);
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the type of node \"%s\" is not %s", NameStr(nodename), mgr_nodetype_str(parm_node->nodetype))));
			}
			heap_freetuple(checktuple);
		}

		memset(datum, 0, sizeof(datum));
		memset(isnull, 0, sizeof(isnull));
		memset(got, 0, sizeof(got));

		/*check the key*/
		foreach(lc, parm_node->options)
		{
			def = lfirst(lc);
			Assert(def && IsA(def, DefElem));
			namestrcpy(&key, def->defname);
			/* check reading and writing separation */
			if (strcmp(key.data, "enable_readsql_on_slave") == 0
				|| strcmp(key.data, "enable_readsql_on_slave_async") == 0)
				mgr_update_cn_pgxcnode_readonlysql_slave(key.data, false, (Node *)parm_node);
			if (strcmp(key.data, "port") == 0 || strcmp(key.data, "synchronous_standby_names") == 0)
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					, errmsg("permission denied: \"%s\" shoule be modified in \"node\" table before init all, \nuse \"list node\" to get information", key.data)));
			}
			/*check the parameter is right for the type node of postgresql.conf*/
			if (!parm_node->is_force)
			{
				resetStringInfo(&enumvalue);
				mgr_check_parm_in_pgconf(rel_parm, parmtype, &key, &defaultvalue, &vartype, &parmunit
					, &parmmin, &parmmax, &effectparmstatus, &enumvalue, bneednotice, ERROR);
				mgr_check_set_value_format(key.data, defaultvalue.data, ERROR);
			}
			else
				mgr_check_set_value_format(key.data, "force", ERROR);

			if (PGC_SIGHUP == effectparmstatus)
				bsighup = true;
			/*get key, value to send string*/
			if (!parm_node->is_force)
				mgr_append_pgconf_paras_str_str(key.data, defaultvalue.data, &paramstrdata);
			else
				mgr_append_pgconf_paras_str_str(key.data, "force", &paramstrdata);
		}
		/*refresh param table*/
		foreach(lc,parm_node->options)
		{
			def = lfirst(lc);
			Assert(def && IsA(def, DefElem));
			namestrcpy(&key, def->defname);
			if (parm_node->is_force)
			{
				/*use "none" to label the row is no use, just to show the node does not set this parameter in its postgresql.conf*/
				namestrcpylocal(&defaultvalue, DEFAULT_VALUE);
			}
			/*if nodename is '*', delete the tuple in mgr_updateparm which nodetype is given and reload the parm if the cluster inited
			* reset gtm all (key=value,...)
			* reset coordinator all (key=value,...)
			* reset datanode all (key=value,...)
			*/
			if (strcmp(nodename.data, MACRO_STAND_FOR_ALL_NODENAME) == 0 && (CNDN_TYPE_GTMCOOR == nodetype || CNDN_TYPE_DATANODE == nodetype ||CNDN_TYPE_COORDINATOR == nodetype))
			{
				ScanKeyInit(&scankey[0],
					Anum_mgr_updateparm_updateparmkey
					,BTEqualStrategyNumber
					,F_NAMEEQ
					,NameGetDatum(&key));
				rel_scan = heap_beginscan_catalog(rel_updateparm, 1, scankey);
				while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
				{
					mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
					Assert(mgr_updateparm);
					nodetypetmp = mgr_updateparm->updateparmnodetype;
					if (CNDN_TYPE_GTMCOOR == nodetype)
					{
						if (CNDN_TYPE_GTM_COOR_MASTER != nodetypetmp && CNDN_TYPE_GTM_COOR_SLAVE != nodetypetmp && CNDN_TYPE_GTMCOOR != nodetypetmp)
							continue;
					}
					else if (CNDN_TYPE_DATANODE == nodetype)
					{
						if (CNDN_TYPE_DATANODE_MASTER != nodetypetmp && CNDN_TYPE_DATANODE_SLAVE != nodetypetmp && CNDN_TYPE_DATANODE != nodetypetmp)
							continue;
					}
					/*for coordinator all*/
					else if (CNDN_TYPE_COORDINATOR == nodetype)
					{
						if (CNDN_TYPE_COORDINATOR_MASTER != nodetypetmp && CNDN_TYPE_COORDINATOR_SLAVE != nodetypetmp && CNDN_TYPE_COORDINATOR != nodetypetmp)
							continue;
					}
					else
						continue;
					/*delete the tuple which nodetype is the given nodetype*/
					CatalogTupleDelete(rel_updateparm, &looptuple->t_self);
				}
				heap_endscan(rel_scan);
			}
			/*the nodename is not MACRO_STAND_FOR_ALL_NODENAME or nodetype is datanode master/slave, refresh the postgresql.conf
			* of the node, and delete the tuple in mgr_updateparm which nodetype and nodename is given;if MACRO_STAND_FOR_ALL_NODENAME
			* in mgr_updateparm has the same nodetype, insert one tuple to mgr_updateparm for record
			*/
			else
			{
				/*check the MACRO_STAND_FOR_ALL_NODENAME has the same nodetype*/
				if (CNDN_TYPE_GTM_COOR_MASTER == nodetype || CNDN_TYPE_GTM_COOR_SLAVE == nodetype)
					allnodetype = CNDN_TYPE_GTMCOOR;
				else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
					allnodetype = CNDN_TYPE_DATANODE;
				else
					allnodetype = CNDN_TYPE_COORDINATOR;
				namestrcpy(&nodenametmp, MACRO_STAND_FOR_ALL_NODENAME);
				bneedinsert = false;
				ScanKeyInit(&scankey[0],
					Anum_mgr_updateparm_updateparmnodename
					,BTEqualStrategyNumber
					,F_NAMEEQ
					,NameGetDatum(&nodenametmp));
				ScanKeyInit(&scankey[1],
					Anum_mgr_updateparm_updateparmnodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(allnodetype));
				ScanKeyInit(&scankey[2],
					Anum_mgr_updateparm_updateparmkey
					,BTEqualStrategyNumber
					,F_NAMEEQ
					,NameGetDatum(&key));
				rel_scan = heap_beginscan_catalog(rel_updateparm, 3, scankey);
				while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
				{
					mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
					Assert(mgr_updateparm);
					kValue = mgr_get_value_in_updateparm(rel_updateparm, looptuple);
					strcpy(allnodevalue.data, kValue);
					pfree(kValue);
					if (strcmp(allnodevalue.data, defaultvalue.data) == 0)
						bneedinsert = false;
					else
						bneedinsert = true;
					break;
				}
				heap_endscan(rel_scan);

				/*delete the tuple*/
				ScanKeyInit(&scankey[0],
					Anum_mgr_updateparm_updateparmnodetype
					,BTEqualStrategyNumber
					,F_CHAREQ
					,CharGetDatum(nodetype));
				ScanKeyInit(&scankey[1],
					Anum_mgr_updateparm_updateparmkey
					,BTEqualStrategyNumber
					,F_NAMEEQ
					,NameGetDatum(&key));
				rel_scan = heap_beginscan_catalog(rel_updateparm, 2, scankey);
				while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
				{
					mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(looptuple);
					Assert(mgr_updateparm);
					/*for reset datanode master|slave all (key,...),
					* reset gtm master gtmname(key,...),
					* reset datanode master|slave dnname(key),
					* reset coordinator cnname (key,...)
					*/
					if (strcmp(NameStr(nodename), MACRO_STAND_FOR_ALL_NODENAME) == 0 || strcmp(NameStr(mgr_updateparm->updateparmnodename), NameStr(nodename)) ==0)
					{
						CatalogTupleDelete(rel_updateparm, &looptuple->t_self);
					}
					else
					{
						/*do nothing*/
					}
				}
				heap_endscan(rel_scan);

				/*insert tuple*/
				if (bneedinsert)
				{
					ScanKeyInit(&scankey[0]
						,Anum_mgr_node_nodetype
						,BTEqualStrategyNumber
						,F_CHAREQ
						,CharGetDatum(nodetype));
					rel_scan = heap_beginscan_catalog(rel_node, 1, scankey);
					while ((tuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
					{
						mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
						Assert(mgr_node);
						if(strcmp(NameStr(nodename), MACRO_STAND_FOR_ALL_NODENAME) != 0)
						{
							if (strcmp(NameStr(nodename), NameStr(mgr_node->nodename)) != 0)
								continue;
						}
						datum[Anum_mgr_updateparm_updateparmnodename-1] = NameGetDatum(&(mgr_node->nodename));
						datum[Anum_mgr_updateparm_updateparmnodetype-1] = CharGetDatum(nodetype);
						datum[Anum_mgr_updateparm_updateparmkey-1] = NameGetDatum(&key);
						datum[Anum_mgr_updateparm_updateparmvalue-1] = CStringGetTextDatum(defaultvalue.data);
						/* now, we can insert record */
						newtuple = heap_form_tuple(RelationGetDescr(rel_updateparm), datum, isnull);
						CatalogTupleInsert(rel_updateparm, newtuple);
						heap_freetuple(newtuple);
					}
					heap_endscan(rel_scan);
				}
			}
		}
		/*if the gtm/coordinator/datanode has inited, it will refresh the postgresql.conf of the node*/
		if (bsighup)
			effectparmstatus = PGC_SIGHUP;
		if (!parm_node->is_force)
			mgr_reload_parm(rel_node, nodename.data, nodetype, &paramstrdata, effectparmstatus, false);
		else
			mgr_reload_parm(rel_node, nodename.data, nodetype, &paramstrdata, PGC_POSTMASTER, true);
	}PG_CATCH();
	{
		pfree(enumvalue.data);
		pfree(paramstrdata.data);
		/*close relation */
		heap_close(rel_updateparm, RowExclusiveLock);
		heap_close(rel_parm, RowExclusiveLock);
		heap_close(rel_node, RowExclusiveLock);
		pfree(parm_node);
		PG_RE_THROW();
	}PG_END_TRY();

	pfree(enumvalue.data);
	pfree(paramstrdata.data);
	/*close relation */
	heap_close(rel_updateparm, RowExclusiveLock);
	heap_close(rel_parm, RowExclusiveLock);
	heap_close(rel_node, RowExclusiveLock);
	pfree(parm_node);
	PG_RETURN_BOOL(true);
}

/*
* check the guc value for postgresql.conf
*/
static int mgr_check_parm_value(char *name, char *value, int vartype, char *parmunit, char *parmmin, char *parmmax, StringInfo enumvalue)
{
	int elevel = WARNING;
	int flags;

	switch (vartype)
	{
		case PGC_BOOL:
			{
				bool		newval;

				if (value)
				{
					if (!parse_bool(value, &newval))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("parameter \"%s\" requires a Boolean value",
								 name)));
						return 0;
					}
				}
				break;
			}

		case PGC_INT:
			{
				int newval;
				int min;
				int max;

				if (value)
				{
					const char *hintmsg;
					char *pvalue;
					int len = strlen(value);
					int times = 1;
					pvalue = (char *)palloc(len+1);
					memset(pvalue, 0, len+1);
					if (len > 2 && value[0] == '\'' && value[len-1] == '\'')
					{
						strncpy(pvalue, value+1, len-2);
					}
					else
						strncpy(pvalue, value, len);
					flags = mgr_get_parm_unit_type(name, parmunit);
					if (!parse_int(pvalue, &newval, flags, &hintmsg))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for parameter \"%s\": \"%s\"",
								name, value),
								 hintmsg ? errhint("%s", _(hintmsg)) : 0));
						return 0;
					}
					if (strcmp(parmmin, "") ==0 || strcmp(parmmax, "") ==0)
					{
						pfree(pvalue);
						return 1;
					}
					else
					{
						if (strspn(pvalue,"-0123456789") != strlen(pvalue))
							times = atoi(parmunit) == 0 ? 1:atoi(parmunit);
					}
					pfree(pvalue);
					min = atoi(parmmin);
					max = atoi(parmmax);
					if (newval < min*times || newval*1.0/times > max)
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)",
										newval, name, min, max)));
						return 0;
					}
				}
				break;
			}

		case PGC_REAL:
			{
				double		newval;
				double min;
				double max;

				if (value)
				{
					if (!parse_real(value, &newval))
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						  errmsg("parameter \"%s\" requires a numeric value",
								 name)));
						return 0;
					}

					if (strcmp(parmmin, "") == 0 || strcmp(parmmax, "") == 0)
					{
						return 1;
					}
					min = atof(parmmin);
					max = atof(parmmax);

					if (newval < min || newval > max)
					{
						ereport(elevel,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("%g is outside the valid range for parameter \"%s\" (%g .. %g)",
										newval, name, min, max)));
						return 0;
					}
				}
				break;
			}

		case PGC_STRING:
			{
				/*nothing to do,only need check some name will be truncated*/
				break;
			}

		case PGC_ENUM:
			{
				if (!mgr_parm_enum_lookup_by_name(value, enumvalue))
				{
					ereport(elevel,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for parameter \"%s\": \"%s\"",
						name, value),
						 errhint("Available values: %s", _(enumvalue->data))));
					return 0;
				}
				break;
			}
		default:
				ereport(elevel, (errcode(ERRCODE_UNDEFINED_OBJECT)
					, errmsg("the param type \"d\" does not exist")));
	}
	return 1;
}

/*
* get unit type from unit and parm name(see guc.c)
*/

static int mgr_get_parm_unit_type(char *nodename, char *parmunit)
{
	if (strcmp(parmunit, "ms") == 0)
	{
		return GUC_UNIT_MS;
	}
	else if (strcmp(parmunit, "s") == 0)
	{
		if(strcmp(nodename, "post_auth_delay") ==0 || strcmp(nodename, "pre_auth_delay") ==0)
		{
			return (GUC_NOT_IN_SAMPLE | GUC_UNIT_S);
		}
		else
			return GUC_UNIT_S;
	}
	else if (strcmp(parmunit, "ms") ==0)
	{
		return GUC_UNIT_MS;
	}
	else if (strcmp(parmunit, "min") ==0)
	{
		return GUC_UNIT_MIN;
	}
	else if (strcmp(parmunit, "kB") ==0)
	{
		return GUC_UNIT_KB;
	}
	else if (strcmp(parmunit, "8kB") ==0)
	{
		/*these parameters effect by configure*/
		if (strcmp(nodename, "wal_buffers") ==0)
		{
			return GUC_UNIT_XBLOCKS;
		}
		else if (strcmp(nodename, "wal_segment_size") ==0)
		{
			return (GUC_UNIT_XBLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE);
		}
		else
			return GUC_UNIT_KB;
	}
	else
	{
		/*these parameters effect by configure*/
		if (strcmp(nodename, "wal_buffers") ==0)
		{
			return GUC_UNIT_XBLOCKS;
		}
		else if (strcmp(nodename, "wal_segment_size") ==0)
		{
			return (GUC_UNIT_XBLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE);
		}
		else
			return 0;
	}
}

/*check enum type of parm's value is right*/
static bool mgr_parm_enum_lookup_by_name(char *value, StringInfo valuelist)
{
	char pvaluearray[256]="";		/*the max length of enumvals in pg_settings less than 256*/
	char *pvaluetmp;
	char *ptr;
	char *pvalue;
	char *pvaluespecial=",debug2,";	/*debug equals debug2*/
	bool ret = false;
	int ipos = 0;
	NameDataLocal	valueinputdata;
	NameDataLocal valuecheckdata;

	Assert(value != NULL);
	Assert(valuelist->data != NULL);
	/*valuelist replaces double quota to single quota*/
	if (strstr(valuelist->data, "\"") != NULL)
	{
		while(ipos < valuelist->len)
		{
			if ('\"' == valuelist->data[ipos])
				valuelist->data[ipos] = '\'';
			ipos++;
		}
	}
	namestrcpylocal(&valueinputdata, value);
	mgr_string_add_single_quota(&valueinputdata);
	/*special handling, because "debug" equals "debug2"*/
	if (strcmp("'debug'", valueinputdata.data) == 0)
	{
		pvalue = strstr(valuelist->data, pvaluespecial);
		if (pvalue != NULL)
			return true;
	}
	/*the content of valuelist like this "{xx,xx,xx}", so it need copy from 1 to len -2*/
	if (valuelist->len > 2)
	{
		strncpy(pvaluearray, &(valuelist->data[1]), (valuelist->len-2) < 255 ? (valuelist->len-2):255);
		pvaluearray[(valuelist->len-2) < 255 ? (valuelist->len-2):255] = '\0';
		resetStringInfo(valuelist);
		appendStringInfoString(valuelist, pvaluearray);
	}
	ptr = strtok_r(pvaluearray, ",", &pvaluetmp);
	while(ptr != NULL)
	{
		namestrcpylocal(&valuecheckdata, ptr);
		mgr_string_add_single_quota(&valuecheckdata);
		if (strcmp(valuecheckdata.data, valueinputdata.data) == 0)
		{
			ret = true;
			break;
		}
		ptr = strtok_r(NULL, ",", &pvaluetmp);
	}

	return ret;
}

/*delete the tuple for given nodename and nodetype*/
void mgr_parmr_delete_tuple_nodename_nodetype(Relation noderel, Name nodename, char nodetype)
{
	HeapTuple looptuple;
	ScanKeyData scankey[2];
	HeapScanDesc rel_scan;

	/*for nodename is MACRO_STAND_FOR_ALL_NODENAME, only when type if master then delete the tuple*/
	if (strcmp(MACRO_STAND_FOR_ALL_NODENAME, nodename->data) == 0)
	{
		if (CNDN_TYPE_COORDINATOR_MASTER == nodetype || CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_GTM_COOR_MASTER == nodetype)
		{
				if (CNDN_TYPE_GTM_COOR_MASTER == nodetype || CNDN_TYPE_GTM_COOR_SLAVE == nodetype)
					nodetype = CNDN_TYPE_GTMCOOR;
				else if (CNDN_TYPE_DATANODE_MASTER == nodetype || CNDN_TYPE_DATANODE_SLAVE == nodetype)
					nodetype = CNDN_TYPE_DATANODE;
				else
					nodetype = CNDN_TYPE_COORDINATOR;
		}
		else
			return;
	}
	ScanKeyInit(&scankey[0],
		Anum_mgr_updateparm_updateparmnodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(nodename));
	ScanKeyInit(&scankey[1],
		Anum_mgr_updateparm_updateparmnodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	rel_scan = heap_beginscan_catalog(noderel, 2, scankey);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		CatalogTupleDelete(noderel, &looptuple->t_self);
	}
	heap_endscan(rel_scan);
}

/*update the tuple for given nodename and nodetype*/
void mgr_parmr_update_tuple_nodename_nodetype(Relation noderel, Name nodename, char oldnodetype, char newnodetype)
{
	HeapTuple looptuple;
	HeapTuple newtuple;
	ScanKeyData scankey[2];
	HeapScanDesc rel_scan;
	TupleDesc tupledsc;
	Datum datum[Natts_mgr_updateparm];
	bool isnull[Natts_mgr_updateparm];
	bool got[Natts_mgr_updateparm];

	ScanKeyInit(&scankey[0],
		Anum_mgr_updateparm_updateparmnodename
		,BTEqualStrategyNumber
		,F_NAMEEQ
		,NameGetDatum(nodename));
	ScanKeyInit(&scankey[1],
		Anum_mgr_updateparm_updateparmnodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(oldnodetype));
	rel_scan = heap_beginscan_catalog(noderel, 2, scankey);
	tupledsc = RelationGetDescr(noderel);
	while((looptuple = heap_getnext(rel_scan, ForwardScanDirection)) != NULL)
	{
		/*update parm's type*/
		memset(datum, 0, sizeof(datum));
		memset(isnull, 0, sizeof(isnull));
		memset(got, 0, sizeof(got));
		datum[Anum_mgr_updateparm_updateparmnodetype-1] = CharGetDatum(newnodetype);
		got[Anum_mgr_updateparm_updateparmnodetype-1] = true;
		newtuple = heap_modify_tuple(looptuple, tupledsc, datum,isnull, got);
		CatalogTupleUpdate(noderel, &looptuple->t_self, newtuple);
	}
	heap_endscan(rel_scan);
}

/*update mgr_updateparm, change * to newmaster name and change its nodetype to mastertype*/
void mgr_update_parm_after_dn_failover(Name oldmastername, char oldmastertype, Name oldslavename,  char oldslavetype)
{
	Relation rel_updateparm;

	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	/*delete old master parameters*/
	mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, oldmastername, oldmastertype);
	/*update the old slave parameters to new master type*/
	mgr_parmr_update_tuple_nodename_nodetype(rel_updateparm, oldslavename, oldslavetype, oldmastertype);

	heap_close(rel_updateparm, RowExclusiveLock);
}

/*when gtm failover, the mgr_updateparm need modify: delete oldmaster parm and update slavetype to master for new master*/
void mgr_parm_after_gtm_failover_handle(Name mastername, char mastertype, Name slavename, char slavetype)
{
	Relation rel_updateparm;

	rel_updateparm = heap_open(UpdateparmRelationId, RowExclusiveLock);
	/*delete old master parameters*/
	mgr_parmr_delete_tuple_nodename_nodetype(rel_updateparm, mastername, mastertype);
	/*update the old slave parameters to new master type*/
	mgr_parmr_update_tuple_nodename_nodetype(rel_updateparm, slavename, slavetype, mastertype);

	heap_close(rel_updateparm, RowExclusiveLock);
}

/*
* show parameter, command: SHOW NODENAME PARAMETER
*/
Datum mgr_show_var_param(PG_FUNCTION_ARGS)
{
	HeapTuple tuple;
	HeapTuple checkTuple;
	HeapTuple out;
	FuncCallContext *funcctx;
	InitNodeInfo *info;
	StringInfoData buf;
	StringInfoData infosendmsg;
	StringInfoData nodetypemsg;
	NameData nodename;
	NameData param;
	NameData nodetypedata;
	ScanKeyData key[1];
	Form_mgr_node mgr_node;
	GetAgentCmdRst getAgentCmdRst;
	Relation relNode;
	char *nodetypestr;
	/*max port is 65535,so the length of portstr is 6*/
	char portstr[6]="00000";
	char checkNodeType;
	Oid masterTupleOid;

	/*get node name and parameter name*/
	namestrcpy(&nodename, PG_GETARG_CSTRING(0));
	namestrcpy(&param, PG_GETARG_CSTRING(1));

	relNode = heap_open(NodeRelationId, AccessShareLock);
	checkTuple = mgr_get_nodetuple_by_name_zone(relNode, nodename.data, mgr_zone);
	if (!HeapTupleIsValid(checkTuple))
	{
		heap_close(relNode, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("the node \"%s\" does not exist", nodename.data)));
	}
	mgr_node = (Form_mgr_node)GETSTRUCT(checkTuple);
	Assert(mgr_node);
	checkNodeType = mgr_node->nodetype;
	if (CNDN_TYPE_GTM_COOR_MASTER == checkNodeType || CNDN_TYPE_COORDINATOR_MASTER == checkNodeType
					|| CNDN_TYPE_DATANODE_MASTER == checkNodeType)
		masterTupleOid = HeapTupleGetOid(checkTuple);
	else
		masterTupleOid = mgr_node->nodemasternameoid;
	heap_freetuple(checkTuple);
	heap_close(relNode, AccessShareLock);



	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		check_nodename_isvalid(nodename.data);
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc(sizeof(*info));
		info->rel_node = heap_open(NodeRelationId, AccessShareLock);
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodeincluster
				,BTEqualStrategyNumber
				,F_BOOLEQ
				,BoolGetDatum(true));
		info->rel_scan = heap_beginscan_catalog(info->rel_node, 1, key);
		info->lcp =NULL;

		/* save info */
		funcctx->user_fctx = info;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);
	initStringInfo(&buf);

	initStringInfo(&(getAgentCmdRst.description));
	/*find the tuple ,which the node name is equal nodename*/

	initStringInfo(&infosendmsg);
	while((tuple = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		if (CNDN_TYPE_GTM_COOR_MASTER == checkNodeType || CNDN_TYPE_COORDINATOR_MASTER == checkNodeType
					|| CNDN_TYPE_DATANODE_MASTER == checkNodeType)
		{
			if (masterTupleOid != HeapTupleGetOid(tuple) && masterTupleOid != mgr_node->nodemasternameoid)
				continue;
		}
		else
			if (strcmp(nodename.data, NameStr(mgr_node->nodename)) != 0)
				continue;
		/*send the command string to agent to get the value*/
		sprintf(portstr, "%d", mgr_node->nodeport);
		resetStringInfo(&(getAgentCmdRst.description));
		resetStringInfo(&infosendmsg);
		appendStringInfo(&infosendmsg, "%s", portstr);
		appendStringInfoCharMacro(&infosendmsg, '\0');
		appendStringInfo(&infosendmsg, "%s", param.data);
		appendStringInfoCharMacro(&infosendmsg, '\0');
		mgr_send_show_parameters(AGT_CMD_SHOW_CNDN_PARAM, &infosendmsg, mgr_node->nodehost, &getAgentCmdRst);

		nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
		initStringInfo(&nodetypemsg);
		appendStringInfo(&nodetypemsg, "%s %s", nodetypestr, NameStr(mgr_node->nodename));
		pfree(nodetypestr);
		namestrcpy(&nodetypedata, nodetypemsg.data);
		pfree(nodetypemsg.data);
		out = build_common_command_tuple(&nodetypedata, getAgentCmdRst.ret, getAgentCmdRst.description.data);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(out));
	}

	heap_endscan(info->rel_scan);
	heap_close(info->rel_node, AccessShareLock);
	pfree(infosendmsg.data);
	pfree(getAgentCmdRst.description.data);

	SRF_RETURN_DONE(funcctx);
}

static void mgr_send_show_parameters(char cmdtype, StringInfo infosendmsg, Oid hostoid, GetAgentCmdRst *getAgentCmdRst)
{
	ManagerAgent *ma;
	StringInfoData sendstrmsg;
	StringInfoData buf;

	initStringInfo(&sendstrmsg);
	mgr_append_infostr_infostr(&sendstrmsg, infosendmsg);
	ma = ma_connect_hostoid(hostoid);
	if(!ma_isconnected(ma))
	{
		/* report error message */
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	getAgentCmdRst->ret = false;
	ma_beginmessage(&buf, AGT_MSG_COMMAND);
	ma_sendbyte(&buf, cmdtype);
	mgr_append_infostr_infostr(&buf, &sendstrmsg);
	pfree(sendstrmsg.data);
	ma_endmessage(&buf, ma);
	if (! ma_flush(ma, true))
	{
		getAgentCmdRst->ret = false;
		appendStringInfoString(&(getAgentCmdRst->description), ma_last_error_msg(ma));
		ma_close(ma);
		return;
	}
	/*check the receive msg*/
	mgr_recv_showparam_msg(ma, getAgentCmdRst);
	ma_close(ma);
}

/*
* get msg from agent
*/
static bool mgr_recv_showparam_msg(ManagerAgent	*ma, GetAgentCmdRst *getAgentCmdRst)
{
	char			msg_type;
	StringInfoData recvbuf;
	bool initdone = false;
	initStringInfo(&recvbuf);
	for(;;)
	{
		msg_type = ma_get_message(ma, &recvbuf);
		if(msg_type == AGT_MSG_IDLE)
		{
			/* message end */
			break;
		}else if(msg_type == '\0')
		{
			/* has an error */
			break;
		}else if(msg_type == AGT_MSG_ERROR)
		{
			/* error message */
			getAgentCmdRst->ret = false;
			appendStringInfoString(&(getAgentCmdRst->description), ma_get_err_info(&recvbuf, AGT_MSG_RESULT));
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
			break;
		}else if(msg_type == AGT_MSG_NOTICE)
		{
			/* ignore notice message */
			ereport(LOG, (errmsg("receive msg: %s", ma_get_err_info(&recvbuf, AGT_MSG_RESULT))));
		}
		else if(msg_type == AGT_MSG_RESULT)
		{
			getAgentCmdRst->ret = true;
			appendStringInfoString(&(getAgentCmdRst->description), recvbuf.data);
			ereport(DEBUG1, (errmsg("receive msg: %s", recvbuf.data)));
			initdone = true;
			break;
		}
	}
	pfree(recvbuf.data);
	return initdone;
}

/*
* if the value is string and not using single quota, add single quota for it
*/
static void mgr_string_add_single_quota(NameLocal value)
{
	int len;
	NameDataLocal valuetmp;

	/*if the value of key is string, it need use single quota*/
	len = strlen(value->data);
	if (0 == len)
	{
		namestrcpylocal(value, "''");
	}
	else if (value->data[0] != '\'' || value->data[len-1] != '\'')
	{
		valuetmp.data[0]='\'';
		strcpy(valuetmp.data+sizeof(char),value->data);
		valuetmp.data[1+len]='\'';
		valuetmp.data[2+len]='\0';
		if (len > sizeof(value->data)-2-1)
		{
			valuetmp.data[sizeof(value->data)-2]='\'';
			valuetmp.data[sizeof(value->data)-1]='\0';
		}
		namestrcpylocal(value, valuetmp.data);
	}
	else
	{
		/*do nothing*/
	}
}

/*
* update param table for gtm failover
*/

Datum mgr_update_param_gtm_failover(PG_FUNCTION_ARGS)
{
	NameData oldmastername;
	NameData newmastername;
	NameData newmastertypestr;
	char oldmastertype = AGT_CMD_SHOW_CNDN_PARAM;
	char newmastertype;

	/*check new master  type*/
	namestrcpy(&oldmastername, PG_GETARG_CSTRING(0));
	namestrcpy(&newmastername, PG_GETARG_CSTRING(1));
	namestrcpy(&newmastertypestr, PG_GETARG_CSTRING(2));

	if (strcmp(newmastertypestr.data, "slave") == 0)
		newmastertype = CNDN_TYPE_GTM_COOR_SLAVE;
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
				,errmsg("nodetype \"%s\" is not recognized", newmastertypestr.data)
				,errhint("nodetype is \"slave\"")));
	if (strcmp(oldmastername.data, newmastername.data) != 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
				,errmsg("the input is not right")
				,errhint("the input are: oldmastername, slavename, slavetype")));
	}
	mgr_parm_after_gtm_failover_handle(&oldmastername, oldmastertype, &newmastername, newmastertype);

	PG_RETURN_BOOL(true);
}

/*
* update param table for datanode failover
*/

Datum mgr_update_param_datanode_failover(PG_FUNCTION_ARGS)
{
	NameData oldmastername;
	NameData newmastername;
	NameData newmastertypestr;
	char oldmastertype = CNDN_TYPE_DATANODE_MASTER;
	char newmastertype;

	/*check new master  type*/
	namestrcpy(&oldmastername, PG_GETARG_CSTRING(0));
	namestrcpy(&newmastername, PG_GETARG_CSTRING(1));
	namestrcpy(&newmastertypestr, PG_GETARG_CSTRING(2));

	if (strcmp(newmastertypestr.data, "slave") == 0)
		newmastertype = CNDN_TYPE_DATANODE_SLAVE;
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
				,errmsg("nodetype \"%s\" is not recognized", newmastertypestr.data)
				,errhint("nodetype is \"slave\"")));

	if (strcmp(oldmastername.data, newmastername.data) != 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
				,errmsg("the input is not right")
				,errhint("the input are: oldmastername, slavename, slavetype")));
	}
	mgr_update_parm_after_dn_failover(&oldmastername, oldmastertype, &newmastername, newmastertype);

	PG_RETURN_BOOL(true);
}

/*
* get the number of character in string
*
*/

static int mgr_get_character_num(char *str, char character)
{
	int result = 0;
	char *p = str;

	if (!str)
		return 0;
	while(*p != '\0')
	{
		if (*p == character)
			result++;
		p++;
	}

	return result;
}

static char *mgr_get_value_in_updateparm(Relation rel_node, HeapTuple tuple)
{
	bool isNull = false;
	char *kValue = NULL;
	Datum datumValue;

	datumValue = heap_getattr(tuple, Anum_mgr_updateparm_updateparmvalue, RelationGetDescr(rel_node), &isNull);
	if(isNull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_updateparm")
			, errmsg("column value is null")));
	}
	kValue = pstrdup(TextDatumGetCString(datumValue));

	return kValue;
}


static int namestrcpylocal(NameLocal name, const char *str)
{
	if (!name || !str)
		return -1;
	StrNCpy(name->data, str, NAMEDATALEN_LOCAL);
	return 0;
}

/*
* set the parameter on given type of node
*/
bool
mgr_set_all_nodetype_param(const char nodetype, char *paramName, char *paramValue)
{
	Relation nodeRel;
	ScanKeyData key[2];
	HeapTuple tuple;
	HeapScanDesc relScan;
	Form_mgr_node mgr_node;
	Datum datumPath;
	char *cndnPath;
	char *address;
	const int maxTry = 3;
	int try;
	Oid hostOid;
	bool isNull = false;
	bool result = true;
	StringInfoData infosendmsg;
	GetAgentCmdRst getAgentCmdRst;

	initStringInfo(&infosendmsg);
	initStringInfo(&(getAgentCmdRst.description));
	nodeRel = heap_open(NodeRelationId, AccessShareLock);
	mgr_append_pgconf_paras_str_quotastr(paramName, paramValue, &infosendmsg);

	ScanKeyInit(&key[0],
		Anum_mgr_node_nodeincluster
		,BTEqualStrategyNumber
		,F_BOOLEQ
		,BoolGetDatum(true));
	ScanKeyInit(&key[1],
		Anum_mgr_node_nodetype
		,BTEqualStrategyNumber
		,F_CHAREQ
		,CharGetDatum(nodetype));
	relScan = heap_beginscan_catalog(nodeRel, 2, key);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);

		hostOid = mgr_node->nodehost;
		datumPath = heap_getattr(tuple, Anum_mgr_node_nodepath, RelationGetDescr(nodeRel), &isNull);
		if(isNull)
		{
			pfree(infosendmsg.data);
			heap_endscan(relScan);
			heap_close(nodeRel, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
				, errmsg("column cndnpath is null")));
		}
		cndnPath = TextDatumGetCString(datumPath);
		try = maxTry;
		address = get_hostaddress_from_hostoid(mgr_node->nodehost);
		while(try-- >= 0)
		{
			resetStringInfo(&(getAgentCmdRst.description));
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGSQLCONF_RELOAD, cndnPath, &infosendmsg, hostOid, &getAgentCmdRst);
			/*sleep 0.1s*/
			pg_usleep(100000L);

			/* check the param */
			if(mgr_check_param_reload_postgresqlconf(mgr_node->nodetype, hostOid, mgr_node->nodeport, address, paramName, paramValue))
			{
				break;
			}
		}
		if (try < 0)
		{
			result = false;
			ereport(WARNING, (errmsg("on coordinator \"%s\" reload \"%s=%s\" fail", NameStr(mgr_node->nodename), paramName, paramValue)));
		}
		pfree(address);

	}

	pfree(infosendmsg.data);
	heap_endscan(relScan);
	heap_close(nodeRel, AccessShareLock);

	return result;
}

/*
* mgr_parm table used to check the set parameters and values allow or not, use the function mgr_flushparam:
* 1. clear the mgr_parm table
* 2. get value from one coordinator then insert into mgr_parm, the param type set if '*'
* 3. get value from gtm master then insert into mgr_parm, the param type set if 'G'
* 4. update the parmtype from '*' to '#' for the parameters just used on coordinator/datanode
* 5. delete the parameters in mgr_parm table for the parameter's parmtype is 'G' and the parameter also
*    in patmtype '*' tuple
* 6. check the set_parameters table, if the parameters not suit for mgr_parm table, it will shows some warning
*
* manual steps to insert mgr_parm table
* get the content "INSERT INTO adbmgr.parm VALUES..."
* create table parm(id1 char,name text,setting text,context text,vartype text,unit text, min_val text
*  ,max_val text,enumvals text[]) distribute by replication;
* insert into parm select '*', name, setting, context, vartype, unit, min_val, max_val, enumvals from
* pg_settings order by 2;
* add pg_stat_statements
* INSERT INTO parm VALUES ('*', 'pg_stat_statements.max', '1000', 'postmaster', 'integer', NULL
	, '100', '2147483647', NULL);
* INSERT INTO parm VALUES ('*', 'pg_stat_statements.track', 'top', 'superuser', 'enum', NULL, NULL
* , NULL, '{none,top,all}');
* INSERT INTO parm VALUES ('*', 'pg_stat_statements.save', 'on', 'sighup', 'bool', NULL, NULL, NULL, NULL);
* INSERT INTO parm VALUES ('*', 'pg_stat_statements.track_utility', 'on', 'superuser', 'bool', NULL, NULL
* , NULL, NULL);
* pg_dump -p xxxx -d postgres -t parm -f cn1.txt --inserts -a
*
*/
void mgr_flushparam(MGRFlushParam *node, ParamListInfo params, DestReceiver *dest)
{
	Relation relParm;
	Relation relNode;
	Relation relUpdateparm;
	HeapScanDesc relUpParmScan;
	Form_mgr_updateparm mgr_updateparm;
	HeapTuple tuple;
	StringInfoData sqlStrmsg;
	StringInfoData conStr;
	StringInfoData cnInfoSendMsg;
	StringInfoData gtmInfoSendMsg;
	StringInfoData enumValue;
	GetAgentCmdRst getAgentCmdRst;
	HeapTuple nodeTuple;
	HeapTuple newTuple;
	Form_mgr_node mgr_node;
	PGconn* conn = NULL;
	PGresult *res = NULL;
	Datum datumValue;
	Datum datum[Natts_mgr_parm];
	Datum datumPath;
	NameData cnName;
	NameData parmname;
	NameData parmvalue;
	NameData parmcontext;
	NameData parmvartype;
	NameData parmUnit;
	NameData parmMin;
	NameData parmMax;
	NameData kName;
	NameDataLocal defaultvalue;
	NameData selfAddress;
	int nodePort;
	int nRows = 0;
	int nColumn = 0;
	int iloop = 0;
	int effectParmStatus;
	int varType;
	int ret;
	bool isnull[Natts_mgr_parm];
	bool isNull;
	bool bNeedNotice = false;
	bool bnormalName = true;
	bool bnormalValue = true;
	bool bgetAddress = false;
	bool breloadCn = false;
	char *user;
	char *hostAddr = NULL;
	char *parmunit;
	char *parmminval;
	char *parmmaxval;
	char *parmenumval;
	char *parmtype;
	char *kValue = NULL;
	char cnPath[MAXPGPATH];
	char kNodeType;
	char ptype;
	Oid coordHostOid;

	struct PG_STAT_STATEMENT
	{
		char parmtype;
		char *parmname;
		char *parmvalue;
		char *parmcontext;
		char *parmvartype;
		char *parmunit;
		char *parmminval;
		char *parmmaxval;
		char *parmenumval;
	}pg_stat_statement[4]=
	{
		{'*', "pg_stat_statements.max", "1000", "postmaster", "integer", "", "100", "2147483647", ""},
		{'*', "pg_stat_statements.track", "top", "superuser", "enum", "", "", "", "{none,top,all}"},
		{'*', "pg_stat_statements.save", "on", "sighup", "bool", "", "", "", ""},
		{'*', "pg_stat_statements.track_utility", "on", "superuser", "bool", "", "", "", ""}
	};
	int i = 0;

	/*check agent running normal*/
	mgr_check_all_agent();
	/*check all master nodes running normal*/
	mgr_make_sure_all_running(CNDN_TYPE_GTM_COOR_MASTER);
	//mgr_make_sure_all_running(CNDN_TYPE_COORDINATOR_MASTER);

	/* check connect adbmgr */
	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));
	/* update the parmtype where the parameters just used for coordinator and datanode, not for gtm */
	ret = SPI_execute("delete from mgr_parm", false, 0);
	if (ret != SPI_OK_DELETE)
			ereport(ERROR, (errmsg("ADB Manager SPI_execute failed, delete all contents in mgr_parm table : error code %d", ret)));
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();

	relNode = heap_open(NodeRelationId, AccessShareLock);
	relParm = heap_open(ParmRelationId, RowExclusiveLock);
	initStringInfo(&conStr);
	initStringInfo(&sqlStrmsg);
	initStringInfo(&cnInfoSendMsg);
	initStringInfo(&gtmInfoSendMsg);
	initStringInfo(&(getAgentCmdRst.description));

	PG_TRY();
	{
		/* connect to coordinator, get the content of pg_settings, insert the content into
		* mgr_parm table; connect the gtm master, get the content of pg_settings, update the
		* guc name set '*' to '#', which in the coordinator pg_settings but not in gtm pg_settings,
		* set 'G' which just in gtm master pg_settings.
		*/
		if (!mgr_get_active_node(&cnName, CNDN_TYPE_GTM_COOR_MASTER, InvalidOid))
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("get active coordinator fail in cluster")));
		/* get node info */
		nodeTuple = mgr_get_tuple_node_from_name_type(relNode, NameStr(cnName));
		if (!HeapTupleIsValid(nodeTuple))
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
				,errmsg("get tuple information of coordinator \"%s\" fail", NameStr(cnName))));
		}
		mgr_node = (Form_mgr_node)GETSTRUCT(nodeTuple);
		Assert(mgr_node);
		coordHostOid = mgr_node->nodehost;
		nodePort = mgr_node->nodeport;
		user = get_hostuser_from_hostoid(coordHostOid);
		hostAddr = get_hostaddress_from_hostoid(coordHostOid);
		appendStringInfo(&conStr, "postgresql://%s@%s:%d/%s", user, hostAddr, nodePort, DEFAULT_DB);
		appendStringInfoCharMacro(&conStr, '\0');
		/* sql string */
		appendStringInfo(&sqlStrmsg, "%s", "select  name, setting, context, vartype, unit, min_val, \
		max_val, enumvals from (select row_number() over (partition by name )as name_idx, * from \
		pg_settings where name not like 'pg_stat_statements%') as s1 where name_idx = 1 order by 2;");
		ereport(LOG, (errmsg("connect info: %s, sql: %s",conStr.data, sqlStrmsg.data)));
		conn = PQconnectdb(conStr.data);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQfinish(conn);
			/* get the adbmgr local address, and set the ip to coordinator pg_hba.conf */
			memset(selfAddress.data, 0, NAMEDATALEN);
			bgetAddress = mgr_get_self_address(hostAddr, nodePort, &selfAddress);
			if (!bgetAddress)
			{
				pfree(hostAddr);
				pfree(user);
				heap_freetuple(nodeTuple);
				ereport(ERROR, (errmsg("on ADB Manager get local address fail and can not connect to coordinator \"%s\" from ADB Manager", NameStr(cnName))));
			}
			mgr_add_oneline_info_pghbaconf(CONNECT_HOST, DEFAULT_DB, user, selfAddress.data, 31, "trust", &cnInfoSendMsg);
			datumPath = heap_getattr(nodeTuple, Anum_mgr_node_nodepath, RelationGetDescr(relNode), &isNull);
			if (isNull)
			{
				pfree(hostAddr);
				pfree(user);
				heap_freetuple(nodeTuple);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_node")
					, errmsg("column nodepath is null")));
			}
			strncpy(cnPath, TextDatumGetCString(datumPath), MAXPGPATH-1);
			cnPath[strlen(cnPath)] = '\0';
			mgr_send_conf_parameters(AGT_CMD_CNDN_REFRESH_PGHBACONF, cnPath, &cnInfoSendMsg, coordHostOid, &getAgentCmdRst);
			mgr_reload_conf(coordHostOid, cnPath);
			if (!getAgentCmdRst.ret)
			{
				pfree(hostAddr);
				pfree(user);
				heap_freetuple(nodeTuple);
				ereport(ERROR, (errmsg("set ADB Manager ip \"%s\" to coordinator \"%s\" %s/pg_hba.conf fail %s", selfAddress.data, NameStr(cnName), cnPath, getAgentCmdRst.description.data)));
			}
			breloadCn = true;
			/* try connect again */
			conn = PQconnectdb(conStr.data);
			if (PQstatus(conn) != CONNECTION_OK)
				ereport(ERROR, (errmsg("%s", PQerrorMessage(conn))));
		}
		heap_freetuple(nodeTuple);
		pfree(hostAddr);
		pfree(user);
		res = PQexec(conn, sqlStrmsg.data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQfinish(conn);
			if (NULL == PQresultErrorMessage(res))
				ereport(ERROR, (errmsg("%s" , "execute the sql fail")));
			else
				ereport(ERROR, (errmsg("%s" , PQresultErrorMessage(res))));
		}
		nRows = PQntuples(res);
		nColumn = PQnfields(res);
		Assert(Natts_mgr_parm == nColumn + 1);
		parmtype = MACRO_STAND_FOR_ALL_NODENAME;
		memset(isnull, 0, sizeof(isnull));
		for (iloop=0; iloop<nRows; iloop++)
		{
			memset(datum, 0, sizeof(datum));
			namestrcpy(&parmname, PQgetvalue(res, iloop, 0));
			namestrcpy(&parmvalue, PQgetvalue(res, iloop, 1));
			namestrcpy(&parmcontext, PQgetvalue(res, iloop, 2));
			namestrcpy(&parmvartype, PQgetvalue(res, iloop, 3));
			parmunit = PQgetvalue(res, iloop, 4);
			parmminval = PQgetvalue(res, iloop, 5);
			parmmaxval = PQgetvalue(res, iloop, 6);
			parmenumval = PQgetvalue(res, iloop, 7);
			datum[Anum_mgr_parm_parmtype-1] = CharGetDatum(*parmtype);
			datum[Anum_mgr_parm_parmname-1] = NameGetDatum(&parmname);
			datum[Anum_mgr_parm_parmvalue-1] = NameGetDatum(&parmvalue);
			datum[Anum_mgr_parm_parmcontext-1] = NameGetDatum(&parmcontext);
			datum[Anum_mgr_parm_parmvartype-1] = NameGetDatum(&parmvartype);
			datum[Anum_mgr_parm_parmunit-1] = CStringGetTextDatum(parmunit);
			datum[Anum_mgr_parm_parmminval-1] = CStringGetTextDatum(parmminval);
			datum[Anum_mgr_parm_parmmaxval-1] = CStringGetTextDatum(parmmaxval);
			datum[Anum_mgr_parm_parmenumval-1] = CStringGetTextDatum(parmenumval);
			/* now, we can insert record */
			newTuple = heap_form_tuple(RelationGetDescr(relParm), datum, isnull);
			CatalogTupleInsert(relParm, newTuple);
			heap_freetuple(newTuple);
		}
		PQclear(res);
		PQfinish(conn);

		/* insert the pg_stat_statement parameters */
		memset(isnull, 0, sizeof(isnull));
		for (i=0; i<sizeof(pg_stat_statement)/sizeof(struct PG_STAT_STATEMENT); i++)
		{
			memset(datum, 0, sizeof(datum));
			namestrcpy(&parmname, pg_stat_statement[i].parmname);
			namestrcpy(&parmvalue, pg_stat_statement[i].parmvalue);
			namestrcpy(&parmcontext, pg_stat_statement[i].parmcontext);
			namestrcpy(&parmvartype, pg_stat_statement[i].parmvartype);
			parmunit = pg_stat_statement[i].parmunit;
			parmminval = pg_stat_statement[i].parmminval;
			parmmaxval = pg_stat_statement[i].parmmaxval;
			parmenumval = pg_stat_statement[i].parmenumval;
			datum[Anum_mgr_parm_parmtype-1] = CharGetDatum(pg_stat_statement[i].parmtype);
			datum[Anum_mgr_parm_parmname-1] = NameGetDatum(&parmname);
			datum[Anum_mgr_parm_parmvalue-1] = NameGetDatum(&parmvalue);
			datum[Anum_mgr_parm_parmcontext-1] = NameGetDatum(&parmcontext);
			datum[Anum_mgr_parm_parmvartype-1] = NameGetDatum(&parmvartype);
			datum[Anum_mgr_parm_parmunit-1] = CStringGetTextDatum(parmunit);
			datum[Anum_mgr_parm_parmminval-1] = CStringGetTextDatum(parmminval);
			datum[Anum_mgr_parm_parmmaxval-1] = CStringGetTextDatum(parmmaxval);
			datum[Anum_mgr_parm_parmenumval-1] = CStringGetTextDatum(parmenumval);
			/* now, we can insert record */
			newTuple = heap_form_tuple(RelationGetDescr(relParm), datum, isnull);
			CatalogTupleInsert(relParm, newTuple);
			heap_freetuple(newTuple);
		}

		/* delete the add adbmgr ip from coordinator pg_hba.conf and gtm master pg_hba.conf if add */
		if (breloadCn)
		{
			resetStringInfo(&(getAgentCmdRst.description));
			mgr_send_conf_parameters(AGT_CMD_CNDN_DELETE_PGHBACONF
									,cnPath
									,&cnInfoSendMsg
									,coordHostOid
									,&getAgentCmdRst);
			if (!getAgentCmdRst.ret)
				ereport(WARNING, (errmsg("remove ADB Manager ip \"%s\" from coordinator \"%s\" %s/pg_hba.conf fail %s", selfAddress.data, NameStr(cnName), cnPath, getAgentCmdRst.description.data)));
			mgr_reload_conf(coordHostOid, cnPath);
		}
	}PG_CATCH();
	{
		pfree(conStr.data);
		pfree(sqlStrmsg.data);
		pfree(cnInfoSendMsg.data);
		pfree(gtmInfoSendMsg.data);
		pfree(getAgentCmdRst.description.data);
		heap_close(relNode, AccessShareLock);
		heap_close(relParm, RowExclusiveLock);
		PG_RE_THROW();
	}PG_END_TRY();

	pfree(sqlStrmsg.data);
	pfree(conStr.data);
	pfree(cnInfoSendMsg.data);
	pfree(gtmInfoSendMsg.data);
	pfree(getAgentCmdRst.description.data);
	heap_close(relNode, AccessShareLock);
	heap_close(relParm, RowExclusiveLock);

	/* check connect adbmgr */
	if ((ret = SPI_connect()) < 0)
		ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));
	/* update the parmtype where the parameters just used for coordinator and datanode, not for gtm */
	ret = SPI_execute("update mgr_parm set parmtype='#' where parmtype='*' and parmname not \
		in (select parmname from mgr_parm where parmtype='G') and parmname not like \
		'pg_stat_statements%';", false, 0);
	if (ret != SPI_OK_UPDATE)
			ereport(ERROR, (errmsg("ADB Manager SPI_execute failed, update the parmtype from '*' \
					to '#' in mgr_parm table : error code %d", ret)));
	SPI_freetuptable(SPI_tuptable);
	/* delete the redundant parameters */
	ret = SPI_execute("delete from mgr_parm where parmtype='G' and parmname in (select parmname \
		from mgr_parm where parmtype='*');", false, 0);
	if (ret != SPI_OK_DELETE)
			ereport(ERROR, (errmsg("ADB Manager SPI_execute failed, delete the redundant parameters \
				in mgr_parm table : error code %d", ret)));
	SPI_finish();

	/* check the parameters in mgr_updateparm table, if the parameters not in mgr_parm, give notice
	* message
	*/
	relParm = heap_open(ParmRelationId, AccessShareLock);
	relUpdateparm = heap_open(UpdateparmRelationId, AccessShareLock);
	relUpParmScan = heap_beginscan_catalog(relUpdateparm, 0, NULL);
	initStringInfo(&enumValue);

	PG_TRY();
	{
		while((tuple = heap_getnext(relUpParmScan, ForwardScanDirection)) != NULL)
		{
			mgr_updateparm = (Form_mgr_updateparm)GETSTRUCT(tuple);
			Assert(mgr_updateparm);
			kNodeType = mgr_updateparm->updateparmnodetype;
			switch(kNodeType)
			{
				case CNDN_TYPE_GTM_COOR_MASTER:
				case CNDN_TYPE_GTM_COOR_SLAVE:
				case CNDN_TYPE_GTMCOOR:
					ptype = CNDN_TYPE_GTMCOOR;
					break;
				case CNDN_TYPE_COORDINATOR_MASTER:
				case CNDN_TYPE_COORDINATOR_SLAVE:
				case CNDN_TYPE_COORDINATOR:
					ptype = CNDN_TYPE_COORDINATOR;
					break;
				case CNDN_TYPE_DATANODE_MASTER:
				case CNDN_TYPE_DATANODE_SLAVE:
				case CNDN_TYPE_DATANODE:
					ptype = CNDN_TYPE_DATANODE;
					break;
				default:
					ptype = '*';
					break;
			}

			namestrcpy(&kName, NameStr(mgr_updateparm->updateparmkey));
			datumValue = heap_getattr(tuple, Anum_mgr_updateparm_updateparmvalue, RelationGetDescr(relUpdateparm),
				&isNull);
			if(isNull)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_updateparm")
					, errmsg("column value is null")));
			}
			kValue = TextDatumGetCString(datumValue);
			/* check the name and value in mgr_parm */
			resetStringInfo(&enumValue);
			if (mgr_check_parm_in_pgconf(relParm, ptype, &kName, &defaultvalue, &varType, &parmUnit
				, &parmMin, &parmMax, &effectParmStatus, &enumValue, bNeedNotice, WARNING))
			{
				if (mgr_check_parm_value(kName.data, kValue, varType, parmUnit.data, parmMin.data
					, parmMax.data, &enumValue) != 1)
					bnormalValue = false;
			}
			else
				bnormalName = false;
		}

		if (!bnormalName)
			ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
				,errmsg("according to the warning, some parameters are unrecognized in the set_parameters table,you should use \"reset ... force\" command to clear it")
				,errhint("try \"\\h reset\" for more information")));
		if (!bnormalValue)
			ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)
				,errmsg("according to the warning, some values are not right in the set_parameters table, you should use \"set\" command to set new values")
				,errhint("try \"\\h set\" for more information")));
	}PG_CATCH();
	{
		pfree(enumValue.data);
		heap_endscan(relUpParmScan);
		heap_close(relUpdateparm, AccessShareLock);
		heap_close(relParm, AccessShareLock);
		PG_RE_THROW();
	}PG_END_TRY();

	pfree(enumValue.data);
	heap_endscan(relUpParmScan);
	heap_close(relUpdateparm, AccessShareLock);
	heap_close(relParm, AccessShareLock);
}

/*
 * find the number char in given string
 */
static int mgr_find_char_num(char *str, char checkValue)
{
	int num = 0;
	char *pstr = NULL;

	if (!str)
		return 0;
	pstr = str;
	while(*pstr)
	{
		if (*pstr == checkValue)
			num++;
		pstr++;
	}

	return num;
}

/*
 * check the value satisfy the value format
 */
static bool mgr_check_set_value_format(char *keyname, char *keyvalue, int level)
{
	int singleQuoteNum  = 0;
	int valueLen = 0;

	Assert(keyname);
	/* check value */
	valueLen = strlen(keyvalue);
	singleQuoteNum = mgr_find_char_num(keyvalue, '\'');
	if (!keyvalue
		|| ((keyvalue[0] == '\'') && ((keyvalue[valueLen-1] != '\'') || valueLen <2))
		|| singleQuoteNum > 2
		|| ((keyvalue[0] != '\'') && (singleQuoteNum > 0)))
	{
		ereport(level,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter \"%s\"", keyname)));
		return false;
	}

	/* check name */
	singleQuoteNum = 0;
	valueLen = strlen(keyname);
	singleQuoteNum = mgr_find_char_num(keyname, '\'');
	if (singleQuoteNum > 0)
	{
		ereport(level,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter \"%s\"", keyname)));
		return false;
	}
	return true;
}
