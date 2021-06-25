/*
 * commands of host
 */

#include "postgres.h"

#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include<arpa/inet.h>

#include "access/htup_details.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_node.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "common/ip.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "pgtar.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"    /* For F_NAMEEQ	*/
#include "executor/spi.h"
#include "access/xlog.h"

typedef struct HostScanInfo
{
	Relation		rel_host;
	TableScanDesc	rel_scan;
	List		   *host_list;
	int				index;
}HostScanInfo;

typedef struct HostScanInfo StopAgentInfo;
typedef struct HostScanInfo InitHostInfo;
typedef struct HostScanInfo InitDeployInfo;

#if (Natts_mgr_host != 9)
#error "need change code"
#endif

static FILE* make_tar_package(void);
static void append_file_to_tar(FILE *tar, const char *path, const char *name);
static bool host_is_localhost(const char *name);
static bool deploy_to_host(FILE *tar, TupleDesc desc, HeapTuple tup, StringInfo msg, const char *password);
static void get_adbhome(char *adbhome);
static void check_host_name_isvaild(List *host_name_list);
static bool mgr_check_address_repeate(char *address);
static bool mgr_check_can_deploy(char *hostname);

void mgr_add_host(MGRAddHost *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_add())
	{
		DirectFunctionCall3(mgr_add_host_func, BoolGetDatum(node->if_not_exists),
								CStringGetDatum(node->name),
								PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_add_host_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	char *address = NULL;
	NameData name;
	NameData user;
	Datum datum[Natts_mgr_host];
	struct addrinfo hint;
	struct addrinfo *addrs;
	bool isnull[Natts_mgr_host];
	bool got[Natts_mgr_host];
	bool res = false;
	char adbhome[MAXPGPATH]={0};
	char abuf[INET_ADDRSTRLEN];
	const char *ipstr;
	struct sockaddr_in *sinp;
	struct in_addr addr;
	int ret;
	bool if_not_exists = PG_GETARG_BOOL(0);
	char *hostname = PG_GETARG_CSTRING(1);
	List *options = (List *)PG_GETARG_POINTER(2);

	rel = table_open(HostRelationId, RowExclusiveLock);
	namestrcpy(&name, hostname);
	/* check exists */
	if(SearchSysCacheExists1(HOSTHOSTNAME, NameGetDatum(&name)))
	{
		if(if_not_exists)
		{
			ereport(NOTICE,  (errcode(ERRCODE_DUPLICATE_OBJECT),
				errmsg("host \"%s\" already exists, skipping", NameStr(name))));
			table_close(rel, RowExclusiveLock);
			PG_RETURN_BOOL(false);
		}
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("host \"%s\" already exists", NameStr(name))));
	}
	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_host_hostname-1] = NameGetDatum(&name);
	foreach(lc,options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "user") == 0)
		{
			if(got[Anum_mgr_host_hostuser-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_host_hostuser-1] = true;
			str = defGetString(def);
			namestrcpy(&user, str);
			datum[Anum_mgr_host_hostuser-1] = NameGetDatum(&user);
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostport-1] = true;
		}else if(strcmp(def->defname, "protocol") == 0)
		{
			if(got[Anum_mgr_host_hostproto-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(strcmp(str, "telnet") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_TELNET);
			}else if(strcmp(str, "ssh") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_SSH);
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"protocol\": \"%s\", must be \"telnet\" or \"ssh\"", str)));
			}
			got[Anum_mgr_host_hostproto-1] = true;
		}else if(strcmp(def->defname, "address") == 0)
		{
			if(got[Anum_mgr_host_hostaddr-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			/*check the address is IPv4 or IPv6, not hostname*/
			if(!(inet_pton(AF_INET, str, &addr)>0))
			{
				if(!(inet_pton(AF_INET6, str, &addr)>0))
				{
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"%s\", is not a valid IPv4 or IPv6 address", "address")));
				}
			}
			address = str;
			datum[Anum_mgr_host_hostaddr-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostaddr-1] = true;
		}else if(strcmp(def->defname, "adbhome") == 0)
		{
			if(got[Anum_mgr_host_hostadbhome-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_host_hostadbhome-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostadbhome-1] = true;
		}else if(strcmp(def->defname, "agentport") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostagentport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostagentport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostagentport-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is user,port,protocol,agentport,address and adbhome")));
		}
	}

	/* if not give, set to default */
	if(got[Anum_mgr_host_hostuser-1] == false)
	{
		namestrcpy(&user, GetUserNameFromId(GetUserId(), false));
		datum[Anum_mgr_host_hostuser-1] = NameGetDatum(&user);
	}
	if(got[Anum_mgr_host_hostproto-1] == false)
	{
		datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_SSH);
	}
	if(got[Anum_mgr_host_hostport-1] == false)
	{
		if(DatumGetChar(datum[Anum_mgr_host_hostproto-1]) == HOST_PROTOCOL_SSH)
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(22);
		else if(DatumGetChar(datum[Anum_mgr_host_hostproto-1]) == HOST_PROTOCOL_TELNET)
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(23);
		else
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				,errmsg("unknown protocol type %d", DatumGetChar(datum[Anum_mgr_host_hostproto-1]))));
	}
	if(got[Anum_mgr_host_hostaddr-1] == false)
	{
		MemSet(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_family = AF_UNSPEC;
		hint.ai_flags = AI_PASSIVE;
		ret = pg_getaddrinfo_all(name.data, NULL, &hint, &addrs);
		if(ret != 0 || addrs == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("could not resolve \"%s\": %s"
				, name.data, gai_strerror(ret))));
		}
		sinp = (struct sockaddr_in *)addrs->ai_addr;
		ipstr = inet_ntop(AF_INET, &sinp->sin_addr, abuf,INET_ADDRSTRLEN);
		address = (char *)ipstr;
		datum[Anum_mgr_host_hostaddr-1] = PointerGetDatum(cstring_to_text(ipstr));
		pg_freeaddrinfo_all(AF_UNSPEC, addrs);
	}
	if(got[Anum_mgr_host_hostadbhome-1] == false)
	{
		get_adbhome(adbhome);
		datum[Anum_mgr_host_hostadbhome-1] = PointerGetDatum(cstring_to_text(adbhome));
	}
	if(got[Anum_mgr_host_hostagentport-1] == false)
	{
		datum[Anum_mgr_host_hostagentport-1] = Int32GetDatum(AGENTDEFAULTPORT);
	}
	/*check address not repeated*/
	res = mgr_check_address_repeate(address);
	if (res)
	{
		table_close(rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			,errmsg("address \"%s\" is already in host table", address)));
	}
	datum[Anum_mgr_host_oid-1] = ObjectIdGetDatum(GetNewOidWithIndex(rel, HostOidIndexId, Anum_mgr_host_oid));
	isnull[Anum_mgr_host_oid-1] = false;
	/* ADB DOCTOR BEGIN */
	datum[Anum_mgr_host_allowcure-1] = BoolGetDatum(true);
	/* ADB DOCTOR END */
	/* now, we can insert record */
	tuple = heap_form_tuple(RelationGetDescr(rel), datum, isnull);
	CatalogTupleInsert(rel, tuple);
	heap_freetuple(tuple);

	/* at end, close relation */
	table_close(rel, RowExclusiveLock);
	PG_RETURN_BOOL(true);
}

void mgr_drop_host(MGRDropHost *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_drop())
	{
		DirectFunctionCall2(mgr_drop_host_func,
							BoolGetDatum(node->if_exists),
							PointerGetDatum(node->hosts));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return;
	}
}

Datum mgr_drop_host_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	HeapTuple tuple;
	ListCell *lc;
	Value *val;
	MemoryContext context, old_context;
	NameData name;
	bool if_exists = PG_GETARG_BOOL(0);
	List *host_list = (List *)PG_GETARG_POINTER(1);

	context = AllocSetContextCreate(CurrentMemoryContext,
									"DROP HOST",
									ALLOCSET_DEFAULT_SIZES);
	rel = table_open(HostRelationId, RowExclusiveLock);
	old_context = MemoryContextSwitchTo(context);

	/* first we need check is it all exists and used by other */
	foreach(lc, host_list)
	{
		val = lfirst(lc);
		Assert(val && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
		if(!HeapTupleIsValid(tuple))
		{
			if(if_exists)
			{
				ereport(NOTICE,  (errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("host \"%s\" dose not exist, skipping", NameStr(name))));
				continue;
			}
			else
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT)
					,errmsg("host \"%s\" dose not exist", NameStr(name))));
		}
		/*check the tuple has been used or not*/
		if(mgr_check_host_in_use(((Form_mgr_host)GETSTRUCT(tuple))->oid, false))
		{
			ReleaseSysCache(tuple);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("\"%s\" has been used, cannot be dropped", NameStr(name))));
		}
		/* todo chech used by other */
		ReleaseSysCache(tuple);
	}

	/* now we can delete host(s) */
	foreach(lc, host_list)
	{
		val = lfirst(lc);
		Assert(val  && IsA(val,String));
		MemoryContextReset(context);
		namestrcpy(&name, strVal(val));
		tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
		if(HeapTupleIsValid(tuple))
		{
			simple_heap_delete(rel, &(tuple->t_self));
			ReleaseSysCache(tuple);
		}
	}

	table_close(rel, RowExclusiveLock);
	(void)MemoryContextSwitchTo(old_context);
	MemoryContextDelete(context);
	PG_RETURN_BOOL(true);
}

void mgr_alter_host(MGRAlterHost *node, ParamListInfo params, DestReceiver *dest)
{
	if (mgr_has_priv_alter())
	{
		DirectFunctionCall3(mgr_alter_host_func,
								BoolGetDatum(node->if_not_exists),
								CStringGetDatum(node->name),
								PointerGetDatum(node->options));
		return;
	}
	else
	{
		ereport(ERROR, (errmsg("permission denied")));
		return ;
	}
}

Datum mgr_alter_host_func(PG_FUNCTION_ARGS)
{
	Relation rel;
	Relation rel_node;
	HeapTuple tuple;
	HeapTuple new_tuple;
	HeapTuple checktuple;
	ListCell *lc;
	DefElem *def;
	char *str;
	NameData name;
	NameData user;
	Datum datum[Natts_mgr_host];
	bool isnull[Natts_mgr_host];
	bool got[Natts_mgr_host];
	Form_mgr_node mgr_node PG_USED_FOR_ASSERTS_ONLY;
	TupleDesc host_dsc;
	TableScanDesc relScan;
	ScanKeyData key[1];
	List *options = (List *)PG_GETARG_POINTER(2);
	bool if_not_exists = PG_GETARG_BOOL(0);
	char *name_str = PG_GETARG_CSTRING(1);
	Assert(name_str != NULL);

	if (RecoveryInProgress())
		ereport(ERROR, (errmsg("cannot assign TransactionIds during recovery")));

	rel = table_open(HostRelationId, RowExclusiveLock);
	host_dsc = RelationGetDescr(rel);
	namestrcpy(&name, name_str);
	/* check whether exists */
	tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	if(!SearchSysCacheExists1(HOSTHOSTNAME, NameGetDatum(&name)))
	{
		if(if_not_exists)
		{
			table_close(rel, RowExclusiveLock);
			PG_RETURN_BOOL(false);
		}

		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT)
				, errmsg("host \"%s\" does not exist", NameStr(name))));
	}

	memset(datum, 0, sizeof(datum));
	memset(isnull, 0, sizeof(isnull));
	memset(got, 0, sizeof(got));

	/* name */
	datum[Anum_mgr_host_hostname-1] = NameGetDatum(&name);
	foreach(lc, options)
	{
		def = lfirst(lc);
		Assert(def && IsA(def, DefElem));
		if(strcmp(def->defname, "user") == 0)
		{
			if(got[Anum_mgr_host_hostuser-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			got[Anum_mgr_host_hostuser-1] = true;
			str = defGetString(def);
			namestrcpy(&user, str);
			datum[Anum_mgr_host_hostuser-1] = NameGetDatum(&user);
		}else if(strcmp(def->defname, "port") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostport-1] = true;
		}else if(strcmp(def->defname, "protocol") == 0)
		{
			if(got[Anum_mgr_host_hostproto-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(strcmp(str, "telnet") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_TELNET);
			}else if(strcmp(str, "ssh") == 0)
			{
				datum[Anum_mgr_host_hostproto-1] = CharGetDatum(HOST_PROTOCOL_SSH);
			}else
			{
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"protocol\": \"%s\", must be \"telnet\" or \"ssh\"", str)));
			}
			got[Anum_mgr_host_hostproto-1] = true;
		}else if(strcmp(def->defname, "address") == 0)
		{
			if(got[Anum_mgr_host_hostaddr-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid value for parameter \"%s\"", "address")));
			datum[Anum_mgr_host_hostaddr-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostaddr-1] = true;
		}else if(strcmp(def->defname, "adbhome") == 0)
		{
			if(got[Anum_mgr_host_hostadbhome-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			str = defGetString(def);
			if(str[0] != '/' || str[0] == '\0')
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("invalid absoulte path: \"%s\"", str)));
			datum[Anum_mgr_host_hostadbhome-1] = PointerGetDatum(cstring_to_text(str));
			got[Anum_mgr_host_hostadbhome-1] = true;
		}else if(strcmp(def->defname, "agentport") == 0)
		{
			int32 port;
			if(got[Anum_mgr_host_hostagentport-1])
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("conflicting or redundant options")));
			port = defGetInt32(def);
			if(port <= 0 || port > UINT16_MAX)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
					,errmsg("%d is outside the valid range for parameter \"%s\" (%d .. %d)", port, "port", 1, UINT16_MAX)));
			datum[Anum_mgr_host_hostagentport-1] = Int32GetDatum(port);
			got[Anum_mgr_host_hostagentport-1] = true;
		}else
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR)
				,errmsg("option \"%s\" not recognized", def->defname)
				,errhint("option is user,port,protocol,agentport, address and adbhome")));
		}
	}

	/*check the tuple has been used or not*/
	if(mgr_check_host_in_use(((Form_mgr_host)GETSTRUCT(tuple))->oid, true))
	{
		if (got[Anum_mgr_host_hostadbhome-1] || got[Anum_mgr_host_hostuser-1]
			|| got[Anum_mgr_host_hostport-1] || got[Anum_mgr_host_hostproto-1] || got[Anum_mgr_host_hostagentport-1])
		{
			ReleaseSysCache(tuple);
			table_close(rel, RowExclusiveLock);
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("\"%s\" has been used, cannot be changed", NameStr(name))));
		}
		rel_node = table_open(NodeRelationId, AccessShareLock);
		ScanKeyInit(&key[0]
			,Anum_mgr_node_nodeincluster
			,BTEqualStrategyNumber
			,F_BOOLEQ
			,BoolGetDatum(true));
		relScan = table_beginscan_catalog(rel_node, 1, key);
		checktuple = heap_getnext(relScan, ForwardScanDirection);
		if (HeapTupleIsValid(checktuple))
		{
			mgr_node = (Form_mgr_node)GETSTRUCT(checktuple);
			Assert(mgr_node);
			if (got[Anum_mgr_host_hostaddr-1])
				ereport(WARNING, (errcode(ERRCODE_OBJECT_IN_USE)
					 ,errmsg("the cluster has been initialized, after command \"alter host\" to modify address, need using the command \"flush host\" to flush address information of all nodes")));
		}
		table_endscan(relScan);
		table_close(rel_node, AccessShareLock);
	}

	new_tuple = heap_modify_tuple(tuple, host_dsc, datum,isnull, got);
	CatalogTupleUpdate(rel, &tuple->t_self, new_tuple);
	ReleaseSysCache(tuple);
	/* at end, close relation */
	table_close(rel, RowExclusiveLock);
	PG_RETURN_BOOL(true);
}

Datum mgr_deploy_all(PG_FUNCTION_ARGS)
{
	InitDeployInfo *info = NULL;
	FuncCallContext *funcctx = NULL;
	FILE volatile *tar = NULL;
	char *str_path = NULL;
	HeapTuple tuple;
	HeapTuple out;
	Form_mgr_host host;
	char *str_addr;
	Datum datum;
	bool isnull = false;
	bool success = false;
	char *password = PG_GETARG_CSTRING(0);
	StringInfoData buf;
	NameData resnamedata;
	NameData restypedata;
	int ret;
	MemoryContext oldcontext;

	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		/*check all node stop*/
		if (!mgr_check_cluster_stop(NULL, &resnamedata, &restypedata))
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("%s \"%s\" still running, please stop it before deploy", restypedata.data, resnamedata.data)
				,errhint("try \"monitor all;\" for more information")));

		/*check the agent all stop*/
		if ((ret = SPI_connect()) < 0)
			ereport(ERROR, (errmsg("ADB Manager SPI_connect failed: error code %d", ret)));
		ret = SPI_execute("select nodename from mgr_monitor_agent_all() where  status = true limit 1;", false, 0);
		if (ret != SPI_OK_SELECT)
			ereport(ERROR, (errmsg("ADB Manager SPI_execute failed: error code %d", ret)));
		if (SPI_processed > 0 && SPI_tuptable != NULL)
		{
			namestrcpy(&resnamedata, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
			SPI_freetuptable(SPI_tuptable);
			SPI_finish();
			ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("on host \"%s\" the agent still running, please stop it before deploy", resnamedata.data)
				,errhint("try \"monitor agent all;\" for more information")));
		}
		SPI_freetuptable(SPI_tuptable);
		SPI_finish();

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc0(sizeof(*info));
		info->rel_host = table_open(HostRelationId, AccessShareLock);

		info->rel_scan = table_beginscan_catalog(info->rel_host, 0, NULL);

		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	tuple = heap_getnext(info->rel_scan,ForwardScanDirection);
	if (tuple == NULL)
	{
		table_endscan(info->rel_scan);
		table_close(info->rel_host, AccessShareLock);
		pfree(info);
		if(tar)
			fclose((FILE*)tar);

		SRF_RETURN_DONE(funcctx);
	}

	host = (Form_mgr_host)GETSTRUCT(tuple);
	if(tar == NULL)
		tar = make_tar_package();

	resetStringInfo(&buf);
	success = deploy_to_host((FILE*)tar, RelationGetDescr(info->rel_host), tuple, &buf, password);

	datum = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(info->rel_host), &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	}
	str_addr = TextDatumGetCString(datum);

	datum = heap_getattr(tuple, Anum_mgr_host_hostadbhome, RelationGetDescr(info->rel_host), &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column _hostadbhome is null")));
	}
	str_path = TextDatumGetCString(datum);

	if (success)
	{
		char adbhome[MAXPGPATH];
		get_adbhome(adbhome);

		if (host_is_localhost(str_addr) && (strcmp(adbhome, str_path) == 0))
		{
			resetStringInfo(&buf);
			appendStringInfoString(&buf, "skip localhost");
		}
		else
		{
			resetStringInfo(&buf);
			appendStringInfo(&buf, "success");
		}
	}

	out = build_common_command_tuple(&host->hostname, success, buf.data);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(out));
}

Datum mgr_deploy_hostnamelist(PG_FUNCTION_ARGS)
{
	InitDeployInfo *info = NULL;
	FuncCallContext *funcctx = NULL;
	ListCell *lc = NULL;
	FILE volatile *tar = NULL;
	HeapTuple out;
	Value *hostname;
	bool success = false;
	bool isnull = false;
	HeapTuple tuple;
	char *password = PG_GETARG_CSTRING(0);
	char *str_addr;
	char *str_path = NULL;
	List *host_list = NIL;
	NameData name;
	Datum datum;
	StringInfoData buf;
	bool res = true;
	MemoryContext oldcontext;

	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		host_list = DecodeTextArrayToValueList(PG_GETARG_DATUM(1));
		check_host_name_isvaild(host_list);
		foreach(lc, host_list)
		{
			hostname = (Value *)lfirst(lc);
			res = mgr_check_can_deploy(strVal(hostname));
			if(!res)
				ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE)
					,errmsg("stop agent and all of nodes on \"%s\" before deploy", strVal(hostname))
					,errhint("try \"monitor all;\" for more information")));
		}

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc(sizeof(*info));
		info->rel_host = table_open(HostRelationId, AccessShareLock);
		info->rel_scan = table_beginscan_catalog(info->rel_host, 0, NULL);
		info->index = 0;

		info->host_list = DecodeTextArrayToValueList(PG_GETARG_DATUM(1));
		check_host_name_isvaild(info->host_list);
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	if (info->index >= list_length(info->host_list))
	{
		table_endscan(info->rel_scan);
		table_close(info->rel_host, AccessShareLock);
		pfree(info);
		if(tar)
			fclose((FILE*)tar);
		SRF_RETURN_DONE(funcctx);
	}

	resetStringInfo(&buf);

	hostname = (Value *)list_nth(info->host_list, info->index);
	++(info->index);

	namestrcpy(&name, strVal(hostname));
	tuple = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	if (tuple == NULL)
	{
		/* end of row */
		table_endscan(info->rel_scan);
		table_close(info->rel_host, AccessShareLock);
		pfree(info);
		ReleaseSysCache(tuple);
		SRF_RETURN_DONE(funcctx);
	}

	if(tar == NULL)
		tar = make_tar_package();
	success = deploy_to_host((FILE*)tar, RelationGetDescr(info->rel_host), tuple, &buf, password);

	datum = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(info->rel_host), &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	}
	str_addr = TextDatumGetCString(datum);

	datum = heap_getattr(tuple, Anum_mgr_host_hostadbhome, RelationGetDescr(info->rel_host), &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column _hostadbhome is null")));
	}
	str_path = TextDatumGetCString(datum);

	if (success)
	{
		char adbhome[MAXPGPATH];
		get_adbhome(adbhome);

		if (host_is_localhost(str_addr) && (strcmp(adbhome, str_path) == 0))
		{
			resetStringInfo(&buf);
			appendStringInfoString(&buf, "skip localhost");
		}
		else
		{
			resetStringInfo(&buf);
			appendStringInfo(&buf, "success");
		}
	}

	out = build_common_command_tuple(&name, success, buf.data);

	ReleaseSysCache(tuple);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(out));
}

static FILE* make_tar_package(void)
{
	FILE volatile *fd;
	DIR volatile *dir;
	struct dirent *item;
	char adbhome[MAXPGPATH];

	fd = NULL;
	dir = NULL;
	PG_TRY();
	{
		/* create an temp file */
		fd = tmpfile();

		/* get package directory */
		get_adbhome(adbhome);

		/* enum dirent */
		dir = opendir(adbhome);
		if(dir == NULL)
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("Can not open directory \"%s\" for read", adbhome)));
		}
		while((item = readdir((DIR*)dir)) != NULL)
		{
			if(strcmp(item->d_name, "..") != 0
				&& strcmp(item->d_name, ".") != 0)
			{
				append_file_to_tar((FILE*)fd, adbhome, item->d_name);
			}
		}
	}PG_CATCH();
	{
		fclose((FILE*)fd);
		if(dir != NULL)
			closedir((DIR*)dir);
		PG_RE_THROW();
	}PG_END_TRY();

	closedir((DIR*)dir);
	return (FILE*)fd;
}

static void append_file_to_tar(FILE *tar, const char *path, const char *name)
{
	DIR volatile* dir;
	FILE volatile *fd;
	struct dirent *item;
	StringInfoData buf;
	StringInfoData full;
	struct stat st;
	char head[512];
	ssize_t ret;
	AssertArg(tar && path);

	dir = NULL;
	fd = NULL;
	initStringInfo(&buf);
	initStringInfo(&full);
	appendStringInfo(&full, "%s/%s", path, name);
	PG_TRY();
	{
		ret = lstat(full.data, &st);
		if(ret != 0)
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("Can not lstat \"%s\":%m", full.data)));
		}
		if(S_ISLNK(st.st_mode))
		{
			for(;;)
			{
				ret = readlink(full.data, buf.data, buf.maxlen-1);
				if(ret == buf.maxlen-1)
				{
					enlargeStringInfo(&buf, buf.maxlen + 1024);
				}else
				{
					break;
				}
			}
			if(ret < 0)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not readlink \"%s\":%m", full.data)));
			}
			Assert(ret < buf.maxlen);
			buf.len = ret;
			buf.data[buf.len] = '\0';
		}
		tarCreateHeader(head, name, S_ISLNK(st.st_mode) ? buf.data : NULL
			, S_ISREG(st.st_mode) ? st.st_size : 0, st.st_mode
			, st.st_uid, st.st_gid, st.st_mtime);
		ret = fwrite(head, 1, sizeof(head), tar);
		if(ret != sizeof(head))
		{
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("Can not append data to tar file:%m")));
		}
		if(S_ISREG(st.st_mode))
		{
			size_t cnt;
			size_t pad;
			fd = fopen(full.data, "rb");
			if(fd == NULL)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not open file \"%s\" for read:%m", full.data)));
			}
			cnt = 0;
			enlargeStringInfo(&buf, 32*1024);
			while((ret = fread(buf.data, 1, buf.maxlen, (FILE*)fd)) > 0)
			{
				if(fwrite(buf.data, 1, ret, tar) != ret)
				{
					ereport(ERROR, (errcode_for_file_access(),
						errmsg("Can not append data to tar file:%m")));
				}
				cnt += ret;
			}
			if(ret < 0)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not read file \"%s\":%m", full.data)));
			}else if(cnt != st.st_size)
			{
				ereport(ERROR, (errmsg("file size changed when reading")));
			}
			pad = ((cnt + 511) & ~511) - cnt;
			enlargeStringInfo(&buf, pad);
			memset(buf.data, 0, pad);
			if(fwrite(buf.data, 1, pad, tar) != pad)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not append data to tar file:%m")));
			}
		}else if(S_ISDIR(st.st_mode))
		{
			dir = opendir(full.data);
			if(dir == NULL)
			{
				ereport(ERROR, (errcode_for_file_access(),
					errmsg("Can not open directory \"%s\" for read", full.data)));
			}
			while((item = readdir((DIR*)dir)) != NULL)
			{
				if(strcmp(item->d_name, "..") == 0
					|| strcmp(item->d_name, ".") == 0)
				{
					continue;
				}
				resetStringInfo(&buf);
				appendStringInfo(&buf, "%s/%s", name, item->d_name);
				append_file_to_tar(tar, path, buf.data);
			}
		}
	}PG_CATCH();
	{
		if(dir != NULL)
			closedir((DIR*)dir);
		if(fd != NULL)
			fclose((FILE*)fd);
		PG_RE_THROW();
	}PG_END_TRY();
	if(dir != NULL)
		closedir((DIR*)dir);
	if(fd != NULL)
		fclose((FILE*)fd);
	pfree(buf.data);
	pfree(full.data);
}

static bool host_is_localhost(const char *name)
{
	struct ifaddrs *ifaddr, *ifa;
	struct addrinfo *addr;
	struct addrinfo *addrs;
	struct addrinfo hint;
	static const char tmp_port[3] = {"22"};
	bool is_localhost;

	if(getifaddrs(&ifaddr) == -1)
		ereport(ERROR, (errmsg("getifaddrs failed:%m")));
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;
	hint.ai_flags = AI_PASSIVE;
	if(pg_getaddrinfo_all(name, tmp_port, &hint, &addrs) != 0)
	{
		freeifaddrs(ifaddr);
		ereport(ERROR, (errmsg("could not resolve \"%s\"", name)));
	}

	is_localhost = false;
	for(ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next)
	{
		if(ifa->ifa_addr == NULL)
			continue;
		if(ifa->ifa_addr->sa_family != AF_INET
#ifdef HAVE_IPV6
			&& ifa->ifa_addr->sa_family != AF_INET6
#endif /* HAVE_IPV6 */
			)
		{
			continue;
		}

		for(addr = addrs; addr != NULL; addr = addr->ai_next)
		{
			if(ifa->ifa_addr->sa_family != addr->ai_family)
				continue;
			if(addr->ai_family == AF_INET)
			{
				struct sockaddr_in *l = (struct sockaddr_in*)ifa->ifa_addr;
				struct sockaddr_in *r = (struct sockaddr_in*)addr->ai_addr;
				if(memcmp(&(l->sin_addr) , &(r->sin_addr), sizeof(l->sin_addr)) == 0)
				{
					is_localhost = true;
					break;
				}
			}
#ifdef HAVE_IPV6
			else if(addr->ai_family == AF_INET6)
			{
				struct sockaddr_in6 *l = (struct sockaddr_in6*)ifa->ifa_addr;
				struct sockaddr_in6 *r = (struct sockaddr_in6*)addr->ai_addr;
				if(memcmp(&(l->sin6_addr), &(r->sin6_addr), sizeof(l->sin6_addr)) == 0)
				{
					is_localhost = true;
					break;
				}
			}
#endif /* HAVE_IPV6 */
		}
		if(is_localhost)
			break;
	}

	pg_freeaddrinfo_all(AF_UNSPEC, addrs);
	freeifaddrs(ifaddr);

	return is_localhost;
}

static bool deploy_to_host(FILE *tar, TupleDesc desc, HeapTuple tup, StringInfo msg, const char *password)
{
	Form_mgr_host host;
	Datum datum;
	char *str_path;
	char *str_addr;
	bool isnull;
	AssertArg(tar && desc && tup && msg);

	host = (Form_mgr_host)GETSTRUCT(tup);
	datum = heap_getattr(tup, Anum_mgr_host_hostaddr, desc, &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column hostaddr is null")));
	}
	str_addr = TextDatumGetCString(datum);
	datum = heap_getattr(tup, Anum_mgr_host_hostadbhome, desc, &isnull);
	if(isnull)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
			, errmsg("column _hostadbhome is null")));
	}
	str_path = TextDatumGetCString(datum);

	if(host_is_localhost(str_addr))
	{
		char adbhome[MAXPGPATH];
		get_adbhome(adbhome);
		if(strcmp(adbhome, str_path) == 0)
		{
			appendStringInfoString(msg, "skip localhost");
			return true;
		}
	}

	if(host->hostproto != HOST_PROTOCOL_SSH)
	{
		appendStringInfoString(msg, "deplory support ssh only for now");
		return false;
	}

	if(password == NULL)
		password = "";

	return ssh2_deplory_tar(str_addr, host->hostport
			, NameStr(host->hostuser), password, str_path
			, tar, msg);
}

static void get_adbhome(char *adbhome)
{
	if(my_exec_path[0] == '\0')
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			,errmsg("can not get the adbhome path")));
	}
	strcpy(adbhome, my_exec_path);
	get_parent_directory(adbhome);
	get_parent_directory(adbhome);
}

/*
* command format: start agent host1 [, ...] password xxx;
*/
Datum mgr_start_agent_hostnamelist(PG_FUNCTION_ARGS)
{
	InitNodeInfo *info;
	HeapTuple tup;
	ManagerAgent *ma;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	int ret;
	NameData name;
	StringInfoData message;
	StringInfoData exec_path;
	Datum datumpath;
	char		*password;
	Value *hostname;
	char *host_addr;
	bool isNull = true;
	FuncCallContext *funcctx;
	Datum datum_hostname_list;

	Assert(PG_NARGS() == 2);
	password = PG_GETARG_CSTRING(0);
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc(sizeof(*info));
		info->rel_node = table_open(HostRelationId, AccessShareLock);
		info->rel_scan = table_beginscan_catalog(info->rel_node, 0, NULL);

		datum_hostname_list = PG_GETARG_DATUM(1);
		info->node_list = DecodeTextArrayToValueList(datum_hostname_list);
		check_host_name_isvaild(info->node_list);

		info->index = 0;
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;
	Assert(info);
	if (info->index >= list_length(info->node_list))
	{
		table_endscan(info->rel_scan);
		table_close(info->rel_node, AccessShareLock);
		SRF_RETURN_DONE(funcctx);
	}
	hostname = (Value *) list_nth(info->node_list, info->index);
	++(info->index);
	namestrcpy(&name, strVal(hostname));
	initStringInfo(&message);
	initStringInfo(&exec_path);
	tup = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	if (HeapTupleIsValid(tup))
	{
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		updateAllowcureOfMgrHost(NameStr(mgr_host->hostname), true);

		ma = ma_connect_hostoid(((Form_mgr_host)GETSTRUCT(tup))->oid);
		if (ma_isconnected(ma))
		{
			appendStringInfoString(&message, "success");
			ret = 0;
		}
		else
		{
			/* get exec path */
			datumpath = heap_getattr(tup, Anum_mgr_host_hostadbhome, RelationGetDescr(info->rel_node), &isNull);
			if(isNull)
			{
				ReleaseSysCache(tup);
				ma_close(ma);
				pfree(message.data);
				pfree(exec_path.data);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
					, errmsg("column hostadbhome is null")));
			}
			appendStringInfoString(&exec_path, TextDatumGetCString(datumpath));
			if(exec_path.data[exec_path.len] != '/')
				appendStringInfoChar(&exec_path, '/');
			appendStringInfoString(&exec_path, "bin/agent");

			/* append argument */
			appendStringInfo(&exec_path, " -b -P %u", mgr_host->hostagentport);

			/* get host address */
			datumpath = heap_getattr(tup, Anum_mgr_host_hostaddr, RelationGetDescr(info->rel_node), &isNull);
			if(isNull)
				host_addr = NameStr(mgr_host->hostname);
			else
				host_addr = TextDatumGetCString(datumpath);

			/* exec start */
			if(mgr_host->hostproto == HOST_PROTOCOL_TELNET)
			{
				appendStringInfoString(&message, _("telnet not support yet"));
				ret = 1;
			}else if(mgr_host->hostproto == HOST_PROTOCOL_SSH)
			{
				ret = ssh2_start_agent(host_addr
					, mgr_host->hostport
					, NameStr(mgr_host->hostuser)
					, password /* password for libssh2*/
					, exec_path.data
					, &message);
			}else
			{
				appendStringInfo(&message, _("unknown protocol '%d'"), mgr_host->hostproto);
				ret = 1;
			}
		}
		ReleaseSysCache(tup);
		ma_close(ma);
	}else
	{
		appendStringInfoString(&message, "host does not exist");
		ret = 1;
	}
	tup_result = build_common_command_tuple(&name, ret == 0 ? true:false, message.data);
	pfree(message.data);
	pfree(exec_path.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}
/*
* command format:  start agent all password xxx;
*/
Datum mgr_start_agent_all(PG_FUNCTION_ARGS)
{
	InitNodeInfo *info;
	HeapTuple tup;
	ManagerAgent *ma;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	int ret;
	NameData name;
	StringInfoData message;
	StringInfoData exec_path;
	Datum datumpath;
	char *password;
	char *hostname;
	char *host_addr;
	bool isNull = true;
	FuncCallContext *funcctx;

	Assert(PG_NARGS() == 1);
	password = PG_GETARG_CSTRING(0);
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc0(sizeof(*info));
		info->rel_node = table_open(HostRelationId, AccessShareLock);
		info->rel_scan = table_beginscan_catalog(info->rel_node, 0, NULL);
		/*get host list*/
		while ((tup = heap_getnext(info->rel_scan, ForwardScanDirection)) != NULL)
		{
			mgr_host = (Form_mgr_host)GETSTRUCT(tup);
			Assert(mgr_host);
			info->node_list = lappend(info->node_list, mgr_host->hostname.data);
		}
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	info = funcctx->user_fctx;
	Assert(info);
	if (info->index >= list_length(info->node_list))
	{
		table_endscan(info->rel_scan);
		table_close(info->rel_node, AccessShareLock);
		SRF_RETURN_DONE(funcctx);
	}
	hostname = (char *) list_nth(info->node_list, info->index);
	(info->index)++;
	namestrcpy(&name, hostname);
	initStringInfo(&message);
	initStringInfo(&exec_path);
	tup = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	if (HeapTupleIsValid(tup))
	{
		mgr_host = (Form_mgr_host)GETSTRUCT(tup);
		Assert(mgr_host);
		updateAllowcureOfMgrHost(NameStr(mgr_host->hostname), true);

		ma = ma_connect_hostoid(((Form_mgr_host)GETSTRUCT(tup))->oid);
		if (ma_isconnected(ma))
		{
			appendStringInfoString(&message, "success");
			ret = 0;
		}
		else
		{
			/* get exec path */
			datumpath = heap_getattr(tup, Anum_mgr_host_hostadbhome, RelationGetDescr(info->rel_node), &isNull);
			if(isNull)
			{
				ReleaseSysCache(tup);
				ma_close(ma);
				pfree(message.data);
				pfree(exec_path.data);
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
					, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
					, errmsg("column hostadbhome is null")));
			}
			appendStringInfo(&exec_path
				, "export LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH;", TextDatumGetCString(datumpath));
			appendStringInfoString(&exec_path, TextDatumGetCString(datumpath));
			if(exec_path.data[exec_path.len] != '/')
				appendStringInfoChar(&exec_path, '/');
			appendStringInfoString(&exec_path, "bin/agent");

			/* append argument */
			appendStringInfo(&exec_path, " -b -P %u", mgr_host->hostagentport);

			/* get host address */
			datumpath = heap_getattr(tup, Anum_mgr_host_hostaddr, RelationGetDescr(info->rel_node), &isNull);
			if(isNull)
				host_addr = NameStr(mgr_host->hostname);
			else
				host_addr = TextDatumGetCString(datumpath);

			/* exec start */
			if(mgr_host->hostproto == HOST_PROTOCOL_TELNET)
			{
				appendStringInfoString(&message, _("telnet not support yet"));
				ret = 1;
			}else if(mgr_host->hostproto == HOST_PROTOCOL_SSH)
			{
				ret = ssh2_start_agent(host_addr
					, mgr_host->hostport
					, NameStr(mgr_host->hostuser)
					, password /* password for libssh2*/
					, exec_path.data
					, &message);
			}else
			{
				appendStringInfo(&message, _("unknown protocol '%d'"), mgr_host->hostproto);
				ret = 1;
			}
		}
		ReleaseSysCache(tup);
		ma_close(ma);
	}else
	{
		appendStringInfoString(&message, "host does not exist");
		ret = 1;
	}
	tup_result = build_common_command_tuple(&name, ret == 0 ? true:false, message.data);
	pfree(message.data);
	pfree(exec_path.data);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

/*
* GetAgentCmdRst.ret == 0, means success,
* otherwise, see error message in GetAgentCmdRst.description.
* The return result is palloced, so pfree it if it useless.
*/
GetAgentCmdRst *mgr_start_agent_execute(Form_mgr_host mgr_host,char* hostaddr,char *hostadbhome, char *password)
{
	int ret;
	char *host_addr;
	GetAgentCmdRst *cmdRst;
	StringInfoData exec_path;

	cmdRst = palloc0(sizeof(GetAgentCmdRst));
	initStringInfo(&cmdRst->description);
	/* get exec path */
	if(hostadbhome == NULL || strlen(hostadbhome) ==0)
	{
		cmdRst->ret = -1;
		appendStringInfoString(&(cmdRst->description), "hostadbhome can be empty");
		return cmdRst;
	}

	initStringInfo(&exec_path);

	appendStringInfo(&exec_path
		, "export LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH;", hostadbhome);
	appendStringInfoString(&exec_path, hostadbhome);
	if(exec_path.data[exec_path.len] != '/')
		appendStringInfoChar(&exec_path, '/');
	appendStringInfoString(&exec_path, "bin/agent");

	/* append argument */
	appendStringInfo(&exec_path, " -b -P %u", mgr_host->hostagentport);

	/* get host address */
	if(hostaddr == NULL || strlen(hostaddr) ==0)
		host_addr = NameStr(mgr_host->hostname);
	else
		host_addr = hostaddr;

	/* exec start */
	if(mgr_host->hostproto == HOST_PROTOCOL_TELNET)
	{
		appendStringInfoString(&cmdRst->description, _("telnet not support yet"));
		ret = 1;
	}
	else if(mgr_host->hostproto == HOST_PROTOCOL_SSH)
	{
		ret = ssh2_start_agent(host_addr,
			mgr_host->hostport,
			NameStr(mgr_host->hostuser),
			password, /* password for libssh2*/
			exec_path.data,
			&cmdRst->description);
	}
	else
	{
		appendStringInfo(&cmdRst->description, _("unknown protocol '%d'"), mgr_host->hostproto);
		ret = 1;
	}
	pfree(exec_path.data);
	cmdRst->ret = ret;
	return cmdRst;
}

Datum mgr_stop_agent_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	StopAgentInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData message;
	char cmdtype = AGT_CMD_STOP_AGENT;
	int retry = 0;
	const int retrymax = 10;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc0(sizeof(*info));
		info->rel_host = table_open(HostRelationId, AccessShareLock);
		info->rel_scan = table_beginscan_catalog(info->rel_host, 0, NULL);

		/* save info */
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	tup = heap_getnext(info->rel_scan, ForwardScanDirection);
	if(tup == NULL)
	{
		/* end of row */
		table_endscan(info->rel_scan);
		table_close(info->rel_host, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_host = (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	updateAllowcureOfMgrHost(NameStr(mgr_host->hostname), false);

	/* test is running ? */
	ma = ma_connect_hostoid(mgr_host->oid);
	if(!ma_isconnected(ma))
	{
		tup_result = build_common_command_tuple(&(mgr_host->hostname), true, _("success"));
		ma_close(ma);
	}else
	{
		initStringInfo(&message);
		initStringInfo(&(getAgentCmdRst.description));

		/*send cmd*/
		ma_beginmessage(&message, AGT_MSG_COMMAND);
		ma_sendbyte(&message, cmdtype);
		ma_sendstring(&message, "stop agent");
		ma_endmessage(&message, ma);
		if (!ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);
			tup_result = build_common_command_tuple(&(mgr_host->hostname)
			, getAgentCmdRst.ret, getAgentCmdRst.description.data);
		}
		else
		{
			mgr_recv_msg(ma, &getAgentCmdRst);
			ma_close(ma);

			/*check stop agent result*/
			retry = 0;
			while (retry++ < retrymax)
			{
				/*sleep 0.2s, wait the agent process to be killed, max try retrymax times*/
				usleep(200000);
				ma = ma_connect_hostoid(mgr_host->oid);
				if(!ma_isconnected(ma))
				{
					getAgentCmdRst.ret = 1;
					resetStringInfo(&(getAgentCmdRst.description));
					appendStringInfoString(&(getAgentCmdRst.description), run_success);
					ma_close(ma);
					break;
				}
				else
				{
					getAgentCmdRst.ret = 0;
					resetStringInfo(&(getAgentCmdRst.description));
					appendStringInfoString(&(getAgentCmdRst.description), "stop agent fail");
					ma_close(ma);
				}
			}

			tup_result = build_common_command_tuple(
				&(mgr_host->hostname)
				, getAgentCmdRst.ret == 0 ? false:true
				, getAgentCmdRst.description.data);

			if(getAgentCmdRst.description.data)
				pfree(getAgentCmdRst.description.data);
		}
	}

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

Datum mgr_stop_agent_hostnamelist(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	StopAgentInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	ManagerAgent *ma;
	GetAgentCmdRst getAgentCmdRst;
	StringInfoData message;
	char cmdtype = AGT_CMD_STOP_AGENT;
	int retry = 0;
	const int retrymax = 10;
	Value *hostname;
	NameData name;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc0(sizeof(*info));

		info->host_list = DecodeTextArrayToValueList(PG_GETARG_DATUM(0));
		check_host_name_isvaild(info->host_list);

		info->index = 0;

		/* save info */
		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	if (info->index >= list_length(info->host_list))
	{
		MgrFree(info);
		SRF_RETURN_DONE(funcctx);
	}

	hostname = (Value *)list_nth(info->host_list, info->index);
	++(info->index);
	namestrcpy(&name, strVal(hostname));

	tup = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	if(tup == NULL)
	{
		/* end of row */
		MgrFree(info);
		ereportWarningLog(errmsg("host name \"%s\" does not exist", NameStr(name)));
		tup_result = build_common_command_tuple(&name, true, _("fail"));
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
	}

	mgr_host = (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);
	updateAllowcureOfMgrHost(NameStr(mgr_host->hostname), false);
	/* test is running ? */
	ma = ma_connect_hostoid(mgr_host->oid);
	if(!ma_isconnected(ma))
	{
		tup_result = build_common_command_tuple(&(mgr_host->hostname), true, _("success"));
		ReleaseSysCache(tup);
		ma_close(ma);
	}else
	{
		initStringInfo(&message);
		initStringInfo(&(getAgentCmdRst.description));

		/*send cmd*/
		ma_beginmessage(&message, AGT_MSG_COMMAND);
		ma_sendbyte(&message, cmdtype);
		ma_sendstring(&message, "stop agent");
		ma_endmessage(&message, ma);
		if (!ma_flush(ma, true))
		{
			getAgentCmdRst.ret = false;
			appendStringInfoString(&(getAgentCmdRst.description), ma_last_error_msg(ma));
			ma_close(ma);
			tup_result = build_common_command_tuple(&(mgr_host->hostname)
													, getAgentCmdRst.ret,
													getAgentCmdRst.description.data);
			ReleaseSysCache(tup);
		}
		else
		{
			mgr_recv_msg(ma, &getAgentCmdRst);
			ma_close(ma);

			/*check stop agent result*/
			retry = 0;
			while (retry++ < retrymax)
			{
				/*sleep 0.2s, wait the agent process to be killed, max try retrymax times*/
				usleep(200000);
				ma = ma_connect_hostoid(mgr_host->oid);
				if(!ma_isconnected(ma))
				{
					getAgentCmdRst.ret = 1;
					resetStringInfo(&(getAgentCmdRst.description));
					appendStringInfoString(&(getAgentCmdRst.description), run_success);
					ma_close(ma);
					break;
				}
				else
				{
					getAgentCmdRst.ret = 0;
					resetStringInfo(&(getAgentCmdRst.description));
					appendStringInfoString(&(getAgentCmdRst.description), "stop agent fail");
					ma_close(ma);
				}
			}

			tup_result = build_common_command_tuple(&(mgr_host->hostname)
												, getAgentCmdRst.ret == 0 ? false:true
												, getAgentCmdRst.description.data);

			ReleaseSysCache(tup);

			if(getAgentCmdRst.description.data)
				pfree(getAgentCmdRst.description.data);
		}
	}

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

Datum mgr_monitor_agent_all(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	InitHostInfo *info;
	HeapTuple tup;
	HeapTuple tup_result;
	Form_mgr_host mgr_host;
	bool success = false;
	ManagerAgent *ma;
	StringInfoData buf;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		info = palloc0(sizeof(*info));
		info->rel_host = table_open(HostRelationId, AccessShareLock);

		info->rel_scan = table_beginscan_catalog(info->rel_host, 0, NULL);

		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);

	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	tup = heap_getnext(info->rel_scan,ForwardScanDirection);
	if (tup == NULL)
	{
		table_endscan(info->rel_scan);
		table_close(info->rel_host, AccessShareLock);
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	mgr_host = (Form_mgr_host)GETSTRUCT(tup);
	Assert(mgr_host);

	initStringInfo(&buf);
	resetStringInfo(&buf);

	/* test is running ? */
	ma = ma_connect_hostoid(mgr_host->oid);
	if(ma_isconnected(ma))
	{
		success = true;
		appendStringInfoString(&buf, "running");
	}
	else
	{
		success = false;
		appendStringInfoString(&buf, "not running");
	}

	tup_result = build_common_command_tuple(&(mgr_host->hostname), success, buf.data);

	ma_close(ma);
	pfree(buf.data);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

Datum mgr_monitor_agent_hostlist(PG_FUNCTION_ARGS)
{
	FuncCallContext * funcctx;
	HeapTuple tup,tup_result;
	InitHostInfo *info;
	Value *hostname;
	ManagerAgent *ma;
	bool success = false;
	NameData name;
	StringInfoData buf;
	initStringInfo(&buf);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		info = palloc0(sizeof(*info));

		info->host_list = DecodeTextArrayToValueList(PG_GETARG_DATUM(0));
		check_host_name_isvaild(info->host_list);

		funcctx->user_fctx = info;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	Assert(funcctx);
	info = funcctx->user_fctx;
	Assert(info);

	if (info->index >= list_length(info->host_list))
	{
		pfree(info);
		SRF_RETURN_DONE(funcctx);
	}

	hostname = (Value *)list_nth(info->host_list, info->index);
	++(info->index);
	namestrcpy(&name, strVal(hostname));

	tup = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));
	resetStringInfo(&buf);

	if (HeapTupleIsValid(tup))
	{
		/* test is running ? */
		ma = ma_connect_hostoid(((Form_mgr_host)GETSTRUCT(tup))->oid);
		if(ma_isconnected(ma))
		{
			success = true;
			appendStringInfoString(&buf, "running");
			ReleaseSysCache(tup);
			ma_close(ma);
		}
		else
		{
			success = false;
			appendStringInfoString(&buf, "not running");
			ReleaseSysCache(tup);
			ma_close(ma);
		}
	}

	tup_result = build_common_command_tuple(&name, success, buf.data);
	pfree(buf.data);

	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tup_result));
}

static void check_host_name_isvaild(List *host_name_list)
{
	ListCell *lc = NULL;
	InitNodeInfo *info;
	Value *value;
	NameData name;
	HeapTuple tup;
	TupleDesc host_desc;

	info = palloc(sizeof(*info));
	info->rel_node = table_open(HostRelationId, AccessShareLock);
	host_desc = CreateTupleDescCopy(RelationGetDescr(info->rel_node));
	table_close(info->rel_node, AccessShareLock);

	foreach(lc, host_name_list)
	{
		value = lfirst(lc);
		Assert(value && IsA(value, String));
		namestrcpy(&name, strVal(value));
		tup = SearchSysCache1(HOSTHOSTNAME, NameGetDatum(&name));

		if (!HeapTupleIsValid(tup))
		{
			ereport(ERROR, (errmsg("host name \"%s\" does not exist", NameStr(name))));
		}

		ReleaseSysCache(tup);
	}

	FreeTupleDesc(host_desc);
	return;
}

/*
* check all node stop in cluster
*/
bool mgr_check_cluster_stop(char *zone, Name nodename, Name nodetypestr)
{
	Relation rel;
	TableScanDesc rel_scan;
	HeapTuple tuple;
	Form_mgr_node mgr_node;
	char *ip_addr;
	int port;
	ScanKeyData 	key[1];

	if (zone != NULL)
		ScanKeyInit(&key[0]
				,Anum_mgr_node_nodezone
				,BTEqualStrategyNumber
				,F_NAMEEQ
				,CStringGetDatum(zone));

	/*check all node stop*/
	rel = table_open(NodeRelationId, AccessShareLock);
	if (zone != NULL)
		rel_scan = table_beginscan_catalog(rel, 1, key);
	else
		rel_scan = table_beginscan_catalog(rel, 0, key);
	
	while((tuple = heap_getnext(rel_scan, ForwardScanDirection))!= NULL)
	{
		mgr_node = (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		ip_addr = get_hostaddress_from_hostoid(mgr_node->nodehost);
		port = mgr_node->nodeport;
		if(check_node_running_by_socket(ip_addr, port))
		{
			get_node_type_str(mgr_node->nodetype, nodetypestr);
			strcpy(nodename->data, mgr_node->nodename.data);
			return false;
		}
	}
	table_endscan(rel_scan);
	table_close(rel, AccessShareLock);
	return true;
}
bool get_node_type_str(int node_type, Name node_type_str)
{
	bool ret = true;
	Assert(node_type_str);
	switch(node_type)
    {
        case CNDN_TYPE_GTM_COOR_MASTER:
			strcpy(NameStr(*node_type_str), "gtm master");
			break;
        case CNDN_TYPE_GTM_COOR_SLAVE:
			strcpy(NameStr(*node_type_str), "gtm slave");
			break;
        case CNDN_TYPE_COORDINATOR_MASTER:
			strcpy(NameStr(*node_type_str), "coordinator master");
			break;
        case CNDN_TYPE_COORDINATOR_SLAVE:
			strcpy(NameStr(*node_type_str), "coordinator slave");
			break;
        case CNDN_TYPE_DATANODE_MASTER:
			strcpy(NameStr(*node_type_str), "datanode master");
			break;
        case CNDN_TYPE_DATANODE_SLAVE:
			strcpy(NameStr(*node_type_str), "datanode slave");
			break;
        default:
			strcpy(NameStr(*node_type_str), "unknown type");
			ret = false;
			break;
    }
	return ret;
}
bool check_node_running_by_socket(char *host, int port)
{
	return port_occupancy_test(host, port);
}

bool port_occupancy_test(const char *ip_address, const int port)
{
	int ret = 0;
	struct sockaddr_in serv_addr;
	int fd = socket(AF_INET, SOCK_STREAM, 0);

	if (fd == -1)
	{
		ereport(ERROR, (errmsg("on ADB manager create sock fail")));
	}
	/*init*/
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);
	serv_addr.sin_addr.s_addr = inet_addr(ip_address);

	/*connect*/
	ret = connect(fd, &serv_addr, sizeof(struct sockaddr));
	close(fd);
	if (ret == -1)
	{
		return false;
	}
	return true;
}

/*
* check the address in host table not repeate
*/
static bool mgr_check_address_repeate(char *address)
{
	HeapTuple tuple;
	Form_mgr_host mgr_host PG_USED_FOR_ASSERTS_ONLY;
	Datum addressDatum;
	Relation relHost;
	TableScanDesc relScan;
	bool isNull = false;
	bool rest = false;
	char *addr;

	Assert(address);

	relHost = table_open(HostRelationId, AccessShareLock);
	relScan = table_beginscan_catalog(relHost, 0, NULL);
	while((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		mgr_host = (Form_mgr_host)GETSTRUCT(tuple);
		Assert(mgr_host);
		addressDatum = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(relHost), &isNull);
		if(isNull)
		{
			rest = true;
			break;
		}
		addr = TextDatumGetCString(addressDatum);
		Assert(addr);
		if (strcmp(addr, address) == 0)
		{
			rest = true;
			break;
		}

	}
	table_endscan(relScan);
	table_close(relHost, AccessShareLock);

	return rest;
}

static bool mgr_check_can_deploy(char *hostname)
{
	Relation rel;
	TableScanDesc scan;
	Datum host_addr;
	HeapTuple tuple;
	Form_mgr_host mgr_host;
	Form_mgr_node mgr_node;
	ScanKeyData key[1];
	char *address = NULL;
	char *nodetypestr;
	int agentPort = 0;
	bool isNull;
	bool rest = true;
	bool bget = false;
	bool res = true;
	Oid hostOid;

	Assert(hostname);
	rel = table_open(HostRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mgr_host= (Form_mgr_host)GETSTRUCT(tuple);
		Assert(mgr_host);

		host_addr = heap_getattr(tuple, Anum_mgr_host_hostaddr, RelationGetDescr(rel), &isNull);

		if(isNull)
		{
			table_endscan(scan);
			table_close(rel, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
				, err_generic_string(PG_DIAG_TABLE_NAME, "mgr_host")
				, errmsg("column hostaddr is null")));
		}
		if (strcasecmp(NameStr(mgr_host->hostname), hostname) == 0)
		{
			bget = true;
			hostOid = ((Form_mgr_host)GETSTRUCT(tuple))->oid;
			agentPort = mgr_host->hostagentport;
			address = pstrdup(TextDatumGetCString(host_addr));
			Assert(address);
			break;
		}
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	/*check agent*/
	if (!bget)
		return true;

	res = port_occupancy_test(address, agentPort);
	if(res)
	{
		ereport(WARNING, (errcode(ERRCODE_OBJECT_IN_USE)
			,errmsg("on address \"%s\" the agent is running", address)));
		pfree(address);
		return false;
	}

	/*check the node*/
	ScanKeyInit(&key[0],
		Anum_mgr_node_nodehost
		,BTEqualStrategyNumber
		,F_OIDEQ
		,ObjectIdGetDatum(hostOid));
	rel = table_open(NodeRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 1, key);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mgr_node= (Form_mgr_node)GETSTRUCT(tuple);
		Assert(mgr_node);
		res = true;
		res = port_occupancy_test(address, mgr_node->nodeport);
		if(res)
		{
			rest = false;
			nodetypestr = mgr_nodetype_str(mgr_node->nodetype);
			ereport(WARNING, (errcode(ERRCODE_OBJECT_IN_USE)
				,errmsg("the %s \"%s\" is running", nodetypestr, NameStr(mgr_node->nodename))));
			pfree(nodetypestr);
			break;
		}
	}

	pfree(address);
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	return rest;
}
