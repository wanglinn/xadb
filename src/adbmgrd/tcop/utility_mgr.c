#include "postgres.h"

#include "mgr/mgr_cmds.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "parser/mgr_node.h"
#include "tcop/utility.h"


CommandTag mgr_CreateCommandTag(Node *parsetree)
{
	CommandTag tag;
	AssertArg(parsetree);

	switch(nodeTag(parsetree))
	{
	case T_MGRAddHost:
		tag = CMDTAG_MGR_ADD_HOST;
		break;
	case T_MGRDropHost:
		tag = CMDTAG_MGR_DROP_HOST;
		break;
	case T_MGRAlterHost:
		tag = CMDTAG_MGR_ALTER_HOST;
		break;
	case T_MGRAddNode:
		tag = CMDTAG_MGR_ADD_NODE;
		break;
	case T_MGRAlterNode:
		tag = CMDTAG_MGR_ALTER_NODE;
		break;
	case T_MGRDropNode:
		tag = CMDTAG_MGR_DROP_NODE;
		break;
	case T_MGRUpdateparm:
		tag = CMDTAG_MGR_SET_PARAM;
		break;
	case T_MGRUpdateparmReset:
		tag = CMDTAG_MGR_RESET_PARAM;
		break;
	case T_MGRStartAgent:
		tag = CMDTAG_MGR_START_AGENT;
		break;
	case T_MGRFlushHost:
		tag = CMDTAG_MGR_FLUSH_HOST;
		break;
	case T_MGRDoctorSet: 
		tag = CMDTAG_MGR_SET_DOCTOR;
		break;
	case T_MonitorJobitemAdd:
		tag = CMDTAG_MGR_ADD_ITEM;
		break;
	case T_MonitorJobitemAlter:
		tag = CMDTAG_MGR_ALTER_ITEM;
		break;
	case T_MonitorJobitemDrop:
		tag = CMDTAG_MGR_DROP_ITEM;
		break;
	case T_MonitorJobAdd:
		tag = CMDTAG_MGR_ADD_JOB;
		break;
	case T_MonitorJobAlter:
		tag = CMDTAG_MGR_ALTER_JOB;
		break;
	case T_MonitorJobDrop:
		tag = CMDTAG_MGR_DROP_JOB;
		break;
	case T_MgrExtensionAdd:
		tag = CMDTAG_MGR_ADD_EXTENSION;
		break;
	case T_MgrExtensionDrop:
		tag = CMDTAG_MGR_DROP_EXTENSION;
		break;
	case T_MgrRemoveNode:
		tag = CMDTAG_MGR_REMOVE_NODE;
		break;
	case T_MGRSetClusterInit:
		tag = CMDTAG_MGR_SET_CLUSTER_INIT;
		break;
	case T_MonitorDeleteData:
		tag = CMDTAG_MGR_CLEAN_MONITOR_DATA;
		break;
	case T_MGRFlushParam:
		tag = CMDTAG_MGR_FLUSH_PARAM;
		break;
	case T_MGRFlushReadonlySlave:
		tag = CMDTAG_MGR_FLUSH_READONLY_SLAVE;
		break;
	default:
		ereport(WARNING, (errmsg("unrecognized node type: %d", (int)nodeTag(parsetree))));
		tag = CMDTAG_UNKNOWN;
		break;
	}
	return tag;
}

void mgr_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									ProcessUtilityContext context, ParamListInfo params,
									QueryEnvironment *queryEnv,
									DestReceiver *dest,
									QueryCompletion *qc)
{
	Node *parsetree = pstmt->utilityStmt;
	AssertArg(parsetree);
	switch(nodeTag(parsetree))
	{
	case T_MGRAddHost:
		mgr_add_host((MGRAddHost*)parsetree, params, dest);
		break;
	case T_MGRDropHost:
		mgr_drop_host((MGRDropHost*)parsetree, params, dest);
		break;
	case T_MGRAlterHost:
		mgr_alter_host((MGRAlterHost*)parsetree, params, dest);
		break;
	case T_MGRAddNode:
		mgr_add_node((MGRAddNode*)parsetree, params, dest);
		break;
	case T_MGRAlterNode:
		mgr_alter_node((MGRAlterNode*)parsetree, params, dest);
		break;
	case T_MGRDropNode:
		mgr_drop_node((MGRDropNode*)parsetree, params, dest);
		break;
	case T_MGRUpdateparm:
		mgr_add_updateparm((MGRUpdateparm*)parsetree, params, dest);
		break;
	case T_MGRUpdateparmReset:
		mgr_reset_updateparm((MGRUpdateparmReset*)parsetree, params, dest);
		break;
	case T_MGRFlushHost:
		mgr_flushhost((MGRFlushHost*)parsetree, params, dest);
		break;
	case T_MGRDoctorSet: 
		mgr_doctor_set_param((MGRDoctorSet*)parsetree, params, dest);
		break;
	case T_MonitorJobitemAdd:
		monitor_jobitem_add((MonitorJobitemAdd*)parsetree, params, dest);
		break;
	case T_MonitorJobitemAlter:
		monitor_jobitem_alter((MonitorJobitemAlter*)parsetree, params, dest);
		break;
	case T_MonitorJobitemDrop:
		monitor_jobitem_drop((MonitorJobitemDrop*)parsetree, params, dest);
		break;
	case T_MonitorJobAdd:
		monitor_job_add((MonitorJobAdd*)parsetree, params, dest);
		break;
	case T_MonitorJobAlter:
		monitor_job_alter((MonitorJobAlter*)parsetree, params, dest);
		break;
	case T_MonitorJobDrop:
		monitor_job_drop((MonitorJobDrop*)parsetree, params, dest);
		break;
	case T_MgrExtensionAdd:
		mgr_extension((MgrExtensionAdd*)parsetree, params, dest);
		break;
	case T_MgrExtensionDrop:
		mgr_extension((MgrExtensionAdd*)parsetree, params, dest);
		break;
	case T_MgrRemoveNode:
		mgr_remove_node((MgrRemoveNode*)parsetree, params, dest);
		break;
	case T_MGRSetClusterInit:
		mgr_set_init((MGRSetClusterInit*)parsetree, params, dest);
		break;
	case T_MonitorDeleteData:
		monitor_delete_data((MonitorDeleteData*)parsetree, params, dest);
		break;
	case T_MGRFlushParam:
		mgr_flushparam((MGRFlushParam*)parsetree, params, dest);
		break;
	case T_MGRFlushReadonlySlave:
		mgr_update_cn_pgxcnode_readonlysql_slave(NULL, NULL, NULL);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR)
			,errmsg("unrecognized node type: %d", (int)nodeTag(parsetree))));
		break;
	}
}
