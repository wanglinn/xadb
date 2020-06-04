/*
 * commands of dbthreshold
 */

#include "../../interfaces/libpq/libpq-fe.h"
#include "postgres.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mgr_host.h"
#include "catalog/mgr_node.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "mgr/mgr_cmds.h"
#include "mgr/mgr_agent.h"
#include "mgr/mgr_msg_type.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/mgr_node.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "funcapi.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "access/xact.h"
#include "utils/date.h"

char *mgr_zone;

/*get timestamptz from given node */
char *monitor_get_timestamptz_onenode(int agentport, char *user, char *address, int port)
{
	char *oneNodeValueStr = NULL;
	char *sqlstr = "select now();";
	StringInfoData resultstrdata;

	initStringInfo(&resultstrdata);
	monitor_get_stringvalues(AGT_CMD_GET_SQL_STRINGVALUES, agentport, sqlstr, user, address, port, "postgres", &resultstrdata);
	if (resultstrdata.len != 0)
		oneNodeValueStr = pstrdup(resultstrdata.data);
	pfree(resultstrdata.data);
	return oneNodeValueStr;
}

/*
* check the monitor value, the threshold include: warning, critical, emergency, the threshold value is large to small
* for example: heaphitrate, commitrate
*/
void mthreshold_levelvalue_impositiveseq(DbthresholdObject objectype, char *address, const char * time, int value, char *descp)
{
	Monitor_Alarm Monitor_Alarm;
	Monitor_Threshold dbthreshold;

	get_threshold(objectype, &dbthreshold);
	if (value > dbthreshold.threshold_warning)
	{
		/*do nothing*/
		return;
	}
	initStringInfo(&(Monitor_Alarm.alarm_text));
	initStringInfo(&(Monitor_Alarm.alarm_source));
	initStringInfo(&(Monitor_Alarm.alarm_timetz));

	if (value <= dbthreshold.threshold_warning && value > dbthreshold.threshold_critical)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d%%, under %d%%", descp,
			value, dbthreshold.threshold_warning);
		Monitor_Alarm.alarm_level = ALARM_WARNING;
	}
	else if (value <= dbthreshold.threshold_critical && value > dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d%%, under %d%%", descp,
			value, dbthreshold.threshold_critical);
		Monitor_Alarm.alarm_level = ALARM_CRITICAL;
	}
	else if (value <= dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d%%, under %d%%", descp,
			value, dbthreshold.threshold_emergency);
		Monitor_Alarm.alarm_level = ALARM_EMERGENCY;
	}

	appendStringInfo(&(Monitor_Alarm.alarm_source), "%s",address);
	appendStringInfo(&(Monitor_Alarm.alarm_timetz), "%s", time);
	Monitor_Alarm.alarm_type = 2;
	Monitor_Alarm.alarm_status = 1;
	/*insert data*/
	insert_into_monitor_alarm(&Monitor_Alarm);
	pfree(Monitor_Alarm.alarm_text.data);
	pfree(Monitor_Alarm.alarm_source.data);
	pfree(Monitor_Alarm.alarm_timetz.data);
}

/*
* check the monitor value, the threshold include: warning, critical, emergency, the threshold value is small to large
* for example: locks, unused_index, connectnums, longtrans, idletrans, unused_index
*/
void mthreshold_levelvalue_positiveseq(DbthresholdObject objectype, char *address, const char *time, int value, char *descp)
{
	Monitor_Alarm Monitor_Alarm;
	Monitor_Threshold dbthreshold;

	get_threshold(objectype, &dbthreshold);
	if (value < dbthreshold.threshold_warning)
	{
		/*do nothing*/
		return;
	}

	initStringInfo(&(Monitor_Alarm.alarm_text));
	initStringInfo(&(Monitor_Alarm.alarm_source));
	initStringInfo(&(Monitor_Alarm.alarm_timetz));

	if (value>=dbthreshold.threshold_warning && value < dbthreshold.threshold_critical)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d, over %d", descp,
			value, dbthreshold.threshold_warning);
		Monitor_Alarm.alarm_level = ALARM_WARNING;
	}
	else if (value>=dbthreshold.threshold_critical && value < dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d, over %d", descp,
			value, dbthreshold.threshold_critical);
		Monitor_Alarm.alarm_level = ALARM_CRITICAL;
	}
	else if (value>=dbthreshold.threshold_emergency)
	{
		appendStringInfo(&(Monitor_Alarm.alarm_text), "%s = %d, over %d", descp,
			value, dbthreshold.threshold_emergency);
		Monitor_Alarm.alarm_level = ALARM_EMERGENCY;
	}

	appendStringInfo(&(Monitor_Alarm.alarm_source), "%s", address);
	appendStringInfo(&(Monitor_Alarm.alarm_timetz), "%s", time);
	Monitor_Alarm.alarm_type = 2;
	Monitor_Alarm.alarm_status = 1;
	/*insert data*/
	insert_into_monitor_alarm(&Monitor_Alarm);
	pfree(Monitor_Alarm.alarm_text.data);
	pfree(Monitor_Alarm.alarm_source.data);
	pfree(Monitor_Alarm.alarm_timetz.data);
}
