#include "postgres.h"

#include "access/rxact_mgr.h"
#include "access/rxact_msg.h"

static const char* remote_type_string(RemoteXactType type);
static void desc_do(StringInfo buf, const char *rec);
static void desc_result(StringInfo buf, const char *rec);

void rxact_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch(info)
	{
	case RXACT_MSG_DO:
		desc_do(buf, rec);
		break;
	case RXACT_MSG_SUCCESS:
	case RXACT_MSG_FAILED:
	case RXACT_MSG_CHANGE:
		desc_result(buf, rec);
		break;
	default:
		appendStringInfo(buf, "UNKNOWN");
	}
}

const char *rxact_identify(uint8 info)
{
	switch(info & ~XLR_INFO_MASK)
	{
	case RXACT_MSG_DO:
		return "BEGIN";
	case RXACT_MSG_SUCCESS:
		return "SUCCESS";
	case RXACT_MSG_FAILED:
		return "FAILE";
	case RXACT_MSG_CHANGE:
		return "CHANGE";
	}
	return NULL;
}

static const char* remote_type_string(RemoteXactType type)
{
	switch(type)
	{
	case RX_PREPARE:
		return "prepare";
	case RX_ROLLBACK:
		return "rollback";
	case RX_COMMIT:
		return "commit";
	}
	return "UNKNOWN";
}

static void desc_do(StringInfo buf, const char *rec)
{
	RemoteXactType type;
	int i;
	int count;
	Oid db_oid;
	Oid *oids;
	const char *action;

	memcpy(&db_oid, rec, sizeof(db_oid));
	rec += sizeof(db_oid);

	type = (RemoteXactType)(*rec++);

	memcpy(&count, rec, sizeof(count));
	rec += sizeof(count);

	if(count > 0)
	{
		Size size = sizeof(oids[0])*count;
		oids = palloc(size);
		memcpy(oids, rec, size);
		rec += size;
	}else
	{
		oids = NULL;
	}

	appendStringInfo(buf, "%s '%s';", remote_type_string(type), rec);
	appendStringInfo(buf, " DB %u;", db_oid);

	if(count > 0)
	{
		action = " node{";
		for(i=0;i<count;++i)
		{
			appendStringInfoString(buf, action);
			appendStringInfo(buf, "%u", oids[i]);
			action = ",";
		}
		pfree(oids);
		appendStringInfoChar(buf, '}');
	}else
	{
		appendStringInfoString(buf, " node{}");
	}
}

static void desc_result(StringInfo buf, const char *rec)
{
	RemoteXactType type;
	type = (RemoteXactType)(*rec++);

	appendStringInfo(buf, "%s '%s'", remote_type_string(type), rec);
}
