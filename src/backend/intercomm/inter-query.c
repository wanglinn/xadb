#include "postgres.h"

#include "intercomm/inter-comm.h"
#include "pgxc/execRemote.h"

#if 0
if (exec_nodes)
{
	if (exec_nodes->en_expr)
	{
		ExecNodes *nodes = reget_exec_nodes_by_en_expr(planstate, exec_nodes);

		if (nodes)
		{
			nodelist = nodes->nodeList;
			primarynode = nodes->primarynodelist;
			pfree(nodes);
		}
	}
	else if (OidIsValid(exec_nodes->en_relid))
	{
		RelationLocInfo *rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
		Datum value = (Datum)0;
		bool null = true;
		Oid type = InvalidOid;
		ExecNodes *nodes = GetRelationNodes(rel_loc_info,
											1,
											&value,
											&null,
											&type,
											exec_nodes->accesstype);
		/*
		 * en_relid is set only for DMLs, hence a select for update on a
		 * replicated table here is an assertion
		 */
		Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE &&
					IsRelationReplicated(rel_loc_info)));

		/* Use the obtained list for given table */
		if (nodes)
			nodelist = nodes->nodeList;

		/*
		 * Special handling for ROUND ROBIN distributed tables. The target
		 * node must be determined at the execution time
		 */
		if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes)
		{
			nodelist = nodes->nodeList;
			primarynode = nodes->primarynodelist;
		}
		else if (nodes)
		{
			if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
			{
				nodelist = exec_nodes->nodeList;
				primarynode = exec_nodes->primarynodelist;
			}
		}

		if (nodes)
			pfree(nodes);
		FreeRelationLocInfo(rel_loc_info);
	}
	else
	{
		if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
			nodelist = exec_nodes->nodeList;
		else if (exec_type == EXEC_ON_COORDS)
			coordlist = exec_nodes->nodeList;

		primarynode = exec_nodes->primarynodelist;
	}
}

void
RemoteQuery2InterXactState(RemoteQuery *node, InterXactState state)
{
	if (!node)
		return ;

	if (!state)
		state = MakeInterXactState(NULL, NIL);

	state->combine_type = node->combine_type;
	state->hastmp = node->is_temp;
	state->need_xact_block = !node->force_autocommit;
	if ()
}
#endif
