/*-------------------------------------------------------------------------
 *
 * plancat.h
 *	  prototypes for plancat.c.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/plancat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLANCAT_H
#define PLANCAT_H

#include "nodes/relation.h"
#include "utils/relcache.h"

/* Hook for plugins to get control in get_relation_info() */
typedef void (*get_relation_info_hook_type) (PlannerInfo *root,
											 Oid relationObjectId,
											 bool inhparent,
											 RelOptInfo *rel);
extern PGDLLIMPORT get_relation_info_hook_type get_relation_info_hook;


extern void get_relation_info(PlannerInfo *root, Oid relationObjectId,
				  bool inhparent, RelOptInfo *rel);

extern List *infer_arbiter_indexes(PlannerInfo *root);

extern void estimate_rel_size(Relation rel, int32 *attr_widths,
				  BlockNumber *pages, double *tuples, double *allvisfrac);

extern int32 get_relation_data_width(Oid relid, int32 *attr_widths);

extern bool relation_excluded_by_constraints(PlannerInfo *root,
								 RelOptInfo *rel, RangeTblEntry *rte);

extern List *get_relation_constraints(PlannerInfo *root,
									  Oid relationObjectId, RelOptInfo *rel,
									  bool include_noinherit,
									  bool include_notnull,
									  bool include_partition);
extern List *get_relation_constraints_base(PlannerInfo *root,
										   Oid relationObjectId, Index varno,
										   bool include_noinherit,
										   bool include_notnull,
										   bool include_partition);


extern List *build_physical_tlist(PlannerInfo *root, RelOptInfo *rel);

extern bool has_unique_index(RelOptInfo *rel, AttrNumber attno);

extern Selectivity restriction_selectivity(PlannerInfo *root,
						Oid operatorid,
						List *args,
						Oid inputcollid,
						int varRelid);

extern Selectivity join_selectivity(PlannerInfo *root,
				 Oid operatorid,
				 List *args,
				 Oid inputcollid,
				 JoinType jointype,
				 SpecialJoinInfo *sjinfo);

extern bool has_row_triggers(PlannerInfo *root, Index rti, CmdType event);

#ifdef ADB

extern bool has_any_triggers_subclass(PlannerInfo *root, Index rti, CmdType event);
extern bool reloid_has_any_triggers_subclass(Oid reloid, CmdType event);
extern bool reloid_list_has_any_triggers(List *list, CmdType event);
extern PartitionScheme build_partschema_from_partkey(PartitionKey partkey);
extern void adb_set_rel_partition_key_exprs(PartitionKey partkey, RelOptInfo *rel);

/* src/backend/optimizer/util/remotetest.c */
typedef enum UseAuxiliaryType
{
	USE_AUX_OFF = 0
	,USE_AUX_NODE
	,USE_AUX_CTID
}UseAuxiliaryType;
struct RelationLocInfo;
extern int use_aux_type;
extern int use_aux_max_times;
extern List *relation_remote_by_constraints(PlannerInfo *root, RelOptInfo *rel, bool modify_info_when_aux);
extern List *relation_remote_by_constraints_base(PlannerInfo *root, Node *quals, struct RelationLocInfo *loc_info, Index varno);
#endif /* ADB */

#endif							/* PLANCAT_H */
