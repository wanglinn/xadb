#ifndef CLUSTER_HEAP_SCAN
#define CLUSTER_HEAP_SCAN

extern List* ExecClusterHeapScan(List *rnodes, Relation rel, Bitmapset *ret_attnos,
								 AttrNumber eq_attr, Datum *datums, int count, bool test_null);
extern void DoClusterHeapScan(StringInfo mem_toc);

#endif /* CLUSTER_HEAP_SCAN */
