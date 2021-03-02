#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for executor
#
# IDENTIFICATION
#    src/backend/executor/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/executor
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	execAmi.o \
	execCurrent.o \
	execExpr.o \
	execExprInterp.o \
	execGrouping.o \
	execIndexing.o \
	execJunk.o \
	execMain.o \
	execParallel.o \
	execPartition.o \
	execProcnode.o \
	execReplication.o \
	execSRF.o \
	execScan.o \
	execTuples.o \
	execUtils.o \
	functions.o \
	instrument.o \
	nodeAgg.o \
	nodeAppend.o \
	nodeBatchSort.o \
	nodeBitmapAnd.o \
	nodeBitmapHeapscan.o \
	nodeBitmapIndexscan.o \
	nodeBitmapOr.o \
	nodeCtescan.o \
	nodeCustom.o \
	nodeForeignscan.o \
	nodeFunctionscan.o \
	nodeGather.o \
	nodeGatherMerge.o \
	nodeGroup.o \
	nodeHash.o \
	nodeHashjoin.o \
	nodeIncrementalSort.o \
	nodeIndexonlyscan.o \
	nodeIndexscan.o \
	nodeLimit.o \
	nodeLockRows.o \
	nodeMaterial.o \
	nodeMergeAppend.o \
	nodeMergejoin.o \
	nodeModifyTable.o \
	nodeNamedtuplestorescan.o \
	nodeNestloop.o \
	nodeProjectSet.o \
	nodeRecursiveunion.o \
	nodeResult.o \
	nodeSamplescan.o \
	nodeSeqscan.o \
	nodeSetOp.o \
	nodeSort.o \
	nodeSubplan.o \
	nodeSubqueryscan.o \
	nodeTableFuncscan.o \
	nodeTidscan.o \
	nodeUnique.o \
	nodeValuesscan.o \
	nodeWindowAgg.o \
	nodeWorktablescan.o \
	spi.o \
	tqueue.o \
	tstoreReceiver.o

ifeq ($(enable_cluster),yes)
  OBJS += clusterHeapScan.o \
	clusterReceiver.o \
	execCluster.o \
	nodeClusterGather.o \
	nodeClusterMergeGather.o \
	nodeClusterReduce.o \
	nodeEmptyResult.o \
	nodeParamTuplestoreScan.o \
	nodeReduceScan.o

	CFLAGS += -I$(top_srcdir)/src/interfaces
	BITCODE_CFLAGS += -I$(top_srcdir)/src/interfaces
endif
ifeq ($(enable_grammar_oracle),yes)
  OBJS += nodeConnectBy.o
endif

include $(top_srcdir)/src/backend/common.mk
