
/*BEGIN_HEAD*/
#ifndef BEGIN_NODE
#	define BEGIN_NODE(t)
#endif
#ifndef END_NODE
#	define END_NODE(t)
#endif

#ifndef NODE_BASE2
#	define NODE_BASE2(t,m) NODE_BASE(t)
#endif
#ifndef NODE_SAME
#	define NODE_SAME(t1,t2) \
		BEGIN_NODE(t1)		\
			NODE_BASE(t2)	\
		END_NODE(t1)
#endif
#ifndef NODE_ARG_
#	define NODE_ARG_ node
#endif

#ifndef NODE_BASE
#	define NODE_BASE(b)
#endif
#ifndef NODE_NODE
#	define NODE_NODE(t,m)
#endif
#ifndef NODE_NODE_MEB
#	define NODE_NODE_MEB(t,m)
#endif
#ifndef NODE_NODE_ARRAY
#	define NODE_NODE_ARRAY(t,m,l)
#endif
#ifndef NODE_BITMAPSET
#	define NODE_BITMAPSET(t,m)
#endif
#ifndef NODE_BITMAPSET_ARRAY
#	define NODE_BITMAPSET_ARRAY(t,m,l)
#endif
#ifndef NODE_RELIDS
#	define NODE_RELIDS(t,m) NODE_BITMAPSET(Bitmapset,m)
#endif
#ifndef NODE_RELIDS_ARRAY
#	define NODE_RELIDS_ARRAY(t,m,l) NODE_BITMAPSET_ARRAY(Bitmapset,m,l)
#endif
#ifndef NODE_LOCATION
#	define NODE_LOCATION(t,m) NODE_SCALAR(t,m)
#endif
#ifndef NODE_SCALAR
#	define NODE_SCALAR(t,m)
#endif
#ifndef NODE_OID
#	define NODE_OID(t,m) NODE_SCALAR(Oid, m)
#endif
#ifndef NODE_SCALAR_POINT
#	define NODE_SCALAR_POINT(t,m,l)
#endif
#ifndef NODE_SCALAR_ARRAY
#	define NODE_SCALAR_ARRAY NODE_SCALAR_POINT
#endif
#ifndef NODE_OTHER_POINT
#	define NODE_OTHER_POINT(t,m)
#endif
#ifndef NODE_STRING
#	define NODE_STRING(m)
#endif
#ifndef NODE_StringInfo
#	define NODE_StringInfo(m)
#endif
#ifndef NODE_STRUCT
#	define NODE_STRUCT(t,m)
#endif
#ifndef NODE_STRUCT_ARRAY
#	define NODE_STRUCT_ARRAY(t,m,l)
#endif
#ifndef NODE_STRUCT_LIST
#	define NODE_STRUCT_LIST(t,m)
#endif
#ifndef NODE_STRUCT_MEB
#	define NODE_STRUCT_MEB(t,m)
#endif
#ifndef NODE_ENUM
#	define NODE_ENUM(t,m)
#endif
#ifndef NODE_DATUM
#	define NODE_DATUM(t, m, o, n)
#endif
#ifndef NODE_OID_LIST
#	define NODE_OID_LIST(t,m) NODE_NODE(List, m)
#endif
/*END_HEAD*/

/*BEGIN_STRUCT_HEAD*/
#ifndef BEGIN_STRUCT
#	define BEGIN_STRUCT(t)
#endif
#ifndef END_STRUCT
#	define END_STRUCT(t)
#endif

#ifndef NODE_BASE2
#	define NODE_BASE2(t,m) NODE_BASE(t)
#endif
#ifndef NODE_SAME
#	define NODE_SAME(t1,t2) \
		BEGIN_NODE(t1)		\
			NODE_BASE(t2)	\
		END_NODE(t1)
#endif
#ifndef NODE_ARG_
#	define NODE_ARG_ node
#endif

#ifndef NODE_BASE
#	define NODE_BASE(b)
#endif
#ifndef NODE_NODE
#	define NODE_NODE(t,m)
#endif
#ifndef NODE_NODE_MEB
#	define NODE_NODE_MEB(t,m)
#endif
#ifndef NODE_NODE_ARRAY
#	define NODE_NODE_ARRAY(t,m,l)
#endif
#ifndef NODE_BITMAPSET
#	define NODE_BITMAPSET(t,m)
#endif
#ifndef NODE_BITMAPSET_ARRAY
#	define NODE_BITMAPSET_ARRAY(t,m,l)
#endif
#ifndef NODE_RELIDS
#	define NODE_RELIDS(t,m) NODE_BITMAPSET(Bitmapset,m)
#endif
#ifndef NODE_RELIDS_ARRAY
#	define NODE_RELIDS_ARRAY(t,m,l) NODE_BITMAPSET_ARRAY(Bitmapset,m,l)
#endif
#ifndef NODE_LOCATION
#	define NODE_LOCATION(t,m) NODE_SCALAR(t,m)
#endif
#ifndef NODE_SCALAR
#	define NODE_SCALAR(t,m)
#endif
#ifndef NODE_OID
#	define NODE_OID(t,m) NODE_SCALAR(Oid, m)
#endif
#ifndef NODE_SCALAR_POINT
#	define NODE_SCALAR_POINT(t,m,l)
#endif
#ifndef NODE_SCALAR_ARRAY
#	define NODE_SCALAR_ARRAY NODE_SCALAR_POINT
#endif
#ifndef NODE_OTHER_POINT
#	define NODE_OTHER_POINT(t,m)
#endif
#ifndef NODE_STRING
#	define NODE_STRING(m)
#endif
#ifndef NODE_StringInfo
#	define NODE_StringInfo(m)
#endif
#ifndef NODE_STRUCT
#	define NODE_STRUCT(t,m)
#endif
#ifndef NODE_STRUCT_ARRAY
#	define NODE_STRUCT_ARRAY(t,m,l)
#endif
#ifndef NODE_STRUCT_LIST
#	define NODE_STRUCT_LIST(t,m)
#endif
#ifndef NODE_STRUCT_MEB
#	define NODE_STRUCT_MEB(t,m)
#endif
#ifndef NODE_ENUM
#	define NODE_ENUM(t,m)
#endif
#ifndef NODE_DATUM
#	define NODE_DATUM(t, m, o, n)
#endif
/*END_STRUCT_HEAD*/

BEGIN_NODE(Const)
	NODE_OID(type,consttype)
	NODE_SCALAR(int32,consttypmod)
	NODE_OID(collation,constcollid)
	NODE_SCALAR(int,constlen)
	NODE_SCALAR(bool,constisnull)	/* offset not here */
	NODE_SCALAR(bool,constbyval)	/* offset not here */
	NODE_DATUM(Datum,constvalue,NODE_ARG_->consttype, NODE_ARG_->constisnull)
	NODE_LOCATION(int,location)
END_NODE(Const)

BEGIN_NODE(PlannerGlobal)
	NODE_STRUCT(ParamListInfoData,boundParams)
	NODE_NODE(List,subplans)
	NODE_NODE(List,subroots)
	NODE_BITMAPSET(Bitmapset,rewindPlanIDs)
	NODE_NODE(List,finalrtable)
	NODE_NODE(List,finalrowmarks)
	NODE_NODE(List,resultRelations)
	NODE_NODE(List,relationOids)
	NODE_NODE(List,invalItems)
	NODE_NODE(List,paramExecTypes)
	NODE_SCALAR(Index,lastPHId)
	NODE_SCALAR(Index,lastRowMarkId)
	NODE_SCALAR(int,lastPlanNodeId)
	NODE_SCALAR(bool,transientPlan)
	NODE_SCALAR(bool,dependsOnRole)
	NODE_SCALAR(bool,parallelModeOK)
	NODE_SCALAR(bool,parallelModeNeeded)
	NODE_SCALAR(char,maxParallelHazard)
#ifdef ADB
	NODE_SCALAR(bool,clusterPlanOK)
	NODE_SCALAR(int,usedRemoteAux)
#endif /* ADB */
END_NODE(PlannerGlobal)

BEGIN_NODE(ColumnRefJoin)
	NODE_LOCATION(int,location)	/* location for "(+)" */
	NODE_NODE(ColumnRef,column)
END_NODE(ColumnRefJoin)

BEGIN_NODE(ForeignKeyOptInfo)
	NODE_SCALAR(Index,con_relid)
	NODE_SCALAR(Index,ref_relid)
	NODE_SCALAR(int,nkeys)
	NODE_SCALAR_ARRAY(AttrNumber,conkey,NODE_ARG_->nkeys)
	NODE_SCALAR_ARRAY(AttrNumber,confkey,NODE_ARG_->nkeys)
	NODE_SCALAR_ARRAY(Oid,conpfeqop,NODE_ARG_->nkeys)
	NODE_SCALAR(int,nmatched_ec)
	NODE_SCALAR(int,nmatched_rcols)
	NODE_SCALAR(int,nmatched_ri)
	NODE_NODE_ARRAY(EquivalenceClass,eclass,NODE_ARG_->nkeys)
	NODE_NODE_ARRAY(List,rinfos,NODE_ARG_->nkeys)
END_NODE(ForeignKeyOptInfo)

/*********************************************************************/

NODE_SPECIAL_MEB(IndexInfo)
	ii_KeyAttrNumbers NODE_SCALAR_ARRAY(AttrNumber,ii_KeyAttrNumbers,NODE_ARG_->ii_NumIndexAttrs)
	ii_ExclusionOps NODE_SCALAR_POINT(Oid,ii_ExclusionOps,NODE_ARG_->ii_NumIndexAttrs)
	ii_ExclusionProcs NODE_SCALAR_POINT(Oid,ii_ExclusionProcs,NODE_ARG_->ii_NumIndexAttrs)
	ii_ExclusionStrats NODE_SCALAR_POINT(uint16,ii_ExclusionStrats,NODE_ARG_->ii_NumIndexAttrs)
	ii_UniqueOps NODE_SCALAR_POINT(Oid,ii_UniqueOps,NODE_ARG_->ii_NumIndexAttrs)
	ii_UniqueProcs NODE_SCALAR_POINT(Oid,ii_UniqueProcs,NODE_ARG_->ii_NumIndexAttrs)
	ii_UniqueStrats NODE_SCALAR_POINT(uint16,ii_UniqueStrats,NODE_ARG_->ii_NumIndexAttrs)
END_SPECIAL_MEB(IndexInfo)

NODE_SPECIAL_MEB(MergeAppend)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(MergeAppend)

NODE_SPECIAL_MEB(RecursiveUnion)
	dupColIdx NODE_SCALAR_POINT(AttrNumber,dupColIdx,NODE_ARG_->numCols)
	dupOperators NODE_SCALAR_POINT(Oid,dupOperators,NODE_ARG_->numCols)
END_SPECIAL_MEB(RecursiveUnion)

NODE_SPECIAL_MEB(MergeJoin)
	mergeFamilies NODE_SCALAR_POINT(Oid,mergeFamilies,list_length(NODE_ARG_->mergeclauses))
	mergeCollations NODE_SCALAR_POINT(Oid,mergeCollations,list_length(NODE_ARG_->mergeclauses))
	mergeStrategies NODE_SCALAR_POINT(int,mergeStrategies,list_length(NODE_ARG_->mergeclauses))
	mergeNullsFirst NODE_SCALAR_POINT(bool,mergeNullsFirst,list_length(NODE_ARG_->mergeclauses))
END_SPECIAL_MEB(MergeJoin)

NODE_SPECIAL_MEB(Sort)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(Sort)

NODE_SPECIAL_MEB(BatchSort)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numSortCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numSortCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numSortCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numSortCols)
	grpColIdx NODE_SCALAR_POINT(AttrNumber,grpColIdx,NODE_ARG_->numGroupCols)
END_SPECIAL_MEB(BatchSort)

NODE_SPECIAL_MEB(Group)
	grpColIdx NODE_SCALAR_POINT(AttrNumber,grpColIdx,NODE_ARG_->numCols)
	grpOperators NODE_SCALAR_POINT(Oid,grpOperators,NODE_ARG_->numCols)
END_SPECIAL_MEB(Group)

NODE_SPECIAL_MEB(Agg)
	grpColIdx NODE_SCALAR_POINT(AttrNumber,grpColIdx,NODE_ARG_->numCols)
	grpOperators NODE_SCALAR_POINT(Oid,grpOperators,NODE_ARG_->numCols)
END_SPECIAL_MEB(Agg)

NODE_SPECIAL_MEB(WindowAgg)
	partColIdx NODE_SCALAR_POINT(AttrNumber,partColIdx,NODE_ARG_->partNumCols)
	partOperators NODE_SCALAR_POINT(Oid,partOperators,NODE_ARG_->partNumCols)
	ordColIdx NODE_SCALAR_POINT(AttrNumber,ordColIdx,NODE_ARG_->ordNumCols)
	ordOperators NODE_SCALAR_POINT(Oid,ordOperators,NODE_ARG_->ordNumCols)
END_SPECIAL_MEB(WindowAgg)

NODE_SPECIAL_MEB(Unique)
	uniqColIdx NODE_SCALAR_POINT(AttrNumber,uniqColIdx,NODE_ARG_->numCols)
	uniqOperators NODE_SCALAR_POINT(Oid,uniqOperators,NODE_ARG_->numCols)
END_SPECIAL_MEB(Unique)

NODE_SPECIAL_MEB(SetOp)
	dupColIdx NODE_SCALAR_POINT(AttrNumber,dupColIdx,NODE_ARG_->numCols)
	dupOperators NODE_SCALAR_POINT(Oid,dupOperators,NODE_ARG_->numCols)
END_SPECIAL_MEB(SetOp)

NODE_SPECIAL_MEB(IndexOptInfo)
	indexkeys NODE_SCALAR_POINT(int,indexkeys,NODE_ARG_->ncolumns)
	indexcollations NODE_SCALAR_POINT(Oid,indexcollations,NODE_ARG_->ncolumns)
	opfamily NODE_SCALAR_POINT(Oid,opfamily,NODE_ARG_->ncolumns)
	opcintype NODE_SCALAR_POINT(Oid,opcintype,NODE_ARG_->ncolumns)
	sortopfamily NODE_SCALAR_POINT(Oid,sortopfamily,NODE_ARG_->ncolumns)
	reverse_sort NODE_SCALAR_POINT(bool,reverse_sort,NODE_ARG_->ncolumns)
	nulls_first NODE_SCALAR_POINT(bool,nulls_first,NODE_ARG_->ncolumns)
	canreturn NODE_SCALAR_POINT(bool,canreturn,NODE_ARG_->ncolumns)
	amcostestimate NODE_OTHER_POINT(void,amcostestimate)
END_SPECIAL_MEB(IndexOptInfo)

NODE_SPECIAL_MEB(RelOptInfo)
	attr_needed NODE_RELIDS_ARRAY(Relids, attr_needed, (NODE_ARG_->max_attr-NODE_ARG_->min_attr))
	attr_widths NODE_SCALAR_POINT(int32, attr_widths,(NODE_ARG_->max_attr-NODE_ARG_->min_attr))
END_SPECIAL_MEB(RelOptInfo)

NODE_SPECIAL_MEB(CustomPath)
	methods NODE_OTHER_POINT(CustomPathMethods,methods)
END_SPECIAL_MEB(CustomPath)

NODE_SPECIAL_MEB(CustomScan)
	methods NODE_OTHER_POINT(CustomPathMethods,methods)
END_SPECIAL_MEB(CustomScan)

/* same special node member */
NODE_SPECIAL_MEB(PathTarget)
	sortgrouprefs NODE_SCALAR_POINT(Index,sortgrouprefs,list_length(NODE_ARG_->exprs))
END_SPECIAL_MEB(PathTarget)

NODE_SPECIAL_MEB(Hash)
	skewColType NODE_OID(type,skewColType)
	skewTable NODE_OID(class,skewTable)
END_SPECIAL_MEB(Hash)

NODE_SPECIAL_MEB(ForeignKeyCacheInfo)
	conkey NODE_SCALAR_ARRAY(AttrNumber,conkey,NODE_ARG_->nkeys)
	confkey NODE_SCALAR_ARRAY(AttrNumber,confkey,NODE_ARG_->nkeys)
	conpfeqop NODE_SCALAR_ARRAY(Oid,conpfeqop,NODE_ARG_->nkeys)
END_SPECIAL_MEB(ForeignKeyCacheInfo)

/* begin ADB */
NODE_SPECIAL_MEB(SimpleSort)
	NODE_SCALAR(int,numCols)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	sortCollations NODE_SCALAR_POINT(Oid,sortCollations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(SimpleSort)

NODE_SPECIAL_MEB(RemoteQuery)
	rq_param_types NODE_SCALAR_POINT(Oid,rq_param_types,NODE_ARG_->rq_num_params)
END_SPECIAL_MEB(RemoteQuery)

NODE_SPECIAL_MEB(Path)
	reduce_info_list NODE_STRUCT_LIST(ReduceInfo, reduce_info_list)
END_SPECIAL_MEB(Path)

/* end ADB */

NODE_SPECIAL_MEB(RestrictInfo)
	scansel_cache NODE_STRUCT_LIST(MergeScanSelCache,scansel_cache)
END_SPECIAL_MEB(RestrictInfo)

NODE_SPECIAL_MEB(ParamListInfoData)
	paramFetch NODE_OTHER_POINT(ParamFetchHook,paramFetch)
	paramCompile NODE_OTHER_POINT(ParamCompileHook,paramCompile)
	parserSetup NODE_OTHER_POINT(ParserSetupHook,parserSetup)
END_SPECIAL_MEB(ParamListInfoData)

NODE_SPECIAL_MEB(ParamExternData)
	value NODE_DATUM(Datum,value,NODE_ARG_->ptype, NODE_ARG_->isnull)
END_SPECIAL_MEB(ParamExternData)

NODE_SPECIAL_MEB(Var)
	vartype NODE_OID(type,vartype)
	varcollid NODE_OID(collation,varcollid)
END_SPECIAL_MEB(Var)

NODE_SPECIAL_MEB(Aggref)
	aggfnoid NODE_OID(proc,aggfnoid)
	aggtype NODE_OID(type,aggtype)
	aggcollid NODE_OID(collation,aggcollid)
	aggtrantype NODE_OID(type,aggtrantype)
	aggtranstype NODE_OID(type,aggtranstype)
	inputcollid NODE_OID(collation,inputcollid)
	aggargtypes NODE_OID_LIST(type,aggargtypes)
END_SPECIAL_MEB(Aggref)

NODE_SPECIAL_MEB(MinMaxAggInfo)
	aggfnoid NODE_OID(proc,aggfnoid)
END_SPECIAL_MEB(MinMaxAggInfo)

NODE_SPECIAL_MEB(CaseExpr)
	casetype NODE_OID(type,casetype)
	casecollid NODE_OID(collation,casecollid)
END_SPECIAL_MEB(CaseExpr)

NODE_SPECIAL_MEB(CaseTestExpr)
	typeId NODE_OID(type,typeId)
	collation NODE_OID(collation,collation)
END_SPECIAL_MEB(CaseTestExpr)

NODE_SPECIAL_MEB(CoalesceExpr)
	coalescetype NODE_OID(type,coalescetype)
	coalescecollid NODE_OID(collation,coalescecollid)
END_SPECIAL_MEB(CoalesceExpr)

NODE_SPECIAL_MEB(CoerceToDomain)
	resulttype NODE_OID(type,resulttype)
	resultcollid NODE_OID(collation,resultcollid)
END_SPECIAL_MEB(CoerceToDomain)

NODE_SPECIAL_MEB(CoerceToDomainValue)
	typeId NODE_OID(type,typeId)
	collation NODE_OID(collation,collation)
END_SPECIAL_MEB(CoerceToDomainValue)

NODE_SPECIAL_MEB(CoerceViaIO)
	resulttype NODE_OID(type,resulttype)
	resultcollid NODE_OID(collation,resultcollid)
END_SPECIAL_MEB(CoerceViaIO)

NODE_SPECIAL_MEB(CollateExpr)
	collOid NODE_OID(collation,collOid)
END_SPECIAL_MEB(CollateExpr)

NODE_SPECIAL_MEB(ColumnDef)
	collOid NODE_OID(collation,collOid)
END_SPECIAL_MEB(ColumnDef)

NODE_SPECIAL_MEB(ConvertRowtypeExpr)
	resulttype NODE_OID(type,resulttype)
END_SPECIAL_MEB(ConvertRowtypeExpr)

NODE_SPECIAL_MEB(EquivalenceClass)
	ec_collation NODE_OID(collation,ec_collation)
END_SPECIAL_MEB(EquivalenceClass)

NODE_SPECIAL_MEB(ExecNodes)
	en_funcid NODE_OID(proc,en_funcid)
END_SPECIAL_MEB(ExecNodes)

NODE_SPECIAL_MEB(FieldSelect)
	resulttype NODE_OID(type,resulttype)
	resultcollid NODE_OID(collation,resultcollid)
END_SPECIAL_MEB(FieldSelect)

NODE_SPECIAL_MEB(FieldStore)
	resulttype NODE_OID(type,resulttype)
END_SPECIAL_MEB(FieldStore)

NODE_SPECIAL_MEB(FuncExpr)
	funcid NODE_OID(proc,funcid)
	funcresulttype NODE_OID(type,funcresulttype)
	funccollid NODE_OID(collation,funccollid)
	inputcollid NODE_OID(collation,inputcollid)
END_SPECIAL_MEB(FuncExpr)

NODE_SPECIAL_MEB(MinMaxExpr)
	minmaxtype NODE_OID(type,minmaxtype)
	minmaxcollid NODE_OID(collation,minmaxcollid)
	inputcollid NODE_OID(collation,inputcollid)
END_SPECIAL_MEB(MinMaxExpr)

NODE_SPECIAL_MEB(OpExpr)
	opno NODE_OID(operator,opno)
	opfuncid NODE_OID(proc,opfuncid)
	opresulttype NODE_OID(type,opresulttype)
	opcollid NODE_OID(collation,opcollid)
	inputcollid NODE_OID(collation,inputcollid)
END_SPECIAL_MEB(OpExpr)

NODE_SPECIAL_MEB(RelabelType)
	resulttype NODE_OID(type,resulttype)
	resultcollid NODE_OID(collation,resultcollid)
END_SPECIAL_MEB(RelabelType)

NODE_SPECIAL_MEB(RowExpr)
	row_typeid NODE_OID(type,row_typeid)
END_SPECIAL_MEB(RowExpr)

NODE_SPECIAL_MEB(ScalarArrayOpExpr)
	opno NODE_OID(operator,opno)
	opfuncid NODE_OID(proc,opfuncid)
	inputcollid NODE_OID(collation,inputcollid)
END_SPECIAL_MEB(ScalarArrayOpExpr)

NODE_SPECIAL_MEB(SetToDefault)
	typeId NODE_OID(type,typeId)
	collation NODE_OID(collation,collation)
END_SPECIAL_MEB(SetToDefault)

NODE_SPECIAL_MEB(SortGroupClause)
	eqop NODE_OID(operator,eqop)
	sortop NODE_OID(operator,sortop)
END_SPECIAL_MEB(SortGroupClause)

NODE_SPECIAL_MEB(TypeName)
	typeOid NODE_OID(type,typeOid)
END_SPECIAL_MEB(TypeName)

NODE_SPECIAL_MEB(WindowFunc)
	winfnoid NODE_OID(proc,winfnoid)
	wintype NODE_OID(type,wintype)
	wincollid NODE_OID(collation,wincollid)
	inputcollid NODE_OID(collation,inputcollid)
END_SPECIAL_MEB(WindowFunc)

NODE_SPECIAL_MEB(AppendRelInfo)
	parent_reltype NODE_OID(type,parent_reltype)
	child_reltype NODE_OID(type,child_reltype)
END_SPECIAL_MEB(AppendRelInfo)

NODE_SPECIAL_MEB(ArrayCoerceExpr)
	elemfuncid NODE_OID(collation,elemfuncid)
	resulttype NODE_OID(type,resulttype)
	resultcollid NODE_OID(collation,resultcollid)
END_SPECIAL_MEB(ArrayCoerceExpr)

NODE_SPECIAL_MEB(ArrayRef)
	refarraytype NODE_OID(type,refarraytype)
	refelemtype NODE_OID(type,refelemtype)
	refcollid NODE_OID(collation,refcollid)
END_SPECIAL_MEB(ArrayRef)

NODE_SPECIAL_MEB(ArrayExpr)
	array_typeid NODE_OID(type,array_typeid)
	array_collid NODE_OID(collation,array_collid)
	element_typeid NODE_OID(type,element_typeid)
END_SPECIAL_MEB(ArrayExpr)

NODE_SPECIAL_MEB(SubPlan)
	firstColType NODE_OID(type,firstColType)
	firstColCollation NODE_OID(collation,firstColCollation)
END_SPECIAL_MEB(SubPlan)

NODE_SPECIAL_MEB(RelationLocInfo)
	roundRobinNode NODE_OTHER_POINT(ListCell, roundRobinNode)
END_SPECIAL_MEB(RelationLocInfo)

NODE_SPECIAL_MEB(ExprContext_CB)
	function NODE_OTHER_POINT(ExprContextCallbackFunction, function)
	arg NODE_SCALAR(Datum, arg)
END_SPECIAL_MEB(ExprContext_CB)

NODE_SPECIAL_MEB(ExecRowMark)
	relation NODE_OTHER_POINT(Relation, relation)
	curCtid NODE_OTHER_POINT(ItemPointerData, curCtid)
END_SPECIAL_MEB(ExecRowMark)

NODE_SPECIAL_MEB(TupleHashEntryData)
	firstTuple NODE_OTHER_POINT(MinimalTuple, firstTuple)
END_SPECIAL_MEB(TupleHashEntryData)

NODE_SPECIAL_MEB(TupleHashTableData)
	hashtab NODE_OTHER_POINT(HTAB, hashtab)
	keyColIdx NODE_SCALAR_POINT(AttrNumber,keyColIdx,NODE_ARG_->numCols)
	tab_hash_funcs NODE_OTHER_POINT(FmgrInfo, tab_hash_funcs)
	tab_eq_funcs NODE_OTHER_POINT(FmgrInfo, tab_eq_funcs)
	in_hash_funcs NODE_OTHER_POINT(FmgrInfo, in_hash_funcs)
	cur_eq_funcs NODE_OTHER_POINT(FmgrInfo, cur_eq_funcs)
END_SPECIAL_MEB(TupleHashTableData)

NODE_SPECIAL_MEB(IndexScan)
	indexid NODE_OID(class,indexid)
END_SPECIAL_MEB(IndexScan)

NODE_SPECIAL_MEB(IndexOnlyScan)
	indexid NODE_OID(class,indexid)
END_SPECIAL_MEB(IndexOnlyScan)

NODE_SPECIAL_MEB(BitmapIndexScan)
	indexid NODE_OID(class,indexid)
END_SPECIAL_MEB(BitmapIndexScan)

NODE_SPECIAL_MEB(Param)
	paramtype NODE_OID(type,paramtype)
	paramcollid NODE_OID(collation,paramcollid)
END_SPECIAL_MEB(Param)

NODE_SPECIAL_MEB(XmlExpr)
	type NODE_OID(type,type)
END_SPECIAL_MEB(XmlExpr)

NODE_SPECIAL_MEB(TargetEntry)
	resorigtbl NODE_OID(class,resorigtbl)
END_SPECIAL_MEB(TargetEntry)

NODE_SPECIAL_MEB(ClusterMergeGather)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(ClusterMergeGather)

NODE_SPECIAL_MEB(GatherMerge)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(GatherMerge)

NODE_SPECIAL_MEB(ClusterReduce)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(ClusterReduce)

NODE_SPECIAL_MEB(OidVectorLoopExpr)
	vector NODE_DATUM(Datum, vector, OIDVECTOROID, false)
END_SPECIAL_MEB(OidVectorLoopExpr)

NODE_SPECIAL_MEB(RangeTblEntry)
	relid NODE_OID(class, relid)
	checkAsUser NODE_OID(authid,checkAsUser)
END_SPECIAL_MEB(RangeTblEntry)

NODE_SPECIAL_MEB(PartitionPruneInfo)
	subplan_map NODE_SCALAR_POINT(int,subplan_map,NODE_ARG_->nparts)
	subpart_map NODE_SCALAR_POINT(int,subpart_map,NODE_ARG_->nparts)
	hasexecparam NODE_SCALAR_POINT(bool,hasexecparam,NODE_ARG_->nexprs)
END_SPECIAL_MEB(PartitionPruneInfo)

NODE_SPECIAL_MEB(ModifyTable)
	arbiterIndexes NODE_OID_LIST(class,arbiterIndexes)
END_SPECIAL_MEB(ModifyTable)

NODE_SPECIAL_MEB(NextValueExpr)
	seqid NODE_OID(class,seqid)
END_SPECIAL_MEB(NextValueExpr)

NODE_SPECIAL_MEB(PartitionedRelPruneInfo)
	subplan_map NODE_SCALAR_POINT(int,subplan_map,NODE_ARG_->nparts)
	subpart_map NODE_SCALAR_POINT(int,subpart_map,NODE_ARG_->nparts)
	hasexecparam NODE_SCALAR_POINT(bool,hasexecparam,NODE_ARG_->nexprs)
END_SPECIAL_MEB(PartitionedRelPruneInfo)

/* ADB_GRAM_ORA */
NODE_SPECIAL_MEB(ConnectByPlan)
	sortColIdx NODE_SCALAR_POINT(AttrNumber,sortColIdx,NODE_ARG_->numCols)
	sortOperators NODE_SCALAR_POINT(Oid,sortOperators,NODE_ARG_->numCols)
	collations NODE_SCALAR_POINT(Oid,collations,NODE_ARG_->numCols)
	nullsFirst NODE_SCALAR_POINT(bool,nullsFirst,NODE_ARG_->numCols)
END_SPECIAL_MEB(ConnectByPlan)
/* ADB_GRAM_ORA */

/*******************************************************************/

/* ENUM_IF_DEFINED(type_name, macro_name [, ...]) */
IDENT_IF_DEFINED(RelationAccessType, ADB)
IDENT_IF_DEFINED(DistributionType, ADB)
IDENT_IF_DEFINED(PGXCSubClusterType, ADB)
IDENT_IF_DEFINED(ParseGrammar, ADB_MULTI_GRAM)
IDENT_IF_DEFINED(CombineType, ADB)
IDENT_IF_DEFINED(RemoteQueryExecType, ADB)
IDENT_IF_DEFINED(ExecDirectType, ADB)
IDENT_IF_DEFINED(CommandMode, ADBMGRD)
IDENT_IF_DEFINED(ClusterGatherType, ADB)

IDENT_IF_DEFINED(RelationLocInfo, ADB)
IDENT_IF_DEFINED(ReduceInfo, ADB)
