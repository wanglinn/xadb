
/*BEGIN_HEAD*/
#ifndef BEGIN_NODE
#	define BEGIN_NODE(t)
#endif
#ifndef END_NODE
#	define END_NODE(t)
#endif
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
/*END_HEAD*/

#ifndef NO_NODE_Const
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
#endif /* NO_NODE_Const */

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
END_SPECIAL_MEB(IndexOptInfo)

NODE_SPECIAL_MEB(RelOptInfo)
	attr_needed NODE_RELIDS_ARRAY(Relids, attr_needed, (NODE_ARG_->max_attr-NODE_ARG_->min_attr))
	attr_widths NODE_SCALAR_POINT(int32, attr_widths,(NODE_ARG_->max_attr-NODE_ARG_->min_attr))
END_SPECIAL_MEB(RelOptInfo)

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

/* end ADB */


/* ENUM_IF_DEFINED(enum_name, macro_name [, ...]) */
ENUM_IF_DEFINED(RelationAccessType, ADB)
