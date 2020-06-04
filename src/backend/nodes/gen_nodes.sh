#! /bin/sh

bin_dir=`dirname $0`
bin_dir=`cd $bin_dir && pwd`
inc_dir=`cd $bin_dir/../../include && pwd`

cat \
	$inc_dir/nodes/nodes.h \
	$inc_dir/nodes/primnodes.h \
	$inc_dir/nodes/parsenodes.h \
	$inc_dir/nodes/pathnodes.h \
	$inc_dir/nodes/plannodes.h \
	$inc_dir/nodes/supportnodes.h \
	$inc_dir/nodes/execnodes.h \
	$inc_dir/nodes/extensible.h \
	$inc_dir/nodes/replnodes.h \
	$inc_dir/nodes/lockoptions.h \
	$inc_dir/parser/mgr_node.h \
	$inc_dir/access/sdir.h \
	$inc_dir/pgxc/locator.h \
	$inc_dir/optimizer/pgxcplan.h \
	$inc_dir/commands/event_trigger.h \
	$inc_dir/commands/trigger.h \
	$inc_dir/utils/rel.h \
	$inc_dir/nodes/params.h \
	$inc_dir/optimizer/planmain.h \
	$inc_dir/optimizer/reduceinfo.h \
	| perl $bin_dir/node_format.pl >/tmp/$$.h \
	&& (clang-format -style="{BasedOnStyle: llvm, ColumnLimit: 0}" /tmp/$$.h || exit 1) \
	   | perl $bin_dir/gen_nodes.pl -s $inc_dir/nodes/node_special.h -o $inc_dir/nodes/
#rm /tmp/$$.h
