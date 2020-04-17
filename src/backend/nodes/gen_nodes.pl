#!/usr/bin/perl

use warnings;

my @node_tags;
my @except_node;
my @node_head;
my @struct_head;
my %all_node;
my $same_node;
my %all_enum;
my %ident_if_defined;
my $ident = '[a-zA-Z_][a-zA-Z0-9_]*';
my $scalar_ident = 'char|bool|int|uint8|int16|uint16|int32|uint32|bits32|uint64|double|long|Cost|AttrNumber|Index|Oid|BlockNumber|Selectivity|Size|float8|TimeLineID|Buffer|AclMode|TriggerEvent|XLogRecPtr|StrategyNumber';
my $reg_args = "\\s*$ident\\s*(,\\s*$ident\\s*)*";
my %special_node;
my %special_member;
my $special_file;
my $output_path = '';
while (@ARGV)
{
	my $arg = shift @ARGV;
	if ($arg =~ /^-o/)
	{
		$output_path = length($arg) > 2 ? substr($arg, 2) : shift @ARGV;
	}
	elsif ($arg =~ /^-s/)
	{
		$special_file = length($arg) > 2 ? substr($arg, 2) : shift @ARGV;
	}
	else
	{
		usage();
	}
}

# Make sure output_path ends in a slash.
if ($output_path ne '' && substr($output_path, -1) ne '/')
{
	$output_path .= '/';
}

if (defined $special_file)
{
	open H, '<', "$special_file" or die "Can not open $special_file:$!";
	while(<H>)
	{
		if (/^\s*BEGIN_NODE\s*\(\s*($ident)\s*\)\s*$/)
		{
			my $node_name = $1;
			my @node_item;
			while(<H>)
			{
				if (/^\s*END_NODE\s*\(\s*($ident)\s*\)\s*$/)
				{
					die "diffent BEGIN_NODE($node_name) and END_NODE($1)" if !($node_name eq $1);
					die "multiple BEGIN_STRUCT($node_name) or BEGIN_NODE($node_name)"	if defined $special_node{$node_name};
					$special_node{$node_name} = \@node_item;
					last;
				}
				push @node_item,$_;
			}
		}
		elsif (/^\s*BEGIN_STRUCT\s*\(\s*($ident)\s*\)\s*$/)
		{
			my $node_name = $1;
			my @node_item;
			while(<H>)
			{
				if (/^\s*END_STRUCT\s*\(\s*($ident)\s*\)\s*$/)
				{
					die "diffent BEGIN_STRUCT($node_name) and END_STRUCT($1)" if !($node_name eq $1);
					die "multiple BEGIN_STRUCT($node_name) or BEGIN_NODE($node_name)"	if defined $special_node{$node_name};
					$special_node{$node_name} = \@node_item;
					last;
				}
				push @node_item,$_;
			}
		}
		elsif (/^\s*NODE_SPECIAL_MEB\s*\(\s*($ident)\s*\)\s*$/)
		{
			my $node_name = $1;
			my %node_mem;
			die "multiple NODE_SPECIAL_MEM($node_name)" if defined $special_member{$node_name};
			while(<H>)
			{
				if(/^\s*END_SPECIAL_MEB\s*\(\s*($ident)\s*\)\s*$/)
				{
					die "diffent NODE_SPECIAL_MEB($node_name) and END_SPECIAL_MEB($1)" if $node_name ne $1;
					die "multiple NODE_SPECIAL_MEB($node_name)"	if defined $special_node{$node_name};
					$special_member{$node_name} = \%node_mem;
					last;
				}
				elsif (/^\s*($ident)\s+(.*)/)
				{
					$node_mem{$1} = $2;
				}
			}
		}
		elsif (/^\s*IDENT_IF_DEFINED\s*\(\s*($ident)\s*,($reg_args)\)\s*$/)
		{
			my $enum_name = $1;
			my $defined_str = $2;
			$defined_str =~ s/\s+//g;
			my @defined_args = split(/,/, $defined_str);
			$ident_if_defined{$enum_name} = \@defined_args;
		}
		elsif (/^\s*\/\*\s*BEGIN_HEAD\s*\*\//)
		{
			while(<H>)
			{
				last if(/^\s*\/\*\s*END_HEAD\s*\*\//);
				push @node_head,$_;
			}
		}
		elsif (/\s*\/\*\s*BEGIN_STRUCT_HEAD\s*\*\//)
		{
			while(<H>)
			{
				last if(/^\s*\/\*END_STRUCT_HEAD\s*\*\//);
				push @struct_head,$_;
			}
		}
	}
	close H;
}

while(<>)
{
	# NodeTag
	if (/^\s*typedef\s+enum\s+NodeTag\s+\{$/)
	{
		my @enum_item;
		while(<>)
		{
			#last if /^\s*\}/;
			if (/^\s*\}/)
			{
				$all_enum{NodeTag} = \@enum_item;
				last;
			}
			elsif (/^\s*#/)
			{
				push @enum_item,$_;
				push @node_tags,$_;
			}
			elsif (/^\s*,*T_($ident)/)
			{
				next if ($1 eq 'Invalid');
				next if ($1 =~ '(MGR_NODE_START|MGR_NODE_END)');
				my $node = $1;
				if ($1 eq 'ExprContext'
					or $1 eq 'TupleTableSlot'
					or $1 eq 'ProjectionInfo'
					or $1 eq 'JunkFilter'
					or $1 eq 'ResultRelInfo'
					or $1 eq 'PlannerInfo'
					or $1 eq 'WindowObjectData'
					or $1 eq 'TsmRoutine'
					or $1 eq 'TriggerData'
					or $1 eq 'ReturnSetInfo'
					or $1 eq 'ForeignKeyOptInfo'
					or $1 eq 'ExprContext_CB'
					or $1 eq 'ForeignKeyCacheInfo'
					or $1 eq 'IndexInfo'
					or $1 eq 'CustomPath'
					or $1 eq 'CustomScan'
					or $1 eq 'ExecRowMark'
					or $1 eq 'StartReplicationCmd'
					or $1 eq 'SlabContext'
					or $1 eq 'TimeLineHistoryCmd'
					or $1 eq 'GenerationContext'
					or $1 eq 'RelOptInfo'
					or $1 =~ '^(FdwRoutine|TIDBitmap|IndexAmRoutine)$'
					or $1 =~ '^(Value|Integer|Float|String|BitString|Null)$'
					or $1 =~ '^(List|IntList|OidList)$'
					or $1 =~ '^(MemoryContext|AllocSetContext)$'
					or $1 =~ 'State$')
				{
					push @except_node,$node;
				}else
				{
					push @node_tags,$node;
				}
				push @enum_item,"T_$node";
			}
		}
	}
	elsif (/^\s*typedef\s+struct\s+$ident\s+\{$/
		or /^\s*typedef\s+struct\s*\{$/)
	{
		my @node_item;
		while(<>)
		{
			if (/\s*\}\s*($ident)\s*;\s*$/)
			{
				$all_node{$1} = \@node_item;
				last;
			}
			push @node_item,$_;
		}
	}
	elsif (/^\s*struct\s+($ident)\s*\{$/)
	{
		my @node_item;
		my $name = $1;
		while(<>)
		{
			if (/\s*\}\s*;\s*$/)
			{
				$all_node{${name}} = \@node_item;
				last;
			}
			push @node_item,$_;
		}

	}
	elsif (/^\s*typedef\s+enum\s+$ident\s+\{$/
		or /^\s*typedef\s+enum\s*\{$/)
	{
		my @enum_item;
		while(<>)
		{
			if(/\s*\}\s*($ident)\s*;\s*$/)
			{
				$all_enum{$1} = \@enum_item;
				last;
			}
			push @enum_item,$_;
		}
	}
	elsif (/^\s*typedef\s+($ident)\s+($ident)\s*;\s*$/)
	{
		$same_node{$2} = ${1};
	}
}
push @node_tags,"JoinPath";
push @except_node,"RelationData";
push @except_node,"ParamExecData";
push @except_node, "ParallelBitmapHeapState";
push @except_node, "TransitionCaptureState";
push @except_node, "PartitionKeyData";

open H,'>',$output_path . 'enum_define.h' or die "can not open $output_path" . "enum_define.h:$!";
print H
qq|
/*
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/nodes/gen_nodes.pl
 */

#ifndef BEGIN_ENUM
#	define BEGIN_ENUM(e)
#endif
#ifndef END_ENUM
#	define END_ENUM(e)
#endif
#ifndef ENUM_VALUE
#	define ENUM_VALUE(v)
#endif
|;
foreach my $key (sort{ $all_enum{$a} <=> $all_enum{$b} } keys %all_enum)
{
	if(defined $ident_if_defined{$key})
	{
		my $tmp_str = "\n#if";
		foreach my $item (@{$ident_if_defined{$key}})
		{
			print H "$tmp_str defined($item)";
			$tmp_str = " ||";
		}
	}
#	if ($key eq 'CostSelector' || $key eq 'CommandMode' || $key eq 'DomainConstraintType' || $key eq 'SetFunctionReturnMode' || $key eq 'ExprDoneCond' || $key eq 'UpperRelationKind' || $key eq 'LimitStateCond' || $key eq 'TableLikeOption' || $key eq 'VacuumOption')
#	{
#		next;
#	}
	print H "\n#ifndef NO_ENUM_$key\nBEGIN_ENUM($key)\n";
	foreach my $item (@{$all_enum{$key}})
	{
		if($item =~ /ifdef/)
		{
			$item =~ s/\n//g;
		}
		else
		{
			$item =~ s/\s|,//g;
		}
		$item =~ s/=.+$//;
		if ($item =~ /^#/)
		{
			print H "$item\n";
		}
		else
		{
			if($item)
			{
				print H "\tENUM_VALUE($item)\n"
			}
		}
	}
	print H "END_ENUM($key)\n#endif /* NO_ENUM_$key */\n";

	if(defined $ident_if_defined{$key})
	{
		print H "#endif\n";
	}
}
close H;

my %map_node_tag = map { $_ => 1 } @node_tags;
my %map_except_node = map { $_ => 1 } @except_node;

open H,'>',$output_path . "nodes_define.h" or die "can not open $output_path" . "nodes_define.h:$!";
print H qq|
/*
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/nodes/gen_nodes.pl
 *
 * not declare:
|;

foreach $item (@except_node)
{
	print H " *   $item\n";
}
print H " */\n\n";

foreach $item (@node_head)
{
	print H $item;
}

foreach my $node_tag (@node_tags)
{
	if ($node_tag =~ /\s*#/)
	{
		print H "\n$node_tag";
		next;
	}

	if (defined $same_node{$node_tag})
	{
		print H "\n#ifndef NO_NODE_$node_tag\nNODE_SAME($node_tag, $same_node{$node_tag})\n#endif /* NO_NODE_$node_tag */\n";
		next;
	}

	if (!defined $all_node{$node_tag})
	{
		print STDERR "not found $node_tag\n";
		next;
	}
	print H "\n#ifndef NO_NODE_$node_tag\nBEGIN_NODE($node_tag)\n";
	if (defined $special_node{$node_tag})
	{
		foreach my $item (@{$special_node{$node_tag}})
		{
			print H "$item";
		}
		print H "END_NODE($node_tag)\n#endif /* NO_NODE_$node_tag */\n";
		next;
	}

	my $special_mem = $special_member{$node_tag};
	my $start = 1;
	foreach my $item (@{$all_node{$node_tag}})
	{
		if ($item =~ /^\s*#/)
		{
			print H $item;
			next;
		}

		# delete space
		$item =~ s/^\s+//;
		$item =~ s/;\s+$//;

		if ($start)
		{
			$start = 0;
			next if $item =~ /^NodeTag\s+$ident$/;
			if ($item =~ /^($ident)\s+($ident)$/)
			{
				if (defined $special_mem and defined $$special_mem{$2})
				{
					print H "\t$$special_mem{$2}\n";
				}
				elsif (defined $map_node_tag{$1} or defined $map_except_node{$1})
				{
					print H "\tNODE_BASE2($1,$2)\n";
				}
				else
				{
					process_node_member($item, $special_mem);
				}
			}
			else
			{
				process_node_member($item, $special_mem);
			}
		}
		else
		{
			process_node_member($item, $special_mem);
		}
	}

	print H "END_NODE($node_tag)\n#endif /* NO_NODE_$node_tag */\n"
}
close H;

open H,'>', $output_path . "struct_define.h" or die "can not open $output_path" . "struct_define.h:$!";
print H qq|
/*
 * NOTES
 *  ******************************
 *  *** DO NOT EDIT THIS FILE! ***
 *  ******************************
 *
 *  It has been GENERATED by src/backend/nodes/gen_nodes.pl
 */
|;
foreach $item (@struct_head)
{
	print H $item;
}

foreach $struct_name (sort{ $all_node{$a} <=> $all_node{$b} } keys %all_node)
{
	next if defined $map_except_node{$struct_name}
		or defined $map_node_tag{$struct_name}
		or $struct_name eq 'Node';
	if(defined $ident_if_defined{$struct_name})
	{
		my $tmp_str = "\n#if";
		foreach my $item (@{$ident_if_defined{$struct_name}})
		{
			print H "$tmp_str defined($item)";
			$tmp_str = " ||";
		}
	}

	my $special_mem = $special_member{$struct_name};
	print H "\n#ifndef NO_STRUCT_$struct_name\nBEGIN_STRUCT($struct_name)\n";
	foreach my $item (@{$all_node{$struct_name}})
	{
		if ($item =~ /^\s*#/)
		{
			print H $item;
			next;
		}

		# delete space
		$item =~ s/^\s+//;
		$item =~ s/;\s*$//;
		process_node_member($item, $special_mem);
	}
	print H "END_STRUCT($struct_name)\n#endif /* NO_STRUCT_$struct_name */\n";

	print H "#endif\n" if defined $ident_if_defined{$struct_name};
}
close H;

open H,'>',$output_path . 'undef_no_all_struct.h' or die "can not open $output_path" . "undef_no_all_struct.h:$!";
open H2,'>',$output_path . 'def_no_all_struct.h' or die "can not open $output_path" . "def_no_all_struct.h:$!";
foreach $struct_name (sort{ $all_node{$a} <=> $all_node{$b} } keys %all_node)
{
	next if defined $map_except_node{$struct_name}
		or defined $map_node_tag{$struct_name}
		or $struct_name eq 'Node';
	print H "#undef NO_STRUCT_$struct_name\n";
	print H2 "#define NO_STRUCT_$struct_name\n";
}
close H;

sub process_node_member
{
	my ($item, $special_mem) = @_;

	# StringInfo ident
	if ($item =~ /^StringInfo\s+($ident)$/)
	{
		print H "\tNODE_StringInfo($1)\n";
	}
	# [const] char *ident;
	elsif ($item =~ /^(const\s*){0,1}char\s*\*\s*($ident)$/)
	{
		if (defined $special_mem and defined $$special_mem{$2})
		{
			print H "\t$$special_mem{$2}\n";
		}
		else
		{
			print H "\tNODE_STRING($2)\n";
		}
	}
	# scalar_ident ident;
	elsif ($item =~ /^($scalar_ident)\s+($ident)$/)
	{
		if (defined $special_mem and defined $$special_mem{$2})
		{
			print H "\t$$special_mem{$2}\n";
		}
		else
		{
			print H "\tNODE_SCALAR($1,$2)\n";
		}
	}
	# scalar_ident *ident;
	elsif ($item =~ /^($scalar_ident)\s*\*\s*($ident)$/)
	{
		if (defined $special_mem and defined $$special_mem{$2})
		{
			print H "\t$$special_mem{$2}\n";
		}
		else
		{
			print H "\tNODE_SCALAR_POINT($1,$2,NODE_ARG_->------)\n";
		}
	}
	# ident ident
	elsif ($item =~ /^($ident)\s+($ident)$/)
	{
		if (defined $special_mem and defined $$special_mem{$2})
		{
			print H "\t$$special_mem{$2}\n";
		}
		elsif (defined $all_enum{$1})
		{
			print H "\tNODE_ENUM($1,$2)\n";
		}
		elsif (defined $map_node_tag{$1} or defined $map_except_node{$1})
		{
			print H "\tNODE_NODE_MEB($1,$2)\n";
		}
		elsif (defined $all_node{$1})
		{
			print H "\tNODE_STRUCT_MEB($1,$2)\n";
		}
		elsif ($1 eq 'Relids')
		{
			print H "\tNODE_RELIDS(Relids,$2)\n";
		}
		else
		{
			print H "$item\n";
		}
	}
	# [const] [struct] ident *ident
	elsif ($item =~ /^(const\s+){0,1}(struct\s+){0,1}($ident)\s*\*($ident)$/)
	{
		if (defined $special_mem and defined $$special_mem{$4})
		{
			print H "\t$$special_mem{$4}\n";
		}
		elsif ($3 eq 'Node'
			or defined $map_except_node{$3}
			or defined $map_node_tag{$3})
		{
			print H "\tNODE_NODE($3,$4)\n";
		}
		elsif($3 eq 'Bitmapset')
		{
			print H "\tNODE_BITMAPSET(Bitmapset,$4)\n";
		}
		elsif(defined $all_enum{$3})
		{
			print H "\tNODE_ENUM_POINT($3,$4,NODE_ARG_->------)\n";
		}
		elsif(defined $all_node{$3})
		{
			print H "\tNODE_STRUCT($3,$4)\n";
		}
		elsif($3 eq 'void')
		{
			print H "\tNODE_OTHER_POINT($3,$4)\n";
		}
		else
		{
			print H "$item\n";
		}
	}
	# function point
	elsif ($item =~ /^(const\s+){0,1}(struct\s+){0,1}$ident(\s*\**\s*|\s+)\(\s*\*\s*($ident)\s*\)\s*\(.*\)$/)
	{
		if (defined $special_mem and defined $$special_mem{$4})
		{
			print H "\t$$special_mem{$4}\n";
		}
		else
		{
			print H "\tNODE_OTHER_POINT(void,$4)\n";
		}
	}
	# [const] scalar_ident ident[ident]
	elsif ($item =~ /^(const\s+){0,1}($scalar_ident|$ident)\s+($ident)\s*\[\s*($ident)\s*\]$/)
	{
		if (defined $special_mem and defined $$special_mem{$3})
		{
			print H "\t$$special_mem{$3}\n";
		}
		#/*ADBQ*/
		elsif ($item =~ /ParamExternData/)
		{
			print H "\tNODE_STRUCT_ARRAY(ParamExternData,params, NODE_ARG_->numParams)\n";
		}else
		{
			print H "$item\n";
		}
	}
	else
	{
		print H "$item\n";
	}
}

sub usage
{
	die <<EOM;
Usage gen_nodes.pl [options]

Options:
    -o               output path
    -s               special node file
EOM
}
