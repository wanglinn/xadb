#!/usr/bin/perl

#
# Portions Copyright (c) 2014-2017, ADB Development Group
#
# drop
# /* ADB_BEGIN */
# ....
# /* ADB_END */
#
# command line
#   modify-line.pl src-filename dest-filename old-filename new-filename
$ARGV[1] or die "usage: $0 src-filename dest-filename\n";
open(my $src_handle, '<', $ARGV[0])
  or die "$0: could not open input file '$ARGV[0]': $!\n";
open(my $dest_handle, '>', $ARGV[1])
  or die "$0: could not open output file '$ARGV[0]': $!\n";

while (my $row = <$src_handle>)
{
	print $dest_handle "$row";
	if ($row =~ /^\s*\/\*\s*ADB_BEGIN\s*\*\/\s*$/)
	{
		while ($row = <$src_handle>)
		{
			if ($row =~ /^\s*\/\*\s*ADB_END\s*\*\/\s*$/)
			{
				print $dest_handle "$row";
				last;
			}else
			{
				print $dest_handle "\n";
			}
		}
	}
}

close $src_handle;
close $dest_handle;
