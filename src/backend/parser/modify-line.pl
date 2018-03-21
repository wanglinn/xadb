#!/usr/bin/perl

#
# Portions Copyright (c) 2014-2018, ADB Development Group
#
# modify
#   #line number "filename"
# to
#   #line number "new filename"
#
# command line
#   modify-line.pl src-filename dest-filename old-filename new-filename

$ARGV[3] or die "usage: $0 src-filename dest-filename old-filename new-filename\n";
open(my $src_handle, '<', $ARGV[0])
  or die "$0: could not open input file '$ARGV[0]': $!\n";
open(my $dest_handle, '>', $ARGV[1])
  or die "$0: could not open output file '$ARGV[0]': $!\n";
my $src_str = $ARGV[2];
my $dest_str = $ARGV[3];

while (my $row = <$src_handle>)
{
	if ($row =~ /^\s*#line\s+(\d+)\s+\".*$src_str\"\s*$/)
	{
		print $dest_handle "#line $1 \"$dest_str\"\n";
	}else
	{
		print $dest_handle "$row";
	}
}

close $src_handle;
close $dest_handle;
