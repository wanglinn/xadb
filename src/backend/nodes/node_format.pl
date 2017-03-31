#!/usr/bin/perl

while(<>)
{
	#skip comment
	s/\/\*.*\*\///;
	next if /^\s*$/;

	#skip multi-line comment
	my $line = $_;
	if ($line =~ /\/\*/)
	{
		until($line =~ /\*\//)
		{
			$line = $line . <>;
		}
		$line =~ s/\/\*(.|\n)*\*\///;
	}
	next if $line =~ /^\s*$/;

	#skip #define
	if ($line =~ /^\s*#\s*define\s/)
	{
		while($line =~ /\\$/)
		{
			$line = <>;
		}
		next;
	}
	print $line;
}
