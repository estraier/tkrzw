#!/bin/perl

use utf8;
use strict;
use English qw( -no_match_vars );

my $infn  = $ARGV[0];
my $outfn = $ARGV[1];

if (0 == length($infn) || 0 == length($outfn))
{
    die "usage: $PROGRAM_NAME input_html_file output_html_file";
}

my $infh;
my $outfh;

open($infh,  '<:raw:utf8', $infn ) or die "cannot open input file [$infn]";
open($outfh, '>:raw:utf8', $outfn) or die "cannot open output file [$outfn]";

my $html = '';

while (my $line = <$infh>)
{
    chomp $line; # CRLF or CR or LF, sigh
    $html .= $line . "\n";
}

my $starter = '<!--TOC_STARTS_HERE: leave this line in the file-->';
my $ender   = '<!--TOC_ENDS_HERE: leave this line in the file-->';

# clear out old TOC contents, if any
#
if ($html !~ s!(\Q$starter\E).*(\Q$ender\E)!$1\n$2!s)
{
    die "you need to have lines in the input file saying:\n" .
        "$starter\n" .
        "$ender\n";
}

my $toc_level = 1;
my $toc = '';
my %seen_ids;

while ($html =~ m!<h(\d)\s+id="([^\"]*)"\s*>(.*?)</h\1>!igs)
{
    my $level = $1;
    my $id    = $2;
    my $guts  = $3;

    my $other_guts = $seen_ids{$id};
    if (defined($other_guts))
    {
        print "ERROR: id [$id] used for both [$other_guts] and [$guts]\n"
    }
    $seen_ids{$id} = $guts;

    #print "[$level] [$id] [$guts]\n";

    next if (1 == $level); # title of whole doc

    next if ('toc' eq $id); # that's me

    while ($level > $toc_level)
    {
        $toc_level++;
        $toc .= "<ul>\n";
    }
    while ($level < $toc_level)
    {
        $toc_level--;
        $toc .= "</ul>\n";
    }

    $toc .=
        "<li>" .
        "<a href=\"\#$id\">$guts</a>" .
        "</li>\n";
}

while ($toc_level > 1)
{
    $toc_level--;
    $toc .= "</ul>\n";
}

#print "[$toc]\n";

# stick in new TOC contents
#
die unless ($html =~ s!(\Q$starter\E).*(\Q$ender\E)!$1\n$toc$2!s);

print $outfh $html;

close $infh;
close $outfh;

print "wrote [$outfn].\n";







