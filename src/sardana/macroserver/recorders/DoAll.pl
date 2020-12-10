#!/usr/bin/env perl
#
use strict;
my $status = 1;

my @nodes = `cat /afs/desy.de/group/hasylab/Tango/HostLists/TangoHosts.lis`; 

foreach my $node (@nodes)
{    
    $node =~ s/^\s*(.*?)\s*$/$1/; 
    if( $node =~ /(\s*#.*)|(^$)/)
    {
        next;
    }
    #
    # remote node online? 
    #
    my $status = !system( "ping -c 1 -w 1 -q ${node} 1>/dev/null 2>&1");
    if( !$status)
    {
        print " $node is offline \n"; 
        next;
    }
    if( system( "./Update_files.pl $node"))
    {
        print "failed to do $node\n";
        goto finish;
    }
}

finish:
    1;
