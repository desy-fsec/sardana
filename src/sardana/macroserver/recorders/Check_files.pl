#!/usr/bin/env perl
#
use strict; 
use Fcntl; 
use DB_File;
my %f = undef;

#
# ==========
#
my $dir_local = "."; 
my $dir_remote_bin = "/usr/bin"; 
my $dir_remote  = "/usr/lib/python2.7/dist-packages/sardana/macroserver/recorders"; 
my $dir_remote3 = "/usr/lib/python3/dist-packages/sardana/macroserver/recorders"; 
#
# ==========
#

my $status = 1;
my ($host, $flag_new) = @ARGV;  

if( !defined( $host))
{
    print "\nUsage: ./Check_files.pl <host> \n\n";
    print "  <host>: e.g. hastodt, haso107d1 \n";
    $status = 0;
    goto finish;
}

$status = !system( "ping -c 1 -w 1 -q $host 1>/dev/null 2>&1");
if( !$status)
{
    print " $host is offline \n"; 
    $status = 0;
    goto finish;
}

my $p2Exists = 0;
my $p3Exists = 0;
my $ret = `ssh -x root\@${host} test -d ${dir_remote} && echo exists`; 
chomp $ret; 
if( $ret =~ /exists/)
{
    $p2Exists = 1; 
}
$ret = `ssh -x root\@${host} test -d ${dir_remote3} && echo exists`; 
chomp $ret; 
if( $ret =~ /exists/)
{
    $p3Exists = 1; 
}

if( !$p2Exists && !$p3Exists)
{
    print "***\n*** error: neither ${dir_remote} nor ${dir_remote3} exists on ${host}\n***\n";
    $status = 0;
    goto finish;
}

my ($dev, $ino, $mode, $nlink, $uid, $gid, 
    $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks);

if( ! -e "${dir_local}/Files.lis")
{
    print " Error: Files.lis is missing \n";
    $status = 0;
    goto finish;
}

my @files = `cat ${dir_local}/Files.lis`;
@files = grep !/^#/, @files;

foreach my $file (@files)
{
    $file =~ s/^\s*(.*?)\s*$/$1/;    
    #
    # ignore comment lines and empty lines
    #
    if( $file =~ /(\s*#.*)|(^$)/)
    {
        next;
    }
    if( ! -e "${dir_local}/${file}")
    {
        print " Error: ${dir_local}/${file} is missing \n";
        $status = 0;
        goto finish;
    }
    
    if( $p2Exists)
    {
        #print " copying root\@${host}:${dir_remote}/${file} to ${dir_local}/${file}_temp \n";
        $status = !system( "scp root\@${host}:${dir_remote}/${file} ${dir_local}/${file}_temp >/dev/null");
        if( !$status)
        {
            print "trouble copying from ${host}:${dir_remote}\n";
        }
        my $ret = `diff ${dir_local}/${file}_temp ${file}`; 
        if( length( $ret) > 0)
        {
            print " ${file}, ${dir_remote}\n   on ${host} is different $ret\n"; 
        }
        else
        {
            print " ${file} on ${host} is up-to-date\n"; 
        }
    }
    if( $p3Exists)
    {
        #print " copying root\@${host}:${dir_remote3}/${file} to ${dir_local}/${file}_temp3 \n";
        $status = !system( "scp root\@${host}:${dir_remote3}/${file} ${dir_local}/${file}_temp3 >/dev/null");
        if( !$status)
        {
            print "trouble copying from ${host}:${dir_remote3}\n";
        }
        my $ret = `diff ${dir_local}/${file}_temp3 ${file}`;
        if( length( $ret) > 0)
        {
            print " ${file}, ${dir_remote3}\n   on ${host} is different $ret\n"; 
        }
        else
        {
            print " ${file} on ${host} is up-to-date\n"; 
        }
        
    }
}

 finish:


!$status;

