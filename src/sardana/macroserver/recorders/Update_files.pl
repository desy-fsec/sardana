#!/usr/bin/env perl
#
use strict; 
use Fcntl; 
use DB_File;
my %f = undef;

my $status = 1;
my ($node, $flag_new) = @ARGV;  

print " --- scripts/Update_files.pl: updating $node \n"; 

my $dir_local = "."; 
my $dir_remote = "/usr/lib/python2.7/dist-packages/sardana/macroserver/recorders"; 
my $dir_remote3 = "/usr/lib/python3/dist-packages/sardana/macroserver/recorders"; 

my ($dev, $ino, $mode, $nlink, $uid, $gid, 
    $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks);

if(  !defined( $node))
{
    print "\n Usage: ./Update_files.pl <node> <new> \n\n";
    $status = 0;
    goto finish;
}
if( ! -e "${dir_local}/Files.lis")
{ 
    print " Error: Files.lis is missing \n";
    $status = 0;
    goto finish;
}
 
my @files = `cat ${dir_local}/Files.lis`;
@files = grep !/^#/, @files;

my $filename = "${dir_local}/Files.db";
my $flagTie = 1;
if( !(tie %f, "DB_File", "$filename", O_RDWR, 0777, $DB_HASH))
{
    $flagTie = 0;
    if( !(tie %f, "DB_File", "$filename", O_CREAT, 0777, $DB_HASH))
    {
        print " Error: failed to open $filename \n";
        $status = 0;
        goto finish;
    }
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

#
my $flag_upToDate = 1; 
foreach my $file (@files)
{
    $file =~ s/^\s*(.*?)\s*$/$1/;   
    if( length( $file) == 0)
    {
        next;
    }
    if( ! -e "${dir_local}/${file}")
    {
        print " Error: ${dir_local}/${file} is missing \n";
        $status = 0;
        goto finish;
    }
    ($dev, $ino, $mode, $nlink, $uid, $gid, 
     $rdev, $size, $atime, $mtime, $ctime, $blksize, $blocks) = 
         stat "${dir_local}/${file}";
    
    if( (defined $f{ "${node}_${file}_mtime"}) &&
        ($mtime <= $f{ "${node}_${file}_mtime"}) &&
        ($flag_new !~ /new/i))
    {	
        next;
    }

    $flag_upToDate = 0;
    if( $p2Exists)
    {
        print " copying ${dir_local}/${file} to ${node}:${dir_remote} \n";
        $status = !system( "scp ${dir_local}/${file} root\@${node}:${dir_remote} >/dev/null");
        if( !$status)
        {
            goto finish;
        }
        $f{ "${node}_${file}_mtime"} =  $mtime;
    }
    if( $p3Exists)
    {
        print " copying ${dir_local}/${file} to ${node}:${dir_remote3} \n";
        $status = !system( "scp ${dir_local}/${file} root\@${node}:${dir_remote3} >/dev/null");
        if( !$status)
        {
            goto finish;
        }
        $f{ "${node}_${file}_mtime"} =  $mtime;
    }
}

 finish:

if( $flagTie)
{
    untie %f;
}

if( $flag_upToDate)
{
    print " --- scripts/Update_files.pl: all files are up-to-date on $node\n"; 
}
else
{
    print " --- scripts/Update_files.pl: done on $node, status $status \n"; 
}

!$status;

