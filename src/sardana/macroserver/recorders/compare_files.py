#!/usr/bin/env python
import sys
import os
import argparse
import HasyUtils

REMOTE_DIR = "/usr/lib/python2.7/dist-packages/sardana/sardana-macros" \
    "/DESY_general"
HOST_LIST = "/afs/desy.de/group/hasylab/Tango/HostLists/TangoHosts.lis"
FILE_LIST = "/home/kracht/Tango/Sardana/sardana-macros.git/DESY_general" \
    "/Files.lis"


def main(hostName, fileName):
    #
    # if a host is offlines, that's not really bad
    #
    if not HasyUtils.isHostOnline(hostName):
        if not args.quiet:
            print("  %s is offline" % hostName)
        return True

    if os.system(
            "scp -q %s:%s/%s tempFile" % (hostName, REMOTE_DIR, fileName)):
        print("Failed to scp %s from %s" % (fileName, hostName))
        return False

    diff = os.popen("diff %s tempFile" % fileName).read()

    if len(diff) == 0:
        return True
    else:
        if not args.quiet:
            print("*** compare_files: differences host %s file %s "
                  % (hostName, fileName))
            print(diff)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="compare local and remote files",
        epilog='''\
Examples:
  compare_files.py hostName fileName
        compare single file on single host
  compare_files.py all fileName
        compare file on all hosts
  compare_files.py all all
        compare all files on all hosts
  compare_files.py -q all all
        display only differences
    ''')
    #
    # notice that 'pattern' is a positional argument
    #
    parser.add_argument('hostName', help='host name')
    parser.add_argument('fileName', help='file name')
    parser.add_argument(
        '-q', dest="quiet", action="store_true",
        help='report differences only')

    args = parser.parse_args()
    #
    # compare single files or the contents of files.lis
    #
    files = []
    if not args.fileName.lower() == 'all':
        files.append(args.fileName)
    else:
        files = HasyUtils.getListFromFile(FILE_LIST)
        if files is None:
            print("%s is empty" % FILE_LIST)
            sys.exit(255)
    #
    # compare files on single hosts or all
    #
    hosts = []
    if args.hostName.lower() != 'all':
        hosts.append(args.hostName)
    else:
        hosts = HasyUtils.getListFromFile(HOST_LIST)
        if hosts is None:
            print("%s is empty" % HOST_LIST)
            sys.exit(255)

    count = 0
    for host in hosts:
        count += 1
        if count % 10 == 0:
            print("%d/%d" % (count, len(hosts)))
        if not args.quiet:
            print("%s" % host)
        for fl in files:
            if not main(host, fl):
                print("***compare_files: trouble with %s on %s" % (fl, host))
            else:
                if not args.quiet:
                    print("  %s OK" % (fl))
