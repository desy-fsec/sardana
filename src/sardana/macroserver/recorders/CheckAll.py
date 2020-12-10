#!/usr/bin/env python
#
# compare the files in Files.lis with the versions on
# the remote hosts
#
import HasyUtils
import os

HOST_LIST = "/afs/desy.de/group/hasylab/Tango/HostLists/TangoHosts.lis"

def main():

    nodes = HasyUtils.readHostList( HOST_LIST)

    sz = len( nodes)
    count = 1
    countFailed = 0
    for host in nodes:
        if not HasyUtils.checkHostRootLogin( host):
            print( "-- checkHostRootLogin returned error %s" % host)
            countFailed += 1
            continue

        if os.system( "./Check_files.pl %s" % host):
            print( "Failed to Check_files %s" % host)
            countFailed += 1
            continue
        print( "CheckAll: %d/%d (failed %d) %s " % (count, sz, countFailed, host))
        count += 1
    return


if __name__ == "__main__":
    main()
