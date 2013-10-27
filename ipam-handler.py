#!/usr/bin/python3

import logging as log
import sys
from optparse import OptionParser
from ipam import ipam 

loglevel=log.DEBUG
#loglevel=log.INFO

def init():
    p = OptionParser(usage="usage: %prog [options]")
    p.add_option(
        "--init", action="store_true", dest="init", default=False,
        help="initialize database")
    p.add_option(
        "-l", "--logfile", dest="logfile", default=logfile,
        help="write output to logfile default=" + logfile ) 
    p.add_option(
                "-e", action="store_true", dest="raise_exception", default=False,
                help="raise an exception if set")
    (options, args) = p.parse_args()

    log.basicConfig(
        filename=options.logfile, level=loglevel,
        format = '%(asctime)s %(levelname)s\t[%(filename)s:%(lineno)d]\t%(message)s', datefmt='%Y-%m-%d/%H:%M:%S' )
    return options

def main():
    #options = init()
    BaseNetwork = { "ipsec-unlimited":"10.210.0.0/16", "ipsec-restricted":"10.211.0.0/16", "sslvpn":"10.211.0.0/16" }
    print("Hello World")
    try:
        for data in sys.stdin:
            log.info("received data: {}".format(data))    
            data = data.split()
            func = data[0]
            root = data[1]
            args = data[2:]
            if root in BaseNetwork: 
                db   = ipam(root + ".sqlite3")
                root = BaseNetwork[root] 
            else:
                print("unknown root network {}".format(root))
                        
            result = "not implemented"
            if func == "init":          result = db.init_db(root)    
            if func == "dump":          result = db.dump(("net_frag",))
            if func == "alloc_net":     result = db.alloc_net(int(args[0]))[1]
            if func == "free_net":      result = db.free_net(args[0])[3:]
            
            if result == "not implemented": raise RuntimeError("{} not implemented".format(func))
            log.info("{} => {}".format(data,result))
            print("ok {}".format(result))
            return 0
                        
    except Exception as ex:
        log.exception(ex)
        print("error {}".format(ex))
        return 99

    
if __name__ == '__main__':
    log.basicConfig(
        filename="ipam.log", level=loglevel,
        format = '%(asctime)s %(levelname)s\t%(message)s', datefmt='%Y-%m-%d/%H:%M:%S' )
    [ log.error("... START ...") for i in range(27) ]
    sys.exit(main())
