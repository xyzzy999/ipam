#!/usr/bin/python3

import logging as log
import sys
from optparse import OptionParser

loglevel=log.DEBUG
#loglevel=log.INFO
logfile="/home/madmin/python-examples/logging/example.log"

dump   = lambda x: log.info( "dump({})".format(x) )
initdb = lambda x: (log.info( "initdb()" ) , 1/x )

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
    options = init()

    try:
        for data in sys.stdin:
            data = data.split()
            msg = "received data: {}".format(data)
            log.info(msg)    
            func = data[0]
            args = data[1:]

            if func == "dump": return dump(args)
            if func == "init": return initdb(int(args[0]))
            raise RuntimeError("illegal function called: " + func )
            return 1
                        
    except Exception as ex:
        log.exception(ex)
        print(ex)
        # raise
        return 1
    else:
        log.info("alles ok")
        return 0
    finally:
        print('In finally block for cleanup')

if __name__ == '__main__':
    sys.exit(main())
