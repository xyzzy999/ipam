#!/usr/bin/python3

import sqlite3
import logging as log
import inspect
import sys

from ipstuff import ip_address
from ipstuff import ip_network
from ipstuff import base    
from ipstuff import bits    
from ipstuff import prefix  
from ipstuff import network 
from ipstuff import parent  
from ipstuff import net_bits
from ipstuff import sibling
from ipstuff import merge

from decorator import dump_args

DB_NAME = "mydb.sqlite3"

is_alloc = lambda self, ip, prefix: self.exists_net( ip, prefix, 1)
is_free  = lambda self, ip, prefix: self.exists_net( ip, prefix, 0) 

class ipamError(Exception): 
    def __init__(self, t=""): 
        self.text = t 
 
    def __str__(self): 
        return self.text

class ipam:
    @dump_args()
    def __init__(self, db=DB_NAME ):
        self.conn = sqlite3.connect(db)
        self.cur  = self.conn.cursor()
        #self.init_db()
        try:
            self.get_root()
        except:
            pass
            
    @dump_args()    
    def init_db(self,root):
        try:
            self.cur.execute('''Drop Table if exists net_root''')
            self.cur.execute('''Create Table net_root( key integer primary key, net text, first text, last text, prefix integer )''')

            self.cur.execute('''Drop Table if exists net_frag''')
            self.cur.execute('''Create Table net_frag( key integer primary key, net text, prefix integer, alloc bool)''')

            self.cur.execute('''Drop Table if exists ip_addr''')
            self.cur.execute('''Create Table ip_addr( key integer primary key, ip text, net_frag_key integer)''')
            
            self.add_root(root)
            self.get_root()
            
        except:
            raise
        
        self.conn.commit()

    @dump_args()   
    def add_root(self, net):
        try:
            self.cur.execute('SELECT count(*) FROM net_root')
            if self.cur.fetchone()[0] != 0: 
                raise ipamError("only one root allowed")
            net = ip_network(net)
            first = str(net[0])
            last  = str(net[-1])
            p     = prefix(net)
            net   = str(net)

            self.cur.execute('INSERT INTO net_root (net, first, last, prefix) VALUES (?,?,?,?)',(net, first, last, p) )        
            self.cur.execute('INSERT INTO net_frag (net, prefix, alloc )      VALUES (?,?,0)',  (net, p) )

        except Exception:
            raise
        
        else:
            self.conn.commit()

    @dump_args() 
    def get_root(self):
        try:
            self.cur.execute('SELECT net, first, last, prefix FROM net_root')
            (self.net, self.ip_first, self.ip_last, self.prefix) = self.cur.fetchone()    
            return (self.net, self.ip_first, self.ip_last, self.prefix)

        except:
            raise            

    @dump_args() 
    def row(self, key, table):
        try:
            self.cur.execute('SELECT * FROM ? WHERE key=?',(table, key) )    
            return self.cur.fetchone()

        except:
            raise
    
    @dump_args("debug")         
    def make_net(self, pref, ip=False ):
        if ip == False:
            for p in range( pref, self.prefix-1, -1):
                try:
                    self.cur.execute('SELECT key, net, prefix, alloc FROM net_frag WHERE prefix=? and alloc=0', (p,) )
                    (key, n, p, a) = self.cur.fetchone()
                    return self.make_net( pref, base(n) )
                except TypeError:
                    continue
                    
                except:
                    raise

        else:
            net = network(ip,pref)
            if ip_address(self.ip_first) <= net[0] and net[-1]  <= ip_address(self.ip_last):
                try:
                    self.cur.execute('SELECT key, net, prefix, alloc FROM net_frag WHERE net=? and prefix=?', (str(net),pref) )
                    (key, n, p, a) = self.cur.fetchone()
                    if a == 1: raise ipamError("make_net " + net + "already allocated" )
                    return (key, n, p, a)

                except TypeError: 
                    (key, n, p, a) = self.make_net(pref-1, base(parent(net))) 
                    if a == 1: raise ipamError("make_net " + net + "already allocated" )
                        
                    try:    # split parent(net) !!!!!!!!
                        n1 = base(parent(net))
                        n1 = network(n1,pref)
                        
                        n2 = base(n1,1)
                        n2 = network(n2,pref)

                        # log.debug("split n1={} n2={} net={} pref={}".format(n1,n2,net,pref))
                        self.cur.execute('UPDATE net_frag SET net=?, prefix=? WHERE key=?',(str(n1),pref,key) )
                        self.cur.execute('INSERT INTO net_frag (net, prefix, alloc ) VALUES (?,?,0)', (str(n2), pref) )
                        self.conn.commit()
                        self.cur.execute('SELECT key, net, prefix, alloc FROM net_frag WHERE net=? and prefix=?', (str(net),pref) )
                        return self.cur.fetchone()
                    except:
                        raise
                    
            else:     
                raise ipamError("make_net " + str(net) + " not in root_net" )
           
    @dump_args()         
    def alloc_net(self, pref, ip=False ):
        try:
            self.make_net(pref, ip)
            if ip == False:
                self.cur.execute('SELECT key, net, prefix, alloc FROM net_frag WHERE prefix=? and alloc=0', (pref,) )
                ip = "*"
            else:
                net = network(ip,pref)
                self.cur.execute('SELECT key, net, prefix, alloc FROM net_frag WHERE net=? and prefix=?', (str(net),pref) )

            (key, n, p, a) = self.cur.fetchone()
            if a == 1: 
                raise ipamError("internal error while allocating fragment")
            self.cur.execute('UPDATE net_frag SET alloc=1 WHERE key=?',(key,) )
            self.conn.commit()
            self.cur.execute('SELECT key, net FROM net_frag WHERE key=?', (key,) )
            return self.cur.fetchone()
        
        except ipamError as iE:
            raise iE

        except:
            raise ipamError("could not allocate net: " + str(ip) + "/" + str(pref) )
    
    @dump_args("debug") 
    def merge_net(self, net):
        try:
            p = prefix(net)
            sib = sibling(net) 
            self.cur.execute('SELECT count(*) FROM net_frag WHERE (net=? OR net=?) AND alloc=0', (str(sib[0]), str(sib[1]),) )
            (n,) =  self.cur.fetchone()
            #log.debug( "merge_net -------------- sib={} n={}".format(sib, n) )
            if n != 2: 
                return "ok nothing to merge" 
            big = merge(sib) 
            self.cur.execute('UPDATE net_frag SET prefix=?, net=? WHERE net=?',(p-1, str(big), str(sib[0]),) )
            self.cur.execute('DELETE from net_frag WHERE net=?',(str(sib[1]),) )
            self.conn.commit()
            self.merge_net(big)
            return "ok merged sib={} to {}".format(sib,big)
        
        except:
            raise
    
    
    @dump_args() 
    def free_net(self, net):
        try:
            self.cur.execute('SELECT key, net, prefix, alloc FROM net_frag WHERE net=? and alloc=1', (str(net),) )
            (key, n, p, a) = self.cur.fetchone()
            if p != prefix(net):
                raise ipamError("free_net internal error, Database corrupted" )
            self.cur.execute('UPDATE net_frag SET alloc=0 WHERE key=?',(key,) )
            self.conn.commit()
            self.merge_net(net)
            return "ok " + net + " is free"

        except TypeError:
            raise ipamError("free_net(" + str(net) + "): net not allocated" )
            
        except:
            raise
    
    @dump_args("debug") 
    def _net2key(self, net):
        try:
            self.cur.execute('SELECT key FROM net_frag WHERE net=? AND alloc=1', (net,) )
            (key,) = self.cur.fetchone()
            return key
        except TypeError:
            raise ipamError("count_ip " + str(net) + ": net not allocated" )
    
    @dump_args("debug") 
    def _is_allocated_ip(self, ip):
        try:
            self.cur.execute('SELECT count(*) from ip_addr WHERE ip=? ',(str(ip),) )
            (hits,) = self.cur.fetchone()
            if( hits == 1):
                self.cur.execute('SELECT key from ip_addr WHERE ip=?', (str(ip),) )
                (ip_key,) = self.cur.fetchone()
                return ip_key
            elif (hits == 0):
                return -1
            else:
                raise ipamError("database error: " + str(ip) +" allocated multiple times" )
        except:
            raise 
        
    @dump_args("debug") 
    def get_ip(self, net):
        try:
            for ip in ip_network(net).iterhosts():
                if self._is_allocated_ip(ip) == -1:
                    return ip
            raise ipamError("get_ip({}) network is full".format(net))
        
        except:
            raise 
        
    @dump_args() 
    def alloc_ip(self, net, ip=None):
        try:
            ip = self.get_ip(net) if ip == None else ip
            net_key = self._net2key(net)
            if self._is_allocated_ip(ip) == -1: 
                self.cur.execute('INSERT INTO ip_addr (ip, net_frag_key) VALUES (?,?)', (str(ip), net_key) )
                return ip
            else:
                raise ipamError("alloc_ip({}) IP already allocated".format(ip))
        except:
            raise 
        
    @dump_args() 
    def free_ip(self, ip):
        try:
            ip_key = self._is_allocated_ip(ip)
            if ip_key != -1: 
                self.cur.execute('DELETE from ip_addr WHERE key=?', (ip_key,) )
            else:
                raise ipamError("free_ip({}) ip not allocated".format(ip))
        except:
            raise     
        
    @dump_args() 
    def count_ip(self, net, alloc=False):
        net_key = self._net2key(net)
        try:   
            self.cur.execute('SELECT count(*) FROM ip_addr WHERE net_frag_key=?', (net_key,))
            (alloc_count,) = self.cur.fetchone()
            return alloc_count if alloc == True else ip_network(net).numhosts-alloc_count
        except:
            raise     
    
    @dump_args() 
    def list_ip(self, net, alloc=False, nmax=-1):
        net_key = self._net2key(net)
        try:
            if alloc == False:
                ip_list = [ ip for ip in ip_network(net).iterhosts() if self._is_allocated_ip(ip) == -1 ]
            else:
                ip_list = [ ip for ip in ip_network(net).iterhosts() if self._is_allocated_ip(ip) != -1 ] 
            
            if nmax == -1:
                return ip_list
            
            nmax = min(nmax,len(ip_list))
            return ip_list[:nmax]
        
        except:
            raise     
    
    @dump_args() 
    def dump(self,what=( "net_root", "net_frag", "ip_addr") ):
        for table in what:
            log.info( table + " dump =======================" )
            query = "SELECT * FROM " + table
            self.cur.execute(query)
            [ log.info(row) for row in self.cur.fetchall() ]

    @dump_args() 
    def print_alloc(self):
        self.cur.execute('SELECT * FROM net_frag WHERE alloc=1')
        print( "allocated networks:")
        print( [ row for row in self.cur.fetchall() ] )

    @dump_args()
    def close(self):
        self.conn.close()
        self.conn = 0


if __name__ == "__main__":
    import random    
    loglevel=log.DEBUG
    #loglevel=log.INFO
    log.basicConfig(
        filename="ipam.log", level=loglevel,
        format = '%(asctime)s %(levelname)s\t%(message)s', datefmt='%Y-%m-%d/%H:%M:%S' )
    [ log.error("... START ...") for i in range(27) ]
    try:
        db = ipam()
        db.init_db('10.200.0.0/16')    
        #db.add_root(ip_network('2001:0db8:1234::/48'))
        db.get_root()
        db.dump( ("net_frag",) )
        
        n = 23
        n1 = db.alloc_net(n)[1]
        n2 = db.alloc_net(n)[1]
        db.dump(("net_frag",))
         
        print("n1=",n1)
        print("allocated_hosts=",db.count_ip(n1,True))
        print("free hosts=",db.count_ip(n1))
        
        for i in range(9): print("allocate ip=", db.alloc_ip(n1))
        print("list_ip",db.list_ip(n1))   
        print("list_ip",db.list_ip(n1,nmax=5))
        print("list_ip",db.list_ip(n1,alloc=True))  
        print("allocated_hosts=",db.count_ip(n1,True))
        print("free hosts=",db.count_ip(n1))
        db.free_ip("10.200.0.4")
        db.free_ip("10.200.0.6")
        print("allocated_hosts=",db.count_ip(n1,True))
        print("free hosts=",db.count_ip(n1))
        print("list_ip",db.list_ip(n1,alloc=True))  
        #db.free_ip("10.200.0.6")
        
           
        ip1 = "10.200.0.3"
        print("allocate ip=", db.alloc_ip(n1,ip1))
        print("allocate ip=", db.alloc_ip(n1,ip1))
        
        
        
        raise 
         
        db.free_net(n1)
        db.dump(("net_frag",))
        db.free_net(n2)
        db.dump(("net_frag",))
            
        rangelist = [ random.randint(29,30) for i in range(50) ]
        print( rangelist )

        netlist = [ db.alloc_net(n)[1] for n in rangelist ]
        db.print_alloc()

        random.shuffle(netlist)
        print( "netlist =", netlist )
        [ db.free_net(n) for n in netlist ]
        # [ db.free_net(n) for n in netlist ]
        print( "sollte leer sein .............." ) 
        db.print_alloc()
        print( "ende" )


        random.shuffle(netlist)
        newnet = [ db.alloc_net(prefix(n),base(n))[1] for n in netlist ]
        # [ db.alloc_net(prefix(n),base(n))[1] for n in netlist ]

        random.shuffle(newnet)
        newnet += [ db.alloc_net(n)[1] for n in rangelist ]

        db.print_alloc()
        db.dump(("net_frag",))
        random.shuffle(newnet)
        print( newnet )
        [ db.free_net(n) for n in newnet ]
        print( "sollte leer sein .............." )
        db.print_alloc()
        db.dump(("net_frag",))
        print( "ende" )

    except Exception as ex:
        log.info("################################ ultra krasser exception catcher at work ####################################")
        log.exception(ex)
        print("fatal error:", ex)
        # raise
        return_val = 1

    else:
        log.info("done")
        return_val = 0

    db.close()
    sys.exit(return_val)
