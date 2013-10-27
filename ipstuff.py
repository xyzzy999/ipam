#!/usr/bin/python3
# -*- coding: utf-8 -*-

from ipaddr import ip_address
from ipaddr import ip_network

base    = lambda net,  i=0: ip_address( ip_network(net)[0] + i*ip_network(net).numhosts )
bits    = lambda ip:   32 if ip.version == 4 else 128
prefix  = lambda net:  int( str(net).split("/")[1] )
network = lambda ip,p: ip_network("{}/{}".format(str(ip),p)) 
parent  = lambda net:  merge(sibling(net))

def net_bits(ip):
    n = int(ip)
    if n == 0: return bits(ip)
    for p in range(bits(ip),0,-1):
        if n & 1 == 1 : return p
        n = n >> 1
        
# ein Netz kann entweder mit seinem (gleichgrossen) Vorgänger oder Nachfolger (sibling) zu einem doppelt so grossen 
# (prefix_neu = prefix-1) Netz "gemerged" werden
# mit dem Vorgänger wird gemerged wenn dieser weniger net_bits hat als das aktuelle Netz
# mit dem Nachfolger wird gemerged wenn das aktuelle Netz weniger net_bits hat als dieser
def sibling(net):
    p = prefix(net)
    ip_list      = [ base(net,-1), base(net), base(net,+1) ]
    prefix_list  = [ net_bits(ip) for ip in ip_list ]
    partner_list = [ ip_network("{}/{}".format(str(ip),p)) for ip in ip_list ]
    if prefix_list[0] < prefix_list[1]: return partner_list[:-1] 
    if prefix_list[1] < prefix_list[2]: return partner_list[1:] 
    raise ValueError("Error while looking merge partner")

def merge(sibling):
    if int(base(sibling[0])) == int(base(sibling[1],-1)):
        if prefix(sibling[0]) == prefix(sibling[1]):
            ip = base(sibling[0])
            p  = prefix(sibling[0])-1
            if p > 0 : 
                return network(ip, p)  
    raise ValueError("Error can't merge")

if __name__ == "__main__":
    import sys
    inp = sys.argv[1]
    
    n = ip_network(inp)
    print( base(n,-1) )
    print( base(n)    )
    print( base(n,+1) )
    print( bits(n), prefix(n) )
    print( sibling(n) )
    print( parent(n) )
    p=prefix(n)
    i=base(n) 
    [ print( network( i,q ) ) for q in range(bits(n),-1,-1) ]
