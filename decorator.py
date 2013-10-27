import logging as log

class dump_args:
    '''Decorator which helps to control what aspects of a program to debug
    on per-function basis. Aspects are provided as list of arguments.
    It DOESN'T slowdown functions which aren't supposed to be debugged.
    '''
    def __init__(self, level="info"):
        self.logger = log.info
        self.logger = log.debug if level == "debug" else self.logger
        
    def __call__(self, f):
            def fn(*args, **kwds):
                argn = f.__code__.co_varnames[:f.__code__.co_argcount]
                arg = ', '.join("{}={}".format(a[0],a[1]) for a in zip(argn,args) if a[0]!="self")
                self.logger("{}({})".format(f.__name__, arg))
                result = f(*args, **kwds)
                self.logger("{} returned {}".format(f.__name__, result) )
                return result
            fn.__doc__ = f.__doc__
            return fn
        
        
if __name__ == '__main__':        
    @dump_args()
    def prn(x):
        print( x )
    
    @dump_args()
    def mult(x, y):
        return x * y
    
    prn(mult(2, 2))