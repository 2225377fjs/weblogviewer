__author__ = 'fjs'


import gevent
import time


a = "123"

b = a[3:]
if b:
    print "aaaa"
else:
    print "nonono"