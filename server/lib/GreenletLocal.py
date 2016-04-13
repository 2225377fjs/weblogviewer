# -*- coding: utf-8 -*-
__author__ = 'fjs'

import gevent.greenlet
from gevent import getcurrent


#############################################################################################
# 用于实现保存当前协程本地变量的一些工具方法
#############################################################################################

def set_greenlet_local(key, value):
    getcurrent().__dict__[key] = value


def get_greenlet_local(key):
    return getcurrent().__dict__.get(key)


def remove_greenlet_local(key):
    if key in gevent.greenlet.getcurrent().__dict__:
        del getcurrent().__dict__[key]
