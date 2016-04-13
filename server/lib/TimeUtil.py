# -*- coding: utf-8 -*-
__author__ = 'fjs'


import gevent
import time
import datetime


#
# 一个简单的时间模块，主要是提供一下低精度的时间，毕竟time.time也是系统调用
#

_NOW_TIME = time.time()


def _update():
    global _NOW_TIME
    _NOW_TIME = time.time()

gevent.get_hub().loop.timer(0, 2).start(_update)   # 每两秒钟更新一下时间


def get_time():
    """
    获取低精度的时间，单位秒
    """
    return _NOW_TIME


def get_real_time():
    return time.time()


def get_date():
    pass


