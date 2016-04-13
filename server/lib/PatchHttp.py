# -*- coding: utf-8 -*-
__author__ = 'fjs'

#############################################################
# 用于打包httplib的相关东西，这样子可以在gevent环境下来使用
# urllib 和 urllib2
#############################################################

from gevent import socket
import httplib
import urllib2


def _patch():
    setattr(httplib, "socket", socket)
    try:
        from gevent import ssl
        setattr(httplib, "ssl", ssl)
    except:
        pass


_patch()                                                       # 这里默认就直接将patch打上


def post(url, headers={}, body=""):
    """
    用于发送post方法的http请求，然后返回服务器的返回数据
    :param url:        请求的路径
    :param headers:    请求附加的http头部
    :param data:       请求的body数据
    """
    req = urllib2.Request(url, data=body)
    for k, v in headers.items():
        req.add_header(k, v)
    res = urllib2.urlopen(req)
    out = res.read()
    res.close()
    return out


def get(url, headers={}):
    """
    用于发送get方法的http请求，然后返回服务器返回的数据
    :param url:        请求的url
    :param headers:    请求带的http头部
    """
    req = urllib2.Request(url)
    for k, v in headers:
        req.add_header(k, v)
    res = urllib2.urlopen(req)
    out = res.read()
    res.close()
    return out

