# -*- coding: utf-8 -*-
__author__ = 'fjs'

############################################################
# 与每一个FSocket对象绑定的上下文对象
############################################################


class Context(object):
    def __init__(self, sock):
        """
        构造函数，这里需要传进来相关联的sock对象，

        注意：这里会导致循环引用，sock引用了context，context又引用socket
             所以要注意sock外部在销毁的时候去掉引用环，降低gc工作量
        :type sock: FSocket.FSocket
        :param sock: 相关联连接
        :return:
        """
        self._sock = sock
        self._dict = dict()

    @property
    def sock(self):
        """
        获取关联的sock对象
        :rtype: FSocket.FSocket
        :return:
        """
        return self._sock

    def set_attr(self, key, value):
        """
        设置一个属性
        :type key: str
        """
        self._dict[key] = value

    def get_attr(self, key):
        """
        返回一个属性的值，这里如果没有这个属性的话会返回None
        :type key: str
        :return:
        """
        return self._dict.get(key)

    def remove_attr(self, key):
        """
        移除一个属性
        :type key: str
        """
        if key in self._dict:
            del self._dict[key]

    def release(self):
        """
        释放资源，这里将引用设置为None主要是防止循环引用，降低内存回收压力
        :return:
        """
        self._sock = None
        self._dict = None




