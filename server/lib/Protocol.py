# -*- coding: utf-8 -*-
__author__ = 'fjs'
from cPickle import dumps as cp_dumps
from cPickle import loads as cp_loads
from json import dumps as json_dumps
from json import loads as json_loads


#
# 用于tcpconnector的协议处理
#
class Protocol(object):
    def decode(self, data):
        """
        收到了一个完整的数据包之后，通过这个方法来将数据反序列化然后交给服务的路由层
        :param data:   一个完整的数据包
        :return servicename, data, rid
        """
        pass

    def encode(self, data):
        """
        在服务处理完之后，返回数据将通过这个方法来序列化，然后返回
        :param data:   服务返回的数据
        :return:
        """
        pass


#
# 基于python cpickle的协议处理
#
class PickleProtocol(Protocol):
    def decode(self, data):
        return cp_loads(data)

    def encode(self, data):
        return cp_dumps(data)


class JsonProtocol(Protocol):
    def decode(self, data):
        """
        因为json没有元组这种概念，所以传过来需要书数组类型的
        """
        message = json_loads(data)
        return message[0], message[1], message[2]

    def encode(self, data):
        return json_dumps(data)


