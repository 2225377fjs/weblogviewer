__author__ = 'fjs'
# -*- coding: utf-8 -*-


from bean.Entity import Entity
from bean.Entity import rpc_method
from app_entity.LogSender import LogSender

#
# 被监控的服务器进程的提供基本服务的entity，每一个被监控的服务器都应该启动一个进程
# 创建一个Node来对外提供服务
#



NODE_NAME = "l_no"


class Node(Entity):
    def __init__(self, node_name, log_infos):
        """
        :param node_name:    当前节点的名字
        :param log_infos:    需要监控的日志  {"name" : "/home/fjs/fjs.log"}  名字到路径的字典
        """
        Entity.__init__(self, NODE_NAME)
        self._node_name = node_name
        self._log_infos = log_infos

    @rpc_method()
    def ping(self):
        """
        在LogCenter里面会定期的调用这个方法来检测当前node节点是否还存活
        """
        return ""

    @rpc_method()
    def get_info(self):
        """
        返回当前节点的名字和所有配置的监控的日志的名字
        """
        return self._node_name, self._log_infos.keys()

    @rpc_method()
    def create_log_sender(self, log_name):
        """
        用于创建一个LogSender对象，用于远端获取log_name对应的日志的信息，
        这里需要返回刚刚创建的sender的entityID，用于远端调用
        """
        log_path = self._log_infos[log_name]
        sender = LogSender(log_path)
        return sender.id

