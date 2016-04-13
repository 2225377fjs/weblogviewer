# -*- coding: utf-8 -*-
__author__ = 'fjs'

import worker.Worker
import bean.LogBean
from lib.TcpConnector import SelectTcpConnector
import bean.Entity
from bean.Entity import GlobalEntityManager


#
# entity类型的worker的父类，可以继承这里，然后在init_entity方法中实现entity的初始化功能
#
class EntityWorker(worker.Worker.Worker):
    def __init__(self, name, work_id, port, config_address, regist_address=None, need_confirm=True):
        """
        :param name:            当前进程的名字
        :param work_id:         当前进程的id
        :param port:            用于进程间服互相调用的connector的监听地址
        :param config_address   config进程的监听地址
        :param regist_address   主要是用于多机器部署的时候，在config上面注册的地址
        """
        worker.Worker.Worker.__init__(self, name, work_id, need_confirm=need_confirm)
        self._port = port                                            # 当前进程监听地址
        self._config_address = config_address                        # config进程的监听地址{"ip": "", "port": 123}
        self._gm = None                                              # 当前进程的entity管理器
        self._reg_address = regist_address

    def do_start(self):
        bean.LogBean.DateLogger(self.worker_id)
        connector = SelectTcpConnector(self._port, regist_address=self._reg_address)  # 别的进程通过与这里建立连接来调用当前进程的entity
        connector.start()
        self._gm = GlobalEntityManager(self._config_address, tcp_con=connector)
        self.init_entity()

    def init_entity(self):
        """
        子类通过实现这个方法来完成entity的创建和初始化过程
        """
        pass



