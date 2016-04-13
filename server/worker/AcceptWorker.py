# -*- coding: utf-8 -*-
__author__ = 'fjs'

from worker.Worker import Worker
from lib.Accepter import Accepter


#
# 一个基础的专门用于获取客户端的进程类型
#
class AcceptWorker(Worker):
    def __init__(self, worker_name, worker_id, listen_path, front_port):
        """
        :param worker_name:   进程的名字
        :param worker_id:     进程的id
        :param listen_path:   建立unix监听的路径
        :param front_port:    建立客户端监听的端口
        """
        Worker.__init__(self, worker_name, worker_id)
        self._path = listen_path
        self._port = front_port

    def do_start(self):
        """
        启动其实很简单，就是建立一个AccepterBean对象，然后启动它就可一了
        """
        Accepter(self._path, self._port).start()

