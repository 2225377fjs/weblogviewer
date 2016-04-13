# -*- coding: utf-8 -*-
__author__ = 'fjs'
import os
import signal
import setproctitle
import gevent
import logging
import traceback
import lib.FClient
import sys


##################################################################################
# 理论上来说，所有的worker都应该由这里扩展而来
##################################################################################
class Worker(object):
    def __init__(self, name, worker_id, need_confirm=True):
        object.__init__(self)
        self._name = name
        self._id = worker_id
        self._main = None                      # 用于保存主进程对象的引用
        self._pid = None                       # 如果当前worker已经有子进程启动，那么将会用这里来保存pid
        self._daemon = False
        self._need_confirm = need_confirm      # 用于标记是否需要向9999端口的住进程发送确定，如果是单进程启动那么不需要

    def set_daemon(self, value):
        self._daemon = value

    @property
    def worker_id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def main(self):
        return self._main

    @main.setter
    def main(self, value):
        """
        main属性的设置会在加入到主进程main对象的时候自动调用
        """
        self._main = value

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, value):
        """
        这个在启动了子进程之后，在main对象中会调用这个方法将pid传进来保存
        """
        self._pid = value

    def _main_loop(self):
        """
        如果需要通知的标志为被设置了，通知父进程，当前进程启动成功
        对于普通进程，如果整个系统在启动的时候是以后台启动的，那么需要将标准输出处理一下
        """
        if self._need_confirm:
            client = lib.FClient.create_share_client("127.0.0.1", 9999)
            client.request("main_ok", "ok")
            client.close()

        if self._daemon:
            sys.stdout.flush()
            sys.stderr.flush()
            si = file("/dev/null", 'r')
            so = file("/dev/null", 'a+')
            se = file("/dev/null", 'a+', 0)
            os.dup2(si.fileno(), sys.stdin.fileno())
            os.dup2(so.fileno(), sys.stdout.fileno())
            os.dup2(se.fileno(), sys.stderr.fileno())

        while True:
            gevent.sleep(10)

    ###########################################################
    # 所有的worer都必须要实现的两个方法，分别是启动和停止
    ###########################################################
    def start(self):
        """
        在子类中实现具体的启动逻辑，这个方法的执行实在创建的子进程中执行的
        这里需要子类扩展实现do_start方法来实现具体的启动

        如果启动失败了，需要直接通知父进程，启动失败

        注意：因为这里例如在重启进程之类的，可能会继承main进程的信号处理
             所以这里在启动的时候将这些信号处理屏蔽掉
        """
        _set_worker_id(self.worker_id)
        setproctitle.setproctitle(self.name)
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)       # 子进程退出的信号，直接不管
        signal.signal(signal.SIGTERM, signal.SIG_DFL)       # kill信号，退出
        try:
            self.do_start()
        except:
            logging.error("进程启动失败: " + get_worker_id())
            logging.error(traceback.format_exc())
            if self._need_confirm:
                client = lib.FClient.create_client("127.0.0.1", 9999)
                client.request("main_ok", "no")
                client.close()
            return
        logging.info("进程启动成功:   " + get_worker_id())
        self._main_loop()

    def do_start(self):
        """
        子类扩展这里来实现具体的启动逻辑
        """
        pass

    def stop(self):
        """
        这里默认是向子进程发送kill
        """
        if self.pid:
            os.kill(self.pid, signal.SIGTERM)

    def on_exit(self):
        """
        在子类中可以实现子进程的退出处理逻辑，例如重启什么的
        """
        pass


WORKER_ID = "ff3"


def _set_worker_id(worker_id):
    """
    在子进程启动的时候，将会调用这个方法，保存当前子进程的workerID
    """
    global WORKER_ID
    WORKER_ID = worker_id


def get_worker_id():
    """
    进程可以通过调用这个方法来获取当前进程所属worker的id
    """
    global WORKER_ID
    return WORKER_ID
