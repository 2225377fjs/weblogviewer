# -*- coding: utf-8 -*-
__author__ = 'fjs'
from multiprocessing import Process
import signal
import os
import time
import sys

import setproctitle
import gevent.event
import gevent

import Worker
from config import ServerConfig
import lib.TcpConnector
import lib.Service


#############################################################################
# main进程，主要是实现worker进程的管理
#
# 主进程会在每一个工作进程启动的时候都临时启动一个9999端口的tcp服务，子进程通过这个tcp
# 服务来告知主进程当前工作进程是否启动成功
#############################################################################



class Main(object):
    def __init__(self, name):
        object.__init__(self)
        self._pid_work = dict()                   # 用于保存pid与worker进程之间的对应关系
        self._works = list()                       # 用于保存所有启动了的子进程的信息
        self._stoped = True                       # 默认刚开始这个标志位为True
        self._name = name                         # 当前主进程的名字
        self._tcp_connector = lib.TcpConnector.SelectTcpConnector(9999)   # 用于子进程启动成功之后通知父进程
        main = self
        self._ok_event = gevent.event.Event()     # 启动了子进程之后，将会在当前event上等待子进程的消息
        self._ok = False                          # 用于标记子进程是否启动成功
        self._daemon = False

        class _OKService(lib.Service.ServiceBean):
            def __init__(self):
                lib.Service.ServiceBean.__init__(self, "main_ok")

            def do_service(self, context, message):
                """
                如果子进程发送来ok，那么表示子进程已经启动成功了
                """
                if message == "ok":
                    main._ok = True
                else:
                    main._ok = False
                main._ok_event.set()
        self._tcp_connector.add_service(_OKService())

    def set_daemon(self, value):
        self._daemon = value

    def _child_dead(self, *args, **kwargs):
        """
        在子进程退出的时候要做的事情，这里需要判断一下当前main的状态，如果已经停止了
        那么这里就直接返回就行了，不做任何处理

        注意：如果子进程退出了，但是当前main进程并没有关闭，那么需要执行一些逻辑
             写日志，调用对应worker对象的on_exit方法，并清理一些资源
        """
        pid, _ = os.wait()
        if self._stoped:
            return
        worker = self._pid_work.get(pid)
        if worker is not None and isinstance(worker, Worker.Worker):
            self._works.remove(worker)
            del self._pid_work[pid]
            self._write_main_log("子进程退出：pid : " + str(pid))
            self._write_main_log("子进程名字:" + worker.name)
            try:
                worker.on_exit()
            except:
                pass

    def _exit(self, *args, **kwargs):
        """
        对于主进程收到了kill，那么默认行为是调用stop来停止
        """
        self.stop()

    def _main_loop(self):
        """
        这里实现很简单，不断的sleep就好了，直到stop标志位被设置就好了
        """
        print ""
        print "########################################################"
        print "server start success"
        print "########################################################"
        if self._daemon:
            print ""
            print "----------------------------------------------------------"
            print "server going to daemon"
            print "----------------------------------------------------------"

            sys.stdout.flush()
            sys.stderr.flush()
            si = file("/dev/null", 'r')
            so = file("/dev/null", 'a+')
            se = file("/dev/null", 'a+', 0)
            os.dup2(si.fileno(), sys.stdin.fileno())
            os.dup2(so.fileno(), sys.stdout.fileno())
            os.dup2(se.fileno(), sys.stderr.fileno())

        while not self._stoped:
            time.sleep(5)

    def start(self):
        """
        (1)注册信号处理
        (2)设置stoped的标志位为False，表示当前已经启动了
        (3)建立子进程，创建子进程来启动worker
        (4)执行子类实现的do_start()逻辑，然后进入mainloop，等待事件的发生
        """
        setproctitle.setproctitle(self._name)
        signal.signal(signal.SIGCHLD, self._child_dead)
        signal.signal(signal.SIGTERM, self._exit)
        self._stoped = False

        for worker in self._works:
            assert isinstance(worker, Worker.Worker)
            worker.set_daemon(self._daemon)
            self._ok_event.clear()                   # 清除event的状态
            self._tcp_connector.stop()               # 在启动子进程之前关闭监听，防止子进程继承这个监听

            now_p = Process(target=worker.start)
            now_p.start()

            self._tcp_connector.start()              # 启动main进程的tcp监听
            self._ok_event.wait()                    # 在事件上等待，等待子进程的启动返回结果
            if self._ok:
                print ""
                print "*******************************************************************"
                print "process start success, process name is : %s" % worker.name
                print "*******************************************************************"
                self._pid_work[now_p.pid] = worker
                worker.pid = now_p.pid
            else:                                    # 如果有一个进程启动失败，那么直接停止整个系统
                print ""
                print "################################################################"
                print "process start fail, process name is : %s" % worker.name
                print "system will exit"
                print "################################################################"
                self.stop()
                return
            gevent.sleep(0.5)
        self._tcp_connector.stop()
        self.do_start()
        self._main_loop()

    def restart_worker(self, worker):
        """
        这个事留个worker对象用于重启一个子进程来用的接口
        这里要做的事情就是直接创建一个子进程启动worker，然后将worker对象保存起来
        :type worker: Worker.Worker
        """
        if self._stoped:
            return
        now_p = Process(target=worker.start)
        now_p.start()
        self._pid_work[now_p.pid] = worker
        worker.pid = now_p.pid
        self._works.append(worker)
        self._write_main_log("重启了worker:" + worker.name)

    def do_start(self):
        """
        对于主进程，应该实现这个方法来实现一些启动执行
        """
        pass

    def stop(self):
        """
        这里默认的行为就是关闭所有启动的worker进程
        """
        if self._stoped:
            return
        self._stoped = True
        for worker in self._works:
            assert isinstance(worker, Worker.Worker)
            worker.stop()

    def add_worker(self, worker):
        """
        加入一个需要管理的worker对象
        """
        assert isinstance(worker, Worker.Worker)
        worker.main = self
        self._works.append(worker)

    def _write_main_log(self, message):
        """
        因为一些特殊的考虑，所以这里main进程的log会比较的特别
        """
        log_path = ServerConfig.MAIN_LOG_DEV
        with open(log_path, "a") as file_fd:
            file_fd.writelines(message + "\n")




