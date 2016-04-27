# -*- coding: utf-8 -*-
__author__ = 'fjs'
import gevent.monkey
gevent.monkey.patch_all()                          # 最开始的时候打包所有的模块
gevent.monkey.patch_all(subprocess=True)           # 在1.1版本之前，subprocess模块是不打包的

import sys


sys.path.append("./server")
sys.path.append("./app")

import worker.Main
import worker.Worker
import sys
import worker.ConfigWorker
import os
from app_worker.LogViewWorker import LogViewWorker
from app_worker.LogCenterWorker import LogCenterWorker
from config.ServerConfig import HTTP_PORT

reload(sys)
sys.setdefaultencoding('utf-8')


def _init_workers(main):
    """
    这里从worker的定义文件中获取数据，然后创建相应的worker对象
    并将创建的worker对象加入到main当中去
    :type main: worker.Main.Main
    """
    main.add_worker(worker.ConfigWorker.ConfigWorker("0.0.0.0", 5678))                # 配置中心进程
    config_address = {"ip": "127.0.0.1", "port": 5678}

    log_center_worker = LogCenterWorker(5670, config_address)                         # LogCenter进程
    main.add_worker(log_center_worker)

    log_view_worker = LogViewWorker(config_address, 7890, HTTP_PORT, HTTP_PORT + 50)  # web进程
    main.add_worker(log_view_worker)


class Start(worker.Main.Main):
    """
    主进程的定义，这里貌似没有做什么事情，也就设置了一个名字
    """
    def __init__(self, name):
        worker.Main.Main.__init__(self, name)

    def do_start(self):
        self._write_main_log("主进程启动")

if __name__ == "__main__":
    """
    这里如果在启动的时候带后daemon参数，那么表示进程需要后台启动
    """
    start = Start("log_main")
    _init_workers(start)

    args = sys.argv
    if len(args) > 1 and args[1] == "daemon":
        start.set_daemon(True)
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
        else:
            start.start()
    else:
        start.start()


