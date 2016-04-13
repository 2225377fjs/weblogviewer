__author__ = 'fjs'
# -*- coding: utf-8 -*-


from bean.Entity import Entity
from gevent.lock import RLock
import subprocess
from collections import deque
from lib.LetPool import run_task
from bean.Entity import rpc_method
from lib.TimeUtil import get_time
import gevent
import logging


GLOBAL_ID = 0       # 计数器，用于为LogSender分配entityID
MOST_TIME = 60      # 最长超过这么多秒都没有访问get_data方法的话，就可以释放资源了


def _create_id():
    """
    用于为LogSender分配ID
    """
    global GLOBAL_ID
    GLOBAL_ID += 1
    if GLOBAL_ID > 100:
        GLOBAL_ID = 1
    return str(GLOBAL_ID)



class LogGetter(object):
    """
    用于创建一个子进程来tail 需要监控的文件，然后在协程中不断的通过pip来读取数据，将其放到队列里面等待
    别的进程来获取
    """
    def __init__(self, log_path):
        self._log_path = log_path
        self._started = False
        self._lock = RLock()
        self._process = None
        self._datas = deque()

    def get_data(self):
        with self._lock:
            if not self._started:
                self._start()
            if self._datas:
                out = self._datas
                self._datas = deque()
                return out
            else:
                return None

    def stop(self):
        """
        用于停止子进程
        """
        if self._started:
            self._started = False
            self._process.kill()

    def _start(self):
        """
        启动一个子进程，然后在协程中不断的通过pip来获取数据，将获取的到数据放到队列中

        注意：如果数据过多，都超过了100行，那么会直接移除，用于保证数据做多100行
        """
        p = subprocess.Popen(["tail", "-f", self._log_path], stdout=subprocess.PIPE)
        self._process = p
        self._started = True

        def _run():
            while self._started:
                try:
                    data = p.stdout.readline()
                    if not data:
                        break
                    self._datas.append(data)
                    if len(self._datas) > 100:
                        self._datas.popleft()
                except:
                    break
            if self._started:
                self._started = False
                try:
                    p.kill()
                except:
                    pass

        run_task(_run)


#
# 远端会要求在Node节点上创建一个这个对象，用于远端通过rpc方法来获取日志的数据
#
class LogSender(Entity):
    def __init__(self, log_path):
        """
        远程会要求log节点进程创建一个这种entity，用于远程通过rpc方法来获取监控的日志的数据

        注意：为了防止一些意外，会启动一个定时器，检测上次访问时间，如果很长时间都没有访问了，那么会自动
             停止LogGetter对象，然后清除当前entity，主要为了在一些异常情况下能够关闭开启了的子进程，释放占用的资源
        :param log_path:    日志的路径
        """
        Entity.__init__(self, _create_id())
        self._log_path = log_path
        self._getter = LogGetter(log_path)
        self._last_time = get_time()                        # 用于记录上一次访问时间
        self._check_timer = gevent.get_hub().loop.timer(40, 40)
        self._check_timer.start(self._check)                # 用于检测上次访问时间的timer

    @rpc_method()
    def get_data(self):
        """
        远端会不断的调用这个方法来获取当前日志的最新数据用于在web界面上显示
        """
        self._last_time = get_time()      # 更新该方法的上次访问时间
        return self._getter.get_data()

    @rpc_method()
    def close(self):
        """
        一般情况下如果websocket断开了，那么会请求这个方法来释放资源，反正以后都不能用了
        """
        self._do_close()

    def _do_close(self):
        """
        用于释放资源
        """
        logging.info("LogSender release, id:%s", self.id)
        self.release()
        self._check_timer.stop()
        self._getter.stop()

    def _check(self):
        """
        用于在定时器中运行，通过检测上次访问时间来及时的释放资源
        """
        now = get_time()
        if now - self._last_time >= 60:
            self._do_close()


