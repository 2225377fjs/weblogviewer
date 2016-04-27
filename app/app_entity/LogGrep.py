# -*- coding: utf-8 -*-
__author__ = 'fjs'

from bean.Entity import Entity, rpc_method
import subprocess
import gevent.timeout
import gevent
import sys
from lib.TimeUtil import get_time
from gevent.queue import Queue
import logging


GLOBAL_ID = 0


def _create_id():
    """
    用于为LogSender分配ID
    """
    global GLOBAL_ID
    GLOBAL_ID += 1
    if GLOBAL_ID > 999:
        GLOBAL_ID = 1
    return str(GLOBAL_ID) + "grep"


class Grep(object):
    """
    用于启动并管理grep的子进程
    """
    def __init__(self, log_path, content, timeout):
        self._log_path = log_path         # 文件路径
        self._time_out = timeout          # 子进程timeout
        self._content = content           # 需要grep的内容

        self._process = None              # 用于引用创建的子进程

        self._datas = Queue(maxsize=10)   # grep出来的数据将会放到这里来等待获取
        self._over = False                # 用于标记子进程是否已经执行完毕了
        self._is_time_out = False         # 用于标记子进程是否执行超时
        gevent.spawn(self._start)         # 在一个单独的协程中启动一个子进程

    def _start(self):
        """
        这个方法需要在一个协程中单独的执行，用于启动一个子进程，然后执行grep方法，并通过过标准输出来获取数据

        通过一个超时了控制子进程的运行时间，防止grep执行过久，占用太多系统资源，在超时之后要保证启动的grep进程被关闭
        """
        try:
            with gevent.timeout.Timeout(self._time_out):
                p = subprocess.Popen(["grep", self._content, self._log_path], stdout=subprocess.PIPE)
                self._process = p
                while 1:
                    line = p.stdout.readline()
                    if not line:                       # 这个一般是子进程退出了
                        break
                    self._datas.put(line)
        except:
            """
            如果发发生了异常，那么判断异常的类型，如果是超时异常，那么需要设置超时标志位
            """
            ex = sys.exc_info()[1]
            if isinstance(ex, gevent.Timeout):
                logging.error(u"grep执行超时 %s:%s", self._log_path, self._content)
                self._is_time_out = True
        finally:
            if self._process and self._process.returncode is None:
                self._process.kill()
            self._over = True

    def get_data(self):
        """
        获取已经grep出来的数据，如果没有数据，那么返回None，同时需要返回启动的子进程是否已经结束，是否是超时
        """
        if self._datas.qsize() > 0:
            out = self._datas
            self._datas = Queue(maxsize=10)
            out_data = []
            while out.qsize() > 0:
                out_data.append(out.get())
            return out_data, self._over, self._is_time_out
        else:
            return None, self._over, self._is_time_out


#
# 用于远端需要一个grep的功能的时候，将会创建这么一个entity，用于启动一个子进程来grep相应的文件
#
class LogGrep(Entity):
    def __init__(self, log_path, content, time_out=10):
        """
        :param log_path:   需要grep的文件路径，这里最好是使用绝对路径
        :param content:    需要grep的内容
        :param time_out:   子进程超时时间，防止grep消耗太多
        """
        Entity.__init__(self, _create_id())
        self._log_path = log_path
        self._time_out = time_out
        self._content = content
        self._last_time = get_time()                          # 记录上次访问时间

        # 创建一个定时器来检查上次访问时间，保证当前Entity一定会被释放
        self._check_timer = gevent.get_hub().loop.timer(20, 20)
        self._check_timer.start(self._check)

        self._grep = Grep(log_path, content, time_out)        # 代理这个的方法来获取grep的数据

    def _check(self):
        """
        在定时器中检查上次访问时间，长时间没有访问了，需要释放当前entity
        """
        if get_time() - self._last_time > 20:
            self.close()

    @rpc_method()
    def get_data(self):
        """
        远端会通过不断的调用这个方法来获取grep出来的数据和grep的状态
        """
        self._last_time = get_time()
        return self._grep.get_data()

    @rpc_method()
    def close(self):
        """
        远端可以调用这个方法来释放当前entity
        这里直接调用release方法，对于grep，反正超时之后都会自己关闭的，就不管了，释放引用就好了
        """
        self._grep = None
        self.release()
        self._check_timer.stop()
