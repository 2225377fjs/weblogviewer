# -*- coding: utf-8 -*-
__author__ = 'fjs'
import gevent.greenlet
import gevent.event
import gevent.queue
from gevent.hub import Waiter
import collections
import logging
import traceback
from lib.TimeUtil import get_time
import sys

MAX_BLOCKING_TIME = 20                        # 业务最多可以阻塞20秒


#
# 用于构建服务端的协程池，对于业务的执行都直接派发到这个协程池中进行
# 实现对于业务阻塞情况的监控，防止一些业务因为一直阻塞导致协程不退出，从而造成内存泄漏
#
# 直接通过模块方法的形式暴露API
#
class _FjsGreenlet(gevent.greenlet.Greenlet):
    """
    对gevent包装过的的greenllet再进行一次包装，主要是让其在因为业务的执行而被调度出去的时候
    可以让上层协程池知道，从而可以让业务的执行阻塞在监控中
    """
    def __init__(self, task, pool, *args, **kwargs):
        gevent.greenlet.Greenlet.__init__(self, *args, **kwargs)
        self._pool = pool                             # 所属的协程池
        self._task = task                             # 当前执行的任务
        self._wait_task = Waiter()                    # 在没有任务的时候，在这个waiter上面等待，用于协程的调度
        self._loop = gevent.get_hub().loop            # gevent的主loop，用于在上面注册回调
        self._last_out_time = None                    # 当前协程因为业务阻塞被切换出去的时间
        self._some_time_block = False                 # 用这个标志为来确定这个协程在执行任务的时候是否曾经阻塞过
        self.start()                                  # 进入业务循环
        self._init_short_cut()

    def _init_short_cut(self):
        self._run_callback = self.loop.run_callback                     # 在主loop上挂起一个任务，待会执行
        self._add_to_wait = self._pool.add_wait_thread                  # 用于在协程池中加入一个协程等待执行任务
        self._get = self._wait_task.get                                 # 等待任务的时候在这个waiter上面等待
        self._clear = self._wait_task.clear                             # 重置waiter的状态
        self._switch = self._wait_task.switch                           # 用于切换到当前协程的执行
        self._add_block_thread = self._pool.add_block_thread            # 用于在协程池中加入一个阻塞的协程
        self._remove_from_block_set = self._pool.remove_block_thread    # 从协程池中移除一个阻塞的协程

    @property
    def last_out_time(self):
        """
        返回当前协程被切换出去的时候的时间
        """
        return self._last_out_time

    @property
    def waiter(self):
        return self._wait_task

    def set_task(self, task):
        """
        如果当前有任务可以执行了，将会通过这个方法将任务派发过来执行，主要是保存要执行的任务，然后
        在loop上面挂起回调，启动当前协程的调度
        :param task: 需要执行的任务
        """
        self._task = task
        self._run_callback(self._switch)

    def _run(self):
        """
        当前协程的主循环
        （1）执行任务，
        （2）将自己挂到pool上面去，等待协程池分配任务
        （3）回到（1）继续执行
        """
        try:
            while 1:
                self._last_out_time = None                     # 将上次因为阻塞切换出去的时间点清空
                if self._task is None:
                    if not self._add_to_wait(self):            # 尝试将自己添加到协程池中去
                        return                                 # 将当前协程添加到协程池中失败，说明协程数量超标了，直接退出
                    self._get()                                # 将当前协程切换出去，等待分配任务
                task = self._task                              # 这个是要执行的任务
                self._clear()                                  # 重置wait对象
                try:
                    task()                                     # 执行任务
                except:                                        # 这里是没有捕获的任务异常
                    logging.error("task execute error")
                    logging.error(traceback.format_exc())
                    sys.exc_clear()                            # 清理异常信息
                finally:
                    self._task = None                          # 将当前的任务设置为空
                    if self._some_time_block:                  # 如果当前协程曾经因为业务被阻塞
                        self._some_time_block = False          # 重置阻塞标志位
                        self._remove_from_block_set(self)      # 将其从阻塞协程的集合中移除
        finally:
            self._pool.thread_exit(self)                       # 用于通知协程池，当前协程退出了，主要是要更改协程数量计数器
            self._pool = None                                  # 释放对pool的引用

    def switch_out(self):
        """
        这里主要是要拦截因为执行的任务导致阻塞从而被切换出去的协程，
        设置标志为，表示当前协程在执行任务额时候曾经阻塞过，记录切换出去的时间点，然后将当前
        协程加入到协程池的阻塞队列里面，接受阻塞超时的监控，配合LetPool的_do_clear方法，用于在协程数量紧张的
        时候直接清除那些阻塞超时的协程

        通过这种方式防止一些因为特殊情况永久阻塞导致内存泄漏的情况
        """
        if self._task is not None:
            self._last_out_time = get_time()
            if not self._some_time_block:
                self._some_time_block = True
                self._add_block_thread(self)


class LetPool(object):
    """
    维护所有协程的调度，在有任务提交过来了之后分配协程来执行，或者创建新的协程来执行
    注意：还需要维护协程的数量情况，以及处理一些阻塞过长时间的协程
    """
    def __init__(self, max_size):
        """
        :param max_size:   协程池中最大协程的数量
        """
        object.__init__(self)
        self._max_size = max_size                           # 能够创建的协程的最大大小
        self._wait_threads = collections.deque()            # 在等待执行任务的协程的集合
        self._block_threads = set()                         # 执行任务中途被阻塞的协程的集合
        self._all_threads_size = 0                          # 已经创建的协程的数量
        self._hub = gevent.get_hub()                        # 主协程对象
        self._is_sch_clear = False                          # 是否调度了清理阻塞协程的处理
        self._init_short_cut()

    def _init_short_cut(self):
        self._pop_left = self._wait_threads.popleft         # 等待的任务队列的popleft方法
        self._append_free = self._wait_threads.append       # 等待的任务队列的append方法

    def thread_exit(self, thread):
        """
        在某一个协程退出之后将会调用这个方法来处理，主要是要更改一下当前所有协程数量的计数器
        :param thread: 退出的协程对象
        """
        self._all_threads_size -= 1

    def add_wait_thread(self, thread):
        """
        那些等待业务执行的协程将会将自己加入到这里来，让当前协程池知道这个协程是空闲的，
        当前可以为其分配任务执行

        注意：因为需要控制协程的数量，所以如果当前协程数量还没有超标的情况下可以让这个协程等待以后来执行任务
             否则需要让这个协程直接退出，降低协程的数量
        :return: True的话，表示可以等待执行任务，False表示协程应该退出了
        """
        if self._all_threads_size <= self._max_size:
            self._append_free(thread)                                 # 当前协程还在范围内，那么可以让这个协程等待任务执行
            return True
        else:
            return False                                              # 通过返回False，协程会自己退出

    def add_block_thread(self, thread):
        """
        那些因为执行业务而导致协程阻塞的，将会把自己加入进来，让协程池来监控
        """
        self._block_threads.add(thread)

    def remove_block_thread(self, thread):
        """
        将协程从阻塞的协程集合中移除
        """
        self._block_threads.remove(thread)

    def _do_clear(self):
        """
        用于在协程数量太多的时候清理一些阻塞了很长时间的业务，直接抛出异常

        遍历所有阻塞的协程，然后计算一下他们阻塞的时间是不是以及功能超时了，如果超时了的话，
        那么直接在协程上面抛出异常，用于中断任务的执行
        """
        self._is_sch_clear = False
        now_time = get_time()
        need_stop_thread = []
        for thread in self._block_threads:
            last_time = thread.last_out_time
            if last_time and (now_time - last_time > MAX_BLOCKING_TIME):  # 阻塞时间长度超标了
                need_stop_thread.append(thread)
        for thread in need_stop_thread:
            thread.throw(Exception("4"))     # 客户端在收到这种异常之后可以知道是因为服务器这边运行超时导致的

    def _sch_clear(self):
        """
        在协程数量特别多的时候，用于调度清除一些阻塞时间特别长的业务
        """
        if not self._is_sch_clear:
            self._is_sch_clear = True                            # 表示已经调度了清理
            self._hub.loop.run_callback(self._do_clear)          # 在loop上面去挂起一个回调

    def run_task(self, task):
        """
        io层提交任务的时候就直接调用这个方法

        问题：如果当前没有空闲的协程，而且协程数量已经达到了最大的协程数量限制，这个时候说明问题在于：
        （1）其实当前cpu是有空闲的，业务并没有因为完全的cpu计算导致连IO都无法处理
        （2）而协程都已经到达了最大的数量限制，只能说明有大量的协程因为业务阻塞了，这个时候可以考虑将一些阻塞了
            很长时间的业务直接中止运行，到达释放协程的目的，同时也防止一些业务永久性的阻塞协程导致内存泄漏的问题
        """
        if self._wait_threads:                           # 如果当前有协程在等待，那么直接用它来执行就好了
            self._pop_left().set_task(task)              # 获取一个可用的协程然后为其分配任务执行
        else:
            _FjsGreenlet(task, self)                     # 并没有在等待的协程，那么直接创建一个新的协程来运行
            self._all_threads_size += 1                  # 已经创建的协程数量加上1
            if self._all_threads_size > self._max_size:  # 如果当前协程的总数量已经超过了上限，那么这里调度一下清理程序
                self._sch_clear()                        # 调度一下阻塞超时协程的清理


DEFAULT_THREAD_SIZE = 1000
_pool = LetPool(DEFAULT_THREAD_SIZE)


# 直接在暴露这个模块方法供调用，理论上所有的业务执行都应该通过这个地方派发到协程池中运行
run_task = _pool.run_task

