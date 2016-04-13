# -*- coding: utf-8 -*-
__author__ = 'fjs'
import lib.FClient
import contextlib
import gevent
import collections
import gevent.lock
from TimeUtil import get_time
import sys

####################################################################################
# 进程管理器，用户维护整个集群的进程的监听信息，
# 并维护与其他进程建立的连接
####################################################################################

CLEAR_INTERVAL = 120                                          # 每隔120秒钟进行一次连接的清理
MAX_NO_USE = 60                                               # 连接超过这个时间都没有用过，那么就将会被移除


#
# 用于记录一个进程的信息
#
class ProcessInfo(object):
    def __init__(self, ip, port):
        self._ip = ip
        self._port = port
        self._address_str = self._ip + ":" + str(self._port)

    @property
    def address(self):
        return self._address_str


#
# 缓存某个进程的监听信息，ip，端口，或者unix域socket的path
# 建立与维护与远端建立的连接
#
class CachedInfo(object):
    def __init__(self, address):
        object.__init__(self)
        self._ip, self._port = address["ip"], address["port"]      # 记录远端的地址
        self._address = address                                    # 远端地址
        self._path = address.get("path", None)                     # 这里远端的地址有可能会是unix域socket的监听地址
        self._clients = collections.deque()                        # 缓存远端进程的连接

    @property
    def address(self):
        """
        返回地址的信息
        """
        return self._address

    def get_client(self):
        """
        获取一个与远端的连接客户端，如果没有缓存的话，那么创建一个，同时需要更新这个连接的访问时间戳
        注意：在这里可能会抛出创建连接失败的异常
        注意：创建连接是一个阻塞的操作，可能会引起协程的挂起调度
        """
        while self._clients:
            client = self._clients.popleft()
            if client.is_closed:                                      # 检测一下连接的状态
                client.close()
            else:
                client.__dict__["last"] = get_time()                  # 记录访问时间
                return client
        client = lib.FClient.create_client(self._ip, self._port)      # 没有可用的连接，那么创建一个
        client.__dict__["last"] = get_time()                          # l记录访问时间
        return client

    def return_client(self, client):
        """
        归还一个连接，这里将连接缓存起来
        :return: lib.FClient.FClient
        """
        self._clients.append(client)

    def clear(self):
        """
        这里将那些长时间没有用的连接都关闭然后移除，在manager里面会定时的调用这个方法
        """
        need_remove = []
        now = get_time()
        for client in self._clients:
            last_time = client.__dict__["last"]
            if (now - last_time) > MAX_NO_USE:
                need_remove.append(client)
        for client in need_remove:
            self._clients.remove(client)
        for client in need_remove:
            client.close()

    def close(self):
        """
        这个将会直接关闭当前缓存的所有连接
        """
        while self._clients:
            client = self._clients.popleft()
            client.close()


#
# 主要是用于维护多个进程的信息，建立进程id与其连接缓存的映射关系
#
class ProcessManager(object):
    def __init__(self):
        object.__init__(self)
        self._pros = dict()                                   # 用于保存进程id与其地址之间的关联关系
        self._cached_clients = dict()                         # 用于缓存一些与别的进程的连接
        self._clear_timer = gevent.get_hub().loop.timer(CLEAR_INTERVAL, CLEAR_INTERVAL)
        self._clear_timer.start(self._do_clear)               # 启动timer，定时的来清理那些空闲的连接

    def _do_clear(self):
        """
        在timer中定时的调度连接清理，将那些长久没有使用的连接关闭
        """
        for info in self._cached_clients.values():
            info.clear()

    def add_process(self, address):
        """
        添加一个进程的信息

        问题：如果现在添加了一个以及功能存在的进程信息，那么这个时候是不是需要做一下处理呢？出要是替换一下进程连接的缓存对象,
             首先系统出现这种情况本身来说应该是一种不正确的情况
             处理：直接将进车功能的信息替换掉，然后将原来的连接缓存删除，下一次获取这个进程的连接就会重新根据信息创建了

        :param p_id:      进程的唯一ID，用于索引
        :param address:   进程的监听地址 {"ip": "", "port": 123, "path": "dfds"}这种类型的
        """
        address_str = address["ip"] + ":" + str(address["port"])
        self._pros[address_str] = address
        if address_str in self._cached_clients:
            info = self._cached_clients[address_str]
            del self._cached_clients[address_str]
            info.close()

    def remove_process(self, address):
        """
        移除一个进程，这里将会关闭与这个进程的所有连接，
        这个一般是在config检测到某个进程已经挂掉了之后，会通知所有别的进程这个信息，那么别的进程就应该调用
        进程管理器的这个方法来移除掉线的进程的信息

        （1）移除进程的信息，（2）移除缓存的连接
        """
        address_str = address["ip"] + ":" + str(address["port"])
        if address_str in self._pros:
            del self._pros[address_str]
        if address_str in self._cached_clients:
            cache = self._cached_clients[address_str]
            del self._cached_clients[address_str]
            cache.close()

    @contextlib.contextmanager
    def get_process_client(self, address_str):
        """
        通过进程的监听地址信息获取 连接
        :param address_str: 127.0.0.1:80
        注意：这是一个上下文管理器，所以必须要通过with语句来访问该方法
        异常：这里可能会有两种类型的异常：
             （1）创建连接失败的异常
             （2）yield之后外部代码的异常
        :rtype: lib.FClient.FClient
        """
        if address_str not in self._pros:
            address_strs = address_str.split(":")
            address = {"ip": address_strs[0], "port": int(address_strs[1])}
            self.add_process(address)
        if address_str not in self._cached_clients:
            address = self._pros.get(address_str)
            self._cached_clients[address_str] = CachedInfo(address)
        client = None
        try:
            client = self._cached_clients[address_str].get_client()
            yield client                                       # 外部代码在这里执行，可以直接捕获外部的异常
        except:
            if client is not None:
                client.handle_request_error()                  # 因为这里使用的是独占连接的客户端，处理方式是直接关闭连接
            raise                                              # 重新抛出异常给外部代码
        else:                                                  # 如果没有出现什么问题，那么将这个连接缓存起来
            self._cached_clients[address_str].return_client(client)

    def for_each_pro(self, fn):
        """
        用于遍历所有别的进程，获取一个进程的连接，然后用传入的函数来处理
        注意：这里在处理的时候可能会抛出异常，例如某个进程的信息还在，但是进程以及停了
        """
        for pid in self._cached_clients:
            try:
                with self.get_process_client(pid) as client:
                    fn(client)
            except:
                pass


#
# 这里维护的客户端，多个请求共享同一个tcp连接，然后通过rid的方法来进行请求之间的区分
#
class SharedCachedInfo(CachedInfo):
    """
    这种类习的客户端缓存，将会创建SharedFClient类型的客户端，因为这种客户端本身是可以并发请求的，
    所以在获取client的时候通过锁了控制，可以保证最终只有一个连接被建立
    """
    def __init__(self, *args, **kwargs):
        CachedInfo.__init__(self, *args, **kwargs)
        self._get_lock = gevent.lock.Semaphore()           # 通过锁来防止有多个连接被建立

    def get_client(self):
        """
        获取一个与远端的连接客户端，如果没有缓存的话，那么创建一个，同时需要更新这个连接的访问时间戳，

        注意：这里可能会抛出连接创建失败的异常
        注意：创建连接是一个阻塞的操作，可能会引起协程的挂起调度，所以为了防止创建多个连接，这里用锁来控制
        """
        with self._get_lock:
            while self._clients:
                client = self._clients[0]
                if client.is_closed:
                    self._clients.popleft()                                  # 将已经关闭的连接移除
                else:
                    client.__dict__["last"] = get_time()
                    return client
            client = lib.FClient.create_share_client(self._ip, self._port)   # 并没有连接可用，这里创建一个
            self._clients.append(client)
            client.__dict__["last"] = get_time()
            return client

    def return_client(self, client):
        """
        归还一个连接，这里将连接缓存起来，对于当前类型，其实什么事情都不许要做
        """
        pass


###############################################################################
# 非独占tcp连接的，这里就必须要使用SharedFClient 这种类型的客户端了
# 因此使用的连接池也不太一样
###############################################################################
class SharedProcessManager(ProcessManager):
    def __init__(self, *args, **kwargs):
        ProcessManager.__init__(self, *args, **kwargs)

    @contextlib.contextmanager
    def get_process_client(self, address_str):
        """
        通过一个地址信息来获取与这个地址关联的连接
        注意：这是一个上下文管理器，所以必须要通过with语句来访问该方法
        异常：这里可能会有几种类型的异常：
             （1）创建连接失败的异常
             （2）yield之后外部代码的异常
             （3）rpc请求超时异常

        对于请求超时的异常，这里因为是请求都共享一个连接，所以不断连接做处理，但是对于别的异常，应该调用handle_request_error
        用于将底层连接直接关闭，虽然可能会影响一些别的协程挂起的请求，但是稳定性安全还是要考虑的
        :rtype: lib.FClient.SharedFClient
        """
        if address_str not in self._pros:
            address_strs = address_str.split(":")
            address = {"ip": address_strs[0], "port": int(address_strs[1])}
            self.add_process(address)
        if address_str not in self._cached_clients:
            address = self._pros.get(address_str)
            self._cached_clients[address_str] = SharedCachedInfo(address)
        client = None
        try:
            client = self._cached_clients[address_str].get_client()
            yield client                                             # 外部代码在这里执行，可以直接捕获外部的异常
        except:
            e = sys.exc_info()[1]
            if not isinstance(e, gevent.Timeout) and client is not None:
                client.handle_request_error()                        # 对于一些奇怪的异常，直接让客户端关闭连接
            raise                                                    # 重新抛出异常给外部代码


