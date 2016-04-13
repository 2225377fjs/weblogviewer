# -*- coding: utf-8 -*-
__author__ = 'fjs'
import struct
import cPickle
import logging
from errno import EWOULDBLOCK
from errno import EAGAIN
import sys
import gevent
from gevent.event import AsyncResult
from cPickle import loads as cp_loads
from cPickle import dumps as cp_dumps
from struct import pack as s_pack, unpack as s_unpack
import gevent.socket
import socket
import gevent.monkey
select = gevent.monkey.get_original("select", "select")    # 使用原生的select

DEFAULT_REQUEST_TIMEOUT = 30                               # 默认客户端这边超时设置为30秒


def socket_closed(sock):
    """
    判断一个socket是不是已经断开了，
    注意：在调用这个方法的时候需要保证这个socket当前是没有数据可以读的
    """
    try:
        rd, _, _ = select([sock], [], [], 0)
    except:
        return True
    return len(rd) > 0   # 这里其实已经保证当前socket没有数据可读，而这里可读，只能说明连接断开了


class FClient(object):
    """
    所有client类型的顶层类
    """
    def __init__(self, sock):
        """
        :param sock:    用到的Gevent的socket类型
        """
        object.__init__(self)
        self._sock = sock
        self._disconnect_listeners = []                # 保存所有的断线监听器

    def add_disconnect_listener(self, fn):
        """
        用于添加断线监听器
        """
        self._disconnect_listeners.append(fn)

    @property
    def sock(self):
        return self._sock

    @property
    def is_closed(self):
        """
        子类需要实现这个地方来实现外部代码来检测当前连接是否已经关闭了
        """
        raise Exception("not implemented")

    def request(self, service_name, data):
        raise Exception("not implemented")

    def handle_request_error(self):
        """
        如果当前socket发出的请求出现了一些问题，例如超时，连接断开啥的，当前
        socket的处理方式，这个在子类中具体的实现
        """
        raise Exception("not implented")

    def _fire_disconnect_listener(self):
        """
        用于在连接断开之后调用那些加入的监听器
        注意：因为大多数这个地方的执行环境其实实在主协程里面，所以这里创建一个新的协程来执行断开的监听
        """
        for fn in self._disconnect_listeners:
            gevent.spawn(fn)
        self._disconnect_listeners = []


class ThreadClient(FClient):
    """
    这种类型的客户端，每一个请求都需要独占一个连接
    """
    def __init__(self, sock):
        FClient.__init__(self, sock)
        self._closed = False

    @property
    def is_closed(self):
        """
        用于检验当前socket是否已经关闭了
        """
        return socket_closed(self._sock)

    def request(self, service_name, data=""):
        """
        发送
        :param service_name:    调用的服务的名字
        :param data:            参数
        """
        req = (service_name, data, 1)
        req_data = cPickle.dumps(req)
        req_data_len = len(req_data)
        head = struct.pack("i", req_data_len)
        here_sock = self._sock
        assert isinstance(here_sock, gevent.socket.socket)
        here_sock.sendall(head + req_data)
        return self.process_response()

    def handle_request_error(self):
        """
        对于一个请求独占一条连接的类型，这里为了安全，直接将连接断开
        """
        self.close()

    def _get_data(self, num):
        """
        这里从socket中读取num字节的数据，这里会抛出异常，在连接断开了之后
        """
        data = self._sock.recv(num)
        if not data:
            self.close()
            raise Exception("socket disconnected")    # 直接抛出异常给上层代码
        return data

    def process_response(self):
        """
        在发送完了请求数据之后，将会调用这里来获取服务器的返回
        首先读取头部，然后读取body，然后调用protobuf解析，然后返回数据
        """
        now_data = ""
        now_status = 1
        body_len = 0
        while 1:
            data = self._get_data(2048)
            now_data += data
            if now_status == 1:
                if len(now_data) >= 4:
                    body_len_data = now_data[0:4]
                    body_len, = struct.unpack("i", body_len_data)
                    now_data = now_data[4:]
                    now_status = 2

            if now_status == 2:
                if len(now_data) >= body_len:
                    message = cPickle.loads(now_data)
                    return message[1]

    def close(self):
        """
        这里其实就是关闭底层的socket链接
        """
        if not self._closed:
            self._closed = True
            if self._sock is None:
                return
            self._fire_disconnect_listener()
            self._sock.close()
            self._sock = None


#
# 与上面类型不同，这里多个请求可以共享统一个客户端，通过rid来进行请求的区分
#
class SharedFClient(FClient):
    """
    多个请求共享一个socket，当提交了请求之后，在event上面等待，知道服务器返回的结果再唤醒协程
    """
    def __init__(self, sock, keep_alive_type=None):
        FClient.__init__(self, sock)
        self._is_closed = False                              # 用于标记当前socket是否以及功能断开了
        self._send_data = []                                 # 需要发送的数据直接放到这个发送队列里面来缓存
        self._real_sock = sock._sock                         # 真正的原生socket
        self._write_open = False                             # 是否已经打开了写监听

        self._read_event = sock._read_event                  # gevent的socket的读watcher
        self._write_event = sock._write_event                # gevent的socket的写watcher
        self._read_event.start(self.process_response)        # 开启读事件的监听
        self._rid = 0                                        # 用于请求编号的分配

        self._events = dict()                                # 用于保存请求以及其等待的event对象

        self._now_data = ""                                  # 缓存读取到的数据
        self._status = 1                                     # 标记当前正在读取header还是body
        self._body_len = 0                                   # 用于记录body数据的长度
        self._ping_timer = gevent.get_hub().loop.timer(20, 20)   # 每隔20秒钟检查发送一次心跳
        self._recv_ping_time = 0                             # 心跳标志位
        self._ping_data = struct.pack("i", -1)               # 每次心跳发送的数据

        if keep_alive_type == 1:
            self._init_heart_beat()                          # 如果长连接标志为1，那么应用层心跳
        elif keep_alive_type == 2:
            self._init_keep_alive()                          # 如果长连接标志为2，那么keepalive
        self._init_short_cut()

    def _init_short_cut(self):
        self._recv = self._real_sock.recv                    # 从socket接收数据
        self._send = self._real_sock.send                    # 从socket发送数据
        self._real_start_write = self._write_event.start     # 启动gevnet的写监听
        self._real_stop_write = self._write_event.stop       # 停止gevent的写监听

    @property
    def rid(self):
        """
        用于分配请求的rid
        """
        self._rid += 1
        if self._rid > 100000000:               # 如果编号已经超级大了，那么将其归一
            self._rid = 1
        return self._rid

    def _init_keep_alive(self):
        """
        用于设置tcp连接的keepalive属性
        """
        sock = self._sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 3)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 20)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 60)

    def _init_heart_beat(self):
        """
        用于开启应用层的心跳监控
        这里之所以做了应用层的心跳，主要原因在于：
           keepalive本身直接在系统层面上都已经做完了，应用层本身不会有任何感知，即使整个应用层已经卡死了
           而应用层层级的心跳，则可以感知是否已经卡死，因为已经完全阻塞掉，那么也就相当与挂掉了

       注意：如果是使用应用层的心跳，那么客户端和服务端两方面都比需要要使用应用层心跳
        """
        self._ping_timer.start(self._ping)

    def _ping(self):
        """
        用于定时的检查和发送心跳数据
        """
        self._recv_ping_time -= 1
        if self._recv_ping_time < -3:                    # 如果有超过60秒钟没有收到过任何数据，那么可以理解这个连接已经断开了
            self.close()                                 # 判断连接断开的情况，直接关闭就好了
        else:
            self._send_data.append(self._ping_data)      # 将心跳数据放到发送队列里面
            if not self._write_open:
                self._start_write()

    @property
    def is_closed(self):
        """
        当前的连接是不是已经断开了
        """
        return self._is_closed

    def handle_request_error(self):
        """
        在用当前连接发送请求的时候出现了异常，因为在使用的时候都是在with语句下面：
        with PROCESS_MANAGER.get_process_client(self._p_id) as client:
            out = client.request(self._entity_id, req_data)
        request内部的异常，为了安全，直接关闭连接
        """
        self.close()

    def _stop_write(self):
        """停止写监听"""
        self._write_open = False
        self._real_stop_write()

    def _start_write(self):
        """开启写监听"""
        self._write_open = True
        self._real_start_write(self._write)

    def _write(self):
        """
        在有数据需要写的时候，会调用这个回调，这个就是socket的写监听的回调
        """
        if self._send_data:
            data = b"".join(self._send_data)
            try:
                sent = self._send(data)              # 通过socket将数据发送出去
            except:
                ex = sys.exc_info()[1]
                if ex.args[0] not in (EWOULDBLOCK, EAGAIN):    # 当前暂时不能发送数据，一般是socket的缓冲区已经满了
                    self.close()                               # 这个一般是连接断开了
                sys.exc_clear()                                # 清空异常信息
                return
            data = data[sent:]                                 # 将发送了的数据移除
            if data:
                self._send_data = [data]                       # 如果还有剩下没有发送完的数据，那么保存起来
            else:
                self._send_data = []                           # 数据发完了，将发送数据的队列清空
                self._stop_write()                             # 所有数据都已经发送出去了，那么可以直接关闭写监听了
        else:
            self._stop_write()

    @property
    def sock(self):
        return self._sock

    def request(self, service_name, data=""):
        """
        这里将数据写到缓冲队列，然后让当前请求协程在event上等待
        """
        if self._is_closed:
            logging.error("request in a close cliet")
            raise Exception("connection closed")
        rid = self.rid
        req = (service_name, data, rid)
        req_data = cp_dumps(req)
        req_data_len = len(req_data)
        head = s_pack("i", req_data_len)
        self._send_data.extend((head, req_data))
        if not self._write_open:
            self._write()                                          # 立即尝试写一下，可以降低延迟
            self._start_write()
        event = AsyncResult()
        self._events[rid] = event
        try:
            out = event.get(timeout=DEFAULT_REQUEST_TIMEOUT)       # 在这个event上面等待，阻塞当前协程
            if self._is_closed:
                raise Exception("connection disconnected")
            return out
        finally:
            del self._events[rid]

    def process_response(self):
        """
        gevent读事件的监听回调处理，用于读取数据
        """
        try:
            now_data = self._recv(8192)
            if not now_data:                             # 如果读取的数据为空，那么socket已经断开了
                self.close()
                return
        except:
            ex = sys.exc_info()[1]                       # 获取异常对象
            error_no = ex.args[0]                        # 获取错误代码
            sys.exc_clear()                              # 当前是在主协程中运行，这里直接清理掉异常信息
            if error_no not in (EWOULDBLOCK, EAGAIN):
                self.close()                             # 一般情况下是一些reset关闭啥的
            return                                       # 根本就没有数据可以处理，还搞毛啊，直接返回
        self._now_data += now_data                       # 将读取的数据保存起来
        self._recv_ping_time = 0                         # 重置心跳标志为
        while 1:
            if self._status == 1 and len(self._now_data) >= 4:         # 当前状态还需要读取数据的头部
                body_len_data = self._now_data[:4]                     # 获取4个字节长度的头部
                self._body_len, = s_unpack("i", body_len_data)         # 计算数据body应该有的长度
                self._now_data = self._now_data[4:]                    # 将4个字节的头部从数据中移除
                if self._body_len == -1:                               # 这里如果数据包的长度为-1，那么就是心跳数据
                    continue                                           # 这里直接略过
                self._status = 2                                       # 更改状态标志
            elif self._status == 2 and len(self._now_data) >= self._body_len:
                response_data = self._now_data[:self._body_len]        # 这个就是客户端发送过来的数据
                self._now_data = self._now_data[self._body_len:]       # 将输入数据清除
                self._status = 1                                       # 恢复状态标志为
                rid, message = cp_loads(response_data)
                if rid in self._events:                                # 找到当前返回数据对应的event
                    self._events[rid].set(message)                     # 找到对应挂起的事件，然后设置，唤醒挂起的协程
            else:
                break

    def close(self):
        """
        这里其实就是关闭底层的socket链接
        """
        if not self._is_closed:
            self._is_closed = True                          # 设置关闭标志为
            self._sock.close()                              # 关闭gevent层的socket
            self._fire_disconnect_listener()                # 通知注册的断线监听器
            self._disconnect_listeners = None               # 释放那些短信监听的引用
            self._real_sock = None                          # 释放python原生socket的引用
            self._read_event.stop()                         # 停止读事件的监听
            self._write_event.stop()                        # 停止写事件的监听
            self._ping_timer.stop()                         # 停止应用层心跳
            for event in self._events.values():
                event.set(None)                             # 唤醒所有还在这里等待返回的协程


def create_client(host, port):
    """
    这种类型的cleint是不能并发请求的，不能多个协程同时在这个客户端上发起请求，
    只能一个请求完全执行完了之后才能继续下一个请求
    """
    sock = gevent.socket.create_connection((host, port))
    client = ThreadClient(sock)
    return client


def create_share_client(host, port):
    """
    创建可以并发请求的客户端类型，多个协程都可以用这同一个client来掉用，通过请求id来区分每一个请求
    """
    sock = gevent.socket.create_connection((host, port))
    client = SharedFClient(sock, keep_alive_type=1)        # 将心跳类型设置为应用层的心跳
    return client






