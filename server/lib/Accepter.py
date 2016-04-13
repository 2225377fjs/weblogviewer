# -*- coding: utf-8 -*-
__author__ = 'fjs'
import os
import gevent
import gevent.socket
import gevent.server
import logging
import cPickle
import traceback
from collections import deque
from lib.sendfd import fjs_send_fd
import socket
from gevent.server import StreamServer


import gevent.monkey
select = gevent.monkey.get_original("select", "select")          # 获取原生的select方法
real_socket = gevent.monkey.get_original("socket", "socket")     # 获取原生的socket对象


#
# 工具方法，用于判断一个连接是不是已经断开了，这里需要保证判断的连接并没有数据可以读取
#
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


#
# 一个基础bean类型，用于建立两个监听
# （1）监听某个端口，等待客户端来建立连接
# （2）建立一个unix的监听，等待别的进程来建立连接
#
# 当有客户端通过端口连接上来之后，将文件描述服发送给某一个与当前unix建立了连接的进程
#
class Accepter(object):
    def __init__(self, path, port):
        """
        :param path:   建立unix域socket监听的路径
        :param port:   建立外部监听的端口
        :return:
        """
        object.__init__(self)
        self._path = path                    # 监听unix的路径
        self._sock = None                    # 建立的unix类型的socket将会用这个来引用，用于获取别的进程建立过来的连接
        self._backs = deque()                # 将与当前进程的unix监听建立了连接的客户端保存在这个地方
        self._port = port                    # 监听前面的端口
        self._stream_server = None           # 一个前端监听的server

    def _handle(self, sock, address):
        """
        :param sock: 外部通过端口与当前进程建立连接的socket对象
        :param address: 远端的地址
        外部基于端口的tcp监听的处理方法
        这里的默认行为就是接收到外部建立的tcp连接之后，将这个链接发送到与当前accepter建立了unix域socket链接的
        别的进程去

        注意：在将文件描述服通过unix域socket发送给别的进程之后，一定要将本进程的这个关闭，否则同一个文件描述符将被多个进程引用
             有可能会导致关闭部分出现一些小问题
        """
        fd = sock.fileno()
        try:
            self.send_fd_to_back(fd, address)
        finally:
            sock.close()

    def start(self):
        """
        （1）创建unix域socket的监听，然后将其包装成gevent的socket类型，别的进程将会通过这个监听连接上来
        （2）创建一个协程，用于从unix域socket获取别的进程建立过来额连接
        （3）创建streamserver，用于获取外部连接
        :return:
        """
        sock = real_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            os.unlink(self._path)                                   # 这里主要是以防这个路径以前有人用过
        except:
            pass
        sock.bind(self._path)
        sock.listen(10)
        sock = gevent.socket.socket(socket.AF_UNIX, _sock=sock)     # 将listener转化成gevent类型的
        self._sock = sock
        gevent.spawn(self._process)                                 # 创建协程，用于接收别的进程通过unix来建立的连接
        logging.info("建立前端监听成功")

        self._stream_server = StreamServer(("", self._port), self._handle)
        self._stream_server.start()

    def _process(self):
        """
        这个是在一个单独的协程中运行，用于接收系统中别的进程与当前进程建立unix域socket的连接并保存起来，
        将别的进程连上来的连接保存到队列
        """
        while 1:
            try:
                client, address = self._sock.accept()
                self._backs.append(client)                         # 将连接放到队列里面
            except:
                logging.error("accepter接收unix域socket连接异常")
                logging.error(traceback.format_exc())

    def send_fd_to_back(self, fd, address):
        """
        将当前这个描述符发送给后面，这里在获取到后端的连接之后，需要先判断一下连接是不是好的
        如果已经断开了，那么可以将这个后端连接关闭，抛弃

        注意：在非阻塞的发送的情况下，有可能会发送失败，返回-1，例如发送的太快，接收的太慢。。
            这个时候需要注意处理
        :return: 发送成功返回True，发送失败返回False
        """
        try:
            if not self._backs:
                logging.error("没有与当前accepter建立连接的unix域socket")
                return False
            while self._backs:
                back = self._backs.popleft()
                if socket_closed(back):
                    back.close()
                    continue
                ret = fjs_send_fd(back.fileno(), fd, cPickle.dumps(address))
                if ret == -1:
                    back.close()                   # 发送失败，将这接收连接的unix域socket关闭
                else:
                    self._backs.append(back)       # 将这个别的进程的连接回收保存，下次接着用
                    return True
            logging.error("发送socket失败，没有找到合适的unix域socket")
        except:
            logging.error("通过unix域socket发送文件描述符异常")
            logging.error(traceback.format_exc())
        return False

