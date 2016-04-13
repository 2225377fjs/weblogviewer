# -*- coding: utf-8 -*-
__author__ = 'fjs'
import BeanManager
import socket
import Constant
import gevent.socket
import gevent


class RecvFdBean(BeanManager.Bean):
    def __init__(self, path):
        BeanManager.Bean.__init__(self, Constant.RECV_FD_BEAN)
        self._back = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._back.connect(path)
        self._sock = gevent.socket.socket(socket.AF_INET, _sock=self._back)
        # self._back.setblocking(True)

    def start(self):
        gevent.spawn(self._start)


    def _start(self):
        """
        当前server的主循环，
        （1）等待当前gevent的socket的读事件
        （2）连续获取两个文件描述符，也就是从前端服务器获取两个客户端的连接
        （3）初始化FScoekt对象，然后初始化Room服务，
        （4）启动两个协程来执行两个FSocket的process方法
        （5）再将socket设置为非阻塞的

        注意：这部分，如果发送过来的连接是已经不行的，例如是已经断开了的，那么
             需要进行处理，这里在fromfd的时候会出错
        :return:
        """
        while 1:
            try:
                self._sock._wait(self._sock._read_event)
                fd = self._sock.fileno()
                out1, data1 = aa.fjs_recv_fd(fd)
                here_sock1 = socket.fromfd(out1, socket.AF_INET, socket.SOCK_STREAM)


                out_sock1 = gevent.socket.socket(socket.AF_INET, _sock=here_sock1)
            except:
                pass


