# -*- coding: utf-8 -*-
__author__ = 'fjs'

import bean.BeanManager
from gevent import server
import lib.Service
import lib.FSocket
import gevent.socket
import logging
import traceback
import cPickle
import gevent.monkey
import socket
real_socket = gevent.monkey.get_original("socket", "socket")     # 获取原生的socket类型

################################################################################################
# 向外暴露TCP的调用接口，这里tcpConnector对象会创建一个
# MainServiceControllerService对象，所有的服务都由它来管理
# 当有外部的socket来的时候，就直接路由到MainServiceControllerService对象
# 上面去

# 注意： 在tcp连接器创建的时候，如果是多物理服务器的部署，那么需要设置一个regis_address，用于在
# config进程上面注册监听的信息，默认的话是使用当前机器的0.0.0.0
################################################################################################


class TcpConnector(object):
    def __init__(self, port, ip="0.0.0.0", regist_address=None, protocol=None):
        """
        这里传进来一个名字用于在bean管理器上进行注册，之所以这样，主要是考虑到一个进程可能会启动多个
        tcp的监听，同一个名字会冲突，所以就设置一下
        :param port:           监听的端口
        :param ip:             监听的地址
        :param regist_address: 在config进程上面注册当前进程的监听信息时候的地址，主要是用于在多物理机上部署的时候用的
        """
        object.__init__(self)
        self._port = port
        self._ip = ip
        self._server = None
        self._service_con = lib.Service.MainServiceControllerService(protocol=protocol)
        self._regist_address = regist_address

    @property
    def address(self):
        return {"ip": self._ip, "port": self._port}

    @property
    def regist_address(self):
        """
        在config进程上面注册进程信息的时候，需要将当前进程的监听信息发送给config进程
        这里就是用于获取注册到config进程上面的地址
        """
        return self._regist_address if self._regist_address else self.address

    def set_con(self, value):
        """
        重新设置service的路由
        """
        self._service_con = value

    def add_service(self, service):
        """
        向服务管理器里面注册服务
        """
        self._service_con.add_service(service)

    def _handle(self, sock, address):
        """
        在gevent的streamserver中，如果接收到了socket，那么会调用这个方法来进行处理
        等到process返回，一般情况下都是socket断开了之后，整个协程也会跟着退出，内存回收

        注意：这里gevent会专门创建一个协程来调用这个方法
        :param sock:    gevent的socket类型
        :param address: 远程的地址
        """
        fsock = lib.FSocket.FSocket(sock, address, self._service_con)
        fsock.process()

    def start(self):
        """
        其实这里就是启动StreamServer
        """
        self._server = server.StreamServer((self._ip, self._port), handle=self._handle)
        self._server.start()

    def stop(self):
        """
        其实就是停止底层的streamserver
        """
        if self._server:
            self._server.stop()


class _FjsStreamServer(server.StreamServer):
    """
    因为默认的streamserver在获取到一个连接之后，为其创建一个协程来处理
    在最后协程退出的时候会默认关闭连接，而在SelectTcpConnector里面，这样就是不行的了
    """
    def __init__(self, *args, **kwargs):
        server.StreamServer.__init__(self, *args, **kwargs)

    def do_handle(self, *args):
        try:
            self.handle(*args)
        except:
            pass


#
# 与普通的connector不同的是，连接并不会独占一个协程来处理io，而是直接通过watcher来进行回调，
# 然后将解析出来的数据交由协程池来处理，这个样子可以在大量连接的情况下降低服务器的负载，提高服务器的吞吐量
# 一般情况下，服务端就直接才采用这种类型的connector就好了
#
class SelectTcpConnector(TcpConnector):
    def __init__(self, port, ip="0.0.0.0", regist_address=None, protocol=None):
        TcpConnector.__init__(self, port, ip, regist_address, protocol=protocol)

    def start(self):
        self._server = _FjsStreamServer((self._ip, self._port), handle=self._handle)
        self._server.start()

    def _handle(self, sock, address):
        """
        这里直接创建一个SelectFSocket类型的就可以了，在其构造函数里面会自动的去启动它的read
        事件的watcher

        注意：因为这里一般是服务端自己各个进程之间的连接，所以将长连接心跳类型设置为应用层心跳
        """
        lib.FSocket.SelectFSocket(sock, address, self._service_con, keep_alive_type=1)



#
# 这种类型的connector自己并不会建立监听，而是与一个accepter建立unix域socket的连接，
# accepter获取连接了之后通过unix域socket发送过来
#
class RecvFdTcpConnector(object):
    def __init__(self, path, service):
        """
        :param path:       accepter进程的监听地址，启动的时候将会发起unix域socket连接
        :param service:    当前connector接收到连接之后的服务路由
        """
        object.__init__(self)
        self._path = path                             # accept建成域socket的监听地址
        self._service_con = service                   # 用于将请求的数据交由其来进行路由处理

    def start(self):
        gevent.spawn(self._start)

    def _start(self):
        """
        （1）外面的大循环确保了在于accepter连接断开或者连接失败之后可以重新尝试与accepter的连接
        （2）在与accepter连接成功之后，就进入了内部的while循环，用于不断的等待与accepter连接的读取事件，
            然后调用扩展模块来获取accepter发送过来的文件描述符
        （3）在获取到文件描述符之后，将其包装成为gevet的socket，然后创建一个新的协程，用于处理连接的数据

        注意：因为fromfd操作会复制文件描述服，所以需要将原来那个删除
        """
        while 1:
            gevent.sleep(1)
            try:
                self._back = real_socket(socket.AF_UNIX, socket.SOCK_STREAM)         # 创建unix域socket
                self._back.connect(self._path)                                       # 与accept进程建立连接
                self._sock = gevent.socket.socket(socket.AF_INET, _sock=self._back)  # 这里会将连接弄成非阻塞的
            except:
                logging.error("与accept进程建立链接失败")
                logging.error(traceback.format_exc())
                self._back, self._sock = None, None
                continue
            logging.info("与accepter建立链接成功")
            while 1:
                try:
                    self._sock._wait(self._sock._read_event)           # 等待读取事件
                    fd = self._sock.fileno()                           # 与前面accepter进程建立的连接的fd编号
                    out1, data1 = lib.sendfd.fjs_recv_fd(fd)           # 接收文件描述
                    address = cPickle.loads(data1)                     # 这个是远端的地址
                    out_sock1 = gevent.socket.fromfd(out1, socket.AF_INET, socket.SOCK_STREAM)
                    self._handle(out_sock1, address)
                except:
                    """
                    确实有可能会出现接收文件描述符的时候出现异常：
                    （1）accept进程挂掉了，这个就不好搞了，不过可能性比较小
                    （2）accetp进程发送的太快了，直接把缓冲区装满了
                    （3）接收了一个本来就已经关闭了的连接
                    """
                    logging.error("接收文件描述符异常")
                    logging.error(traceback.format_exc())
                    self._back, self._sock = None, None
                    break
                finally:
                    lib.sendfd.fjs_close_fd(out1)  # 因为fromfd操作会复制文件描述符，所以一定要关闭多的这个

    def _handle(self, sock, address):
        """
        这里直接创建一个SelectFSocket类型的就可以了，在其构造函数里面会自动的去启动它的read
        事件的watcher

        注意：这里一般都是外部与系统建立的连接，所以使用keepalive
        """
        lib.FSocket.SelectFSocket(sock, address, self._service_con, keep_alive_type=2)









