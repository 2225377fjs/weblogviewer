# -*- coding: utf-8 -*-
__author__ = 'fjs'

import Service
import struct
import traceback
import logging

from Context import Context
import sys

from errno import EWOULDBLOCK
from errno import EAGAIN

import gevent
import gevent.socket
from LetPool import run_task
from struct import unpack as s_unpack, pack as s_pack
import socket




#########################################################################
# 对用户连接的一层封装，也就是对gevnet的socket的一层封装，主要是加上了数据包的解析
# 以及对一些异常情况的处理
#########################################################################


HEAD_LEN = 4                                            # 数据包头部的长度


###############################################################
# 对gevent的socket进行了一层的封装，加上了底层数据包的解析
# 这里数据包的定义很简单，首先是一个4字节的整形，记录接下来数据的长度
# 然后加上了一层service的路由，这里要区分为两个部分，首先是mainService部分
# 它是全局共享的，然后就是roomService部分，它是属于所有对战的人之间独享的
###############################################################
class FSocket(object):
    def __init__(self, socket, address, service_con):
        """
        主要是完成一些基本的初始化，创建context上下文
        一开始mainService的controller使用全局唯一的那个
        而对于room部分，则等到匹配成功之后创建了再设置，

        最后还要调用一下当前main环境的on_connection方法，用于通知main环境的服务，
        当前有客户端连上来了
        :type address:
        :param socket:   gevent的socket类型
        :param address:  客户端连接远程地址
        :type service_con: Service.MainServiceControllerService
        :return:
        """
        object.__init__(self)
        self._sock = socket
        self._address = address
        self._closed = False                                    # 用于标记当前连接是否已经关闭了
        self._context = Context(self)
        self._need_late_close = False

        self._now_con = service_con                             # 解析出来的body数据交给它来处理
        self._now_con.on_connection(self, self._context)        # 调用服务的on_connection方法
        self._dic_connect_listener = set()                      # 注册的断线监听器将会保存在这里

    @property
    def context(self):
        """
        :rtype: Context.Context
        """
        return self._context

    @property
    def sock(self):
        """
        返回gevent类型的socket
        """
        return self._sock

    def need_close(self):
        """
        通过这个标志位让在发送完了一次数据之后关闭当前的socket
        这个主要是为了切换socket用，将socket传给另外一个进程
        防止当前进程读取下一个进程应该要读取的数据
        """
        self._need_late_close = True

    def add_disconnect_listener(self, fn):
        """
        添加断线监听器
        """
        self._dic_connect_listener.add(fn)

    def process(self):
        """
        经过测试，这种比较的直接的处理方法在响应时间方面具有优势，而且吞吐量好像也差不多
        (1)读取4个字节长度的头部，用于标记body的长度
        (2)通过获取的body的长度，读取相应长度的body数据，然后交给上层来处理
        """
        now_data = ""
        status = 1
        body_len = 0                                   # 用于记录body数据的长度
        while not self._closed:
            try:
                data = self._sock.recv(2048)
            except:                                    # 一些紧急rest 关闭，会导致这里出现异常
                break
            if not data:
                break                                  # 这里其实是连接已经断开了
            now_data += data
            if status == 1:                            # 当前为接收header的状态
                if len(now_data) >= HEAD_LEN:
                    body_len_data = now_data[0:HEAD_LEN]      # 获取4个字节长度的头部
                    body_len, = struct.unpack("i", body_len_data)
                    now_data = now_data[HEAD_LEN:]            # 将4个字节的头部从数据中移除
                    status = 2                         # 更改状态标志

            if status == 2:                           # 当前为接收body的状态
                if len(now_data) >= body_len:
                    input_data = now_data             # 这个就是客户端发送过来的数据
                    now_data = ""                     # 清空data
                    status = 1                        # 恢复状态标志为
                    try:
                        out = self._now_con.service(self._context, input_data)
                    except:
                        # 这里这里在处理的过程中出现了异常，那么关闭，结束
                        logging.error("服务处理错误")
                        logging.error(traceback.format_exc())
                        break
                    self.write(out)

                    # 这个字段都是在service里面调用的，表示服务返回的数据发送完成之后，需要将
                    # 这个socket关闭
                    if self._need_late_close:
                        self.close()
        self.close()

    def close(self):
        """
        这里主要是关闭当前的链接，然后调用当前服务controller的on_disconnect方法
        通知上层的代码当前这个链接已经断开了
        通过closed标志位来保证关闭逻辑只执行一次
        分别调用main和room环境的on_disconnect方法

        注意：只有当前在main环境下，客户端关闭才会顺带将context释放掉，如果是在room环境下
             那么将不会调用context的release方法，他会在整个战场结束之后才调用release方法
             这里主要是为以后实现重连接准备的

        注意： 因为这里context与sock对象形成了循环引用，所以这里将_context设置为None来取消循环引用
        :return:
        """
        if not self._closed:
            self._closed = True
            self._now_con.on_disconnect(self, self._context)
            self._sock.close()
            self._context.release()
            self._context = None
            self._now_con = None
            for fn in self._dic_connect_listener:
                run_task(fn)                         # 因为当前的执行环境是在主协程上面，所以这里将其派发到协程池中运行
            self._dic_connect_listener = None        # 将断线监听的引用都释放

    def write(self, data):
        """
        这里完成的事情是将服务层叫发送的数据加上头部，然后通过底层发送出去

        注意：这个方法不是协程安全的，所以不能在协程环境下并发调用
        注意：这里还要注意data是None的情况，这一般都是服务没有返回数据，所以就直接发送长度为0的标志数据

        这里注意了data为None的情况，这个时候返回给客户端的是头部为0的数据
        :param data:
        :return:
        """
        if self._closed:
            logging.error("write data on close socket")
            return
        length = len(data) if data else 0
        out_data = struct.pack("i", length)
        if data:
            out_data += data
        try:
            self._sock.sendall(out_data)
        except:
            self.close()


#################################################################################################
# 并没有独占协程来处理的socket，这个是在SelectTcpConnector里面使用的
# 所有的socket都直接调用readevent的start方法，其回调设置为process
# 默认情况下，服务端都应该使用这种类型的socket，可以提高服务端支撑连接的数目，提高整个系统的吞吐量
#################################################################################################
class SelectFSocket(FSocket):
    """
    因为很多请求共享同一个连接，那么本身就会有一定的风险，特别是在连接异常中止，但是两边都无法感知到的情况下尤为的危险，
    所以加上了应用的层的心跳，如果超过60秒钟没有收到任何数据，那么就直接认为连接已经关闭了
    """
    def __init__(self, sock, address, service_con, keep_alive_type=None):
        FSocket.__init__(self, sock, address, service_con)
        self._now_data = ""                                     # 缓存读取到的数据
        self._status = 1                                        # 标记当前正在读取header还是body，1头部，2body
        self._body_len = 0                                      # 用于记录body数据的长度
        self._read_event = sock._read_event                     # gevent的socket的读watcher
        self._write_event = sock._write_event                   # gevent的socket的写watcher
        self._read_event.start(self.process)                    # 启动读watcher
        self._send_data = []                                    # 这里可以理解为write其实是非阻塞的，数据先写到这里来
        self._write_open = False                                # 标记是否打开了写的监听
        self._real_sock = self._sock._sock                      # 真正的socket对象
        self._write_event.stop()                                # 默认是关闭写事件的监听的

        self._ping_timer = gevent.get_hub().loop.timer(20, 20)  # 默认每20秒钟发送一次心跳数据
        self._recv_ping_time = 0                                # 心跳标志位
        self._ping_data = struct.pack("i", -1)                  # 每次心跳发送4字节的数据

        if keep_alive_type == 1:
            self._init_heart_beat()                             # 如果长连接标志为1，那么应用层心跳
        elif keep_alive_type == 2:
            self._init_keep_alive()                             # 如果长连接标志为2，那么keepalive
        self._init_short_cut()                                  # 初始化一些快捷调用

    def _init_short_cut(self):
        """
        初始化一些常用的快捷调用

        其实主要还是为了释放send和recv对python层的socket的引用，让python层的socket能够尽快的关闭，
        防止出现CLOSE_WAIT的状态
        """
        self._send = self._real_sock.send                       # 通过socket发送数据
        self._recv = self._real_sock.recv                       # 通过socket读取数据
        self._service = self._now_con.service                   # 请求数据的路由处理
        self._real_stop_write = self._write_event.stop          # 停止write监听
        self._real_start_write = self._write_event.start        # 开启write监听

    def _clear_short_cut(self):
        """
        清空那些快捷的引用，因为这里直接引用了python层socket的recv和send方法，有可能会导致
        socket释放变慢，导致一些CLOSE_WAIT状态的TCP连接，所以需要在关闭的时候将这些引用释放掉
        """
        self._send = None
        self._recv = None
        self._service = None
        self._real_stop_write = None
        self._real_start_write = None

    def _ping(self):
        """
        这里检测心跳的标准是如果已经超过三次心跳检测都没有收到任何数据，那么就认定当前已经断开了
        """
        self._recv_ping_time -= 1                        # 每一次心跳周期将心跳计数器减1
        if self._recv_ping_time < -3:                    # 超过3次心跳检测都没有，那么就认为连接已经断开了
            self.close()                                 # 这里直接关闭
        else:
            self._send_data.append(self._ping_data)      # 将要发送的心跳数据放到发送队列里面
            if not self._write_open:                     # 如果没有打开写监听，那么打开写监听
                self._start_write()

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

    def process(self):
        """
        因为启动了当前socket的读监听，所以在有数据可以读取的时候，会调用这个方法来处理
        这里要做的事情就是读取出来数据然后处理

        注意：因为这个回调直接就是watcher的回调，是在loop协程上运行的，不能够阻塞loop协程，所以
            数据包解析出来之后服务的调用都需要派发到别的协程中运行
        注意：这里并没有不断的读取数据直到将数据全部读取完，因为默认水平触发，这样子可以降低一些recv系统调用的次数
        """
        try:
            now_data = self._recv(8192)              # 这里直接从底层的socket读取数据
            if not now_data:                         # 如果读取的数据为空，那么socket已经断开了
                self.close()                         # 关闭当前的连接
                return
        except:
            ex = sys.exc_info()[1]                       # 获取异常对象
            error_no = ex.args[0]                        # 错误号码
            sys.exc_clear()                              # 当前是在主协程中运行，这里直接清理掉异常信息
            if error_no not in (EWOULDBLOCK, EAGAIN):    # 这里判断一下是不是正常的异常情况
                self.close()                             # 一般情况下是一些reset关闭啥的
            return                                       # 根本就没有数据可以处理，直接返回
        self._now_data += now_data                       # 将刚刚收到的数据拼起来
        self._recv_ping_time = 0                         # 只要有数据来了就可以将心跳标志为设置为0了
        while 1:
            if self._status == 1 and len(self._now_data) >= HEAD_LEN:          # 当前为接收header的状态
                body_len_data = self._now_data[0:HEAD_LEN]                     # 获取头部数据
                self._body_len, = s_unpack("i", body_len_data)                 # 相应数据的body的长度
                self._now_data = self._now_data[HEAD_LEN:]                     # 将头部数据移除
                if self._body_len == -1:                                       # 心跳包，长度直接为-1
                    continue                                                   # 心跳数据
                self._status = 2                                               # 更改状态标志，接下来读取body
            elif self._status == 2 and len(self._now_data) >= self._body_len:  # 读取body的数据
                input_data = self._now_data[0:self._body_len]                  # 这个就是客户端发送过来的数据
                self._now_data = self._now_data[self._body_len:]               # 将输入数据清除
                self._status = 1                                               # 恢复状态标志为，下次就再次读取头部
                """
                注意：因为python本身并没有段作用域这种事说法，所以在构建异步执行的时候一定要非常小心
                     如果在异步执行的任务中直接引用外部闭包的变量，有可能会有一致性的问题，尤其是在循环内部，
                     外部的闭包变量可能已经更改，等到异步执行的时候，它所引用的变量也就更改了，从而导致
                     逻辑上的不正确

                     这里解决方案就是直接造再重新构建一层闭包，将外部变量重新包到新的闭包环境中去
                """
                run_task(self._generate_task(input_data))                      # 将任务派发到协程池中运行
            else:
                break                                                          # 数据还无法处理，那么跳出循环

    def _generate_task(self, data):
        """
        用于在解析出来了请求数据之后，生成回调执行函数，将其派发到协程池里面运行
        这里将其封装成一个函数来执行，主要是为了将调用参数通过闭包传递进去

        注意：因为当前是异步执行的，所以有可能在执行的时候连接就已经断开了，所以需要判断一下
        """
        def _task():
            """
            这部分的执行已经是在协程池环境中了
            """
            if self._closed:
                return                         # 因为是异步执行的，确实存在可能执行的时候连接已经断开了，那么就不搞了
            try:
                out = self._service(self._context, data)     # 调用上层代码来处理数据
                self.write(out)                              # 将返回的数据发送给调用方
            except:
                logging.error("服务处理错误")
                logging.error(traceback.format_exc())
                self.close()                   # 服务处理错误，这里安全起见，直接关闭连接
                sys.exc_clear()                # 因为当前是在协程池环境中运行，所以需要清理一下异常的状态

        return _task

    def _stop_write(self):
        """
        停止当前socket的写事件的监听
        """
        self._write_open = False
        self._real_stop_write()

    def _start_write(self):
        """
        开启当前socket的写监听
        """
        self._write_open = True
        self._real_start_write(self._write)

    def _write(self):
        """
        gevent的写监听事件的回调，这里通过真实的socket发送数据出去
        """
        if self._send_data:
            data = b"".join(self._send_data)                   # 合并需要发送的数据
            try:
                sent = self._send(data)                        # 直接通过底层socket的send发送数据，然后获取发送长度
                data = data[sent:]                             # 将发送了的数据移除，保留还剩下没有发送的数据
                if data:
                    self._send_data = [data]                   # 将那些还没有发送完的数据继续放到等待发送的队列里面
                else:
                    self._send_data = []                       # 清空发送队列
                    self._stop_write()                         # 所有数据都已经发送出去了，那么可以直接关闭写监听了
            except:
                ex = sys.exc_info()[1]
                sys.exc_clear()                                # 因为这里是在主协程中，所以清空异常信息
                if ex.args[0] not in (EWOULDBLOCK, EAGAIN):    # 检查一下是不是正常的异常
                    self.close()                               # 这个一般是连接断开了，那么直接关闭连接就好了
        else:
            self._stop_write()                                 # 当前根本就没有数据需要写，那么直接取消write的监听

    def write(self, data):
        """
        这里写直接实现成了非阻塞的，目的是为了尽快释放业务协程
        （1）先直接将数据写到当前的数据队列里面去
        （2）检查当前进程是不是打开了写监听，如果是的话，那么就不用管了，说明当前socket已经挂起了写事件，
            但是还在等待写事件的回调，

        注意：存在一些状况，当前连接其实已经关闭了，但是业务层返回数据来发送，这个时候就直接忽略数据，
            因为确实存在业务层都还没有执行完，还有这个socket的引用，但是其实底层已经断开了
        """
        if self._closed:
            return
        length = len(data) if data else 0
        header_data = s_pack("i", length)             # 构建数据的头部
        self._send_data.extend((header_data, data))   # 将头部和数据加入到发送队列里面
        if not self._write_open:                      # 如果没有激活写watcher，那么这里打开
            self._start_write()

    def close(self):
        """
        这里在原来的基础上扩展出了读写watcher的停止

        注意：这里一定要将真正socket的引用释放掉，否则，会出现服务端socket的状态为close_wait,客户端为fin_wait2的状态
              因为服务端并没有发送fin数据包
        """
        if not self._closed:                            # 确保close方法只被执行一次
            FSocket.close(self)
            self._clear_short_cut()
            self._ping_timer.stop()                     # 停止心跳定时
            self._read_event.stop()                     # 停止读取watcher
            self._write_event.stop()                    # 停止写watcher
            self._send_data = None                      # 直接清空要发送的数据
            self._real_sock = None                      # 将对真正的socket的引用释放掉

