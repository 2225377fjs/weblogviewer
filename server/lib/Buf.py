# -*- coding: utf-8 -*-
__author__ = 'fjs'

import gevent.socket

import Constant


#############################################################################
# 对socket接收数据进行buffer话，可以降低很多内存的分配操作，用于降低服务器的负载
#
# todo 现在做了三种类型的buf，512字节，1024字节和2048字节，理论上来说在recv的时候
# todo 构建buf的大小应该根据客户端的网络状况进行一些分配策略的控制，类似于netty的那一套
# todo 这应该涉及一些算法的实现，来自动化的分配，不过暂时这里先不做，有时间再说
# todo 对于每一次内存分配，都分配1024字节大小的buf
#############################################################################


#只有这三种固定大小的Buf
BUF_TYPES = [Constant.BUF_TYPE_SMALL, Constant.BUF_TYPE_MIDDLE, Constant.BUF_TYPE_BIG]


###########################################################
# 这里主要是对buf的类型进行了一次包装，在buf的控制器上，用next引用
# 来组成了一个单链表
#
# 注意：向buf中写数据是一次性的，不能多次写
###########################################################
class Buf(object):
    def __init__(self, buf_type):
        assert buf_type in BUF_TYPES
        self._buf_type = buf_type
        self._next_buf = None                #用于构建buf链
        self._last_cur = 0                   #在调用了recvinto之后将会将这个字段设置为读取的数据量
        self._cur = 0                        #从这个buf中读取数据的下标，它最多可以读取到last_cur那去
        self._buf = bytearray(buf_type)      #分配存储数据的内存
        self._view = memoryview(self._buf)   #构建视图
        self._can_write = True               #当前buf对象是否可以写数据，一个buf只能写一次数据，下次要写系需要reset

    @property
    def buf(self):
        return self._buf

    @property
    def next(self):
        return self._next_buf

    @next.setter
    def next(self, next_buf):
        self._next_buf = next_buf

    @property
    def buf_type(self):
        return self._buf_type

    def __len__(self):
        """
        获取当前buf中可以读取的数据的长度
        :return:
        """
        return self._last_cur - self._cur

    def read_from_sock(self, fsock):
        """
        从FSocket对象中读取数据写到buf中去，更新标志位，然后返回这次读取的数据的长度

        注意：buf只能写一次，然后就只能读取了，下一次要写，必须要调用reset，多次写数据直接assert错误
             另外这里没有做异常的处理，应该在外部环境中处理
        :type fsock: FSocket.FSocket
        :param fsock:
        :return:
        """
        assert self._can_write
        self._can_write = False
        assert isinstance(fsock, gevent.socket.socket)
        nu = fsock.recv_into(self._buf, self._buf_type)
        self._last_cur = nu
        return nu

    def consume(self, nu):
        """
        当前这个buf中读取nu字节的数据，这里要一定要注意，
        调用这个方法之前一定要确定当前buf可以读取的数据要足够才行
        :param nu:
        :return:
        """
        assert len(self) >= nu
        out_data = self._view[self._cur:self._cur + nu]
        self._cur += nu
        return out_data.tobytes()

    def reset(self):
        """
        重置当前的buf对象，这里主要是重置一些标志位
        :return:
        """
        self._can_write = True
        self._cur = 0
        self._last_cur = 0



################################################################
# buf的管理器，这里将会为每种类型的buf都建立一个单链表
#
# todo 这里应该建立一个定时器，在buf链很长的时候，也就代表这种buf分配了过多
# todo 的时候，解除一些buf的引用，让内存被回收，以后再说吧
################################################################
class BufManager(object):
    def __init__(self):
        self._buffers = dict()            #保存当前所有的buf，type-->buf

    def get_buf(self, buf_type):
        """
        这里获取这种类型的的buf
        （1）如果当前的缓存里面没有这种类型的buf，那么创建一个，然后返回
        （2）如果有的话，那么获取第一个buf，然后将将next保存在buffers字典里面
        :param buf_type:
        :rtype: Buf.Buf
        :return:
        """
        assert buf_type in BUF_TYPES
        buf_first = self._buffers.get(buf_type)

        ###########################################################
        # 注意：因为Buf类型有__len__方法，所以这里不能用not来直接判断是否是
        # None，如果buf没有数据可读，not也是返回True的
        ###########################################################
        if buf_first is None:
            buf_first = Buf(buf_type)
        else:
            assert isinstance(buf_first, Buf)
            self._buffers[buf_type] = buf_first.next
        return buf_first

    def return_buf(self, buf):
        """
        将当前这个buf重置，然后插入到当前这种类型的buf链的头部
        :param buf:
        :return:
        """
        assert isinstance(buf, Buf)
        buf.reset()
        buf_type = buf.buf_type
        first_buf = self._buffers.get(buf_type)
        buf.next = first_buf
        self._buffers[buf_type] = buf






##############################################
# 这是一个全局唯一的单例对象
##############################################
BUF_MANAGER = BufManager()
def get_buf_manager():
    return BUF_MANAGER


