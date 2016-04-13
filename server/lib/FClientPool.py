# -*- coding: utf-8 -*-
__author__ = 'fjs'
import FClient
import gevent.queue
import contextlib
import select
import logging



def socket_closed(sock):
    """
    判断一个socket是不是已经断开了，
    注意：在调用这个方法的时候需要保证这个socket当前是没有数据可以读的
    """
    try:
        rd, _, _ = select.select([sock], [], [], 0)
    except:
        return True
    return len(rd) > 0   # 这里其实已经保证当前socket没有数据可读，而这里可读，只能说明连接断开了


################################################################################
# 用于维护与fsocket服务端的连接池，这里从连接池中获取连接的时候一定要通过with语句
# 另外在每一次归还连接的时候都判断一下当前连接是否正常
################################################################################


class FClientPool(object):
    def __init__(self, host, port, max_connection=2):
        """
        默认的最大连接数是两个
        """
        self._host = host
        self._port = port
        self._max_connection = max_connection
        self._connection_num = 0    # 一开始的时候建立的连接数是0
        self._connection_queue = gevent.queue.Queue()

    @property
    def remote_address(self):
        """"""
        return str(self._host) + ":" + str(self._port)

    @contextlib.contextmanager
    def get_connection(self, timeout=3):
        """
        首先判断当前队列里面是不是有数据连接可用，如果有的话，如果没有了，而且创建的连接数还没有
        达到最大的限制，那么创建一个连接
        默认超时是3秒钟，这里如果出现了异常，那么会将异常抛出给上层，通知出现异常，因为这里的异常不光可能是超时的异常，
        也有可能是外部环境的异常，而且对于外部的异常，这里应该要将当前利用的连接关闭保证安全
        """
        con = None
        try:
            if self._connection_queue.qsize() == 0 and self._connection_num < self._max_connection:
                self._make_connection()
            con = self._connection_queue.get(block=True, timeout=timeout)
            yield con
        except Exception, e:
            if con:
                assert isinstance(con, FClient.FClient)
                con.close()
                self._connection_num -= 1
            raise e
        else:
            self._return_connection(con)

    def _make_connection(self):
        """
        创建一个链接，并将其放到队列里面去，注意这里可能会出现建立连接失败的异常，所以
        在建立连接失败之后将创建的连接数量减去1
        """
        client = None
        try:
            self._connection_num += 1
            client = FClient.create_client(self._host, self._port)
            self._connection_queue.put(client)
        except Exception, e:
            if client:
                client.close()
            logging.error("创建连接失败:" + str(self))
            self._connection_num -= 1
            raise e

    def _return_connection(self, con):
        """
        在连接使用完了之后，将会调用这个方法来将连接返回
        这里做了一个连接状态的检查，如果连接已经断开了，那么就将其移除
        否则才将其放到连接队列里面
        """
        assert isinstance(con, FClient.FClient)
        if socket_closed(con.sock):
            logging.error("FClient连接断开了，连接信息: " + str(self))
            con.close()
            self._connection_num -= 1
        else:
            self._connection_queue.put(con)

    def __str__(self):
        return self._host + ":" + str(self._port)

    def close(self):
        """
        用于关闭与与远端服务器之间的tcp连接
        注意：在调用这个方法的时候需要确保没有连接已经拿出去用了，否则可能会有连接不会关闭
        """
        while self._connection_queue.qsize() > 0:
            con = self._connection_queue.get()
            if con:
                con.close()



# data = "fjsfjs" * 1000
# pool = FClientPool("127.0.0.1", 8001, max_connection=3)

# with pool.get_connection() as con:
#     pass

# number = 0
#
# def test():
#     with pool.get_connection() as con:
#         global number
#         assert isinstance(con, FClient.FClient)
#         import Constant
#         for i in xrange(150000):
#             out = con.request(Constant.GET_KEY_SERVICE, data)
#             number += 1
#             if number == 300000:
#                 after = time.time()
#                 print after - before
#
#
#
# import time
# before = time.time()
#
# gevent.spawn(test)
# gevent.spawn(test)
#
# while True:
#     gevent.sleep(10000000)
#
#
