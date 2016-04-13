# -*- coding: utf-8 -*-
__author__ = 'fjs'
from bean.BeanManager import Bean
import gevent
from lib.LetPool import run_task


WS_MANAGER = "ws_m"
TIME_OUT = 60                 # token的超时时间是60秒


#
# 因为不能随便让websocekt就直接脸上来获取数据，所以websocekt连接上来之后需要用一个
# token来进行标识，这个token会先存在服务器，只有token有了，才能获取数据
#
class WebSocketManager(Bean):
    def __init__(self):
        Bean.__init__(self, WS_MANAGER)
        self._tokens = set()

    def add_token(self, token):
        """
        在添加了token之后，会创建一个任务，在超时之后将token直接移除
        :param token:   一个字符串，其实是一个uuid
        """
        self._tokens.add(token)

        def _clear():
            gevent.sleep(TIME_OUT)
            if token in self._tokens:
                self._tokens.remove(token)
        run_task(_clear)

    def consume(self, token):
        """
        websocket连接上来之后，需要通过这里来验证token
        :return:  验证成功返回True，失败返回False
        """
        if token in self._tokens:
            self._tokens.remove(token)
            return True
        return False


