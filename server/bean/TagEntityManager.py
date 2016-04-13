# -*- coding: utf-8 -*-
__author__ = 'fjs'

from bean.BeanManager import Bean
from bean.Entity import get_gem
from bean.Entity import rpc_method
import logging
from bean.Entity import Entity
import uuid


#
# 因为tag类型的entity可能会随着进程的加入和退出发送变动，所以需要有一个管理器来进行管理
# 实现的原理是：向config进程注册当前tag管理器，那么相关的tag有变动的时候将会发送信息来通知
#
class TagEntityManager(Bean, Entity):
    def __init__(self, name, tag):
        """
        对于tag管理器，虽然在创建的时候会向config进程注册当前entity，但是并不需要以后刻意的向config进程来销毁当前
        tag管理器的注册，因为config部分在掉哟功能当前entity方法的时候，如果出现了异常，会自动将这个enttiy的stub直接
        移除
        :param name:    因为是一个bean，所以需要设置一个名字
        :param tag:     需要管理的entity的tag的名字
        """
        Bean.__init__(self, name)
        Entity.__init__(self, str(uuid.uuid4()))  # 对于entity的id，这里创建一个唯一的就行
        self._tag = tag                           # 需要维护的entity的tag值
        self._stubs = []                          # 从config上面获取的指定的tag的entity将会保存到这里来
        self._status = False                      # 如果进程信息有变动，将会将这个标志为设置为True

        # 用于向config进程注册当前tag管理器
        self.config_stub.add_tag_manager(self._id, self._manager.address, self._tag)

    @property
    def stubs(self):
        """
        获取对应所有的tag类型的entity的stub序列
        """
        if not self._status:                  # 如果进程信息有变动，那么需要重新获取一下entity的信息
            self._status = True
            self._stubs = get_gem().get_all_tag_entities(self._tag)
        return self._stubs

    @rpc_method()
    def need_change(self):
        """
        在config进程中，如果与当前关注的tag有entity的变动，那么config将会调用这个方法来通知，
        那么下次在获取tag相关的entity的信息的时候，就会重新获取一次
        """
        logging.info("recv tag:%s change", self._tag)
        self._status = False
