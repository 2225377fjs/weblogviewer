__author__ = 'fjs'
# -*- coding: utf-8 -*-

from worker.EntityWorker import EntityWorker
from app_entity.LogCenter import LogCenter


#
# 一个中心进程，用于管理注册的Node，这里通过创建LogCenter来暴露服务出去
#
class LogCenterWorker(EntityWorker):
    def __init__(self, entity_port, config_address):
        EntityWorker.__init__(self, "LogCenter", "LogCenter", entity_port, config_address)

    def init_entity(self):
        LogCenter()
