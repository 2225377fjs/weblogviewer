__author__ = 'fjs'
# -*- coding: utf-8 -*-

from worker.EntityWorker import EntityWorker
from app_entity.Node import Node
from config import NodeConfig


#
# 用于代表一个log日志节点
#
class NodeWorker(EntityWorker):
    def __init__(self, port):
        EntityWorker.__init__(self, "log_node", "log_node", port, None, need_confirm=False)

    def init_entity(self):
        node_info = NodeConfig.NODE
        node_name = node_info["name"]

        logs = node_info["logs"]
        Node(node_name, logs)



