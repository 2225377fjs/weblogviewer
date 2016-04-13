__author__ = 'fjs'
# -*- coding: utf-8 -*-


from bean.Entity import Entity
from bean.Entity import rpc_method
from app_entity.Node import NODE_NAME
import gevent
from lib.LetPool import run_task
import logging
import traceback
import json


CHECK_TIME = 60                            # 每过一段时间来check一下节点的状态
LOG_CENTER_NAME = "l_cen"                  # log center在config上面注册的名字


#
# 生活在中心管理进程的Entity，主要用于管理注册的Log节点信息，提供一些基础的服务给别的进程
#
class LogCenter(Entity):
    def __init__(self):
        Entity.__init__(self, LOG_CENTER_NAME, broadcast=True)
        self._nodes = {}
        self._ping_timer = gevent.get_hub().loop.timer(CHECK_TIME, CHECK_TIME)
        self._ping_timer.start(self._do_ping)      # 这个用于定期的检查node节点是否还存活
        self._init_from_persist()

    def _persist(self):
        """
        用于将已经注册的节点的信息保存到文件里面
        """
        out_data = dict()
        for node_name, node_info in self._nodes.items():
            info = dict()
            info["online"] = False
            info["log_list"] = node_info["log_list"]
            info["address"] = node_info["address"]
            out_data[node_name] = info

        with open("./config/nodes.json", "w") as open_file:
            open_file.write(json.dumps(out_data))

    def _init_from_persist(self):
        """
        在启动的时候首先从本地文件中获取注册节点的信息

        这里需要先测试一下节点是否存活，并且从新获取一下节点的日志列表
        """
        try:
            with open("./config/nodes.json") as open_file:
                json_data = open_file.read()
                infos = json.loads(json_data)
                for node_name, node_info in infos.items():
                    self._nodes[node_name] = node_info
                    stub = self.manager.create_remote_stub(NODE_NAME, {"ip": node_info["address"][0], "port": node_info["address"][1]})
                    node_info["node_stub"] = stub
                    try:
                        stub.ping()
                        node_name, log_name_list = stub.get_info()
                        node_info["log_list"] = log_name_list
                        node_info["online"] = True
                    except:
                        pass
        except:
            logging.error("init from persist error")
            logging.error(traceback.format_exc())

    def _do_ping(self):
        """
        用于定期的遍历所有的远程节点，测试他们是否还存活，如果已经无法联通了，那么需要将状态标志为设置为False
        表示这个节点不在线
        """
        def _do_task():
            for node_name, node_info in self._nodes.items():
                stub = node_info["node_stub"]
                try:
                    stub.ping()
                    if not node_info["online"]:
                        node_info["online"] = True
                        node_name, log_name_list = stub.get_info()
                        node_info["log_list"] = log_name_list
                except:
                    node_info["online"] = False
                    logging.error("ping error")
                    logging.error(node_name)
        run_task(_do_task)

    @rpc_method()
    def get_node_info(self):
        """
        web进程通过调用这个接口来获取当前所有加入了的Node节点的信息

        注意：这里对已经不在线了的节点做了一些特殊的处理，将其log列表移除了，防止
            在前台界面上看到他们的日志列表
        """
        out = dict()
        for node_name, node_info in self._nodes.items():
            out[node_name] = dict(online=node_info["online"], address=node_info["address"])
            if "log_list" in node_info and node_info["online"]:
                out[node_name]["log_list"] = node_info["log_list"]
            else:
                out[node_name]["log_list"] = []
        return out

    @rpc_method()
    def get_remote_info(self, node_name, log_name):
        """
        在web端建立了websocket连接之后，将会通过调用这个方法来获取远端node的stub信息，用于从远端获取log日志

        （1）获取这个节点对应的监听信息
        （2）在远程节点上面创建LogSender对象，获取这个对象的id
        （3）将远程节点的监听信息和刚刚创建的LogSender的id信息一起返回回去
        :param node_name:  远端节点的名字
        :param log_name:   远端节点需要监控的log的名字
        """
        try:
            node_info = self._nodes[node_name]
            stub = node_info["node_stub"]
            sender_id = stub.create_log_sender(log_name)
            return node_info["address"], sender_id
        except:
            logging.error("创建远端sender异常")
            logging.error(traceback.format_exc())

    @rpc_method()
    def add_node(self, address):
        """
        用于监控服务器上面的进程启动之后向当前对象发送其node的信息，主要是address的信息

        这个方法一般实在web界面的node配置界面配置了一个远程节点之后掉调用的

        这里要做的事情是：
        （1）创建远端进程的NodeEntity的stub对象
        （2）检测是否存活，获取其监控的日志的名字列表
        （3）获取其log列表

        节点信息的存储格式：
        _nodes--> {
            "node1": {
                "address": (ip, port),
                "log_list": ["log1", "log2"],
                 "node_stub": stub
            }
        }
        :param address:  (ip, port)
        :return: 添加成功，返回True，否则返回False
        """
        try:
            node_stub = self.manager.create_remote_stub(NODE_NAME, {"ip": address[0], "port": address[1]})
            node_name, log_name_list = node_stub.get_info()
            if node_name in self._nodes:
                logging.error("添加node节点失败，重复的node名字: %s", node_name)
                return False
            self._nodes[node_name] = dict(address=address, online=True)
            self._nodes[node_name]["log_list"] = log_name_list
            self._nodes[node_name]["node_stub"] = node_stub

            self._persist()
            return True
        except:
            logging.error("添加node节点异常")
            logging.error(traceback.format_exc())
            return False

    @rpc_method()
    def del_node(self, node_name):
        """
        用于移除一个注册的节点
        """
        if node_name in self._nodes:
            del self._nodes[node_name]
            self._persist()


