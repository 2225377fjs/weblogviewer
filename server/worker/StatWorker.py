# -*- coding: utf-8 -*-
__author__ = 'fjs'
from worker.EntityWorker import EntityWorker
from bean.Entity import Entity, rpc_method, rpc_message
from lib.HttpConnector import HttpConnector
import tornado.web
import json
import gevent
import datetime
import pymongo
from lib.LetPool import run_task


STATE_ENTITY_NAME = "state_entity"
_STATE_ENTITY = None                     # 直接申明一个模块变量，用于因引用StatEntity对象
SAVE_CLO = "rpchistory"                  # 历史记录如果要村mongo，那么将会存在这个集合里面



#
# 整个entity集群的监控进程，将会向外部提供一个http接口，用于查询整个集群在config进程上面注册的
# entity，以及tag
# 用于实现集群中entity的调用的监控，也就是在包装的时候monitor参数为True的方法rpc方法的调用
# 次数，调用耗时，出错次数
#
class InvokeInfo(object):
    """
    用于记录一个受监控的entity的调用信息
    """
    def __init__(self, entity_id, entity_type, process_info):
        self._id = entity_id                       # entity的id
        self._type = entity_type                   # entity的类型
        self._process_info = process_info          # 所在的进程信息(pid, {ip, port})
        self._detail = []                          # 每一次上传上来的数据都保存一份
        self._all_count = dict()                   # 总的调用数据

    @property
    def all_count(self):
        return self._all_count

    def _add_all_info(self, method_name, info):
        """
        这里记录各个方法总的调用信息
        """
        count, time, error_time = info[0], info[1], info[2]
        if method_name not in self._all_count:
            self._all_count[method_name] = dict()
            self._all_count[method_name]["count"] = 0     # 总的调用次数
            self._all_count[method_name]["time"] = 0      # 总的耗时
            self._all_count[method_name]["error"] = 0     # 出错的次数
        self._all_count[method_name]["count"] += count
        self._all_count[method_name]["time"] += time
        self._all_count[method_name]["error"] += error_time

    def add_rpc_info(self, info):
        """
        将worker进程push上来的数据保存起来
        (1)detail,(2)all
        :param info:
        {
            "entity_id": "321",
            "entity_type": "fdsa",
            "process_info": (pid, (ip, port))
            "invoke": {
                method_name: [count, time, error_time],
            }
        }
        :return:
        """
        invokes = info.get("invoke", [])
        self._detail.append(info)
        for m_name, invoke_info in invokes.items():
            self._add_all_info(m_name, invoke_info)

    def get_method_detail(self, method_name):
        """
        获取某个特定的方法的详细调用信息
        """
        out = []
        for item in self._detail:
            invoke_info = item["invoke"]
            if method_name in invoke_info:
                out.append(invoke_info[method_name])
        return out

    def get_json_detail(self):
        """
        为了方便在web页面展示，这里需要将调用的数据转化为json数据
        另外，如果历史数据需要保存到mongo里面，那么保存每一个entity的调用数据的时候也会通过这个方法来获取数据，
        然后保存到mongo里面
        {
            "id": "entityid",
            "type": "entityclass",
            "pid": "pid",
            "address": "123.32.213.1:9090"
            "allcount": {
                "method1": {
                    "count": 123,
                    "time": 32131
                }
            },
            "detail": {
                "method1": [[count, time, error_time], [count, time, error_time]],
                "method2": [[count, time, error_time], [count, time, error_time]]
            }
        }
        """
        address_str = self._process_info[1]["ip"] + ":" + str(self._process_info[1]["port"])
        out = dict(id=self._id, type=self._type, pid=self._process_info[0], address=address_str)
        out["allcount"] = self._all_count
        out["detail"] = dict()
        for item in self._detail:
            invoke_item = item["invoke"]
            for method_name, invoke_info in invoke_item.items():
                if method_name not in out["detail"]:
                    out["detail"][method_name] = []
                out["detail"][method_name].append(invoke_info)
        return out


class StatEntity(Entity):
    """
    别的进程通过调用这个entity的rpc方法来将一些调用的信息发送上来
    """
    def __init__(self, mongo_info):
        """
        :param mongo_info: {"ip": "", "port": 123, "db": "", "account": "", "pwd": ""}
        :return:
        """
        Entity.__init__(self, STATE_ENTITY_NAME, broadcast=True)
        self._data = dict()                       # 用于记录entity的调用信息 entity_id -> InvokeInfo
        if mongo_info is not None:
            self._mongo_info = mongo_info
            self._timer = None
            self._last_day = self.get_today_day_str()
            self._init_mongo()

    @staticmethod
    def get_today_day_str():
        return str(datetime.datetime.now().day)

    def _init_mongo(self):
        """
        初始化用于记录历史记录的timer，并启动
        """
        self._timer = gevent.get_hub().loop.timer(300, 300)
        self._timer.start(self._do_save)

    def _do_save(self):
        """
        用于在日期改变了之后将数据存到mongo数据库，并直接接记录的rpc数据清空

        这部分代码在定时器中执行，因为在hub协程上，所以需要派遣到协程吃中执行具体的存储操作，首先需要判断一下
        日期是不是已经改变了，如果改变了，那么再执行存储操作
        """
        def _process():
            new_date_str = str(datetime.datetime.now().date())
            client = pymongo.MongoClient(host=self._mongo_info["ip"], port=self._mongo_info["port"])
            database = client.get_database(self._mongo_info["db"])
            if "account" in self._mongo_info:
                database.authenticate(self._mongo_info["account"], self._mongo_info["pwd"])
            col = database.get_collection(SAVE_CLO)
            infos, self._data = self._data, dict()
            for info in infos.values():
                data = info.get_json_detail()
                data["date"] = new_date_str
                col.insert_one(data)

            client.close()
        now_day_str = self.get_today_day_str()
        if now_day_str != self._last_day:
            self._last_day = now_day_str
            run_task(_process)

    @rpc_message()
    def process_info(self, infos):
        """
        :param infos: [
                         {
                            "entity_id": "321",
                            "entity_type": "fdsa",
                            "process_info": (pid, (ip, port))
                            "invoke": {
                                method_name: [count, time, error_time],
                            }
                        },
                    ]
        普通的entityworker对于一些受监控的entity，每过一个监控周期就会将这个周期以来的rpc调用的监控信息发送上来,
        这里对收到的数据进行保存处理，都是在entity进程的GlobalEntityManager定时将这些调用信息发送上来的
        """
        for info in infos:
            entity_id = info["entity_id"]
            entity_type = info["entity_type"]
            process_info = info["process_info"]
            if entity_id not in self._data:
                self._data[entity_id] = InvokeInfo(entity_id, entity_type, process_info)
            self._data[entity_id].add_rpc_info(info)

    def get_rpc_info(self):
        """
        用于获取所有的监控的entity的rpc信息，这个方法用于做一个综合性的显示
        """
        out = dict()
        for entity_id, invoke_info in self._data.items():
            out[entity_id] = invoke_info.get_json_detail()
        return out

    def get_entity_method_detail(self, entity_id, method_name):
        """
        用于获取某个entity的某个方法的详细调用监控信息
        """
        entity_info = self._data.get(entity_id)
        if entity_info:
            return entity_info.get_method_detail(method_name)
        else:
            return []

    def get_entity_info(self):
        """
        从config进程获取当前注册的entity的信息，用于在web界面上展示
        get_all_entity_info方法的返回值:
        {
            "entity_id": EntityInfo,
        }
        """
        return self.config_stub.get_all_entity_info()


#
# 接下来是一些http handler
#
class GetInfo(tornado.web.RequestHandler):
    """
    用于在web页面上展示当前所有注册的entity的基本信息，包括名字，进程信息啥的
    """
    def get(self, *args, **kwargs):
        data = _STATE_ENTITY.get_rpc_info()
        self.render("rpcinfo.html", data=json.dumps(data))


class AllEntityInfo(tornado.web.RequestHandler):
    """
    获取整个集群需要监控的entity的基本信息，包括名字，类型啥，调用次数啥的
    """
    def get(self, *args, **kwargs):
        info = _STATE_ENTITY.get_entity_info()
        out = []
        for entity_id, entity_info in info.items():
            address = ":".join([entity_info.address["ip"], str(entity_info.address["port"])])
            out.append((entity_id, entity_info.pid, entity_info.tag, address))
        self.render("allentityinfo.html", infos=out)


class RpcDetail(tornado.web.RequestHandler):
    """
    用于获取某个entity的某个方法的具体的rpc方法的执行监控情况，然后在web页面上绘制关于调用总次数，
    平均耗时，错误次数的曲线图
    """
    def get(self, *args, **kwargs):
        entity_id = self.get_argument("id")
        method_name = self.get_argument("mname")
        data = _STATE_ENTITY.get_entity_method_detail(entity_id, method_name)
        self.render("rpcdetail.html", data=json.dumps(data), id=entity_id, name=method_name)





class StatWorker(EntityWorker):
    def __init__(self, config_address, http_port, mongo_info=None, **kwargs):
        """
        :param config_address:    config进程的监听地址
        :param http_port:         创建http的监听端口
        :param mongo_info:        用于存储历史数据的mongo {"ip": "", "port": 123, "db": "", "account": "", "pwd": ""}
        :param regist_address:    如果是多物理机部署的话，需要传入一个注册地址
        :param need_confirm:      默认是有意个main进程监控的，单进程启动的话不许要
        :return:
        """
        EntityWorker.__init__(self, "stat_worker", "stat_worker", 7890, config_address, **kwargs)
        self._http_port = http_port         # 用于创建http服务器的端口
        self._http_con = None
        self._mongo_info = mongo_info

    def _init_http(self):
        """
        初始化http的部分
        """
        self._http_con = HttpConnector(self._http_port)
        self._http_con.add_route("/rpcinfo", GetInfo)
        self._http_con.add_route("/entityinfo", AllEntityInfo)
        self._http_con.add_route("/rpcdetail", RpcDetail)
        self._http_con.start()

    def init_entity(self):
        self._init_http()
        global _STATE_ENTITY
        _STATE_ENTITY = StatEntity(self._mongo_info)
