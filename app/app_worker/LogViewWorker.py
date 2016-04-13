__author__ = 'fjs'
# -*- coding: utf-8 -*-



from worker.EntityWorker import EntityWorker
import tornado.web
from geventwebsocket import WebSocketServer, WebSocketApplication, Resource
from collections import OrderedDict
from lib.HttpConnector import HttpConnector
import uuid
from app_bean.WebSocketManager import WebSocketManager, WS_MANAGER
from bean.BeanManager import get_manager
from bean.Entity import get_gem
from app_entity.LogCenter import LOG_CENTER_NAME
import json
from lib.LetPool import run_task
import gevent


#
# 提供一个展示监控信息的web界面，并提供websocket的连接接口，用于主动想前端推送log数据
#


WS_PORT = 0               # 用于记录当前进程的websocket的监听端口
DEFAULT_SLEEP_TIME = 0.2


class ShowLog(tornado.web.RequestHandler):
    """
    显示指定log的handler，这里需要从url里面获取制定的节点的名字和log的名字，另外还有一个连接
    websocket的token，只有这个token才能
    """
    def get(self, *args, **kwargs):
        node_name = self.get_query_argument("node")
        log_name = self.get_query_argument("log")
        web_socekt_uuid = str(uuid.uuid4())                  # 为连接websocket分配一个token
        get_manager().get_bean(WS_MANAGER).add_token(web_socekt_uuid)  # 将这个token保存起来
        render_parms = dict(ws_port=WS_PORT, node_name=node_name, log_name=log_name)
        render_parms["ws_id"] = web_socekt_uuid
        self.render("showlog.html", **render_parms)


class AllNode(tornado.web.RequestHandler):
    """
    用于在web界面上显示当前所有的节点的信息
    其实就是直接到logcenter里面去获取当前配置的所有的日志节点的信息就可以了
    """
    def get(self, *args, **kwargs):
        log_center = get_gem().get_remote_entity(LOG_CENTER_NAME)
        node_info = log_center.get_node_info()
        self.render("allnodes.html", data=json.dumps(node_info))


class NodeManage(tornado.web.RequestHandler):
    """
    用于在web界面上显示添加和移除log节点的表单
    """
    def get(self, *args, **kwargs):
        try:
            self.render("nodemanage.html")
        except:
            import traceback
            print traceback.format_exc()


class AddNode(tornado.web.RequestHandler):
    """
    web界面添加node信息将会把数据post到这里来处理，这里调用center的方法来添加node，
    然后将数据通过页面的方式通知用户
    """
    def post(self, *args, **kwargs):
        try:
            ip = self.get_body_argument("ip")
            port = int(self.get_body_argument("port"))
            log_center = get_gem().get_remote_entity(LOG_CENTER_NAME)
            success = log_center.add_node((ip, port))
            self.render("addresult.html", success=success)
        except:
            self.render("addresult.html", success=False)


class DelNode(tornado.web.RequestHandler):
    """
    用于一个注册的节点
    """
    def get(self, *args, **kwargs):
        try:
            node_name= self.get_query_argument("name")
            log_center = get_gem().get_remote_entity(LOG_CENTER_NAME)
            log_center.del_node(node_name)
            self.redirect("/nodes")
        except:
            pass


#
# 前端页面将会通过这里来建立websocket的连接，用于服务器想前端页面推送最新的log日志
#
class LogWebSocket(WebSocketApplication):
    def __init__(self, *args, **kwargs):
        WebSocketApplication.__init__(self, *args, **kwargs)
        self._auth = False                 # 用于标记还没有通过token的验证
        self._node_name = None             # 监控的远程节点的名字
        self._log_name = None              # 需要监控的日志文件的名字
        self._send_stub = None             # 具体需要监控的进程上的sender对象的stub
        self._closed = False               # 当前连接是否已经关闭了

        self._last_sleep_time = DEFAULT_SLEEP_TIME        # 初始等待时间
        self._last_get = False             # 用于标记上次获取远程log数据，是否获取到了数据

        self._stop = False                 # 客户端的是否以及功能暂停的标志

    def on_open(self):
        pass

    def on_message(self, message):
        """
        客户端websocket发送消息将会到这里来处理

        注意：如果连接断开了，其实也会触发一次这个方法，不过message是个None，所以需要注意处理
        """
        if not message:
            return
        data = json.loads(message)
        method = data["m"]
        args = data["args"]
        my_method = getattr(self, method)
        my_method(*args)

    def pause(self):
        self._stop = True

    def start(self):
        self._stop = False

    def register(self, token, node_name, log_name):
        """
        websocket连接上来之后，先要表示直接要监听的节点的名字和日志的名字
        这里通过center来到具体的log进程上去创建LogSender对象，然后这里来获取

        一切都成功之后，需要启动一个任务来专门执行获取日志的操作，其实就是不断的调用sender_stub的get_data操作

        这里如果出现了异常，可能是远端创建entity的异常，也不好处理，就通知一下客户端
        :param token:      用于进行连接授权的token，只有token服务器有记录才能连接上
        :param node_name:  监听的节点的名字
        :param log_name:   日志的名字
        """
        try:
            if not get_manager().get_bean(WS_MANAGER).consume(token):
                self.ws.close()
            self._auth = True
            self._node_name = node_name
            self._log_name = log_name
            center_stub = get_gem().get_remote_entity(LOG_CENTER_NAME)
            remote_info = center_stub.get_remote_info(self._node_name, self._log_name)
            address, entity_id = remote_info
            sender_stub = get_gem().create_remote_stub(entity_id, {"ip": address[0], "port": address[1]})
            self._send_stub = sender_stub
            self._start_get()
        except:
            pass

    def _start_get(self):
        """
        用于在协程中不断的从远端获取日志最新数据，然后通过websocket推送到web端
        如果出现了异常，那么多半是远端的异常，这里也没啥可做的，通知一下客户端就好了把
        """
        def _run():
            try:
                while not self._closed:
                    log_data = self._send_stub.get_data()
                    if log_data and not self._stop:      # 这里如果暂停了，那么就直接跳过数据
                        # 重置标志为以及sleep等待时间
                        self._last_get = True
                        self._last_sleep_time = DEFAULT_SLEEP_TIME
                        out = []
                        for line in log_data:
                            out.append(line)
                        json_data = json.dumps(out)
                        data = dict(method="messages", data=json_data)
                        self.ws.send(json.dumps(data))
                    else:
                        if self._last_get:
                            self._last_get = False
                        else:
                            self._last_sleep_time += 0.1      # 将sleep的时间延长一点
                            if self._last_sleep_time > 1:
                                self._last_sleep_time = 1
                        gevent.sleep(self._last_sleep_time)
            except:
                pass
            finally:
                if not self._closed:     # 退出了while循环，多半除了问题，就关闭websocket连接吧
                    self.ws.close()

        run_task(_run)

    def on_close(self, reason):
        """
        在websocket连接断开之后，这里需要通知远端的sender对象，及时的释放资源
        """
        if self._send_stub:
            self._send_stub.close()
            self._send_stub = None
        self._closed = True


class LogViewWorker(EntityWorker):
    def __init__(self, config_address, entity_port, http_port, ws_port):
        """
        :param config_address:  config进程的监听地址
        :param entity_port:     entity的服务接口
        :param http_port:       http的监听端口
        :param ws_port:         websocket的监听端口
        :return:
        """
        EntityWorker.__init__(self, "log_view", "log_view", entity_port, config_address)
        self._http_port = http_port
        self._ws_port = ws_port
        self._ws_server = None
        self._http_server = None

        global WS_PORT
        WS_PORT = ws_port

    def init_entity(self):
        self._ws_server = WebSocketServer(
            ('', self._ws_port),
            Resource(OrderedDict({"/": LogWebSocket}))
        )
        self._ws_server.start()

        self._http_server = HttpConnector(self._http_port)
        self._http_server.add_route("/showlog", ShowLog)
        self._http_server.add_route("/nodes", AllNode)
        self._http_server.add_route("/nodemanage", NodeManage)
        self._http_server.add_route("/addnode", AddNode)
        self._http_server.add_route("/delnode", DelNode)
        self._http_server.start()

        WebSocketManager()
