# -*- coding: utf-8 -*-
__author__ = 'fjs'

from worker.EntityWorker import EntityWorker
from bean.Entity import Entity
from worker.Worker import get_worker_id
from bean.Entity import rpc_method
import uuid
import gevent
from lib.TimeUtil import get_time
from bean.TagEntityManager import TagEntityManager
from bean.Entity import get_gem


DEFAULT_TIMEOUT = 300                                  # session的默认超时是5分钟
SESSION_KEY = "FJSSESSIONID"                           # sessionid在cookie里面保存的key
_ALL_SESSION_MANAGER = None                            # sessionmanager这种类型的entity的管理器，tag管理器


#
# session对象的实现，由SessionManager对象创建和管理，因此本质上只存在与sessionworker这种进程中
#
class Session(Entity):
    def __init__(self, session_id, manager):
        Entity.__init__(self, session_id)
        self._attrs = dict()                          # 保存属性
        self._last_time = get_time()                  # session的上次访问时间
        self._session_manager = manager               # 其所属的manager对象
        self._timeout = DEFAULT_TIMEOUT               # 超时时间

        # 通过这个定时器来定时的检测sessoion的上次访问时间，如果超时了，那么将会移除session
        self._check_timer = gevent.get_hub().loop.timer(DEFAULT_TIMEOUT, DEFAULT_TIMEOUT)
        self._check_timer.start(self._check)          # 启动超时的检测定时器

    def _check(self):
        """
        用于检查session的超时，如果session超时了，那么移除这个session
        """
        if (get_time() - self._last_time) >= self._timeout:
            self.release()

    @rpc_method()
    def set_attr(self, k, v):
        """
        调用这个远程方法来保存一些值到session上面来
        """
        self._last_time = get_time()
        self._attrs[k] = v

    @rpc_method()
    def get_attr(self, k):
        """
        通过调用这个远程方法来获取保存在session的值，如果没有的话，那么将会返回None
        """
        self._last_time = get_time()
        return self._attrs.get(k)

    @rpc_method()
    def set_timeout(self, time_out):
        """
        用于重新设置session的超时
        """
        assert time_out > 0
        self._check_timer.stop()
        self._check_timer = gevent.get_hub().loop.timer(time_out, time_out)
        self._timeout = time_out
        self._check_timer.start(self._check)

    @rpc_method()
    def release(self):
        """
        主要是扩展了从session管理器中移除当前session的逻辑，以及超时检测定时器的关闭，移除设置的一些值
        这个也是一个远程方法，用于显示的调用移除session对象
        """
        Entity.release(self)
        self._session_manager.remove_session(self.id)
        self._check_timer.stop()
        self._attrs.clear()
        self._timer = None
        self._manager = None


#
# sessionworker通过创建这个entity来暴露相关的api
#
class _SessionManager(Entity):
    def __init__(self):
        Entity.__init__(self, "session_mana" + get_worker_id(), tag="web_session", broadcast=True)
        self._sessions = dict()               # 用保存当前进程上的session的id与其对象之间的关系
        self._workerid = get_worker_id()      # 当前所属进程的id

    @rpc_method()
    def get_session(self, session_id):
        """
        收到客户端的http请求之后，首先获取一下cookie里面的session的id，如果有的话，那么将会调用相应的sessionmanager
        的这个方法，用于确保session的存在，如果这个session不存在，那么创建一个新的，例如一些session超时了什么的

        注意：在获取session的时候，先根据id的信息做了一次检查，如果有问题，那么直接抛出异常给客户端
        :param session_id:   客户端http请求带上来的session的id，格式 worker_id + "#" + uuid
        """
        assert isinstance(session_id, str)
        strs = session_id.split("#")
        if strs:
            pid = strs[0]
            if pid != self._workerid:
                raise Exception("session not belong to this worker")
        else:
            raise Exception("session id error")
        if session_id not in self._sessions:
            session = Session(session_id, self)
            self._sessions[session_id] = session
        return True

    @rpc_method()
    def create_session(self):
        """
        通过调用这个接口，用于创建一个session，这里的返回值是session的id
        session_id的格式：sessionworker的id + "#" + uuid

        一般是在客户端的cookie里面并没有sessionid值的时候会调用这个方法来创建一个session对象
        """
        preifx = self._workerid
        session_id = "".join([preifx, "#", str(uuid.uuid4())])
        session = Session(session_id, self)
        self._sessions[session_id] = session
        return session_id

    def remove_session(self, session_id):
        """
        用于移除session，在session销毁的时候调用
        """
        if session_id in self._sessions:
            del self._sessions[session_id]


#
# 用于配合webworker，为tornado加上session的支持
#
class SessionWorker(EntityWorker):
    def __init__(self, name, worker_id, port, config_address):
        """
        :param name:                   启动的进程的名字
        :param worker_id:              进程的id
        :param port:                   供远程调用的端口
        :param config_address:         config进程的监听信息
        """
        EntityWorker.__init__(self, name, worker_id, port, config_address)

    def init_entity(self):
        _SessionManager()        # 创建session管理器entity对象




def _create_new_session(stubs, request):
    """
    根据tornado的request的信息选择一个sessionwoker的stub对象，
    创建一个新的session对象，并返回这个远程session对象的stub对象

    注意：因为是通过设置cookie的方式的，所以需要客户端支持
    :param stubs:            当前集群中所有SessionManager对下的stub，一个序列
    :param request:          tornado的请求对象
    :return:                 远程session的stub
    """
    manager = stubs.pop(0)
    stubs.append(manager)                                     # 相当与实现负载均衡
    session_id = manager.create_session()
    request.set_cookie(SESSION_KEY, session_id)               # 设置客户端的cookie
    address = manager.address
    return get_gem().create_remote_stub(session_id, address)      # 返回对应session对象的stub


#
# 暴露这个模块方法，在httphandler里面可以通过调用这个方法来获取对应的session对象
# 注意：使用这个方法的前提是当前整个集群中存在SessionWorker进程
#
def get_session(request):
    """
    获取这个http请求所关联的session，这里是从请求的cookie里面回去id，然后创建session对象或者创建远程stub对象
    """
    global _ALL_SESSION_MANAGER
    if not _ALL_SESSION_MANAGER:
        _ALL_SESSION_MANAGER = TagEntityManager("web_session_tag_manager", "web_session")
    stubs = _ALL_SESSION_MANAGER.stubs
    if not stubs:
        raise Exception("no session worker can use")
    session_id = request.get_cookie(SESSION_KEY)
    if session_id is None:
        return _create_new_session(stubs, request)
    else:
        strs = session_id.split("#")                                # http请求里面有sessionid，那么获取它
        if strs:
            pid = strs[0]                                           # 这个session应该对应在的sesion进程的id
            for manager in stubs:                                   # 找到对应的manager对象
                if manager.pid == pid:
                    manager.get_session(session_id)                 # 确保对应session对象的存在
                    return get_gem().create_remote_stub(session_id, manager.address)

            # 到这里说明并没有对应的进程存在，出现这种情况的几率比较小，一般是在session进程挂掉了
            return _create_new_session(stubs, request)
        else:
            # 这个一般是错误的id，那么重新创建一个新的session对象
            return _create_new_session(stubs, request)
