# -*- coding: utf-8 -*-
__author__ = 'fjs'

import gevent
import bean.BeanManager
import lib.Service
import worker.Worker
import lib.ProcessManager
import gevent.greenlet
import gevent.queue
import lib.GreenletLocal
import logging
import traceback
import socket
import sys
import errno
from lib.LetPool import run_task
from cPickle import loads as cp_loads
from cPickle import dumps as cp_dumps
from lib.GreenletLocal import set_greenlet_local, remove_greenlet_local
from bean.BeanManager import get_manager
import time


GEMBN = "gl_en_ma"                  # 因为entitymanager本身是一个bean，所以用这个名字来注册


RPC_INTERVAL = 600                  # 每间隔这么长时间将当前进程上监控的rpc执行的数据发送到stat进程

GLOBAL_ENTITY_SERVICE = "gls"                                # 用于标记远程调用entity的服务名字，客户端通过访问这个服务来远程调用entity

PROCESS_MANAGER = lib.ProcessManager.SharedProcessManager()  # 维护与远端进程的连接，默认在模块中就会自动创建一个
# PROCESS_MANAGER = lib.ProcessManager.ProcessManager()      # 独占tcp连接类型的


#########################################################################################
# 两种类型的rpc调用的装饰方法
# 通过设置monitor参数为True，可以让方法的执行得到监控
# 为此集群中需要启动一个statworker
#########################################################################################

def _get_wrapper(fn):
    def _wrapper(self, *args, **kwargs):
        """
        对entity的rpc方法做了一层简单的包装，主要是记录执行缩消耗的时间以及出错
        :type self: Entity
        """
        before = time.time()
        try:
            out_data = fn(self, *args, **kwargs)
            use = time.time() - before
            self.manager.add_rpc_info(self.id, self.__class__.__name__, fn.__name__, use)
            return out_data
        except:
            use = time.time() - before
            self.manager.add_rpc_info(self.id, self.__class__.__name__, fn.__name__, use, error=True)
            raise

    return _wrapper


def rpc_method(monitor=False):
    def _do_it(fn):
        if monitor:
            _wrapper = _get_wrapper(fn)
            setattr(_wrapper, "r", "r")
            return _wrapper
        else:
            setattr(fn, "r", "r")
            return fn
    return _do_it


def rpc_message(monitor=False):
    def _do_it(fn):
        if monitor:
            _wrapper = _get_wrapper(fn)
            setattr(_wrapper, "r", "m")
            return _wrapper
        else:
            setattr(fn, "r", "m")
            return fn
    return _do_it


#################################################################################################
# 根据远程方法返回的结果，如果出现了问题，那么抛出相应的异常给调用上下文
#################################################################################################
class RpcException(Exception):
    """
    所有远程调用异常的父类
    """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class EntityNotExist(RpcException):
    """
    调用的远程entity不存在
    """
    def __init__(self, eid):
        RpcException.__init__(self, "entity do not exist id:%s" % eid)


class MethodNotExist(RpcException):
    """
    调用的远程entity不存在这个方法
    """
    def __init__(self, method_name, e_id):
        RpcException.__init__(self, "method:%s do not exist in remote Entity, id:%s" % (method_name, e_id))


class MethodCanNotAccess(RpcException):
    """
    调用的远程entity的方法不能远程访问
    """
    def __init__(self, method_name, e_id):
        RpcException.__init__(self, "method:%s can not invoke by rpc, id:%s" % (method_name, e_id))


class RemoteTimeout(RpcException):
    """
    远程调用方法在远程执行太长时间，这个一般是因为业务阻塞造成的
    """
    def __init__(self, method_name, e_id):
        RpcException.__init__(self, "method:%s remote run timeout, id:%s" % (method_name, e_id))


#
# entity的父类，理论上entity都应该继承自该类型，主要是用于统一对注册和释放的管理，同时还有tick的管理
#
class Entity(object):
    def __init__(self, e_id=None, tick_time=-1, tag=None, broadcast=False):
        """
        在构造的时候需要传入实体的id记录起来，需要保证id是字符串
        这里会区分是将要广播给所有进程的远程entity还是不广播的本地entity

        注意：默认entity并不会将自己的信息广播到config进程上面去，必须要手工的这是broadcast值为True，
             entity才会将自己的信息广播到config进程上面
        :type e_id: str
        :param tag: entity所对应的tag
        :param broadcast: 是否要将entity的信息广播到config进程，默认是要广播的
        """
        object.__init__(self)
        self._broad_cast = broadcast                       # 当前进程是否需要将进程的信息发送到config进程上面去
        if e_id is None or not isinstance(e_id, str):
            raise Exception("entity id error")
        self._id = e_id                                    # 当前entity的id
        self._tag = tag                                    # entity对应的tag
        self._timer = None                                 # 如果有tick的话，将会创建周期定时
        self._tick_time = tick_time                        # tick的间隔时间
        self._manager = get_gem()                          # 获取entiyt的管理器，因此管理器必须要在entity创建之前创建
        self._manager.add_entity(self)                     # 将自己放到当前进程的entity管理器上面，如果需要的话信息将会注册到config上面
        self._init_tick()                                  # 如果传入了tick值，那么初始化tick
        self._released = False                             # release标志位
        self._pid = worker.Worker.get_worker_id()          # 当前进程的进程id

    @property
    def manager(self):
        return self._manager

    @property
    def config_stub(self):
        """
        获取cofnig进程的configbean的stub对象
        """
        return self._manager.config_stub

    @property
    def address(self):
        """
        当前entity所在进程的监听信息
        """
        return self._manager.address

    @property
    def pid(self):
        """
        与entitystub对象的api保持一致，返回当前entity所在的进程id
        """
        return self._pid

    @property
    def broadcast(self):
        """
        当前entity是否将自己的信息注册到config进程上面
        """
        return self._broad_cast

    def _init_tick(self):
        """
        如果用于传入了tick参数，那么表示这个entity需要进行tick，那么将会设置定时
        """
        if self._tick_time > 0:
            self._timer = gevent.get_hub().loop.timer(self._tick_time, self._tick_time)
            self._timer.start(self._do_tick)

    @property
    def id(self):
        """
        返回当前实体的id
        """
        return self._id

    @property
    def tag(self):
        """
        返回当前entity的tag信息
        """
        return self._tag

    def _do_tick(self):
        """
        对上层tick代码进行一层封装，主要是用于获取其中的异常情况

        注意：因为这里的执行实在主loop上面的，所以tick的逻辑不能执行有阻塞的内容，如果需要执行阻塞的逻辑，
             需要自己启动协程来处理
        """
        try:
            self.tick()
        except:
            logging.error("tick error")
            logging.error(traceback.format_exc())

    def tick(self):
        """
        用于实现周期性的逻辑，如果在构建的时候传入了tick时间，那么将会在管理上注册这个tick
        """
        raise Exception("not implement tick method")

    def get_remote_entity(self, eid):
        """
        entity本身就可以直接通过entitymanager对象来直接获取别的entity
        """
        return self._manager.get_remote_entity(eid)

    def get_tag_entities(self, tag):
        """
        主要是代理当前进程的entity管理器的方法，用于向config进程上面获取所有tag类型的entity的信息，
        然后构建他们的stub对象的列表
        """
        return self._manager.get_all_tag_entities(tag)

    def release(self):
        """
        用于实现释放的功能，这里需要从管理器中移除当前entity的注册，如果有定时的话，需要停止
        """
        if not self._released:
            self._released = True
            if self._timer is not None:
                self._timer.stop()
                self._timer = None
            self._manager.remove_entity(self)
            self._manager = None


class EntityStub(object):
    """
    在访问一个全局entity的时候，如果这个entity不在当前的进程，那么将会创建一个这样的代理对象
    用于将rpc访问代理到别的进程上去
    """
    def __init__(self, entity_id, address, entity_manager, process_manager, pid=None):
        """
        :param entity_id:           关联的远程entity的id
        :param p_id:                该entity所在的进程的id
        :param entity_manager:      当前进程的全局entity_manager
        :param process_manager:     用到的进程连接管理器
        :type entity_manager: GlobalEntityManager
        :return:
        """
        object.__init__(self)
        self._entity_id = entity_id                     # 需要访问的远程entity的id
        self._address = address                               # 该entity所在的进程的id，通过这个来索引到该进程的连接信息
        self._global_entity_manager = entity_manager    # 全局entity管理器
        self._process_manager = process_manager         # 用到的进程连接管理器
        self._address_str = address["ip"] + ":" + str(address["port"])
        self._pid = pid

    @property
    def address_str(self):
        """
        entity所在的进程的监听地址信息的字符串表示  192.111.32.32:80
        """
        return self._address_str

    @property
    def address(self):
        """
        当前entity所在进程的监听信息
        """
        return self._address

    @property
    def pid(self):
        """
        当前entity所在进程的id
        """
        return self._pid

    def __getattr__(self, name):
        def _call(*args, **kwargs):
            """
            对远程entity的方法进行一个代理，并对返回值进行判断，如果是异常的话，那么需要抛出异常信息
            注意：如果抛出来的异常是entity不存在，那么可能是中途entity在别的进程中已经释放了，这里要再通知一次config进程

            因为这里在globalentitymanager里面已经对服务的处理进行了替换，所以这里与一般的服务调用稍有不同，这里服务的名字
            直接就是需要调用的entity的id，然后够参数就是调用的方法和参数
            """
            try:
                req_data = cp_dumps((name, args, kwargs))
                with PROCESS_MANAGER.get_process_client(self._address_str) as client:
                    out = client.request(self._entity_id, req_data)     # 这部分可能会有连接异常，或者超时的异常，不过异常将会被上下文管理器捕获
                if isinstance(out, BaseException):                      # 这部分是服务端返回的异常信息
                    str_tag = str(out)
                    if str_tag == "1":
                        """有的时候确实会出现这种问题，毕竟entity状态一致性还是很难保证"""
                        raise EntityNotExist(self._entity_id)                      # 抛出entity不存在的异常
                    elif str_tag == "2":
                        raise MethodNotExist(name, self._entity_id)                # 不存在调用的方法
                    elif str_tag == "3":
                        raise MethodCanNotAccess(name, self._entity_id)            # 调用的方法不能rpc
                    elif str_tag == "4":
                        raise RemoteTimeout(name, self._entity_id)                 # 远端执行超时
                    else:
                        raise out                                                  # 别的异常，直接抛出就好了
                else:
                    return out
            except:
                e = sys.exc_info()[1]
                if isinstance(e, socket.error) and e.errno == errno.ECONNREFUSED:  # 与远端建立连接失败，一般是远端已经挂了
                    self._global_entity_manager.check_process(self._p_id)          # 这里让config进程来检查一下这个进程
                raise                                                              # 将异常抛给上层代码

        return _call


# 如果一个rpc方法的执行出现了异常，那么将这个异常信息返还给rpc的调用客户端
RPC_EXECUTE_EXCEPTION_DATA = cp_dumps(Exception("rpc execute error"))


############################################################################################################
# 全局entity的管理器
#
# 注意：本身它自己也是一个service，它会直接将用到的tcpconnector里面的服务管理器替换为当前对象
#      这样子做的好处是可以节省一次服务的查询
############################################################################################################
class GlobalEntityManager(bean.BeanManager.Bean, Entity, lib.Service.Service):
    def __init__(self, remote_address, process_manager=PROCESS_MANAGER, tcp_con=None):
        """
        这里在构造的时候需要传入当前用到的tcp连接器，用于服务器进程之间的相互通信，
        （1）config进程会通过这个tcp连接器来通知当前进程的一些配置信息
        （2）别的进程要调用当前进程的服务和entity，也需要通过这个tcp连接器
        :param tcp_con: 当前进程的监听器，别的进程要调用当前进程的entity，就需要与这里建立tcp连接
        :param remote_address: config进程的监听信息，这里有可能是None，在config进程中就是
        :param process_manager: 用于管理与别的进程的连接的管理器，如果没有传入的值的话，那么将会默认使用一个默认值
        """
        bean.BeanManager.Bean.__init__(self, GEMBN)         # 这里主要是要在bean管理器上面注册当前对象
        self._process_manager = process_manager             # 设置用到的连接管理器
        self._process_infos = dict()                        # 保存pid与监听地址的关联
        self._pid = worker.Worker.get_worker_id()           # 当前进程的进程id
        self._local_es = dict()                             # 记录本地entity与其id的关联
        self._tcp_con = tcp_con                             # 记录当前用到的tcp连接器

        self._rpc_infos = dict()                            # 用于记录监控entity的一些rpc方法的数据
        # 定时将rpc的监控数据推送到stat进程上面去
        self._rpc_push_timers = gevent.get_hub().loop.timer(RPC_INTERVAL, RPC_INTERVAL)

        if self._tcp_con:
            tcp_con.set_con(self)
            if remote_address:                              # 只有在正常的集群环境先才有可能开启监控功能
                self._rpc_push_timers.start(self._push_rpc_info)

        Entity.__init__(self, e_id=GEMBN)                   # 这种entity对象不许要将信息发送到config进程
        if remote_address is not None:
            self._process_manager.add_process(remote_address)                                     # 添加config进程的信息
            self._config_stub = EntityStub("config_entity_manager", remote_address, self, self._process_manager)  # 直接默认创建config的stub
            if self._tcp_con:
                self._config_stub.add_process(worker.Worker.get_worker_id(), tcp_con.regist_address)            # 将当前进程的信息注册到config上面去

    @property
    def process_manager(self):
        return self._process_manager

    @property
    def address(self):
        """
        返回当前进程的监听地址，别的进程通过与这个地方建立连接然后来调用这个进程上面的entity
        """
        return self._tcp_con.address

    def _push_rpc_info(self):
        """
        将会定时的将收集到的rpc调用信息发送到stat进程上面去保存起来
        """
        def _doit():
            infos = self._rpc_infos
            self._rpc_infos = dict()
            items = infos.values()
            if len(items):
                try:
                    self.get_remote_entity("state_entity").process_info(items)
                except:
                    import traceback
                    print traceback.format_exc()
        run_task(_doit)

    def service(self, context, data):
        """
        与当前用到的connector建立连接的socket，它的数据将会直接调用这里来处理
        首先是一个三元组
        （entity的id，请求的参数，请求的rid）
        接下来请求参数优势一个三元组：
        （请求的方法名字，序列参数，默认参数）

        这里会直接返回好几种异常给客户端：
        （1）entity不存在的异常
        （2）调用方法不存在的异常
        （3）调用方法不支持远程调用的异常
        （4）还有一种在协程池中会抛出，就是业务阻塞太长时间，直接会终止，抛出服务端的超时异常

        注意：这段代码理论上是不会出现异常的，如果这部分出现了异常，那么当前用到的连接将会被直接被关闭
             因为这里出现了异常只能是数据有问题，断开连接也是为了安全
        """
        entity_id, req_data, rid = cp_loads(data)          # 先解析出当前调用的enttiy的id，请求参数和请求id
        method_name, args, kwargs = cp_loads(req_data)     # 解析出调用的方法，参数
        entity = self.get_entity(entity_id)                # 获取相应的对象
        if entity is None:
            return cp_dumps((rid, Exception("1")))         # entity不存在的异常
        method = getattr(entity, method_name, None)        # 获取相应访问的方法
        if method is None:
            return cp_dumps((rid, Exception("2")))         # 方法不存在的异常
        r_type = getattr(method, "r", None)                # 看一下这个方法有没有做rpc调用的标记
        if r_type is None:
            return cp_dumps((rid, Exception("3")))         # 这个方法不支持远程调用
        if r_type == "r":                                  # 同步rpc调用
            try:
                set_greenlet_local("context", context)     # 将当前底层连接的context保存起来，业务层可能会用到
                return cp_dumps((rid, method(*args, **kwargs)))  # 当前的执行环境已经是在协程池里面了，所以直接执行，然后返回
            except:
                logging.error("rpc execute error")         # 除了上面3中异常，还有一种预定义的异常就是4，letpool报的超时
                logging.error(traceback.format_exc())
                # ex = sys.exc_info()[1]                   # 获取当前的异常对象
                sys.exc_clear()                            # 清理异常信息
                return cp_dumps((rid, RPC_EXECUTE_EXCEPTION_DATA))      # 运行中出现了一些异常，告知客户端出错了
            finally:
                remove_greenlet_local("context")           # 移除当前连接的context
        else:                                                              # 异步rpc调用
            def _call():
                try:
                    set_greenlet_local("context", context)              # 将当前请求的context保存起来
                    method(*args, **kwargs)
                except:
                    logging.error("rpc message execute error")
                    logging.error(traceback.format_exc())
                    sys.exc_clear()                             # 因为在协程池环境中运行，所以清理异常信息
                finally:
                    remove_greenlet_local("context")            # 移除保存的context

            run_task(_call)                                     # 将任务放到协程池中异步运行，当前请求立即返回
            return cp_dumps((rid, None))                        # 对于异步的请求，直接返回空数据回去，让调用客户端无需等待

    @property
    def config_stub(self):
        """
        对config进程上面的config对象的远程引用
        """
        return self._config_stub

    def get_remote_entity(self, e_id):
        """
        获取远程entity的时候，直接到config进程里面去查询其，获取其所在进程的信息
        如果这个entity不存在的话，那么直接抛出异常

        注意，这里需要注意有可能这个entity本来就是在当前这个进程上面
        """
        pid, address = self._config_stub.get_entity(e_id)
        if pid is None:
            raise Exception("can not find remote entity, id:%s", e_id)
        if self._tcp_con is not None and pid == self._pid:
            if e_id in self._local_es:
                return self._local_es[e_id]
            else:
                raise Exception("can not find entity in local, id:%s", e_id)
        return EntityStub(e_id, address, self, self._process_manager, pid=pid)

    def create_remote_stub(self, e_id, address):
        """
        用于在已知entity信息的时候创建entity的stub对象
        :param e_id: entity的id
        :param address: 该进程所在的进程的监听信息  {"ip", "", "port": 123}
        """
        return EntityStub(e_id, address, self, self._process_manager)

    def get_all_tag_entities(self, tag):
        """
        获取标签为tag的所有的entity的stub，首先从config进程上面获取tag下面的entity的信息，然后构建stub序列返回

        注意：这里有可能entity本身就在当前进程上面，所以需要注意判断一下entity所在的进程的id
        """
        infos = self._config_stub.get_tag_infos(tag)
        all_stub = []
        for info in infos:
            e_id, pid, address = info.id, info.pid, info.address
            if pid == self._pid and e_id in self._local_es:
                all_stub.append(self.get_entity(e_id))      # 这个entity本身就在自己这个进程上面
            else:
                # 创建entity的stub对象
                all_stub.append(EntityStub(e_id, address, self, self._process_manager, pid=pid))
        return all_stub

    def add_entity(self, entity):
        """
        添加一个entity，因为是全局的entity，所以这里需要将这个entity的信息发送给config进程

        注意：这里先是掉哟功能config进程的add_entity方法，将当前entity的信息注册到config进程上面去，
            然后再将entity放到本地保存起来，这里主要是考虑到一些一致性的问题，因为entity在config进程上面的注册
            其实是有可能失败了，例如重复的entity id啥的，这个样子会直接抛出异常，那么在业务层代码就可以知道这个异常，从而
            代表entity创建失败

        另外：这里会过滤掉一些默认的entity的信息广播
        (1)config_entity_manager在config进程中默认就存在的一个全局entity
        (2)gl_en_ma，每一个普通进程都有一个，用于config进程通知当前进程一些配置信息
        """
        if entity.id in self._local_es:
            raise Exception("duplicate local global entity, id: " + entity.id)

        if entity.broadcast:
            assert self._tcp_con is not None
            self._config_stub.add_entity(entity.id, self._pid, tag=entity.tag)  # 在需要的时候，将entity的信息注册到config进程上面
        self._local_es[entity.id] = entity                                      # 将这个对象保存起来

    def get_entity(self, e_id):
        """
        别的进程在访问当前进程的entity的方法的时候，将会通过这个方法来获取entitty对象
        """
        if e_id == "gl_en":                # 如果是获取全局entity管理器，那么就直接返回当前对象就可以了
            return self
        return self._local_es.get(e_id)

    def remove_entity(self, entity):
        """
        移除一个实体，这里一般实在实体的release方法里面调用
        注意：因为这里是全局entity，所以在移除之后还需要广播消息给别的进程，通知他们当前这个entity已经销毁了
        """
        e_id = entity.id
        if e_id in self._local_es:
            del self._local_es[e_id]
        if entity.broadcast:
            self._config_stub.remove_entity(e_id)

    def check_process(self, pid):
        """
        在当前进程调用一个远端的entity的时候，如果发现无法与远端进程建立连接，那么将会管理器这个方法用于
        检查远端进程是否还好，其实也就是调用config进程，让它来做一次检查
        """
        pass

    def add_rpc_info(self, e_id, e_type, m_name, time, error=False):
        """
        如果有的rpc方法是需要监控时间的，那么方法在调用之后将会将调用的数据通过这个方法来保存起来
        :param e_id:      调用的entity的id
        :param e_type:    调用的entity的类型
        :param m_name:    方法的名字
        :param time:      消耗的时间
        :param error:     是否出错了
        """
        if e_id not in self._rpc_infos:
            self._rpc_infos[e_id] = dict(entity_id=e_id, entity_type=e_type, process_info=(self._pid, self.address))
            self._rpc_infos[e_id]["invoke"] = dict()
        item = self._rpc_infos[e_id]["invoke"]
        if m_name not in item:
            item[m_name] = [0, 0, 0]    # 分别表示调用的次数，调用消耗的时间，以及出错的次数
        item[m_name][0] += 1
        item[m_name][1] += time
        if error:
            item[m_name][2] += 1


def get_gem():
    """
    一个工具方法，用于获取当前进程的全局entity管理器，因为每一个进程最多只能拥有一个GlobalEntityManager对象
    :rtype :GlobalEntityManager
    """
    return get_manager().get_bean(GEMBN)
