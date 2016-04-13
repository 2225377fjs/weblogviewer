# -*- coding: utf-8 -*-
__author__ = 'fjs'

import logging

import bean.BeanManager
import lib.TcpConnector
import bean.Entity
from bean.Entity import rpc_method
import lib.FClient
from bean.Entity import get_gem
from bean.Entity import GEMBN
import gevent



EMPTY_SET = dict()


#
# 对于注册上来的entity信息，用下面这个对象来表达
#
class EntityInfo(object):
    def __init__(self, e_id, pid, address, tag):
        self.id = e_id         # entity的id
        self.pid = pid         # 所属对象的pid
        self.tag = tag         # 所属的tag标签
        self.address = address


#######################################################################################
# 用于实现配置中心的逻辑，进程的信息，entity的信息
# 为了解决entity状态一致性的问题，所有的entity注册到config进程上来之后，不会将entity的信息
# 广播给所有的进程
# 但是别的进程要获取entity的时候，就需要来config进程上面来拿，这样子就会存在一定的开销，所以
# 对于一些全局生命周期的entity，进程应该直接缓存这些entity的stub对象，而不是每一次都获取
#######################################################################################
class ConfigEntity(bean.Entity.Entity, bean.BeanManager.Bean):
    """
    在config进程中，将会创建这个bean，用于实现进程信息，以及entity信息的管理逻辑
    保存entity的信息有三个地方：
    （1）_entities保存id与info的对应
    （2）_process_entities，保存属于某一个进程的所有entity的信息
    （3）_tag_entities保存某种tag类型的所有entity的信息
    因此在处理entity信息的添加，移除都需要操作这三个部分的数据
    """
    def __init__(self):
        bean.BeanManager.Bean.__init__(self, "config_entity_manager")
        bean.Entity.Entity.__init__(self, e_id="config_entity_manager", broadcast=False)
        self._global_entity_manager = get_gem()

        self._entities = dict()                               # 用于保存entity的id与其对应info的关联关系
        self._processes = dict()                              # 用于保存进程的信息 pid->(address, stub)
        self._process_entities = dict()                       # 保存一个进程上所有的entity的信息 pid -> set(EntityInfo)
        self._tag_entities = dict()                           # 保存entity的信息的集合与其tag标签之间的对应关系
        self._tag_manager_stubs = dict()

    @rpc_method()
    def get_all_entity_info(self):
        """
        stat进程的StatEntity会调用这个方法来获取当前在config进程上面注册的所有的entity的信息
        """
        return self._entities

    @rpc_method()
    def add_process(self, p_id, address):
        """
        添加一个进程的数据，在进程的globalentitymanager创建的时候会将自己的连接信息发送到config进程，
        这里需要创建这个进程的globalentitymanager的stub对象，还要建立连接来监控这个进程

        注意：这里需要确保加入了这个进程的id的唯一，如果有重复的id会直接抛出异常

        :param p_id:                进程的id，字符串类型
        :param address:             进程的地址，{"ip": "", "port": 213, "path": ""}
        """
        if p_id in self._processes:
            raise Exception("duplicate process id:" + str(p_id))     # 重复进程id，直接抛出异常
        logging.info("add process process in config id:%s, address:%s", p_id, str(address))
        self._init_disconnect_listener(p_id, address)                # 立即与这个进程创建连接，然后建立断线的监听，可能抛出异常

        # 创建当前连接上来进程的globalentitymanager的代理
        client_stub = self._global_entity_manager.create_remote_stub(GEMBN, address)
        self._processes[p_id] = (address, client_stub)               # 将当前新加入的进程的信息保存起来

    @rpc_method()
    def get_process_address(self, p_id):
        """
        通过进程的id来获取这个进程的地址信息，如果不存在的话，那么直接返回None
        """
        if p_id in self._processes:
            return self._processes[p_id][0]
        return None

    def _add_entity_to_process(self, info, p_id):
        """
        用于在注册entity的时候将这个entity的id加入到这个进程的集合中，主要是为了在进程掉线之后，知道这个进程上都注册了
        哪些entity，好将它们移除
        """
        if p_id not in self._process_entities:
            self._process_entities[p_id] = set()
        es = self._process_entities[p_id]
        es.add(info)

    def _add_entity_to_tag(self, tag, info):
        """
        如果entity是带有标签的，那么将这个entity保存到其对应的tag所在的entity集合
        """
        if tag not in self._tag_entities:
            self._tag_entities[tag] = set()
        self._tag_entities[tag].add(info)
        self._fire_tag_change(tag)

    @rpc_method()
    def add_entity(self, e_id, p_id, tag=None):
        """
        用于添加一个entity的信息，将entity以及它所在的进程信息保存起来

        注意：如果添加的entity的id重复，那么将会直接抛出异常，异常信息将会直接反馈给客户端
        :param e_id:    entity的id                 str
        :param p_id:    其所在的进程的id             str
        :param tag:     entity所属的tag标签         str
        """
        if e_id in self._entities:
            logging.info("entity had regist before, %s, %s", e_id, p_id)
            raise Exception("duplicate entity id:" + e_id)
        elif p_id not in self._processes:
            raise Exception("process not exist " + p_id)
        else:
            info = EntityInfo(e_id, p_id, self._processes[p_id][0], tag)
            logging.info("add entity in config, id:%s, tag:%s, worker:%s", e_id, str(tag), p_id)
            self._entities[e_id] = info
            self._add_entity_to_process(info, p_id)    # 将entity的信息也保存到对应的进程的entity集合里面
            if tag is not None:
                self._add_entity_to_tag(tag, info)     # 如果当前entity有tag，那么将其保存到对应的tag集合

    @rpc_method()
    def get_entity(self, e_id):
        """
        通过entity的id来获取这个entity的信息

        注意：考虑到系统负载，config进程本身是一个单点，所以大量的获取远程entity的信息会给系统造成压力，
            所以对于一些长生命周期的entity，应该要缓存这些entity的stub，而不是每一次都来获取这些entity的嘻嘻尼
        :return:   当前entity所在额进程的id和对应进程的监听信息
        """
        if e_id in self._entities:
            info = self._entities.get(e_id)
            return info.pid, info.address
        return None, None

    @rpc_method()
    def get_tag_infos(self, tag):
        """
        用于获取所有tag类型的entity的信息，如果没有这个tag相关的entity，那么将会返回一个空的集合，
        进程可以通过返回的集合数据来构建这些entity的stub对象
        """
        return self._tag_entities.get(tag, EMPTY_SET)

    @rpc_method()
    def remove_entity(self, e_id):
        """
        移除某一个entity的信息，调用发起的时候：
        一般是在这个entity被销毁的时候调用，在entity的release方法里面

        注意：这个时候需要广播消息给所有的进程，告诉他们这个entity已经被销毁了
        :param e_id:    要移除的entity的id
        """
        if e_id in self._entities:
            info = self._entities[e_id]                         # 获取这个entity的info信息
            del self._entities[e_id]                            # 将其从entity的信息字典里面移除
            if info.pid in self._process_entities:
                self._process_entities[info.pid].remove(info)   # 将这个entity的id从其所在进程的entity集合里面移除
            if info.tag is not None and info.tag in self._tag_entities:
                self._tag_entities[info.tag].remove(info)       # 将这个entity从其所属的tag集合里面移除
                self._fire_tag_change(info.tag)
            logging.info("entity info deleted in config, id:%s", e_id)
        else:
            logging.info("entity do not exist, id:%s when remove in config", e_id)

    def _clear_process_entities(self, pid):
        """
        用于在检测到一个进程死掉了之后，移除这个进程注册的所有的entity，
        这里需要注意移除三个地方的信息：（1）_entities里面，（2）另外就是可能会tag部分，（3）_process_entities里面的信息

        在tag相关的entity发生了变动，那么需要调用_fire_tag_change方法来通知
        """
        entity_infos = self._process_entities.get(pid, set())
        tags = set()
        for info in entity_infos:
            e_id = info.id
            if e_id in self._entities:
                del self._entities[e_id]
            if info.tag is not None and info.tag in self._tag_entities:
                self._tag_entities[info.tag].remove(info)
                tags.add(info.tag)

        for change_tag in tags:
            self._fire_tag_change(change_tag)

        self._process_entities[pid] = set()

    def _init_disconnect_listener(self, pid, address):
        """
        在有进程加入了之后，config进程会立即创建一个与该进程的长连接，然后通过掉线检测来判断进程是否已经挂掉了
        在调用的时候，可能会抛出连接无法建立的异常，在add_process的时候，这里异常了也就代表添加进程失败了
        另外就是在重连接的时候，如果抛出了异常，那么将会判定进程挂掉了，然后将进程的信息移除
        """
        client = lib.FClient.create_share_client(address["ip"], address["port"])   # 直接创建一个与进程的连接用于检测

        def _listener():
            """
            用于监听当前config进程与加入的进程建立的连接是不是已经挂了，因为建立的长连接不会主动断线，所以
            如果连接断开了，多半是进程已经挂掉了

            不过也不应该直接认为进程挂掉了，再尝试建立连接，这里如果进程确实是已经挂掉了，那么
            在创建连接的时候就会报异常，这样就可以断定进程确实是已经挂掉了，那么需要清理这个进车功能注册所有entity
            """
            logging.error("connection with process:%s,address:%s is over", pid, str(address))
            for _ in xrange(3):
                gevent.sleep(1.5)          # 休息一下再尝试重新连接，不要太激动
                try:
                    self._init_disconnect_listener(pid, address)
                    logging.info("reconnect success, pid:%s, address:%s", pid, str(address))
                    return
                except:
                    logging.error("reconnect fail pid:%s, address:%s", pid, str(address))
            logging.error("can not connect pid:%s, address:%s, please check", pid, str(address))
            self._clear_process_entities(pid)
            if pid in self._processes:
                del self._processes[pid]

        client.add_disconnect_listener(_listener)                 # 挂一个断线的监听，当连接断开之后会被调用

    @rpc_method()
    def add_tag_manager(self, eid, address, tag):
        """
        entity进程如果需要管理tag类型的entity，那么会创建TagEntityManager对象，它在创建的时候会
        在cofnig进程来注册自己的信息，用于config进程知道tag变动的时候来通知
        """
        stub = self._global_entity_manager.create_remote_stub(eid, address)
        if tag not in self._tag_manager_stubs:
            self._tag_manager_stubs[tag] = set()
        self._tag_manager_stubs[tag].add(stub)

    def _fire_tag_change(self, tag):
        """
        用于在tag entity有变动的时候，通知别的进程有关注这种tag的tag管理器

        注意：因为调用别的进程的方法，可能会出现异常，那么在除了异常之后将相应的stub对象删除
        """
        need_remove = set()
        if tag in self._tag_manager_stubs:
            for stub in self._tag_manager_stubs[tag]:
                try:
                    stub.need_change()
                except:
                    need_remove.add(stub)
        for stub in need_remove:
            self._tag_manager_stubs[tag].remove(stub)


