# -*- coding: utf-8 -*-
__author__ = 'fjs'
import logging
import bean.BeanManager
import Protocol


class Service(object):
    """
    服务的顶层接口
    """
    def __init__(self, name):
        """
        每一个服务都需要有一个名字，用于在服务的controller中进行索引
        :param name:
        :return:
        """
        object.__init__(self)
        self._name = name

    @property
    def name(self):
        """
        返回当前服务的名字
        :return:
        """
        return self._name

    def service(self, context, data):
        """
        调用这个方法来处理下层的数据，
        (1)调用当前服务的decode方法，将传过来的数据解码
        (2)调用do_service方法来具体的处理
        (3)调用encode方法将返回数据编码返回出去

        这里需要注意不同部分的异常处理分类
        要是在解码或者编码的时候出现了异常，那么需要抛出异常，外层代码的处理逻辑是直接断开当前的连接
        :param data: 二进制数据，所以服务层需要自己实现自己的decode和encode
        :return:     返回二进制数据
        """
        try:
            message = self.decode(data)
        except:
            logging.error(u"解码错误，服务名字:" + self.name)
            raise Exception("解码错误")
        out = self.do_service(context, message)
        try:
            new_out = self.encode(out)
        except:
            logging.error(u"编码错误，服务名字:" + self.name)
            raise Exception("编码错误")
        return new_out

    def decode(self, data):
        """
        解码方法，所有的服务都需要实现这个方法来进行数据的解码
        将二进制数据data转化为当前service处理的的数据，默认是没有解码

        注意：推荐在这个地方进行数据转化，因为在外部做了这部分的异常处理，这样能够定位出异常的地方
             不会因为解码的错误导致整个socket的中断

        需要子类具体的实现自己的数据解析，毕竟不同的服务定义的数据也不一样
        :param data:
        :return:         需要返回解析出来的数据
        """
        return data

    def encode(self, data):
        """
        将服务返回的数据经过编码转化为二进制数据，默认是不编码

        需要子类自己实现自己的返回数据的编码
        :param data:
        :return:
        """
        return data

    def do_service(self, context, message):
        """
        子类中实现的方法，用于具体的处理请求
        :param context:  与当前socket关联的上下文
        :param message:  已经通过decode解码之后的数据
        :return:
        """
        pass

    def on_connection(self, sock, context):
        """
        一个新的连接建立了之后将会调用这个方法
        :param sock:
        :return:
        """
        pass

    def on_disconnect(self, sock, context):
        """
        一个连接断开之后将会调用的方法，传进来的参数是断开的连接对象和它的上下文
        :type sock: FSocket.FSocket
        :type context: Context.Context
        :return:
        """
        pass


# 这里将service也转化为bean，主要是为了方便使用bean的API
class ServiceBean(Service, bean.BeanManager.Bean):
    def __init__(self, name):
        Service.__init__(self, name)
        bean.BeanManager.Bean.__init__(self, name)



import collections

####################################################################
# 它主要是用于实现服务的管理逻辑，包括注册以及获取
####################################################################
class ServiceController:
    def __init__(self):
        """
        这里主要就是创建一个字典，用于维护name-service的索引
        同时建立一个任务队列
        :param nu: 建立任务处理协程的数量
        :return:
        """
        self._services = collections.defaultdict()
        self._short_cut_name = None
        self._short_cut_service = None

    def add_service(self, service):
        """
        添加服务，这里通过名字进行索引，如果名字重复了，那么会抛出异常
        :type service: Service
        :return:
        """
        assert isinstance(service, Service)
        if service.name in self._services:
            raise Exception("服务添加错误，名字重复了")
        else:
            self._services[service.name] = service

    def get_service(self, name):
        """
        通过name来索引service，这里如果没有的话，会返回None
        :rtype: Service
        :return:
        """
        return self._services.get(name)

    def clear_service(self):
        """
        这里将对所有服务的引用清空，RoomService里面会用到
        在退出战场的时候，清理服务的引用，减少循环引用的可能性
        :return:
        """
        self._services.clear()


#####################################################################
# 这个是用来管理全局的服务，包括登陆，啥的
# 这里还在controller的基础上扩展了服务的路由功能
#####################################################################
class MainServiceControllerService(Service, ServiceController):
    def __init__(self, protocol=None):
        """
        分别初始化service父类和servicecontroller父类
        :param name:
        :param protocol: 使用的协议解析，默认使用的是cpickle
        :return:
        """
        Service.__init__(self, "main_con")
        ServiceController.__init__(self)
        self._protocol = Protocol.PickleProtocol() if protocol is None else protocol

    def decode(self, data):
        """
        通过protobuf进行解码
        （1）客户端为阻塞的请求，这个时候没有带rid
        （2）客户端为非阻塞的请求，这个时候带有rid
        :param data:
        :return:
        """
        return self._protocol.decode(data)

    def encode(self, data):
        return self._protocol.encode(data)

    def on_connection(self, sock, context):
        """
        调用所有的服务的on_connection方法
        :type sock:FSocket.FSocket
        :type context:Context.Context
        :return:
        """
        for ser in self._services.values():
            ser.on_connection(sock, context)

    def on_disconnect(self, sock, context):
        """
        这里的处理逻辑很简单，就是调用所有注册的服务的on_disconnect方法
        :type sock: FSocket.FSocket
        :type context: Context.Context
        :return:
        """
        for ser in self._services.values():
            ser.on_disconnect(sock, context)

    def do_service(self, context, message):
        """
        根据客户端传过来的数据调用相应的处理，这里要注意判断传过来的数据是否是压缩过的
        如果找不到服务的注册信息，那么需要报错
        否则返回处理的结果，下层代码会将数据发送给客户端

        另外在main环境下，如果访问了没有的服务，那么这里并没有通过抛出异常来终结当前这个链接
        而是直接返回一个None

        :type context: Context.Context
        :param context:  当前底层socket关联的context对象
        :param message:  已经解码之后的数据，这里是Request类型的
        :return:
        """
        service_name, data, rid = message        # 分别为请求的服务名字，参数，请求id，和是否压缩
        service = self.get_service(service_name)
        if not service:
            logging.error("在mainservicecontroller里面没有找到对应的服务，名字: %s", service_name)
            return None

        out_data = service.service(context, data)
        if not out_data:
            out_data = ""
        return rid, out_data
