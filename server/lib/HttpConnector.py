# -*- coding: utf-8 -*-
__author__ = 'fjs'
import bean.BeanManager
import tornado
import gevent.wsgi
import tornado.web
import tornado.wsgi
import gevent.socket
import logging
import traceback
import cPickle
import socket
import gevent.monkey

try:
    import lib.sendfd      # 这个本来就不是必须要引进来的，只是在需要的时候引入进来
except:
    pass


# 这个是用于unixsocket部分用的
real_socket = gevent.monkey.get_original("socket", "socket")


class HttpServerInfo(object):
    """
    用于承载http服务的基本信息，包括web路径，管理路由
    """
    def __init__(self, web_path, app_setting=dict(), ssl_setting={}):
        self._web_path = web_path            # 主要是web方面的root路径
        self._route = list()                 # 路由项目
        self._static = False                 # 用于标记是否加入了静态文件的路由
        self._setting = app_setting          # 用于tornado的wsgiapplication的配置
        self._ssl_setting = ssl_setting

    def add_route(self, url, handler_class):
        """
        向web应用程序中添加一条路由规则
        """
        self._route.append((url, handler_class))

    @property
    def route(self):
        if not self._static:
            self._static = True
            self._route.append((r'/(.*)', tornado.web.StaticFileHandler, {"path": self._web_path + "public"}))
        return self._route

    @property
    def app_setting(self):
        return self._setting

    @property
    def ssl_setting(self):
        return self._ssl_setting


class HttpConnector(HttpServerInfo):
    def __init__(self, port, web_path="./app/web/", **kwargs):
        """
        httpconnector的构造函数
        :param port:     监听的端口
        :param web_path: web资源所在的路径
        """
        HttpServerInfo.__init__(self, web_path, **kwargs)
        self._port = port                                 # 监听端口
        self._webapp = None                               # wsgi app
        self._wsgi_server = None                          # wsgi server

    def start(self):
        """
        启动当前的connector，其实也就是启动当前的wsgi服务器
        注意：这里先要预定义好了静态文件以及模板所在的路径
        """
        self._webapp = tornado.wsgi.WSGIApplication(self.route, template_path=self._web_path + "view", **self.app_setting)
        self._wsgi_server = gevent.wsgi.WSGIServer(("", self._port), self._webapp, log=None, **self.ssl_setting)
        self._wsgi_server.start()

    def stop(self):
        """
        其实这里就是停止wsgi服务器就好了
        """
        if self._wsgi_server:
             self._wsgi_server.stop()


class SharedHttpConnector(bean.BeanManager.Bean):
    def __init__(self, name, listen):
        """
        共享监听的httpconnector，这里listen既是监听的套接字
        一般是在主进程中创建监听，然后在子进程中共享这些监听套接字
        :param name:     当前注册到bean管理器上的名字
        :param route:    web应用程序的路由
        """
        bean.BeanManager.Bean.__init__(self, name)
        self._route = list()
        self._webapp = None
        self._wsgi_server = None
        self._listen = listen

    def add_route(self, url, handler_class):
        """
        向web应用程序中添加一条路由规则
        """
        self._route.append((url, handler_class))

    def start(self):
        """
        启动当前的connector，其实也就是启动当前的wsgi服务器
        注意：这里先要预定义好了静态文件以及模板所在的路径
        """
        env = dict()
        env["wsgi.multiprocess"] = True
        listen = gevent.socket.socket(socket.AF_INET, _sock=self._listen)
        self._route.append((r'/(.*)', tornado.web.StaticFileHandler, {"path": "./web/public"}))
        self._webapp = tornado.wsgi.WSGIApplication(self._route, template_path="./web/view")
        self._wsgi_server = gevent.wsgi.WSGIServer(listen, self._webapp, environ=env)
        self._wsgi_server.start()

    def stop(self):
        """
        其实这里就是停止wsgi服务器就好了
        """
        if self._wsgi_server:
             self._wsgi_server.stop()


#######################################################################################
# 这种connector需要一个专门的进程用于接收外部的链接，然后当前connector通过unix域socket与其建立
# 连接，当接收外部链接的进程接收到链接之后，通过unix域socket将文件描述符发送过来，然后在
# 当前connector所在的进程中进行接下来的处理
#######################################################################################
class RecvFdHttoConnector(HttpServerInfo):
    def __init__(self, path, web_path="./app/web/", **kwargs):
        HttpServerInfo.__init__(self, web_path, **kwargs)
        self._webapp = None
        self._wsgi_server = None
        self._path = path                               # accepter监听的unix地址

    def start(self):
        """
        启动当前的connector，其实也就是启动当前的wsgi服务器
        注意：这里先要预定义好了静态文件以及模板所在的路径
        """
        self._webapp = tornado.wsgi.WSGIApplication(self.route, template_path=self._web_path + "view", **self.app_setting)
        self._wsgi_server = gevent.wsgi.WSGIServer(("", 10000), self._webapp, log=None, **self.ssl_setting)
        gevent.spawn(self._start)

    def _start(self):
        """
        （1）外面的大循环确保了在于accepter连接断开或者连接失败之后可以重新尝试与accepter的连接
        （2）在与accepter连接成功之后，就进入了内部的while循环，用于不断的等待与accepter连接的读取事件，
            然后调用扩展模块来获取accepter发送过来的文件描述符
        （3）在获取到文件描述符之后，将其包装成为gevet的socket，然后创建一个新的协程，用于处理连接的数据
        """
        while 1:
            gevent.sleep(2)
            try:
                self._back = real_socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._back.connect(self._path)
                self._sock = gevent.socket.socket(socket.AF_INET, _sock=self._back)
                self._back.setblocking(True)
            except:
                logging.error("与accept进程建立链接失败")
                logging.error(traceback.format_exc())
                self._back, self._sock = None, None
                continue
            logging.info("与accepter建立链接成功")
            while 1:
                try:
                    self._sock._wait(self._sock._read_event)
                    fd = self._sock.fileno()
                    out1, data1 = lib.sendfd.fjs_recv_fd(fd)
                    address = cPickle.loads(data1)
                    try:
                        out_sock1 = gevent.socket.fromfd(out1, socket.AF_INET, socket.SOCK_STREAM)
                    except:
                        logging.error("从fd组件socket错误")
                        logging.error(traceback.format_exc())
                        continue
                    finally:
                        lib.sendfd.fjs_close_fd(out1)

                    self._wsgi_server.do_handle(out_sock1, address)

                except:
                    """到这里的唯一原因是因为accept进程死掉了"""
                    logging.error("与accept进程连接断开了")
                    logging.error(traceback.format_exc())
                    self._back, self._sock = None, None
                    break

