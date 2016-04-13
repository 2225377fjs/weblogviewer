# -*- coding: utf-8 -*-
__author__ = 'fjs'
import bean.ConfigBean
import lib.TcpConnector
import bean.LogBean
import bean.Entity


##############################################################################
# 配置中心进程
# （1）别的进程在启动成功之后，可以将自己的进程id和监听的地址端口发布到当前的配置中心
# （2）当有新的进程发送配置数据到这里，那么配置中心将会广播当前所有进程数据到已经存在的进程
#
# （3）用于entity信息的注册和广播，当有entity创建之后，可以将enttiy的id和其所在的进程
#     发送到配置中心，用于别的进程的查询，同时在entity销毁的时候也需要发送数据来告诉配置中心
##############################################################################


import worker.Worker

class ConfigWorker(worker.Worker.Worker):
    def __init__(self, ip, port):
        """
        进程的名字: config_worker
        进程的ID: config
        :param ip: 监听的地址
        :param port: 监听的端口
        """
        worker.Worker.Worker.__init__(self, "log_config", "log_config")
        self._ip = ip
        self._port = port

    def do_start(self):
        """
        （1）创建configbean，配置的逻辑
        （2）创建tcp连接器，并初始化
        （3）启动tcp监听
        :return:
        """
        try:
            bean.LogBean.LogBean(self.worker_id)
            tcp_con = lib.TcpConnector.SelectTcpConnector(self._port)
            tcp_con.start()
            bean.Entity.GlobalEntityManager(None, tcp_con=tcp_con)  # 创建全局的entity管理器，注意，这里本远程地址是None，因为本来自己就是config进程
            bean.ConfigBean.ConfigEntity()                  # 创建配置管理entity，用于进行全局entity的管理
        except:
            import traceback
            print traceback.format_exc()
