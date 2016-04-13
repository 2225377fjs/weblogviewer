# -*- coding: utf-8 -*-
__author__ = 'fjs'
import BeanManager
import logging
import sys
import datetime
import gevent
##############################################
# log部分的bean的定义
##############################################

log_path = "./logs/"
SYS_LOG_NAME = "Fjslogger"
import os


class LogBean(BeanManager.Bean):
    def __init__(self, name, log_path="./logs/", bean_name=SYS_LOG_NAME):
        """
        :param name: 将会用这个名字来创建log文件
        :param log_path: log文件创建的路径
        :param bean_name: 因为是一个bean，所以需要传入一个bean注册的名字
        这里默认采用当前worker进程的id来创建log文件

        注意：这里为了方便，直接将logging模块的方法修改了，导入到当前bean里面的log来执行
        注意：因为这里logger本身是一个bean，所以需要注册一个名字，默认Fjslogger，这个是一般worker会自动带一个，根据
             进程的id绑定，而且这种logger会自动替换系统的logging的方法
             如果要新建独立的别的logger，那么需要传入bean_name参数，这样就可以了
        """
        BeanManager.Bean.__init__(self, bean_name)
        self._name = name
        self._logger = logging.getLogger("message")
        self._log_file_path = log_path + name + ".log"

        self._file_handler = None
        self._console_handler = None

        self._start()                                           # 完成各种handler的初始化
        # 如果没有传入beanname，那么是默认的进程log，那么替换logging的方法
        if bean_name == SYS_LOG_NAME:
            setattr(logging, "error", self._logger.error)
            setattr(logging, "info", self._logger.info)
            setattr(logging, "debug", self._logger.debug)
            setattr(logging, "warning", self._logger.warning)

    def _start(self):
        logger = logging.getLogger("message")
        self._console_handler = logging.StreamHandler(sys.stderr)
        logger.setLevel(logging.DEBUG)
        self._file_handler = logging.FileHandler(self._log_file_path)
        format = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s")
        self._file_handler.setFormatter(format)
        logger.addHandler(self._file_handler)
        logger.addHandler(self._console_handler)

    def get_logger(self):
        """
        这里其实所有得到的log对象都是同一个
        :return:
        """
        return self._logger


#
# 这种类型的log主要是每天都会生成一个新的log文件，对于过期的log文件，将会在名字的前面加上一个日期作为标记
#
class DateLogger(LogBean):
    def __init__(self, name, log_path="./logs/", bean_name=SYS_LOG_NAME):
        """
        因为需要每天生成新的日志文件，所以这里会建立一个定时器，定期的去检查日期是否更新了，如果更新了
        那么需要重新创建文件的handler
        """
        self._fromat = logging.Formatter("%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s")
        LogBean.__init__(self, name, log_path=log_path, bean_name=bean_name)
        self._day = datetime.datetime.now().day
        self._timer = gevent.get_hub().loop.timer(180, 180)
        self._timer.start(self._renew_file_handler)

    def _get_file_handler(self):
        """
        重新创建一个filehandler
        """
        file_path = "".join([log_path, self._name, ".log"])
        file_handler = logging.FileHandler(file_path)
        return file_handler

    def _start(self):
        logger = logging.getLogger("message")
        self._console_handler = logging.StreamHandler(sys.stderr)
        logger.setLevel(logging.DEBUG)
        self._file_handler = self._get_file_handler()
        self._file_handler.setFormatter(self._fromat)
        logger.addHandler(self._file_handler)
        logger.addHandler(self._console_handler)

    def _get_last_day_name(self):
        """
        用于在切换log文件的时候，获取对应应该改名的名字，也就是在日志的名字前面加上上一天的日期
        """
        last_day = datetime.datetime.now() - datetime.timedelta(1)
        file_path = "".join([log_path, str(last_day.date()), "_", self._name, ".log"])
        return file_path

    def _renew_file_handler(self):
        """
        在定时器的回调中将会被调用，用于检查日期是否已经更新了，如果更新了的话，那么创建新的文件和handler，
        替换掉原来的handler

        注意：因为是每180秒检查一次日期，所以这里的文件更新并不是及时的
        """
        now_day = datetime.datetime.now().day
        if now_day != self._day:
            self._day = now_day
            self._logger.removeHandler(self._file_handler)
            self._file_handler.close()
            file_path = self._file_handler.baseFilename      # 原来log的全名
            os.rename(file_path, self._get_last_day_name())  # 更换原来log文件的名字，在名字前面加上日期

            self._file_handler = self._get_file_handler()
            self._file_handler.setFormatter(self._fromat)
            self._logger.addHandler(self._file_handler)


