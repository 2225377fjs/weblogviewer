# -*- coding: utf-8 -*-
__author__ = 'fjs'
import bean.BeanManager
import Constant
import leveldb

####################################################################
# 将levelDB的一些api封装起来，变成bean暴露出去
# 注意：因为bean注册名字是固定的，所以这里一个进程只能有一个
#
# 另外这里因为levelDb会使用mmap的方式来读取文件，而一般默认配置最大
# 打开的文件描述符数量是1000，一个文件分片大概2M多一些，所以就会出现
# 使用levelDb的进程内存很大，所以这里可以通过设置max_open_files参数来
# 限制进程的内存使用情况
####################################################################

MAX_OPEN_FILE    = 100    # 用于控制levelDb最多打开的文件描述符的数量


class LevelDbBean(bean.BeanManager.Bean):
    def __init__(self, path):
        """
        这里在构造的时候，需要文件路径
        """
        bean.BeanManager.Bean.__init__(self, Constant.LEVEL_DB_BEAN)
        self._path = path
        self._db = leveldb.LevelDB(self._path, max_open_files=MAX_OPEN_FILE)
        self._num = 0

    def remove(self, key):
        """
        移除一个key的数据
        """
        self._db.Delete(key)

    def get(self, key):
        """
        这里做了异常处理，因为在key不存在的时候，会抛出异常，这个时候返回None就好了
        """
        try:
            return self._db.Get(key)
        except:
            return None

    def set(self, key, value):
        """
        暴露put的api出去
        """
        self._db.Put(key, value)

    def init_from_file(self, key_man):
        """
        每一次启动的时候应该从本地文件当中获取所有的key到内存当中去
        """
        assert isinstance(key_man, bean.KeyManager.KeyManager)
        for k, _ in self._db.RangeIter():
            key_man.push(k)


