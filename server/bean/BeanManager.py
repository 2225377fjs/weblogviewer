# -*- coding: utf-8 -*-
__author__ = 'fjs'


#############################################
# 用于管理所有的bean
# 构建了一个全局唯一的Bean管理器
# 所有需要暴露API的对象都可以通过bean注解来注册自己
#############################################




##################################################
# bean的顶层类
##################################################
class Bean(object):
    def __init__(self, name):
        """
        设置当前bean的名字，并将当前对象注册到bean管理器上去

        这里可能会抛出异常如果有重名的bean的话
        """
        self._name = name
        get_manager().add_bean(self)

    @property
    def name(self):
        return self._name

    def get_bean(self, name):
        """
        为子类提供的获取bean的api
        :param name:
        :return:
        """
        mana = get_manager()
        return mana.get_bean(name)

    def release(self):
        """
        这里要做的事情主要是在bean管理器上移除当前对象
        子类也可以扩展这个方法来实现资源清理的工作
        :return:
        """
        get_manager().remove_bean(self)


class BeanManager(object):
    def __init__(self):
        self._beans = dict()

    def add_bean(self, obj):
        """
        向当前管理器中加入一个bean

        注意：名字不能够重复
        :type obj: Bean
        """
        assert obj.name not in self._beans
        self._beans[obj.name] = obj

    def remove_bean(self, obj):
        """
        将一个bean从当前管理器上移除
        """
        name = getattr(obj, "name", None)
        if name in self._beans:
            del self._beans[name]

    def get_bean(self, name):
        return self._beans.get(name)



#####################################################
# 这里实现了一个进程全局单例的Bean管理器
#####################################################
BEAN_MANAGER = BeanManager()


def get_manager():
    return BEAN_MANAGER
