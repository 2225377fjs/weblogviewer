# -*- coding: utf-8 -*-
__author__ = 'fjs'

import gevent.monkey
gevent.monkey.patch_all()
gevent.monkey.patch_all(subprocess=True)           # 在1.1版本之前，subprocess模块是不打包的


import sys

sys.path.append("./server")
sys.path.append("./app")


from app_worker.NodeWorker import NodeWorker





if __name__ == "__main__":
    NodeWorker(9000).start()
