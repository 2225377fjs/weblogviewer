__author__ = 'fjs'
import gevent.monkey
gevent.monkey.patch_all()

import sys

sys.path.append("./server")
sys.path.append("./app")


from app_worker.NodeWorker import NodeWorker





if __name__ == "__main__":
    NodeWorker(9000).start()
