import json
import random
import logging

from redis import Connection
from redis import BlockingConnectionPool
from redis._compat import LifoQueue

from kazoo.client import KazooClient
from kazoo.protocol.states import KeeperState


_LOGGER = logging.getLogger(__name__)

_CODIS_PROXY_STATE_ONLINE = 'online'
_ZK_MAX_RETRY_INTERVAL = 10
_ZK_MAX_RETRY_TIMES = -1


class CodisConnectionPool(BlockingConnectionPool):
    """
    Codis Proxy Connection Pool::

        >>> from redis import StrictRedis
        >>> client = StrictRedis(connection_pool=CodisConnectionPool())

    It performs the same funxtion as the default
    ``:py:class: ~redis.connection.BlockingConnectionPool`` implementation.

    The difference is that, this connection pool implement
    auto-balance and auto-discovery for connection to Codis Proxy.
    """

    def __init__(self, zk_client, zk_proxy_dir, auto_close_zk_client=True,
                 max_connections=50, timeout=20, connection_class=Connection,
                 queue_class=LifoQueue, **connection_kwargs):

        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.zk_client = zk_client
        self.zk_proxy_dir = zk_proxy_dir
        self.auto_close_zk_client = auto_close_zk_client
        self.connection_kwargs = connection_kwargs
        self.connection_class = connection_class
        self.queue_class = queue_class
        self.max_connections = max_connections
        self.timeout = timeout

        if self.zk_client.client_state == KeeperState.CLOSED:
            self.zk_client.start()
        # self.zk_client.ensure_path(self.zk_proxy_dir)
        self.reset()
        self._init_proxy_watcher()

    def _init_proxy_watcher(self):
        @self.zk_client.ChildrenWatch(self.zk_proxy_dir, allow_session_lost=True, send_event=True)
        def proxyChanged(children, event):
            if event:
                print 'receive zk proxy changed event: type=%s' % event.type
                self.reset()

    def reset(self):
        super(CodisConnectionPool, self).reset()
        # reset codis proxy list
        self.proxy_list = []
        for child in self.zk_client.get_children(self.zk_proxy_dir):
            try:
                child_path = '/'.join((self.zk_proxy_dir, child))
                data, stat = self.zk_client.get(child_path)
                proxy_info = json.loads(data)
                state, addr = proxy_info["state"], proxy_info["addr"]
                if state != _CODIS_PROXY_STATE_ONLINE:
                    continue
                addr = addr.split(':')
                self.proxy_list.append((addr[0], int(addr[1])))
            except Exception, e:
                print 'parse %s failed.(%s)' % (child, e)

    def make_connection(self):
        "Make a fresh random connection from proxy list."
        host, port = random.choice(self.proxy_list)
        self.connection_kwargs.update({'host': host, 'port': port})
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def get_connection(self, command_name, *keys, **options):
        return super(CodisConnectionPool, self).get_connection(command_name, *keys, **options)

    def release(self, connection):
        "Release the connection back to the pool, meanwhile check validation"
        if (connection.host, connection.port) in self.proxy_list:
            super(CodisConnectionPool, self).release(connection)
        else:
            connection.disconnect()

    def disconnect(self):
        super(CodisConnectionPool, self).disconnect()
        if self.auto_close_zk_client:
            self.zk_client.close()

    @staticmethod
    def create():
        return CodisConnectionPool.Builder()

    class Builder:
        """
        Builder class used to build CodisConnectionPool step by step
        """

        def __init__(self):
            pass

        def zk_client(self, zk_hosts, max_delay=_ZK_MAX_RETRY_INTERVAL,
                      max_tries=_ZK_MAX_RETRY_TIMES):
            self.zk_hosts = zk_hosts
            self.zk_max_delay = max_delay
            self.zk_max_tries = max_tries
            return self

        def zk_proxy_dir(self, zk_proxy_dir):
            self.zk_proxy_dir = zk_proxy_dir
            return self

        def build(self, **connection_kwargs):
            assert self.zk_hosts
            assert self.zk_proxy_dir

            retry = {
                "max_delay": self.zk_max_delay,
                "max_tries": self.zk_max_tries
            }
            zk_client = KazooClient(hosts=self.zk_hosts, connection_retry=retry)
            zk_client.start()
            zk_client.ensure_path(self.zk_proxy_dir)
            return CodisConnectionPool(zk_client, self.zk_proxy_dir, True, **connection_kwargs)
