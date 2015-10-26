import unittest
import redis
import time
import log_config
from codis import CodisConnectionPool


class TestConnectionPool(unittest.TestCase):

    def setUp(self):
        self.connection_pool = CodisConnectionPool.create().zk_client("127.0.0.1:2181").zk_proxy_dir("/zk/codis/db_test/proxy").build()
        self.client = redis.StrictRedis(connection_pool=self.connection_pool)

    def tearDown(self):
        self.connection_pool.disconnect()

    def test_command(self):
        self.client.set('test_codis', 'ok')
        self.assertEqual(self.client.get('test_codis'), 'ok')
        # here maybe we can stop the current proxy which client connecting
        time.sleep(10)
        # then we can see whether client redirect to the right another proxy
        self.assertEqual(self.client.get('test_codis'), 'ok')
        self.assertEqual(self.client.get('test_codis'), 'ok')


if __name__ == "__main__":
    unittest.main()
