pycodis
=======

The Python interface to the Codis proxy.

Installation
------------

To install pycodis from source:

.. code-block:: bash

    $ sudo python setup.py install

How to use
----------

.. code-block:: pycon

    >>> import redis 
    >>> from codis import CodisConnectionPool
    >>> connection_pool = CodisConnectionPool.create().zk_client("127.0.0.1:2181").zk_proxy_dir("/zk/codis/db_test/proxy").build()
    >>> r = redis.StrictRedis(connection_pool=connection_pool)
    >>> r.set('foo', 'bar')
    True
    >>> r.get('foo')
    'bar'
