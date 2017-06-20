#!/usr/bin/python
# -*- coding: utf-8 -*

'''
ADB连接池模块，提供ConnectionPool和Connection两个类
'''
import logging
import threading

import psycopg2
from psycopg2.extensions import cursor as _cursor
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursorBase, RealDictRow


from errors import *
from _adbconn import *


class ConnectionPool(object):
    '''数据库连接池
    Attribute:
        _maxConn: 最大连接数
        _lock: 锁
        _connArgs: 连接参数
        _connections: 连接列表
        _logger: 日志对象
    '''
    def __init__(self, maxConn=1, lock=None, debug=False, *args, **kwargs):
        '''初始化连接池
        PostgreSQL使用的连接参数有: host, port, database, user, password
        Args:
            maxConn:池中最大连接数，超出此连接数的连接在用完后即会被销毁
            lock: 连接池的锁。默认为threading.Lock
            *args: 连接参数
            **kwargs: 连接参数

        Returns:
            None

        Raises:
            None
        '''
        self._maxConn = maxConn
        if not lock:
            self._lock = threading.Lock()
        else:
            self._lock = lock
        self._connArgs = (args, kwargs)
        self._connections = []
        self._logger = logging.getLogger('adbLogger')
        if debug:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)
        self._closed = False

    def pop(self):
        '''从池中获取连接
        Args:
            None

        Returns:
            Connecton

        Raises:
            None
        '''
        with self._lock:
            if self._connections:
                return self._connections.pop()
        args, kwargs = self._connArgs
        pgconn = createPGConnection(self._logger, *args, **kwargs)
        conn = Connection(pgconn, self._logger)
        return conn

    def put(self, conn):
        '''把连接放回到池中。如果池中连接已满则关闭conn
        Args:
            conn: 连接对象

        Returns:
            None

        Raises:
            None
        '''
        if not conn.invalid: #有效连接才可以放回池中
            with self._lock:
                if len(self._connections) < self._maxConn:
                    newconn = copyConn(conn)
                    conn._pgconn = None
                    self._connections.append(newconn)
                    return
        #如果超出了最大连接数则直接关闭多余的连接
        conn.close()


    def __len__(self):
        return self._connections and len(self._connections) or 0


    def _destroy(self):
        '''关闭连接池，支持SingleMixin
        '''
        if not self._closed:
            self._closed = True
            with self._lock:
                conns = self._connections
                self._connections = None
            for conn in conns:
                conn.close()

    destroy = _destroy
















