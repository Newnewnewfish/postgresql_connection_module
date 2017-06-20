#!/usr/bin/python
# -*- coding: utf-8 -*

import logging
from contextlib import contextmanager
import time

import psycopg2
from psycopg2.extensions import cursor as _cursor
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursorBase, RealDictRow

import gevent
from gevent.event import Event, AsyncResult
from gevent.pool import Pool

from errors import *
from _adbconn import *


_ALIVE_TIMEOUT = 60
_POP_INTERVAL = 1

class ConnectionPool(object):
    '''数据库连接池
    Attribute:
        _maxConn: 最大连接数
        _createPool: 创建PGConn时用的pool，保证同时创建postgresql连接的个数不会超过_maxConn
        _connArgs: 连接参数
        _connections: 当前可用连接列表
        _allConnections: 系统中存在的所有连接
        _logger: 日志对象
        _sweepPeriod: 清扫间隔，单位秒。如果>0，则每隔_sweepPeriod会执行一次清扫操作
        _aliveTimeout: 存活时间，单位秒。空闲时间大于此值的连接会被清扫掉
    '''
    def __init__(self, maxConn=1, debug=False, sweepPeriod=0, aliveTimeout=_ALIVE_TIMEOUT,
            *args, **kwargs):
        '''初始化连接池
        PostgreSQL使用的连接参数有: host, port, database, user, password
        Args:
            maxConn:池中最大连接数，超出此连接数的连接在用完后即会被销毁
            *args: 连接参数
            **kwargs: 连接参数

        Returns:
            None

        Raises:
            None
        '''
        self._maxConn = maxConn
        #kwargs['connection_factory'] = MyLoggingConnection
        self._connArgs = (args, kwargs)
        self._connections = []
        self._allConnections = {}
        self._logger = logging.getLogger('adbLogger')
        if debug:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.ERROR)
        self._waitEvent = Event()
        self._closeEvent = Event()
        self._sweepPeriod = sweepPeriod
        if self._sweepPeriod > 0:
            self._sweepGT = gevent.spawn(self._sweep)
        else:
            self._sweepGT = None
        self._aliveTimeout = aliveTimeout
        self._createPool = Pool()


    def pop(self):
        '''从池中获取连接
        Args:
            None

        Returns:
            Connecton

        Raises:
            None
        '''
        while True:
            if self._connections:
                return self._connections.pop()
            if len(self._allConnections)+len(self._createPool) < self._maxConn:
                self._createPool.spawn(self._createAndPut)

            self._waitEvent.wait()

            if self._connections:
                return self._connections.pop()

            self._waitEvent.clear()

            if self._closeEvent.wait(_POP_INTERVAL):
                raise PoolDestroyError


    def _createAndPut(self):
        self._logger.debug('create postgresql connection')
        try:
            args, kwargs = self._connArgs
            pgconn = createPGConnection(self._logger, *args, **kwargs)
            conn = Connection(pgconn, self._logger)
            self.put(conn)
        except:
            self._logger.exception('_createAndPut failed')
            self._waitEvent.set()
            raise


    def _sweep(self):
        '''定期逐出不活跃的连接
        '''
        self._logger.debug('start ConnectionPool evict thread:%s', self._sweepPeriod)

        try:
            while not self._closeEvent.wait(self._sweepPeriod):
                newConnections = []
                for conn in self._connections[:]:
                    now = time.time()
                    #超过时间没有活动的，则从connections和allConnections里删除掉
                    if now - self._allConnections[conn] > self._aliveTimeout:
                        conn.close()
                        del self._allConnections[conn]
                    else:
                        newConnections.append(conn)
                self._connections = newConnections
        except:
            logging.exception('ConnectionPool sweep failed')
            raise


    def put(self, conn):
        '''把连接放回到池中。如果池中连接已满则关闭conn
        Args:
            conn: 连接对象

        Returns:
            None

        Raises:
            None
        '''
        if self._closeEvent.is_set():
            conn.close()
            self._waitEvent.set()
            return

        if not conn.invalid: #有效连接才可以放回池中
            if len(self._connections) < self._maxConn:
                newconn = copyConn(conn)
                conn._pgconn = None
                self._connections.append(newconn)
                #更新连接活动时间戳
                self._allConnections[newconn] = time.time()
            else:
                #如果超出了最大连接数则直接关闭多余的连接
                conn.close()
        #删除掉老的Connection
        try:
            del self._allConnections[conn]
        except KeyError:
            pass
        self._waitEvent.set()


    def __len__(self):
        return self._connections and len(self._connections) or 0


    def _destroy(self):
        '''关闭连接池，支持SingleMixin
        '''
        if not self._closeEvent.is_set():
            self._closeEvent.set()
            self._waitEvent.set()
            #等待sweep线程和createPool退出
            self._sweepGT.join()
            self._createPool.join()
            #conns = self._allConnections.copy()
            #正在执行sql的conn关闭可能会出现操作不执行也不返回的问题，这应该是psycopg2的bug
            #所以这里只关掉空闲连接
            conns = self._connections[:]
            self._connections = None
            for conn in conns:
                conn.close()

    destroy = _destroy
