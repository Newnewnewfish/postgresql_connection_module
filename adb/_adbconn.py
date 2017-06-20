#!/usr/bin/python
# -*- coding: utf-8 -*

import logging

import psycopg2
from psycopg2.extensions import cursor as _cursor
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursorBase, RealDictRow

from errors import *

class LoggingRealDictCursor(DictCursorBase):
    def __init__(self, *args, **kwargs):
        kwargs['row_factory'] = RealDictRow
        DictCursorBase.__init__(self, *args, **kwargs)
        self._prefetch = 0
        self._logger = self.connection._logger

    def execute(self, query, vars=None):
        self.column_mapping = []
        self._query_executed = 1
        try:
            return _cursor.execute(self, query, vars)
        finally:
            self._logger.debug('\n%s\n', self.query)

    def callproc(self, procname, vars=None):
        self.column_mapping = []
        self._query_executed = 1
        try:
            return _cursor.callproc(self, procname, vars)
        finally:
            self._logger.debug('\n%s\n', self.query)

    def _build_index(self):
        if self._query_executed == 1 and self.description:
            for i in range(len(self.description)):
                self.column_mapping.append(self.description[i][0])
            self._query_executed = 0


class MyLoggingCursor(_cursor):
    def execute(self, query, vars=None):
        try:
            return _cursor.execute(self, query, vars)
        finally:
            self.connection._logger.debug('\n%s\n', self.query)

    def callproc(self, procname, vars=None):
        try:
            return _cursor.callproc(self, procname, vars)
        finally:
            self.connection._logger.debug('\n%s\n', self.query)


class MyLoggingConnection(_connection):
    def initialize(self, logger):
        self._logger = logger


def createPGConnection(logger, *args, **kwargs):
    kwargs['connection_factory'] = MyLoggingConnection
    pgconn=psycopg2.connect(*args, **kwargs)
    pgconn.initialize(logger)
    return pgconn

def copyConn(conn):
    '''拷贝Connection对象
    '''
    newconn = Connection(pgconn=conn._pgconn, logger=conn._logger,
            autocommit=conn._autocommit)
    return newconn

_errorDict = {
    psycopg2.OperationalError:OperationalError,
    psycopg2.Warning:Warning,
    psycopg2.InterfaceError:InterfaceError,
    psycopg2.DatabaseError:DatabaseError,
    psycopg2.DataError:DataError,
    psycopg2.OperationalError:OperationalError,
    psycopg2.IntegrityError:IntegrityError,
    psycopg2.InternalError:InternalError,
    psycopg2.ProgrammingError:ProgrammingError}


def convertError(ex):
    '''转换错误类型
    '''
    exType = type(ex)
    try:
        return _errorDict[exType](ex)
    except KeyError:
        return ADBError(ex)


def catchPGError(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except psycopg2.Warning:
            #警告不被视为错误，参考psycopg2的文档
            logging.exception('psycopg2 warnning')
        except psycopg2.Error, ex:
            logging.exception('origin psycopg2 error')
            adbError = convertError(ex)
            if adbError.connectionError():
                self.setInvalid()
            raise adbError
    return wrapper


class Connection(object):
    '''连接对象
    注意：目前不支持嵌套事务，并且Connection对象不可跨线程共享
    Attribute:
        _pgconn: psycopg2的connection对象
        _cursor: psycopg2的cursor对象
        _autocommit: 为True，则每次execute执行完后自动提交
    '''
    def __init__(self, pgconn, logger, autocommit=True):
        self._logger = logger
        self._pgconn = pgconn
        self._autocommit = autocommit
        self._invalid = False
        pgconn.autocommit = autocommit


    def setInvalid(self):
        self._invalid = True

    @property
    def invalid(self):
        return self._invalid


    @catchPGError
    def execute(self, sql, parameters=None):
        '''执行sql语句，用于没有返回值的场合
        Args:
            sql: sql语句。可以是带参数的python字符串形式
            parameters: sql语句参数。替换sql语句中的参数

        Returns:
            None

        Raises:
            None
        '''
        cursor = self._pgconn.cursor(cursor_factory=LoggingRealDictCursor)
        sql = cursor.mogrify(sql, parameters)
        cursor.execute(sql)


    @catchPGError
    def executemany(self, sql, parameters=None):
        '''执行sql语句，用于没有返回值的场合
        Args:
            sql: sql语句。可以是带参数的python字符串形式
            parameters: sql语句参数。替换sql语句中的参数

        Returns:
            None

        Raises:
            None
        '''
        cursor = self._pgconn.cursor(cursor_factory=LoggingRealDictCursor)
        cursor.executemany(sql, parameters)


    @catchPGError
    def query(self, sql, parameters=None):
        '''执行sql查询语句，用于有返回值的场合。返回查询结果的详细列信息和查询结果
        Args:
            sql: sql语句。可以是带参数的python字符串形式
            parameters: sql语句参数。替换sql语句中的参数

        Returns:
            查询结果列表。每行记录为一个字典。[{},{},...]

        Raises:
            None
        '''
        cursor = self._pgconn.cursor(cursor_factory=LoggingRealDictCursor)
        sql = cursor.mogrify(sql, parameters)
        cursor.execute(sql)
        return cursor.fetchall()


    @catchPGError
    def queryDetail(self, sql, parameters=None):
        '''执行sql查询语句，用于有返回值的场合。返回查询结果的详细列信息和查询结果
        Args:
            sql: sql语句。可以是带参数的python字符串形式
            parameters: sql语句参数。替换sql语句中的参数

        Returns:
            查询结果列描述和查询结果

        Raises:
            None
        '''
        cursor = self._pgconn.cursor(cursor_factory=MyLoggingCursor)
        sql = cursor.mogrify(sql, parameters)
        cursor.execute(sql)
        return cursor.description, cursor.fetchall()


    def begin(self):
        '''开始事务
        '''
        if self.beginning(): #不允许嵌套事务
            raise NotSupportedError('Nest transaction not support!')
        self._pgconn.autocommit = False

    @catchPGError
    def commit(self):
        '''提交事务
        '''
        self._pgconn.commit()
        self._pgconn.autocommit = self._autocommit


    @catchPGError
    def rollback(self):
        '''回退事务
        '''
        self._pgconn.rollback()
        self._pgconn.autocommit = self._autocommit


    @catchPGError
    def close(self):
        '''关闭连接
        '''
#        self._cursor = None
        self._pgconn.close()
        self._pgconn = None

    def closed(self):
        return not self._pgconn or self._pgconn.closed

    def beginning(self):
        '''如果已开始事务则返回True
        '''
        return not self._pgconn.autocommit

    def __enter__(self):
        if not self.beginning():
            self.begin()
        else:
            raise NotSupportedError('Nest transaction not support!')

    def __exit__(self, exType, exValue, traceback):
        if exType:
            self.rollback()
        else:
            self.commit()
