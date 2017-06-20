#!/usr/bin/python
# -*- coding: utf-8 -*

import sys, traceback
import copy

import psycopg2
from psycopg2.extras import register_composite

from adb import acquire, initGreen

value1 = None

_pg_pool = None
def pgConnPool():
    return _pg_pool

#用with acquire() as conn 获取postgresql连接的方法
acquirePGConn = lambda : acquire(_pg_pool)


def init(comm, adapter):
    '''
    模块初始化
    Args:
        comm:   通讯器
        adapter:    默认适配器

    Returns:
        None

    Raises:
        None
    '''
    global _pg_pool, value1, value2, value3
    initGreen()

    properties = comm.getProperties()
    host = properties.getProperty('PGDB.Host')
    port = properties.getPropertyAsInt('PGDB.Port')
    user = properties.getProperty('PGDB.User')
    password = properties.getProperty('PGDB.Password')
    database = properties.getProperty('PGDB.Database')
    dbdebug = properties.getPropertyAsIntWithDefault('PGDB.Debug', 0)
    maxConn = properties.getPropertyAsIntWithDefault('PGDB.MaxConn', 50)

    #register compsitor type 'value1'
    conn = psycopg2.connect(host=host,
            port=port,
            user=user,
            password=password,
            database=database)

    caster = register_composite('value1', conn, True)
    value1 = caster.type

    _pg_pool = adb.ConnectionPool(maxConn=maxConn,
            sweepPeriod=120,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            debug=dbdebug)


def destroy(comm, adapter):
    '''
    模块销毁
    Args:
        comm:   通讯器
        adapter:    默认适配器

    Returns:
        None

    Raises:
        None
    '''
    _pg_pool.destroy()










