#!/usr/bin/python
# -*- coding: utf-8 -*

from contextlib import contextmanager

from connection import ConnectionPool


_greenInited = False

def initGreen():
    global _greenInited
    if not _greenInited:
        import psyco_gevent
        psyco_gevent.make_psycopg_green()
        import green_connection
        globals()['ConnectionPool'] = green_connection.ConnectionPool
        _greenInited = True


#使用with acquire()获得连接，不需要手动销毁
@contextmanager
def acquire(pool):
    conn = pool.pop()
    try:
        yield conn
    finally:
        pool.put(conn)

