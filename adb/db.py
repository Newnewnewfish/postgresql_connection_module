#!/usr/bin/python
# -*- coding: utf-8 -*

'''
ADB数据库模块
'''
import threading
import logging

import psycopg2
from psycopg2.extensions import STATUS_BEGIN

from errors import *
from connection import ConnectionPool, acquire

LOGGER_NAME = 'adbLogger'


class Database(object):
    def __init__(self, maxConn=1, lock=None, *args, **kwargs):
        if not lock:
            self._lock = threading.Lock()
        else:
            self._lock = lock
        self._pool = ConnectionPool(maxConn, lock=self._lock, *args, **kwargs)

    def select(self, obj, fields=None, where=None, order=None, limit=None):
        pass

