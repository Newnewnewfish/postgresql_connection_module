#!/usr/bin/python
# -*- coding: utf-8 -*

import logging

import psycopg2
from psycopg2 import errorcodes


class ADBError(Exception):
    def uniqueError(self):
        pgex = self.args[0]
        return errorcodes.UNIQUE_VIOLATION == pgex.pgcode

    def connectionError(self):
        pgex = self.args[0]
        if pgex.pgcode:
            return pgex.pgcode.startswith(errorcodes.CLASS_CONNECTION_EXCEPTION)
        elif type(pgex) not in (psycopg2.DataError, \
                psycopg2.IntegrityError, psycopg2.ProgrammingError):
            #OperationalError也包含一些必须重建连接的错误
            #logging.debug('connection error because OperationalError')
            return True
        else:
            return False

