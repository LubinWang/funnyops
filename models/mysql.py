#!/usr/bin/env python
#coding: utf-8
#author: Lubin

import pymysql
import sys
import os
import exceptions
import warnings
import urlparse

#----------------  DB Exception Class ----------------------------------
class Error(exceptions.StandardError):
  pass

class DatabaseError(exceptions.StandardError):
  pass

class Warning(exceptions.StandardError):
  pass

class InterfaceError(Error):
  pass

class InternalError(DatabaseError):
  pass

class OperationalError(DatabaseError):
  pass

class ProgrammingError(DatabaseError):
  pass

class NotSupportedError(ProgrammingError):
  pass

class IntegrityError(DatabaseError):
  pass

class PartialCommitError(IntegrityError):
  pass

class RetryError(OperationalError):
  pass

class FatalError(OperationalError):
  pass

class TimeoutError(OperationalError):
  pass

class TxPoolFull(DatabaseError):
  pass

class RequestBacklog(DatabaseError):
  pass

class ThrottledError(DatabaseError):
  pass

#----------------  Exception Class End----------------------------------

def parse_database_url(url):
  if not url.startswith('mysql'):
    url = 'mysql://' + url
  url = 'http' + url[len('mysql'):]
  parsed = urlparse.urlparse(url)
  params = {
    'user': parsed.username,
    'host': parsed.hostname,
    'db': parsed.path[1:]
  }

  if parsed.username:
    params['user'] = parsed.username
  if parsed.password:
    params['password'] = parsed.password
  if parsed.port:
    params['port'] = parsed.port
  params.update(dict(urlparse.parse_qsl(parsed.query)))
  return params

#----------------  DB Class----------------------------------
class MyDB(object):

  def __init__(self, dburl):
    self._conn = self.Connect(parse_database_url(dburl))

  #连接数据库
  def Connect(self, params):
    try:
      return pymysql.connect(**params)
    except Exception:
      sys.exit(1)

  #获取游标
  def Cursor(self):
    return self._Cursor(self._conn)

  #关闭数据库连接
  def Close(self):
    self._conn.close()

  #析构
  def __del__(self):
    try:
      self.Close()
    except DatabaseError:
      pass


  #内部游标类
  class _Cursor(object):
    def __init__(self, conn):
      self._conn = conn
      self._cursor = self._conn.cursor()
      self.arraysize = 1
      self.lastrowid = None
      self.rowcount = 0
      self.results = None
      self.description = None
      self.index = None

    #关闭游标
    def Close(self):
      self.results = None
      self._cursor.close()

    #提交
    def Commit(self):
      return self._conn.commit()

    #事物开始
    def Begin(self):
      return self._conn.begin()

    #事物回滚
    def Rollback(self):
      return self.      return self.      return self.      return selonn):
      if isinstance(args, (tuple, list)):
        return         return         return     args)
      elif isinstance      elift):
        return dict((key, conn.escape(val)) for (key, val) in args.items())
      else:
        return conn.escape(args)

    #执行SQL
    def _execute(self, sql, bind_variables):

      encoding = self._conn.encoding
      def ensure_bytes(x):
        if isinstance(x, unicode):
          x = x.encode(encoding)
        return x

      sql = ensure_bytes(sql)
      if bind_variables is not None:
        if isinstance(bind_variables, (tuple, list)):
          bind_variables = tuple(map(ensure_bytes, bind_variables))
        elif isinstance(bind_variables, dict):
          bind_variables = dict((ensure_bytes(key), ensure_bytes(val)) for (key, val) in bind_variables.items())
        else:
          bind_variables = ensure_bytes(bind_variables)

      if bind_variables is not None:
        sql = sql % self._escape_args(bind_variables, self._conn)

      self._conn.query(sql)
      self.index = 0
      result = self._conn._result

      if result.warning_count > 0:
        for warn in self._conn.show_warnings():
          warnings.warn(warn[-1], Warning, stacklevel = 4)

      return result.rows, result.affected_rows, result.insert_id, result.description

    # 执行SQL
    def Execute(self, sql, bind_variables = None):
      sql_check = sql.strip().lower()
      if sql_check == 'begin':
        self.Begin()
        return
      elif sql_check == 'commit':
        self.Commit()
        return
      elif sql_check == 'rollback':
        self.Rollback()
        return

      self.results, self.rowcount, self.lastrowid, self.description = self._execute(sql, bind_variables)
      self.index = 0
      return self.rowcount

    # 获取一条结果
    def FetchOne(self):
      if self.results is None:
        raise ProgrammingError('Fetch called before execute')
      if self.index >= len(self.results):
        return None
      self.index += 1
      return self.results[self.index-1]

    #获取多条结果
    def FetchMany(self, size = None):
      if self.results is None:
        raise ProgrammingError('Fetch called before execute')
      if self.index >= len(self.results):
        return []
      if size is None:
        size = self.arraysize
      res = self.results[self.index:self.index+size]
      self.index += size
      return res

    #获取所有结果
    def FetchAll(self):
      if self.results is None:
        raise ProgrammingError('Fetch called before execute')
      return self.FetchMany(len(self.results)-self.index)

    #获取单条关联记录
    def FetchOneAssoc(self):
      result = self.FetchOne()
      keys = []
      assoc = {}
      for item in self.description:
        keys.append(item[0])

      for i in range(len(result)):
        assoc[keys[i]] = result[i]

      return assoc

    #获取多条关联记录
    def FetchManyAssoc(self, size = None):
      result = self.FetchMany(size)
      if len(result) <= 0:
        return []
      
      keys = []
      for item in self.description:
        keys.append(item[0])

      assocs = []
      for item in result:
        map(lambda x, y: assocs.append({x:y}), keys, item)
      return assocs


    #获取所有关联记录
    def FetchAllAssoc(self):
      return self.FetchMany(len(self.results)-self.index)


    #获取当前结果数
    @property
    def Rownumber(self):
      return self.index


    #迭代Cursor对象
    def __iter__(self):
      return self

    def next(self):
      val = self.FetchOne()
      if val is None:
        raise StopIteration
      return val

    def __del__(self):
      self.Close()

#----------------  DB Class End----------------------------------
if __name__ == '__main__':
  pass