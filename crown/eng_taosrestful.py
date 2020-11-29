import logging
import requests
from datetime import datetime
from .common import logger

# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger('crown')

PATH='rest/sql'

class Row(list):
    def __init__(self,arr,head):
        super(Row, self).__init__(arr)
        self.head = head
    def __getitem__(self, key):
        try:
            if isinstance(key,int):
                return super(Row, self).__getitem__(key)
            else:
                return super(Row, self).__getitem__(self.head.index(key))
        except:
            return None
        
class Cursor(list):
    def __init__(self,conn):
        super(Cursor, self).__init__([])
        self.conn = conn
        self.status=''
        self.rowcount = 0
        self.head = []
        self.data = []
        self.err_code = None
        self.err_desc = ''
    def execute(self,sql,param=()):
        if param:
            param = map(lambda x: '"%s"' % x if isinstance(x,str) or isinstance(x,datetime) else x, param)
            sql = sql.format(*param)
        res = self.conn.execute_sql(sql)
        if res:
            self.status=res.get('status')
            if self.status == 'succ':
                self.rowcount = res.get('rows')
                self.head = res.get('head')
                self.data = res.get('data')
                # for d in res.get('data'):
                #     self.data.append(Row(d,self.head))
                logger.debug((res.get('data'), res.get('head')))
                super(Cursor, self).__init__(self.data)
                return True
            else:
                self.err_code = res.get('code')
                self.err_desc = res.get('desc')
                #自动建立数据库
                if self.err_code == 896:
                    res = self.execute('create database %s' % self.conn.database)
                    if res:
                        return self.execute(sql)
                raise Exception(self.err_desc)
        else:
            raise Exception('server error')


class Conn():
    def __init__(self,database,url,user,passwd):
        self.user = user
        self.passwd = passwd
        self.url = url
        self.database = database

    def execute_sql(self,sql):
        reponse = requests.post(self.url, auth=(self.user, self.passwd),data=sql.encode())
        # if reponse.status_code == requests.codes.ok:
        data = reponse.json()
        return data
    
    def cursor(self):
        cursor = Cursor(self)
        return cursor
    
    def commit(self):
        pass
    def rollback(self):
        pass
class TaosRestful():
    
    def connect(self,database, host='localhost',port=6041,user='root',passwd='taosdata'):
        conn = Conn(database,'http://%s:%s/%s' % (host,port,PATH),user,passwd)
        return conn
        
taos_resetful = TaosRestful()

       