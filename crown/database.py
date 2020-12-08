import threading
from .common import *
from .query import *
from .eng_taosrestful import taos_resetful
class Database(object):
    commit_select = False
    compiler_class = QueryCompiler
    empty_limit = False
    field_overrides = {}
    for_update = False
    interpolation = '{}'
    op_overrides = {}
    quote_char = '"'
    reserved_tables = []
    sequences = False
    subquery_delete_same_table = True

    def __init__(self, database, threadlocals=False, autocommit=True,
                 fields=None, ops=None, **connect_kwargs):
        self.init(database, **connect_kwargs)

        if threadlocals:
            self.__local = threading.local()
        else:
            self.__local = type('DummyLocal', (object,), {})

        self._conn_lock = threading.Lock()
        self.autocommit = autocommit
        self.databases = {}
        self.tables = {}
        self.stables = {}
        self.field_overrides = dict_update(self.field_overrides, fields or {})
        self.op_overrides = dict_update(self.op_overrides, ops or {})

    def init(self, database, **connect_kwargs):
        self.deferred = database is None
        self.database = database
        self.connect_kwargs = connect_kwargs

    def connect(self):
        with self._conn_lock:
            if self.deferred:
                raise Exception('Error, database not properly initialized before opening connection')
            self.__local.conn = self._connect(self.database, **self.connect_kwargs)
            self.__local.closed = False
            self.create_database(safe=True)
            # self.get_databases()

    def close(self):
        with self._conn_lock:
            if self.deferred:
                raise Exception('Error, database not properly initialized before closing connection')
            self._close(self.__local.conn)
            self.__local.closed = True

    def get_conn(self):
        if not hasattr(self.__local, 'closed') or self.__local.closed:
            self.connect()
        return self.__local.conn

    def is_closed(self):
        return getattr(self.__local, 'closed', True)

    def get_cursor(self):
        return self.get_conn().cursor()

    def _close(self, conn):
        conn.close()

    def _connect(self, database, **kwargs):
        raise NotImplementedError

    @classmethod
    def register_fields(cls, fields):
        cls.field_overrides = dict_update(cls.field_overrides, fields)

    @classmethod
    def register_ops(cls, ops):
        cls.op_overrides = dict_update(cls.op_overrides, ops)

    def rows_affected(self, cursor):
        return cursor.rowcount

    def get_compiler(self):
        return self.compiler_class(
            self.quote_char, self.interpolation, self.field_overrides,
            self.op_overrides)

    def execute(self, query):
        sql, params = query.sql(self.get_compiler())
        if isinstance(query, SelectQuery):
            commit = self.commit_select
        else:
            commit = query.require_commit
        return self.execute_sql(sql, params, commit)

    def execute_sql(self, sql, params=None, require_commit=True):
        cursor = self.get_cursor()
        logger.debug((sql, params))
        res = cursor.execute(sql, params or ())
        if res:
            if require_commit:
                self.commit()
            logger.debug(cursor,cursor.head)
            return cursor
        return None

    def commit(self):
        self.get_conn().commit()

    def rollback(self):
        self.get_conn().rollback()
    
    def get_supertables(self):
        qc = self.get_compiler()
        res= self.execute_sql(qc.show_tables(self.database,super=True))
        self.stables={}
        for item in res.data:
            if item:
                self.stables[item[0]] = dict(zip(res.head, item))
        return self.stables

    def get_tables(self):
        qc = self.get_compiler()
        res= self.execute_sql(qc.show_tables(self.database,super=False))
        self.tables={}
        for item in res.data:
            if item:
                self.tables[item[0]] = dict(zip(res.head, item))
        return self.tables

    # def get_indexes_for_table(self, table):
    #     raise NotImplementedError

    def create_database(self,safe=False,keep= None,comp=None,replica=None,quorum=None,blocks=None):
        qc = self.get_compiler()
        res = self.execute_sql(qc.create_database(self.database,safe=safe,keep= keep,comp=comp,replica=replica,quorum=quorum,blocks=blocks))
        self.get_databases()
        return [[0]] == res
    
    def get_databases(self):
        qc = self.get_compiler()
        res= self.execute_sql(qc.show_database(self.database))
        self.databases={}
        for item in res.data:
            if item:
                self.databases[item[0]] = dict(zip(res.head, item))
        return self.databases
    
    def drop_database(self,safe=True):
        qc = self.get_compiler()
        res = self.execute_sql(qc.drop_database(self.database,safe=safe))
        self.get_databases()
        return [[0]] == res
    
    def alter_database(self,keep= None,comp=None,replica=None,quorum=None,blocks=None):
        qc = self.get_compiler()
        res = self.execute_sql(qc.alter_database(self.database,keep= keep,comp=comp,replica=replica,quorum=quorum,blocks=blocks))
        self.get_databases()
        return [[0]] == res

    def create_table(self, model_class,safe=False):
        qc = self.get_compiler()
        return [[0]] == self.execute_sql(qc.create_table(model_class,safe=safe))

    def drop_table(self, model_class, safe=False):
        qc = self.get_compiler()
        return [[0]] == self.execute_sql(qc.drop_table(model_class, safe))
    
    def describe_table(self, model_class):
        qc = self.get_compiler()
        return self.execute_sql(qc.describe_table(model_class))
    def describe_table_name(self, table_name):
        return self.execute_sql('DESCRIBE %s.%s' % (self.database,table_name),[])
    def raw_sql(self, sql, *params):
        return self.execute_sql(sql.replace("?", "{}"),params)
class TdEngineDatabase(Database):
    def _connect(self, database, **kwargs):
        return taos_resetful.connect(database=database, **kwargs)
default_database = TdEngineDatabase('demo',host='localhost')