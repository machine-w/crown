# from types import SimpleNamespace
# from crown import Model
# from attr import field
from .common import *
from .field import *
from functools import reduce
import operator

class QueryCompiler(object):
    field_map = {
        'int': 'INT',
        'smallint': 'SMALLINT',
        'tinyint': 'TINYINT',
        'bigint': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'nchar': 'NCHAR',
        'binary': 'BINARY',
        'datetime': 'TIMESTAMP',
        'bool': 'BOOL',
        'primary_key': 'TIMESTAMP',
    }

    op_map = {
        OP_EQ: '=',
        OP_LT: '<',
        OP_LTE: '<=',
        OP_GT: '>',
        OP_GTE: '>=',
        OP_NE: '!=',
        OP_IN: 'IN',
        OP_IS: 'IS',
        OP_LIKE: 'LIKE',
        OP_ILIKE: 'ILIKE',
        OP_ADD: '+',
        OP_SUB: '-',
        OP_MUL: '*',
        OP_DIV: '/',
        OP_XOR: '^',
        OP_AND: 'AND',
        OP_OR: 'OR',
    }

    def __init__(self, quote_char='"', interpolation='?', field_overrides=None,
                 op_overrides=None):
        self.quote_char = quote_char
        self.interpolation = interpolation
        self._field_map = dict_update(self.field_map, field_overrides or {})
        self._op_map = dict_update(self.op_map, op_overrides or {})

    def quote(self, s):
        return ''.join((self.quote_char, s, self.quote_char))

    def get_field(self, f):
        return self._field_map[f]

    def get_op(self, q):
        return self._op_map[q]

    def _max_alias(self, am):
        max_alias = 0
        if am:
            for a in am.values():
                i = int(a.lstrip('t'))
                if i > max_alias:
                    max_alias = i
        return max_alias + 1

    def parse_expr(self, expr, alias_map=None):
        s = self.interpolation
        p = [expr]
        if isinstance(expr, Expr):
            lhs, lparams = self.parse_expr(expr.lhs, alias_map)
            rhs, rparams = self.parse_expr(expr.rhs, alias_map)
            s = '(%s %s %s)' % (lhs, self.get_op(expr.op), rhs)
            p = lparams + rparams
        elif isinstance(expr, Field):
            s = expr.db_column
            if alias_map and expr.model_class in alias_map:
                s = '.'.join((alias_map[expr.model_class], s))
            p = []
        elif isinstance(expr, Func):
            p = []
            exprs = []
            for param in expr.params:
                parsed, params = self.parse_expr(param, alias_map)
                exprs.append(parsed)
                p.extend(params)
            
            s = '%s(%s)' % (expr.name, ', '.join(exprs))
        elif isinstance(expr, Param):
            s = self.interpolation
            p = [expr.data]
        elif isinstance(expr, Ordering):
            s, p = self.parse_expr(expr.param, alias_map)
            s += ' ASC' if expr.asc else ' DESC'
        elif isinstance(expr, R):
            s = expr.value
            p = []
        elif isinstance(expr, SelectQuery):
            max_alias = self._max_alias(alias_map)
            clone = expr.clone()
            if not expr._explicit_selection:
                clone._select = (clone.model_class._meta.primary_key,)
            subselect, p = self.parse_select_query(clone, max_alias, alias_map)
            s = '(%s)' % subselect
        elif isinstance(expr, (list, tuple)):
            exprs = []
            p = []
            for i in expr:
                e, v = self.parse_expr(i, alias_map)
                exprs.append(e)
                p.extend(v)
            s = '(%s)' % ','.join(exprs)
        # elif isinstance(expr, Model):
        #     s = self.interpolation
        #     p = [expr.get_id()]

        if isinstance(expr, Leaf):
            if expr.negated:
                s = 'NOT %s' % s
            if expr._alias:
                s = ' '.join((s, 'AS', expr._alias))

        return s, p

    def parse_query_node(self, qnode, alias_map):
        if qnode is not None:
            return self.parse_expr(qnode, alias_map)
        return '', []
    def parse_expr_list(self, s, alias_map):
        parsed = []
        data = []
        for expr in s:
            expr_str, vars = self.parse_expr(expr, alias_map)
            parsed.append(expr_str)
            data.extend(vars)
        return ', '.join(parsed), data


    def parse_select_query(self, query, start=1, alias_map=None):
        model = query.model_class
        db = model._meta.database
        parts = ['SELECT']
        params = []

        selection = query._select
        select, s_params = self.parse_expr_list(selection, alias_map)

        parts.append(select)
        params.extend(s_params)

        parts.append('FROM %s ' % (self.quote(model._meta.db_table),))

        # joins = self.parse_joins(query._joins, query.model_class, alias_map)
        # if joins:
        #     parts.append(' '.join(joins))

        where, w_params = self.parse_query_node(query._where, alias_map)
        if where:
            parts.append('WHERE %s' % where)
            params.extend(w_params)
        if query._interval:
            if query._interval_offset:
                parts.append('INTERVAL(%s,%s)' % (query._interval,query._interval_offset))
            else:
                parts.append('INTERVAL(%s)' % (query._interval,))
            parts.append('FILL(%s)' % (query._fill,))

        if query._group_by:
            group_by, g_params = self.parse_expr_list(query._group_by, alias_map)
            parts.append('GROUP BY %s' % group_by)
            params.extend(g_params)

        if query._order_by:
            order_by, _ = self.parse_expr_list(query._order_by, alias_map)
            parts.append('ORDER BY %s' % order_by)

        if query._limit or (query._offset and not db.empty_limit):
            limit = query._limit or -1
            parts.append('LIMIT %s' % limit)
        if query._offset:
            parts.append('OFFSET %s' % query._offset)

        return ' '.join(parts), params

    def _parse_field_dictionary(self, d):
        sets, params = [], []
        for item in d:
            field = item.get('obj')
            expr = item.get('value')
            field_str, _ = self.parse_expr(field)
            val_str, val_params = self.parse_expr(expr)
            val_params = [field.db_value(vp) for vp in val_params]
            sets.append((field_str, val_str))
            # sets.append((field_str, val_params[0]))
            params.extend(val_params)
        return sets, params

    def parse_insert_query(self, query):
        model = query.model_class

        parts = ['INSERT INTO %s' % self.quote(model._meta.db_table)]
        sets, params = self._parse_field_dictionary(query._insert)

        parts.append('(%s)' % ', '.join(s[0] for s in sets))
        parts.append('VALUES (%s)' % ', '.join(s[1] for s in sets))

        return ' '.join(parts), params
    def parse_createsontable_query(self,query,safe=True):
        model = query.model_class
        parts = ['CREATE TABLE']
        if safe:
            parts.append('IF NOT EXISTS')
        parts.append('%s USING %s TAGS ' % (query.table_name,model._meta.db_table))
        sets, params = self._parse_field_dictionary(query._tags)
        parts.append('(%s)' % ', '.join(s[1] for s in sets))
        return ' '.join(parts), params

    def field_sql(self, field):
        attrs = field.attributes
        attrs['column_type'] = self.get_field(field.get_db_field())
        template = field.template
        parts = [self.quote(field.db_column), template]
        return ' '.join(p % attrs for p in parts)

    def parse_create_table(self, model_class, safe=False):
        parts = ['CREATE TABLE']
        if safe:
            parts.append('IF NOT EXISTS')
        parts.append(self.quote(model_class._meta.db_table))
        columns = ', '.join(self.field_sql(f) for f in model_class._meta.get_fields())
        parts.append('(%s)' % columns)
        if model_class._tags != None:
            tags = ', '.join(self.field_sql(f) for f in model_class._tags.get_fields())
            parts.append(' TAGS (%s)' % tags)
        return parts

    def parse_create_database(self, database, safe=False,keep= None,comp=None,replica=None,quorum=None,blocks=None):
        parts = ['CREATE DATABASE']
        if safe:
            parts.append('IF NOT EXISTS')
        parts.append(database)
        if keep != None:
            parts.append('KEEP %s' % keep)
        if comp != None:
            parts.append('COMP %s' % comp)
        if replica != None:
            parts.append('REPLICA %s' % replica)
        if quorum != None:
            parts.append('QUORUM %s' % quorum)
        if blocks != None:
            parts.append('BLOCKS %s' % blocks)
        return parts
    def parse_alter_database(self, database,keep= None,comp=None,replica=None,quorum=None,blocks=None):
        parts = ['ALTER DATABASE']
        parts.append(database)
        if keep != None:
            parts.append('KEEP %s' % keep)
        if comp != None:
            parts.append('COMP %s' % comp)
        if replica != None:
            parts.append('REPLICA %s' % replica)
        if quorum != None:
            parts.append('QUORUM %s' % quorum)
        if blocks != None:
            parts.append('BLOCKS %s' % blocks)
        return parts
    def parse_drop_database(self, database, safe=False):
        parts = ['DROP DATABASE']
        if safe:
            parts.append('IF EXISTS')
        parts.append(database)
        return parts
    def create_database(self, database, safe=False,keep= None,comp=None,replica=None,quorum=None,blocks=None):
        return ' '.join(self.parse_create_database(database,safe,keep,comp,replica,quorum,blocks))
    def alter_database(self, database,keep= None,comp=None,replica=None,quorum=None,blocks=None):
        return ' '.join(self.parse_alter_database(database,keep,comp,replica,quorum,blocks))
    def show_database(self, database):
        return 'SHOW DATABASES'
    def show_tables(self, database,super=False):
        if super:
            return 'SHOW %s.STABLES' % database
        else:
            return 'SHOW %s.TABLES' % database
    def drop_database(self, database, safe=False):
        return ' '.join(self.parse_drop_database(database, safe))
    def create_table(self, model_class, safe=False):
        return ' '.join(self.parse_create_table(model_class, safe))
    def describe_table(self,model_class):
        parts = ['DESCRIBE ']
        parts.append(self.quote(model_class._meta.db_table))
        return ' '.join(parts)
    def drop_table(self, model_class, fail_silently=False, cascade=False):
        parts = ['DROP TABLE']
        if fail_silently:
            parts.append('IF EXISTS')
        parts.append(self.quote(model_class._meta.db_table))
        return ' '.join(parts)

class QueryResultWrapper(list):
    def __init__(self, model, cursor):
        self.model = model
        self.cursor = cursor
        cols = []
        non_cols = []
        for i in range(len(self.cursor.head)):
            col = self.cursor.head[i]
            if col in model._meta.columns:
                cols.append((i, model._meta.columns[col]))
            else:
                non_cols.append((i, col))
        self._cols = cols
        self._non_cols = non_cols
        super(QueryResultWrapper, self).__init__([self.simple_mode(row) for row in cursor.data])


    def simple_mode(self, row):
        instance = self.model()
        for i, f in self._cols:
            setattr(instance, f.name, f.python_value(row[i]))
        for i, f in self._non_cols:
            setattr(instance, f, row[i])
        return instance
class Query(object):
    require_commit = True

    def __init__(self, model_class):
        self.model_class = model_class
        self.database = model_class._meta.database

        self._dirty = True
        self._query_ctx = model_class
        # self._joins = {self.model_class: []} # adjacency graph
        self._where = None

    def clone(self):
        query = type(self)(self.model_class)
        if self._where is not None:
            query._where = self._where.clone()
        # query._joins = self.clone_joins()
        query._query_ctx = self._query_ctx
        return query

    @returns_clone
    def where(self, *q_or_node):
        if self._where is None:
            self._where = reduce(operator.and_, q_or_node)
        else:
            for piece in q_or_node:
                self._where &= piece
    def sql(self, compiler):
        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError
class SelectQuery(Query):
    require_commit = False

    def __init__(self, model_class, *selection):
        self._explicit_selection = len(selection) > 0
        all_selection=model_class._meta.get_fields()
        if model_class._tags:
            all_selection.extend(model_class._tags.get_fields())
        self._select = self._model_shorthand(selection or all_selection)
        self._group_by = None
        self._order_by = None
        self._limit = None
        self._offset = None
        self._qr = None
        self._interval= None
        self._interval_offset= None
        self._fill = None
        super(SelectQuery, self).__init__(model_class)

    def clone(self):
        query = super(SelectQuery, self).clone()
        query._explicit_selection = self._explicit_selection
        query._select = list(self._select)
        if self._group_by is not None:
            query._group_by = list(self._group_by)
        if self._order_by is not None:
            query._order_by = list(self._order_by)
        query._limit = self._limit
        query._offset = self._offset
        return query

    def _model_shorthand(self, args):
        accum = []
        for arg in args:
            if isinstance(arg, Leaf):
                accum.append(arg)
        return accum

    @returns_clone
    def group_by(self, *args):
        self._group_by = self._model_shorthand(args)

    @returns_clone
    def order_by(self, *args):
        self._order_by = list(args)
    
    @returns_clone
    def desc(self):
        self._order_by = list([self.model_class._meta.primary_key.desc()])

    @returns_clone
    def asc(self):
        self._order_by = list([self.model_class._meta.primary_key.asc()])

    @returns_clone
    def limit(self, lim):
        self._limit = lim

    @returns_clone
    def offset(self, off):
        self._offset = off

    @returns_clone
    def interval(self, interval_value, fill='NONE',offset = None,):
        self._interval= interval_value
        self._interval_offset= offset
        if fill in ['NONE','PREV','NULL','LINEAR']:
            self._fill = fill #{NONE | | PREV | NULL | LINEAR}
        else:
            self._fill = "VALUE, %s" % str(fill) #VALUE

    @returns_clone
    def paginate(self, page, page_size=20):
        if page > 0:
            page -= 1
        self._limit = page_size
        self._offset = page * page_size

    def count(self,field=None):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [fn.Count(field if field else clone.model_class._meta.primary_key)]
        # if self._group_by:
        #     res = clone.execute()
        #     if len(res) > 0:
        #         return res
        #     else:
        #         return None
        # else:
        res = clone.database.execute(clone)
        if res and res[0]:
            return res[0][0]
        else:
            return 0
    def avg(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.AVG) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    
    def twa(self,*fields):
        clone = self.order_by()
        if clone._where is None:
            return None
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.TWA) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def sum(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.SUM) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def stddev(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.STDDEV) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def min(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.MIN) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def max(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.MAX) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def first(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.FIRST) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def last(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.LAST) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def last_row(self,*fields):
        clone = self.order_by()
        if clone._where is not None:
            raise Exception('last_row not allow where clause')
        clone._limit = clone._offset = clone._where = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.LAST_ROW) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def spread(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.SPREAD) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    # tdengine目前只支持一列，为以后支持多列准备的函数
    def diff(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias(field,fn.DIFF) for field in fields]
        res = clone.execute()
        return res
    def top(self,field,top=1,alias=None):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        if alias:
            clone._select = [fn.TOP(field,top).alias(alias)]
        else:
            clone._select = [fn.TOP(field,top)]
        res = clone.execute()
        return res
    def bottom(self,field,bottom=1,alias=None):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        if alias:
            clone._select = [fn.BOTTOM(field,bottom).alias(alias)]
        else:
            clone._select = [fn.BOTTOM(field,bottom)]
        
        res = clone.execute()
        return res
    def apercentile(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias_tuple_field(field,3,fn.APERCENTILE) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def percentile(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias_tuple_field(field,3,fn.PERCENTILE) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None
    def leastsquares(self,*fields):
        clone = self.order_by()
        clone._limit = clone._offset = None
        # TODO: 分组的情况下如何统计
        if self._group_by:
            clone._group_by = None
        clone._select = [out_alias_tuple_field(field,4,fn.LEASTSQUARES) for field in fields]
        res = clone.execute()
        if len(res) > 0:
            return res[0]
        else:
            return None

    def exists(self):
        clone = self.paginate(1, 1)
        res = clone.execute()
        if len(res) >0:
            return True
        else:
            return False

    def one(self):
        clone = self.paginate(1, 1)
        res = clone.execute()
        if len(res) >0:
            return res[0]
        else:
            return None
       
    def sql(self, compiler):
        return compiler.parse_select_query(self)

    def execute(self):
        if self._dirty or not self._qr:
            self._qr = QueryResultWrapper(self.model_class, self.database.execute(self))
            self._dirty = False
            return self._qr
        else:
            return self._qr
    def all_raw(self):
        return self.database.execute(self)
    def all(self):
        return self.execute()
class InsertQuery(Query):
    def __init__(self, model_class, insert=None):
        # mm = model_class._meta
        # query = dict((mm.fields[f], v) for f, v in mm.get_default_dict().items())
        # query.update(insert)
        self._insert = insert
        super(InsertQuery, self).__init__(model_class)

    def clone(self):
        query = super(InsertQuery, self).clone()
        query._insert = list(self._insert)
        return query

    where = not_allowed('where clause')

    def sql(self, compiler):
        return compiler.parse_insert_query(self)

    def execute(self):
        result = self.database.execute(self)
        return result
class CreateSonTableQuery(Query):
    def __init__(self, model_class, values=None,table_name=None):
        # mm = model_class._tags
        # query = dict((mm.fields[f], v) for f, v in mm.get_default_dict().items())
        # query.update(values)
        self._tags = values
        self.table_name = "%s.%s" % (model_class._meta.database.database,table_name)
        super(CreateSonTableQuery, self).__init__(model_class)

    where = not_allowed('where clause')

    def clone(self):
        query = super(CreateSonTableQuery, self).clone()
        query._tags = list(self._tags)
        return query

    def sql(self, compiler):
        return compiler.parse_createsontable_query(self)

    def execute(self):
        result = self.database.execute(self)
        return result