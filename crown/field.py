import datetime
import re
from .common import *

class Leaf(object):
    def __init__(self):
        self.negated = False
        self._alias = None

    def __invert__(self):
        self.negated = not self.negated
        return self

    def alias(self, a):
        self._alias = a
        return self

    def asc(self):
        return Ordering(self, True)

    def desc(self):
        return Ordering(self, False)

    def _e(op, inv=False):
        def inner(self, rhs):
            if inv:
                return Expr(rhs, op, self)
            return Expr(self, op, rhs)
        return inner
    __and__ = _e(OP_AND)
    __or__ = _e(OP_OR)

    __add__ = _e(OP_ADD)
    __sub__ = _e(OP_SUB)
    __mul__ = _e(OP_MUL)
    __div__ = _e(OP_DIV)
    __xor__ = _e(OP_XOR)
    __radd__ = _e(OP_ADD, inv=True)
    __rsub__ = _e(OP_SUB, inv=True)
    __rmul__ = _e(OP_MUL, inv=True)
    __rdiv__ = _e(OP_DIV, inv=True)
    __rand__ = _e(OP_AND, inv=True)
    __ror__ = _e(OP_OR, inv=True)
    __rxor__ = _e(OP_XOR, inv=True)

    __eq__ = _e(OP_EQ)
    __lt__ = _e(OP_LT)
    __le__ = _e(OP_LTE)
    __gt__ = _e(OP_GT)
    __ge__ = _e(OP_GTE)
    __ne__ = _e(OP_NE)
    __lshift__ = _e(OP_IN)
    __rshift__ = _e(OP_IS)
    __mod__ = _e(OP_LIKE)
    __pow__ = _e(OP_ILIKE)

class Expr(Leaf):
    def __init__(self, lhs, op, rhs, negated=False):
        super(Expr, self).__init__()
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self.negated = negated

    def clone(self):
        return Expr(self.lhs, self.op, self.rhs, self.negated)

class DQ(Leaf):
    def __init__(self, **query):
        super(DQ, self).__init__()
        self.query = query

    def clone(self):
        return DQ(**self.query)

class Param(Leaf):
    def __init__(self, data):
        self.data = data
        super(Param, self).__init__()

class Func(Leaf):
    def __init__(self, name, *params):
        self.name = name
        self.params = params
        super(Func, self).__init__()

    def clone(self):
        return Func(self.name, *self.params)

    def __getattr__(self, attr):
        def dec(*args, **kwargs):
            return Func(attr, *args, **kwargs)
        return dec

fn = Func(None)

class FieldDescriptor(object):
    def __init__(self, field):
        self.field = field
        self.att_name = self.field.name

    def __get__(self, instance, instance_type=None):
        if instance:
            return instance._data.get(self.att_name)
        return self.field

    def __set__(self, instance, value):
        instance._data[self.att_name] = value

class Field(Leaf):
    _field_counter = 0
    _order = 0
    db_field = 'unknown'
    template = '%(column_type)s'

    def __init__(self, null=False, index=False,  verbose_name=None, #unique=False,primary_key=False, sequence=None,choices=None,
                 help_text=None, db_column=None, default=None, *args, **kwargs):
        self.null = null
        self.index = index
        # self.unique = unique
        self.verbose_name = verbose_name
        self.help_text = help_text
        self.db_column = db_column
        self.default = default
        # self.choices = choices
        # self.primary_key = primary_key
        # self.sequence = sequence

        self.attributes = self.field_attributes()
        self.attributes.update(kwargs)

        Field._field_counter += 1
        self._order = Field._field_counter

        super(Field, self).__init__()
    def __str__(self) -> str:
        return "null: %s\nindex: %s\nverbose_name: %s\ndb_column: %s\ndefault: %s\n" % \
            (self.null,self.index,self.verbose_name,self.db_column,self.default)

    def add_to_class(self, model_class, name):
        self.name = name
        self.model_class = model_class
        self.db_column = self.db_column or self.name
        self.verbose_name = self.verbose_name or re.sub('_+', ' ', name).title()

        model_class._meta.fields[self.name] = self
        model_class._meta.columns[self.db_column] = self
        setattr(model_class, name, FieldDescriptor(self))

    def add_tags_to_class(self, model_class, name):
        self.name = name
        self.model_class = model_class
        self.db_column = self.db_column or self.name
        self.verbose_name = self.verbose_name or re.sub('_+', ' ', name).title()

        model_class._tags.fields[self.name] = self
        model_class._tags.columns[self.db_column] = self
        setattr(model_class, name, FieldDescriptor(self))
    def field_attributes(self):
        return {}

    def get_db_field(self):
        return self.db_field

    def coerce(self, value):
        return value

    def db_value(self, value):
        return 'null' if value is None else self.coerce(value)
    def python_value(self, value):
        if value == 'null':
            return None
        return value if value is None else self.coerce(value)
    def count(self):
        return fn.COUNT(self)
    def avg(self):
        return fn.AVG(self)
    def twa(self):
        return fn.TWA(self)
    def sum(self):
        return fn.SUM(self)
    def stddev(self):
        return fn.STDDEV(self)
    def min(self):
        return fn.MIN(self)
    def max(self):
        return fn.MAX(self)
    def last(self):
        return fn.LAST(self)
    def first(self):
        return fn.FIRST(self)
    def last_row(self):
        return fn.LAST_ROW(self)
    def spread(self):
        return fn.SPREAD(self)
    def diff(self):
        return fn.DIFF(self)
    def top(self,top=1):
        return fn.TOP(self,top)
    def bottom(self,bottom=1):
        return fn.BOTTOM(self,bottom)
    def apercentile(self,p=1):
        return fn.APERCENTILE(self,p)
    def percentile(self,p=1):
        return fn.PERCENTILE(self,p)
    def leastsquares(self,start_val,step_val):
        return fn.LEASTSQUARES(self,start_val,step_val)
class IntegerField(Field):
    db_field = 'int'

    def coerce(self, value):
        return int(value)
class BigIntegerField(IntegerField):
    db_field = 'bigint'
class SmallIntegerField(IntegerField):
    db_field = 'smallint'
class TinyIntegerField(IntegerField):
    db_field = 'tinyint'
class FloatField(Field):
    db_field = 'float'

    def coerce(self, value):
        return float(value)
class DoubleField(FloatField):
    db_field = 'double'
class NCharField(Field):
    db_field = 'nchar'
    template = '%(column_type)s(%(max_length)s)'

    def field_attributes(self):
        return {'max_length': 255}

    def coerce(self, value):
        value = value or ''
        return value[:self.attributes['max_length']]
class BinaryField(Field):
    db_field = 'binary'
    template = '%(column_type)s(%(max_length)s)'

    def field_attributes(self):
        return {'max_length': 255}

    def coerce(self, value):
        value =  value or ''
        return value[:self.attributes['max_length']]
def format_date_time(value, formats, post_process=None):
    post_process = post_process or (lambda x: x)
    for fmt in formats:
        try:
            return post_process(datetime.datetime.strptime(value, fmt))
        except ValueError:
            pass
    return value
class DateTimeField(Field):
    db_field = 'datetime'

    def field_attributes(self):
        return {
            'formats': [
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
            ]
        }

    def python_value(self, value):
        if value and isinstance(value, str):
            return format_date_time(value, self.attributes['formats'])
        return value
class PrimaryKeyField(DateTimeField):
    db_field = 'primary_key'
class BooleanField(Field):
    db_field = 'bool'

    def coerce(self, value):
        return bool(value)
