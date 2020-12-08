from copy import deepcopy
from .field import *
from .database import *
from .query import *
import re
from datetime import datetime
# __all__ = [
#     'IntegerField', 'BigIntegerField', 'PrimaryKeyField', 'FloatField', 'DoubleField',
#     'DateTimeField','BooleanField', 'Model', 'DoesNotExist',  'Field','SmallIntegerField',
#     'TinyIntegerField','NCharField','BinaryField','TdEngineDatabase','SuperModel'
# ]
class ModelOptions(object):
    def __init__(self, cls, database=None, db_table=None,
                 order_by=None, primary_key=None):
        self.model_class = cls
        self.name = cls.__name__.lower()
        self.fields = {}
        self.columns = {}
        self.defaults = {}
        self.super_table = False
        self.child_table = False
        self.my_template =''

        self.database = database or default_database
        self.db_table = db_table
        self.order_by = order_by
        self.primary_key_name = primary_key
        self.primary_key = None
        # self.rel = {}
        # self.reverse_rel = {}
    def __str__(self) -> str:
        return "name: %s\ndatabase: %s\norder_by: %s\nprimary_key: %s\ndb_table: %s\nfields: %s\ncolumns: %s\ndefaults: %s\n" % \
            (self.name,self.database,self.order_by,self.primary_key,self.db_table,self.fields,self.columns,self.defaults)
    def prepared(self):
        for field in self.fields.values():
            if field.default is not None:
                self.defaults[field] = field.default

        if self.order_by:
            norm_order_by = []
            for clause in self.order_by:
                if isinstance(clause,str):
                    field = self.fields[clause.lstrip('-')]
                    if clause.startswith('-'):
                        norm_order_by.append(field.desc())
                    else:
                        norm_order_by.append(field.asc())
                else:
                    norm_order_by.append(clause)
            self.order_by = norm_order_by

    def get_default_dict(self):
        dd = {}
        for field, default in self.defaults.items():
            if callable(default):
                dd[field.name] = default()
            else:
                dd[field.name] = default
        return dd

    def get_sorted_fields(self):
        return sorted(self.fields.items(), key=lambda v: (isinstance(v[1], PrimaryKeyField) and 1 or 2, v[1]._order))

    def get_field_names(self):
        return [f[0] for f in self.get_sorted_fields()]

    def get_fields(self):
        return [f[1] for f in self.get_sorted_fields()]


class BaseModel(type):
    inheritable_options = ['database', 'order_by']

    def __new__(cls, name, bases, attrs):
        if not bases:
            return super(BaseModel, cls).__new__(cls, name, bases, attrs)

        meta_options = {}
        tags_options = {}
        meta = attrs.pop('Meta', None)
        if meta:
            meta_options.update((k, v) for k, v in meta.__dict__.items() if not k.startswith('_') and not isinstance(v, Field))
            tags_options.update((k, v) for k, v in meta.__dict__.items() if not k.startswith('_') \
                                                                            and isinstance(v, Field) \
                                                                            and not isinstance(v, PrimaryKeyField) \
                                                                            and not isinstance(v, DateTimeField) \
                                                                            and not isinstance(v, NCharField))

        for b in bases:
            if not hasattr(b, '_meta'):
                continue
            base_meta = getattr(b, '_meta')
            for (k, v) in base_meta.__dict__.items():
                if k in cls.inheritable_options and k not in meta_options:
                    meta_options[k] = v

            for (k, v) in b.__dict__.items():
                if isinstance(v, FieldDescriptor) and k not in attrs:
                    if not isinstance(v.field, PrimaryKeyField):
                        attrs[k] = deepcopy(v.field)

        cls = super(BaseModel, cls).__new__(cls, name, bases, attrs)
        cls._meta = ModelOptions(cls, **meta_options)
        cls._tags = ModelOptions(cls) if tags_options else None
        for name, attr in tags_options.items():
            if isinstance(attr, Field):
                attr.add_tags_to_class(cls, name)
        cls._data = None

        primary_key = None
        for name, attr in cls.__dict__.items():
            if isinstance(attr, Field):
                attr.add_to_class(cls, name)
                if isinstance(attr, PrimaryKeyField):
                    primary_key = attr
        if not primary_key:
            primary_key = PrimaryKeyField()
            primary_key.add_to_class(cls, cls._meta.primary_key_name or 'ts')

        cls._meta.primary_key = primary_key

        if not cls._meta.db_table:
            cls._meta.db_table = re.sub('[^\\w]+', '_', cls.__name__.lower())
        
        cls._meta.db_table = "%s.%s" % (cls._meta.database.database,cls._meta.db_table)

        # create a repr and error class before finalizing
        if hasattr(cls, '__unicode__'):
            setattr(cls, '__repr__', lambda self: '<%s: %r>' % (
                cls.__name__, self.__unicode__()))

        exception_class = type('%sDoesNotExist' % cls.__name__, (DoesNotExist,), {})
        cls.DoesNotExist = exception_class
        cls._meta.prepared()
        if cls._tags:
            cls._tags.prepared()

        return cls


class Model(metaclass=BaseModel):
    # __metaclass__ = BaseModel

    def __init__(self, *args, **kwargs):
        self._data = self._meta.get_default_dict()
        self._obj_cache = {} # cache of related objects

        for k, v in kwargs.items():
            setattr(self, k, v)
    def get_ts(self):
        return getattr(self, self._meta.primary_key.name)
    def set_ts(self, ts):
        setattr(self, self._meta.primary_key.name, ts)
    @classmethod
    def create_table(cls,safe=True):
        return cls._meta.database.create_table(cls,safe)
    @classmethod
    def dynamic_create_table(cls,table_name,database=default_database,safe=True,**fields):
        fields['Meta']= type('Meta', (object,), dict(database = database,db_table=table_name))
        resModel = type(table_name, (cls,), fields)
        resModel.create_table(safe=safe)
        return resModel
    @classmethod
    def model_from_table(cls,table_name,database=default_database):
        if table_name in database.get_tables():
            fields = {}
            fields['Meta']= type('Meta', (object,), dict(database = database,db_table=table_name))
            desc_fields = database.describe_table_name(table_name)
            for i,f in enumerate(desc_fields):
                if f[3] != 'TAG':
                    val = None
                    if f[1] == 'TIMESTAMP':
                        val = PrimaryKeyField(column_name=f[0]) if i == 0 else DateTimeField(column_name=f[0])
                    elif f[1] == 'FLOAT':
                        val = FloatField(column_name=f[0])
                    elif f[1] == 'DOUBLE':
                        val = DoubleField(column_name=f[0])
                    elif f[1] == 'INT':
                        val = IntegerField(column_name=f[0])
                    elif f[1] == 'BIGINT':
                        val = BigIntegerField(column_name=f[0])
                    elif f[1] == 'SMALLINT':
                        val = SmallIntegerField(column_name=f[0])
                    elif f[1] == 'TINYINT':
                        val = TinyIntegerField(column_name=f[0])
                    elif f[1] == 'NCHAR':
                        val = NCharField(column_name=f[0],max_length=f[2])
                    elif f[1] == 'BINARY':
                        val = BinaryField(column_name=f[0],max_length=f[2])
                    elif f[1] == 'BOOL':
                        val = BooleanField(column_name=f[0])
                    else:
                        raise Exception('have unknow field')
                    fields[f[0]] = val
            resModel = type(table_name, (cls,), fields)
            return resModel
        else:
            return None
        
    @classmethod
    def drop_table(cls,safe=True):
        return cls._meta.database.drop_table(cls,safe)
    @classmethod
    def describe_table(cls):
        return cls._meta.database.describe_table(cls)
    @classmethod
    def table_exists(cls):
        return cls._meta.db_table[cls._meta.db_table.index('.')+1:] in cls._meta.database.get_tables()
        # return cls._meta.db_table in ["%s.%s" % (cls._meta.database.database,x[0]) for x in cls._meta.database.get_tables()]
    @classmethod
    def select(cls, *selection):
        query = SelectQuery(cls, *selection)
        if cls._meta.order_by:
            query = query.order_by(*cls._meta.order_by)
        return query
    @classmethod
    def insert(cls, **insert):
        pk = cls._meta.primary_key
        if pk.name not in insert:
            now = datetime.now()
            insert[pk.name] = now
        fdict = []
        for field in cls._meta.get_field_names():
            if field in insert:
                value = insert.get(field)
                if isinstance(value,datetime):
                    value = value.strftime("%Y-%m-%d %H:%M:%S.%f")
                fdict.append({'obj':cls._meta.fields[field],'value':value})
        return InsertQuery(cls, fdict).execute()
    def save(self):
        field_dict = dict(self._data)
        pk = self._meta.primary_key
        if pk.name not in field_dict:
            now = datetime.now()
            self.set_ts(now)
            field_dict[pk.name] = now
        # insert = self.insert(**field_dict)
        return self.insert(**field_dict)
    def get(self,expr):
        p1 = re.compile(r'^\((.*?)\)$', re.S)
        query_expr=type(self)._meta.database.get_compiler().parse_expr(expr)
        if query_expr:
            param = map(lambda x: '"%s"' % x if isinstance(x,str) or isinstance(x,datetime) else x, query_expr[1])
            queryori = query_expr[0].format(*param)
            query_bracket = re.findall(p1,queryori)
            if len(query_bracket) >0:
                query_str = query_bracket[0].lower()
            else:
                query_str = queryori.lower()
            return getattr(self,query_str) if hasattr(self,query_str) else None
        else:
            return None
class SuperModel(metaclass=BaseModel):
    def __init__(self, *args, **kwargs):
        self._data = self._meta.get_default_dict()
        self._obj_cache = {} # cache of related objects
        for k, v in kwargs.items():
            setattr(self, k, v)
    @classmethod
    def create_table(cls,safe=True):
        return cls._meta.database.create_table(cls,safe)
    @classmethod
    def dynamic_create_table(cls,table_name,database=default_database,safe=True,tags={},**fields):
        attr = dict(database =  database,db_table=table_name)
        attr.update(tags)
        _meta= type('Meta', (object,), attr)
        fields['Meta'] = _meta
        resModel = type(table_name, (cls,), fields)
        resModel.create_table(safe=safe)
        return resModel
    @classmethod
    def supermodel_from_table(cls,table_name,database=default_database):
        if table_name in database.get_supertables():
            fields = {}
            tags =dict(database = database,db_table=table_name)
            desc_fields = database.describe_table_name(table_name)
            for i,f in enumerate(desc_fields):
                val = None
                if f[1] == 'TIMESTAMP':
                    val = PrimaryKeyField(column_name=f[0]) if i == 0 else DateTimeField(column_name=f[0])
                elif f[1] == 'FLOAT':
                    val = FloatField(column_name=f[0])
                elif f[1] == 'DOUBLE':
                    val = DoubleField(column_name=f[0])
                elif f[1] == 'INT':
                    val = IntegerField(column_name=f[0])
                elif f[1] == 'BIGINT':
                    val = BigIntegerField(column_name=f[0])
                elif f[1] == 'SMALLINT':
                    val = SmallIntegerField(column_name=f[0])
                elif f[1] == 'TINYINT':
                    val = TinyIntegerField(column_name=f[0])
                elif f[1] == 'NCHAR':
                    val = NCharField(column_name=f[0],max_length=f[2])
                elif f[1] == 'BINARY':
                    val = BinaryField(column_name=f[0],max_length=f[2])
                elif f[1] == 'BOOL':
                    val = BooleanField(column_name=f[0])
                else:
                    raise Exception('have unknow field')
                if f[3] == 'TAG':
                    tags[f[0]] = val
                else:
                    fields[f[0]] = val
            fields['Meta']= type('Meta', (object,), tags)
            resModel = type(table_name, (cls,), fields)
            return resModel
        else:
            return None
    @classmethod
    def create_son_table(cls,name,**kwargs):
        cls._meta.database.create_table(cls,safe=True)
        tags =[]
        for field in cls._tags.get_field_names():
            if field not in kwargs:
                raise Exception('tag %s not have' % field)
            tags.append({'obj':cls._tags.fields[field],'value':kwargs.get(field)})
        # fdict = dict((cls._tags.fields[f], v) for f, v in tags.items())
        CreateSonTableQuery(cls, tags,name).execute()
        son_model = type(name, (cls,Model), dict())
        #########################################fix custom primary bug
        primary_column_name = cls._meta.primary_key.db_column
        if primary_column_name != 'ts':
            son_model._meta.primary_key = cls._meta.primary_key
            son_model._meta.fields.pop('ts')
            son_model._meta.columns.pop('ts')
            son_model._meta.fields[primary_column_name] = cls._meta.fields[primary_column_name]
            son_model._meta.columns[primary_column_name] = cls._meta.columns[primary_column_name]
        #############################################
        for name in cls._tags.get_field_names():
            delattr(son_model,name)
            field = son_model._meta.fields[name]
            del son_model._meta.columns[field.db_column]
            del son_model._meta.fields[name]
        son_model._tags =None
        return son_model
    @classmethod
    def drop_table(cls,safe=True):
        return cls._meta.database.drop_table(cls,safe)
    @classmethod
    def describe_table(cls):
        return cls._meta.database.describe_table(cls)
    @classmethod
    def supertable_exists(cls):
        # tables = ["%s.%s" % (cls._meta.database.database,x[3]) for x in cls._meta.database.get_tables()]
        return cls._meta.db_table[cls._meta.db_table.index('.')+1:] in cls._meta.database.get_supertables()
    @classmethod
    def select(cls, *selection):
        query = SelectQuery(cls, *selection)
        if cls._meta.order_by:
            query = query.order_by(*cls._meta.order_by)
        return query
    def get(self,expr):
        p1 = re.compile(r'^\((.*?)\)$', re.S)
        query_expr=type(self)._meta.database.get_compiler().parse_expr(expr)
        if query_expr:
            param = map(lambda x: '"%s"' % x if isinstance(x,str) or isinstance(x,datetime) else x, query_expr[1])
            queryori = query_expr[0].format(*param)
            query_bracket = re.findall(p1,queryori)
            if len(query_bracket) >0:
                query_str = query_bracket[0].lower()
            else:
                query_str = queryori.lower()
            return getattr(self,query_str) if hasattr(self,query_str) else None
        else:
            return None

if __name__ == "__main__":
    pass