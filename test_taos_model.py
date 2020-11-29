import pytest
from faker.generator import Generator
from crown import *
import datetime
DATABASENAME = 'taos_test'
HOST = '121.36.56.117'
db = TdEngineDatabase(DATABASENAME,host=HOST)
# def test_faker(faker):
#     """Faker factory is a fixture."""
#     assert isinstance(faker, Generator)
#     assert isinstance(faker.name(), str)
# test all field 
class AllField(Model):
        name_float = FloatField(column_name='n_float')
        name_double = DoubleField()
        name_bigint = BigIntegerField()
        name_int = IntegerField()
        name_smallint = SmallIntegerField()
        name_tinyint = TinyIntegerField()
        name_nchar = NCharField(max_length=59,)
        name_binary = BinaryField(max_length=3)
        name_ = BooleanField()
        dd = PrimaryKeyField()
        birthday = DateTimeField()
        class Meta:
            database = db
            db_table = 'all_field'
# test table
class Meter1(Model):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')
        class Meta:
            # order_by= ['-ts']
            database = db
            db_table = 'meter1'
class Meter(Model):
        cur = FloatField(db_column='c1')

def test_raw_sql():
    res = db.raw_sql('CREATE DATABASE IF NOT EXISTS "test_raw" KEEP 50 COMP 1 REPLICA 1 BLOCKS 100 QUORUM 2')
    # res = db.raw_sql('SELECT * FROM test5.d0003 where cur > ?',100)
    # print(res,res.head)
    assert res

def test_safe_create_and_drop_db():
    db.create_database(safe=True)
    assert DATABASENAME in db.databases
    db.drop_database()
    assert DATABASENAME not in db.databases

def test_param_create_alter_and_drop_db():
    db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115)
    assert db.databases[DATABASENAME]['quorum'] == 2
    assert db.databases[DATABASENAME]['replica'] == 1
    assert db.databases[DATABASENAME]['comp'] == 0
    assert db.databases[DATABASENAME]['blocks'] == 115
    assert db.databases[DATABASENAME]['keep1,keep2,keep(D)'] == '100,100,100'
    db.alter_database(keep= 120,comp=1,replica=1,quorum=1,blocks=156)
    assert db.databases[DATABASENAME]['quorum'] == 1
    assert db.databases[DATABASENAME]['replica'] == 1
    assert db.databases[DATABASENAME]['comp'] == 1
    assert db.databases[DATABASENAME]['blocks'] == 156
    assert db.databases[DATABASENAME]['keep1,keep2,keep(D)'] == '100,100,120'
    db.drop_database()
    assert DATABASENAME not in db.databases

def test_create_db_error():
    db.create_database(safe=True)
    try:
        db.create_database()
    except Exception as e:
        assert str(e) == 'Database already exists'
    db.drop_database()

def test_create_drop_table():
    assert Meter1.create_table()
    print(db.get_tables())
    assert Meter1.table_exists()
    assert Meter1.drop_table()
    assert not Meter1.table_exists()

@pytest.fixture()
def insertData():
    db.create_database(safe=True)
    Meter1.create_table()
    for i in range(1,11):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(hours=(12-i)))
        m.save()
    for i in range(1,21):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g2',ts= datetime.datetime.now() - datetime.timedelta(hours=(21-i)))
        m.save()

    yield

    Meter1.drop_table()
    
# 

def test_Meter1_groupby(insertData):
    groups= Meter1.select(Meter1.desc,Meter1.curInt.count(),Meter1.cur.count().alias('cc1')).group_by(Meter1.desc).all()
    for group in groups:
        # print(group.desc)
        if group.desc == 'g1':
            assert group.get(Meter1.curInt.count()) == 10
        if group.desc == 'g2':
            assert group.cc1 == 20


# TODO: invalid SQL: start(end) time of query range required or time range too large
# def test_Meter1_interval(insertData):
#     results= Meter1.select(Meter1.cur.avg().alias('aa'),Meter1.cur.first().alias('bb')).where(Meter1.ts > (datetime.datetime.now()-datetime.timedelta(days=1))).interval('10s',fill='PREV').all()
#     for result in results:
#         print(result.aa,result.bb)
#     assert True




