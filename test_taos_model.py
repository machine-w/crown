import pytest
from faker.generator import Generator
from crown import *
import datetime
DATABASENAME = 'taos_test'
HOST = '121.36.56.117'
db = TdEngineDatabase(DATABASENAME,host=HOST,user="root",passwd="msl110918")
class AllField(Model):
        name_float = FloatField(column_name='nf1')
        name_double = DoubleField()
        name_bigint = BigIntegerField()
        name_int = IntegerField()
        name_smallint = SmallIntegerField()
        name_tinyint = TinyIntegerField()
        name_nchar = NCharField(max_length=59,db_column='n1')
        name_binary = BinaryField(max_length=3)
        name_bool = BooleanField()
        dd = PrimaryKeyField()
        birthday = DateTimeField()
        class Meta:
            database = db
            db_table = 'allfield'
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
# class Meter(Model):
#         cur = FloatField(db_column='c1')

def test_create_drop_table():
    assert Meter1.create_table()
    print(db.get_tables())
    assert Meter1.table_exists()
    assert Meter1.drop_table()
    assert not Meter1.table_exists()
def test_table_primary():
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        class Meta:
            database = db
    TestPri.create_table(safe=True)
    res = TestPri.describe_table()
    assert res[0][0] == 'ts'
    TestPri.drop_table(safe=True)

def test_table_primary2():
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        timeline = PrimaryKeyField()
        class Meta:
            database = db
    TestPri.create_table(safe=True)
    res = TestPri.describe_table()
    assert res[0][0] == 'timeline'
    TestPri.drop_table(safe=True)

def test_table_primary3():
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        class Meta:
            database = db
            primary_key = 'ttt'
    TestPri.create_table(safe=True)
    res = TestPri.describe_table()
    assert res[0][0] == 'ttt'
    TestPri.drop_table(safe=True)

def test_table_save_one():
    AllField.create_table(safe=True)
    m = AllField(name_float = 1.1,\
        name_double = 1.2,\
        name_bigint = 999999999,\
        name_int = 1000,\
        name_smallint = 100,\
        name_tinyint = 1,\
        name_nchar = "test",\
        name_binary = "tes",\
        name_bool = True,\
        birthday = datetime.datetime.now()\
    )
    m.save()
    m1=AllField.select().one()
    assert m1.name_float==1.1
    assert m1.name_double==1.2
    assert m1.name_bigint==999999999
    assert m1.name_int==1000
    assert m1.name_smallint==100
    assert m1.name_tinyint==1
    assert m1.name_nchar=="test"
    assert m1.name_binary=="tes"
    assert m1.name_bool==True
    assert m1.birthday<=datetime.datetime.now()
    AllField.drop_table()

def test_table_save():
    Meter1.create_table(safe=True)
    for i in range(1,101):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(seconds=(102-i)))
        m.save()
    assert Meter1.select().count() == 100
    for i in range(1,101):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1')
        m.save()
    assert Meter1.select().count() == 200
    Meter1.drop_table()

def test_table_insert():
    Meter1.create_table(safe=True)
    for i in range(1,11):
        Meter1.insert(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(seconds=(12-i)))
    res = Meter1.select().all()
    for item in res:
        print(item.ts)
    assert Meter1.select().count() == 10
    for i in range(1,11):
        Meter1.insert(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1')
    res = Meter1.select().all()
    for item in res:
        print(item.ts)
    assert Meter1.select().count() == 20
    Meter1.drop_table()


@pytest.fixture()
def insertData():
    db.create_database(safe=True)
    Meter1.create_table(safe=True)
    for i in range(1,11):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(hours=(12-i)))
        m.save()
    for i in range(1,21):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g2',ts= datetime.datetime.now() - datetime.timedelta(hours=(21-i)))
        m.save()

    yield

    Meter1.drop_table()
def test_Meter1_select_one(insertData):
    res = Meter1.select().one()
    assert res.desc == 'g2'
    assert res.curDouble == 11.0
    assert res.curInt == 1
    assert res.cur == 1
    assert res.ts<=datetime.datetime.now()
    res = Meter1.select(Meter1.cur,Meter1.desc).one()
    assert res.desc == 'g2'
    assert res.curDouble == None
    assert res.curInt == None
    assert res.cur == 1
    assert res.ts == None

def test_Meter1_select_all(insertData):
    ress = Meter1.select().all()
    assert len(ress) == 30
    for res in ress:
        assert res.desc == 'g2' or res.desc == 'g1'
        assert isinstance(res.curDouble,float)
        assert isinstance(res.curInt,int)
        assert isinstance(res.cur,float)
        assert res.ts<=datetime.datetime.now()
    ress = Meter1.select(Meter1.cur,Meter1.desc).all()
    assert len(ress) == 30
    for res in ress:
        assert res.desc == 'g2' or res.desc == 'g1'
        assert res.curDouble == None
        assert res.curInt == None
        assert isinstance(res.cur,float)
        assert res.ts == None
    # TODO: select operation


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