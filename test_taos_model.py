import pytest
from faker.generator import Generator
from crown import *
import datetime
import numpy as np
import pandas as pd
import logging
# DATABASENAME = 'taos_test'
# HOST = 'localhost'
# db = TdEngineDatabase(DATABASENAME,host=HOST,user="root",passwd="taosdata")
logger.setLevel(logging.DEBUG)
DATABASENAME = 'taos_test'
HOST = 'dev.tjulib.com.cn'
PORT = 13011
# 默认端口 6041，默认用户名：root,默认密码：taosdata
db = TdEngineDatabase(DATABASENAME,host=HOST,port=PORT,passwd="taosdata")
class AllField(Model):
        name_float = FloatField(db_column='nf1')
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
            db_table = 'allfield1'
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
    print(db.curSql)
    # print(Meter1.__dict__)
    assert Meter1.table_exists()
    assert Meter1.drop_table()
    print(db.curSql)
    assert not Meter1.table_exists()
def test_dynamic_create_table():
    Meter_dynamic= Model.dynamic_create_table('meterD',db,\
                                            test1 = FloatField(db_column='t1'),\
                                            test2 = IntegerField(db_column='t2'),\
                                            test3 = DoubleField(),\
                                            test4 = BigIntegerField(),\
                                            test5 = SmallIntegerField(),\
                                            test6 = TinyIntegerField(),\
                                            test7 = NCharField(max_length=20),\
                                            test8 = BinaryField(max_length=20),\
                                            test9 = BooleanField()
                                            )
    tabledes = Meter_dynamic.describe_table()
    print(tabledes)
    assert 'ts' in tabledes[0]
    assert 't1' in tabledes[1]
    assert 't2' in tabledes[2]
    assert Meter_dynamic.table_exists()
    assert Meter_dynamic.drop_table()
    assert not Meter_dynamic.table_exists()
def test_get_model_from_table():
    assert AllField.create_table(safe=True)
    assert AllField.table_exists()
    Meter_dynamic= Model.model_from_table('allfield1',db)
    tabledes = Meter_dynamic.describe_table()
    print(tabledes)
    m = Meter_dynamic(nf1 = 1.1,\
        name_double = 1.2,\
        name_bigint = 999999999,\
        name_int = 1000,\
        name_smallint = 100,\
        name_tinyint = 1,\
        n1 = "test",\
        name_binary = "tes",\
        name_bool = True,\
        birthday = datetime.datetime.now()\
    )
    m.save()
    m1=Meter_dynamic.select().one()
    assert m1.nf1==1.1
    assert m1.name_double==1.2
    assert m1.name_bigint==999999999
    assert m1.name_int==1000
    assert m1.name_smallint==100
    assert m1.name_tinyint==1
    assert m1.n1=="test"
    assert m1.name_binary=="tes"
    assert m1.name_bool==True
    assert m1.birthday<=datetime.datetime.now()
    assert Meter_dynamic.drop_table()
    assert not AllField.table_exists()


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
    print(m1.birthday)
    # assert m1.birthday<=datetime.datetime.now()
    AllField.drop_table()

def test_table_save():
    Meter1.create_table(safe=True)
    for i in range(1,11):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(seconds=(102-i)))
        m.save()
    assert Meter1.select().count() == 10
    for i in range(1,11):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1')
        m.save()
    assert Meter1.select().count() == 20
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
        m = Meter1(cur = 1/i,curInt=None,curDouble=1/i+10,desc=None,ts= datetime.datetime.now() - datetime.timedelta(hours=(12-i)))
        m.save()
    for i in range(1,21):
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g2',ts= datetime.datetime.now() - datetime.timedelta(hours=(21-i)))
        m.save()

    yield

    Meter1.drop_table()
def test_Meter1_select_one(insertData):
    logger.setLevel(logging.DEBUG)
    res = Meter1.select().one()
    assert res.desc == 'g2'
    assert res.curDouble == 11.0
    assert res.curInt == 1
    assert res.cur == 1
    assert res.ts<=datetime.datetime.now()
    res = Meter1.select(Meter1.cur,Meter1.desc,'curDouble','c2').one()
    assert res.desc == 'g2'
    assert res.curDouble == 11.0
    assert res.curInt == 1
    assert res.cur == 1
    assert res.ts == None

def test_Meter1_select_all(insertData):
    ress = Meter1.select().all()
    assert len(ress) == 30
    for res in ress:
        assert res.desc == 'g2' or res.desc == 'g1' or res.desc ==  None
        assert isinstance(res.curDouble,float)
        assert isinstance(res.curInt,int) or res.curInt is None
        assert isinstance(res.cur,float)
        assert res.ts<=datetime.datetime.now()
    # a = Meter1.fc('c2')
    # print(a)
    # print(Meter1.desc)
    ress = Meter1.select(Meter1.desc,Meter1.fc('c2'),Meter1.f('cur')).all()
    assert len(ress) == 30
    for res in ress:
        assert res.desc == 'g2' or res.desc == 'g1' or res.desc == None
        assert res.curDouble == None
        assert res.fc('c2') != None
        assert isinstance(res.cur,float)
        assert res.ts == None

def test_Meter1_select_operation(insertData):
    ress = Meter1.select(((Meter1.curDouble+Meter1.cur)*Meter1.curDouble).alias('aa'),Meter1.ts).all()
    assert len(ress) == 30
    for res in ress:
        assert isinstance(res.aa,float) or isinstance(res.aa,int)
        # assert res.bb == 1.1
        assert res.ts<=datetime.datetime.now()
    
    # TODO: tdengine restful api bug: miss bracket
    ress = Meter1.select((Meter1.curDouble+Meter1.cur),Meter1.ts).all()
    assert len(ress) == 30
    for res in ress:
        # print(res.get(Meter1.curDouble+Meter1.cur))
        assert isinstance(res.get(Meter1.curDouble+Meter1.cur),float) or isinstance(res.get(Meter1.curDouble+Meter1.cur),int)
        # assert res.bb == 1.1
        assert res.ts<=datetime.datetime.now()

def test_Meter1_select_where(insertData):
    ress = Meter1.select().where(Meter1.cur > 0,Meter1.ts > datetime.datetime.now() - datetime.timedelta(hours=10),Meter1.desc % '%2').all()
    assert len(ress) == 9
    for res in ress:
        assert res.desc == 'g2' or res.desc == 'g1'
        assert isinstance(res.curDouble,float)
        assert isinstance(res.curInt,int)
        assert isinstance(res.cur,float)
        print(res.ts,res.cur)
        assert res.ts<=datetime.datetime.now()

def test_Meter1_select_paginate(insertData):
    ress_1 = Meter1.select().paginate(1,page_size=5).all()
    ress_2 = Meter1.select().paginate(2,page_size=5).all()
    assert len(ress_1) == 5 and len(ress_2) == 5
    for i in range(5):
        assert ress_1[i].ts < ress_2[i].ts

def test_Meter1_select_limit(insertData):
    ress_1 = Meter1.select().limit(2).offset(5).all()
    assert len(ress_1) == 2
    assert ress_1[0].curInt ==6
    assert ress_1[1].curInt ==7
    ress_1 = Meter1.select().limit(2).all()
    assert len(ress_1) == 2
    assert ress_1[0].curInt ==1
    assert ress_1[1].curInt ==2

def test_Meter1_select_one_desc(insertData):
    res = Meter1.select().desc().one()
    assert res.desc == 'g2'
    assert res.curDouble == 10.05
    assert res.curInt == 20
    assert res.cur == 0.05
    assert res.ts<=datetime.datetime.now()

def test_Meter1_count(insertData):
    count = Meter1.select().count()
    assert count == 30
    count = Meter1.select().count(Meter1.desc)
    assert count == 30

def test_Meter1_avg(insertData):
    avg1 = Meter1.select().avg(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert avg1
    if avg1:
        assert avg1.get(Meter1.cur.avg()) == 0.21755693269272644
        assert avg1.aa == 10.217556930370396

def test_Meter1_twa(insertData):
    twa1 = Meter1.select().where(Meter1.ts > datetime.datetime(2020, 11, 19, 15, 9, 12, 946118),\
                                Meter1.ts < datetime.datetime.now()).twa(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert twa1
    if twa1:
        assert twa1.get(Meter1.cur.twa()) > 0
        assert twa1.aa > 0

def test_Meter1_sum(insertData):
    sum1 = Meter1.select().sum(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert sum1
    if sum1:
        assert sum1.get(Meter1.cur.sum()) == 6.526707980781794
        assert sum1.aa == 306.5267079111119

def test_Meter1_stddev(insertData):
    stddev1 = Meter1.select().stddev(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert stddev1
    if stddev1:
        assert stddev1.get(Meter1.cur.stddev()) == 0.23986110147960907
        assert stddev1.aa == 0.23986110123120702

def test_Meter1_min(insertData):
    min1 = Meter1.select().min(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert min1
    if min1:
        assert min1.get(Meter1.cur.min()) == 0.05
        assert min1.aa == 10.05

def test_Meter1_max(insertData):
    max1 = Meter1.select().max(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert max1
    if max1:
        assert max1.get(Meter1.cur.max()) == 1.0
        assert max1.aa == 11.0

def test_Meter1_first(insertData):
    first1 = Meter1.select().first(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert first1
    if first1:
        assert first1.get(Meter1.cur.first()) == 1.0
        assert first1.aa == 11.0
def test_Meter1_last(insertData):
    last1 = Meter1.select().last(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert last1
    if last1:
        assert last1.get(Meter1.cur.last()) == 0.05
        assert last1.aa == 10.05

def test_Meter1_last_row(insertData):
    last_row1 = Meter1.select().last_row(Meter1.cur,Meter1.curDouble.alias('aa'))
    assert last_row1
    if last_row1:
        assert last_row1.get(Meter1.cur.last_row()) == 0.05
        assert last_row1.aa == 10.05

def test_Meter1_spread(insertData):
    spread1 = Meter1.select().spread(Meter1.curInt,Meter1.curDouble.alias('aa'))
    assert spread1
    if spread1:
        assert spread1.get(Meter1.curInt.spread()) == 19.0
        assert spread1.aa == 0.9499999999999993

def test_Meter1_diff(insertData):
    # TODO: bug多列报错
    diffs = Meter1.select().diff(Meter1.curInt.alias('aa'))
    for diff1 in diffs:
        assert diff1.aa in [1,-8,9]
        # assert diff1.ts<=datetime.datetime.now()
def test_Meter1_top(insertData):
    tops = Meter1.select().top(Meter1.cur,3,alias='aa')
    assert len(tops) == 3
    for top1 in tops:
        assert top1.aa in [0.5,1.0]
        assert top1.cur == None
        # assert top1.ts<=datetime.datetime.now()
def test_Meter1_bottom(insertData):
    bottoms = Meter1.select().bottom(Meter1.cur,3,alias='aa')
    assert len(bottoms) == 3
    for bottom1 in bottoms:
        assert bottom1.aa in [0.05,0.05556,0.05263,0.05263158,0.055555556]
        assert bottom1.cur == None
        # assert bottom1.ts<=datetime.datetime.now()
        
def test_Meter1_apercentile(insertData):
    apercentile1 = Meter1.select().apercentile((Meter1.cur,1,'aa'),(Meter1.curDouble,2))
    assert apercentile1.aa == 0.05000000074505806
    assert apercentile1.cur == None
    assert apercentile1.get(Meter1.curDouble.apercentile(2)) == 10.05
    try:
        apercentile1 = Meter1.select().apercentile((Meter1.cur,))
    except Exception as e:
        assert str(e) == 'field param less than 2'
    try:
        apercentile1 = Meter1.select().apercentile(Meter1.cur)
    except Exception as e:
        assert str(e) == 'field is not a tuple or list'

def test_Meter1_percentile(insertData):
    percentile1 = Meter1.select().percentile((Meter1.cur,1,'aa'),(Meter1.curDouble,2))
    assert percentile1.aa > 0.0
    assert percentile1.cur == None
    # assert percentile1.bb == 10.051526316
    assert percentile1.get(Meter1.curDouble.percentile(2)) == 10.051526315789474
    try:
        percentile1 = Meter1.select().percentile((Meter1.cur,))
    except Exception as e:
        assert str(e) == 'field param less than 2'
    try:
        percentile1 = Meter1.select().percentile(Meter1.cur)
    except Exception as e:
        assert str(e) == 'field is not a tuple or list'
def test_Meter1_leastsquares(insertData):
    leastsquares1 = Meter1.select().leastsquares((Meter1.cur,1,1,'aa'),(Meter1.curDouble,2,2))
    assert leastsquares1.aa.find('slop')
    assert leastsquares1.cur == None
    assert leastsquares1.get(Meter1.curDouble.leastsquares(2,2)).find('slop')
    try:
        leastsquares1 = Meter1.select().leastsquares((Meter1.cur,))
    except Exception as e:
        assert str(e) == 'field param less than 3'
    try:
        leastsquares1 = Meter1.select().leastsquares(Meter1.cur)
    except Exception as e:
        assert str(e) == 'field is not a tuple or list'

def test_Meter1_groupby(insertData):
    groups= Meter1.select(Meter1.desc,Meter1.curInt.avg().alias('intavg'),Meter1.cur.count().alias('curcount')).group_by(Meter1.desc).all()
    for group in groups:
        print(group.desc)
        if group.desc == 'g1':
            # assert group.get(Meter1.curInt.count()) == 10
            assert group.intavg == 5.5
            assert group.curcount == 10
        if group.desc == 'g2':
            assert group.intavg == 10.5
            assert group.curcount == 20

# TODO: invalid SQL: start(end) time of query range required or time range too large
# def test_Meter1_interval(insertData):
#     results= Meter1.select(Meter1.cur.avg().alias('aa'),Meter1.cur.first().alias('bb')).where(Meter1.ts > (datetime.datetime.now()-datetime.timedelta(days=1))).interval('10s',fill=1.2).all()
#     for result in results:
#         print(result.aa,result.bb)
#     assert True

def test_raw_sql_numpy(insertData):
    raw_results = Meter1.select(Meter1.cur,Meter1.curInt,Meter1.curDouble).all_raw()
    np_data = np.array(raw_results)
    print(np_data)
    assert np_data.shape == (30,3)

def test_raw_sql_pandas(insertData):
    raw_results = Meter1.select().all_raw()
    pd_data = pd.DataFrame(raw_results,columns=raw_results.head).set_index('ts')
    print(pd_data)
    print(pd_data.index)
    assert pd_data.shape == (30,4)