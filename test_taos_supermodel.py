import pytest
from crown import *
import datetime
DATABASENAME = 'taos_test'
HOST = 'localhost'
db = TdEngineDatabase('taos_test',host=HOST)

# test all field 
class AllFields(SuperModel):
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
            location = BinaryField(max_length=30)
            groupid = IntegerField(db_column='gid')
AllField1 = AllFields.create_son_table('d3',location='beijing',groupid=3)
class Meters(SuperModel):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')
        class Meta:
            order_by= ['-ts']
            database = db
            db_table = 'meters'
            location = BinaryField(max_length=30)
            groupid = IntegerField(db_column='gid')
TableT = Meters.create_son_table('d3',location='beijing',groupid=3)

def test_create_drop_stable():
    assert Meters.create_table()
    print(db.get_supertables())
    assert Meters.supertable_exists()
    assert Meters.drop_table()
    assert not Meters.supertable_exists()

def test_dynamic_create_stable():
    Meter_dynamic= SuperModel.dynamic_create_table('meterSD',db,tags={'gid':IntegerField(db_column='tag1')},test1 = FloatField(db_column='t1'),test2 = IntegerField(db_column='t2'))
    tabledes = Meter_dynamic.describe_table()
    print(tabledes)
    assert 'ts' in tabledes[0]
    assert 't1' in tabledes[1]
    assert 't2' in tabledes[2]
    assert 'tag1' in tabledes[3]
    assert 'TAG' in tabledes[3]
    assert Meter_dynamic.supertable_exists()
    assert Meter_dynamic.drop_table()
    assert not Meter_dynamic.supertable_exists()

def test_create_drop_sontable():
    son = Meters.create_son_table('d1',location='beijing',groupid=3)
    print(db.get_tables())
    assert son.table_exists()
    assert son.drop_table()
    assert not son.table_exists()


@pytest.fixture()
def insertData():
    for i in range(1,11):
        # time.sleep(30)
        m = TableT(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(hours=(12-i)))
        m.save()
    for i in range(1,21):
        # time.sleep(30)
        m = TableT(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g2',ts= datetime.datetime.now() - datetime.timedelta(hours=(21-i)))
        m.save()
    yield

    TableT.drop_table()





