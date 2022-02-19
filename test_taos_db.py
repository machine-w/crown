import pytest
from faker.generator import Generator
from crown import *
import datetime
DATABASENAME = 'taos_tests'
HOST = '121.36.56.117'
PORT = 8091
db = TdEngineDatabase(DATABASENAME,host=HOST,port=PORT)

import logging

# 通过下面的方式进行简单配置输出方式与日志级别
logging.basicConfig(level=logging.DEBUG)
logger.setLevel(logging.DEBUG)
def test_db_connect():
    db.connect()
    assert DATABASENAME in db.databases

def test_raw_sql():
    res = db.raw_sql('CREATE DATABASE IF NOT EXISTS "test_raw" KEEP 50 COMP 2 REPLICA 2 BLOCKS 100 QUORUM 1')
    # res = db.raw_sql('SELECT * FROM test5.d0003 where cur > ?',100)
    # print(res,res.head)
    assert res

def test_safe_create_and_drop_db():
    db.create_database(safe=True)
    assert DATABASENAME in db.databases
    db.drop_database()
    assert DATABASENAME not in db.databases

def test_param_create_alter_and_drop_db():
    db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=1,blocks=115)
    assert db.databases[DATABASENAME]['quorum'] == 1
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