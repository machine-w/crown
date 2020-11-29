from crown import *
import datetime
import logging

# logger.setLevel(logging.DEBUG)
DATABASENAME = 'taos_test'
HOST = '121.36.56.117'
PORT = 6041
# 默认端口 6041，默认用户名：root,默认密码：taosdata
db = TdEngineDatabase(DATABASENAME,host=HOST)
# 如不使用默认值，可以如下传入参数
# db = TdEngineDatabase(DATABASENAME,host=HOST,port=PORT,user='yourusername',passwd='yourpassword')



class Meter1(Model):
    cur = FloatField(db_column='c1')
    curInt = IntegerField(db_column='c2')
    curDouble = DoubleField(db_column='c3')
    desc = BinaryField(db_column='des')
    class Meta:
        database = db #指定表所使用的数据库
        db_table = 'meter1' #指定表名

class AllField(Model):
    name_float = FloatField(column_name='n_float') #可选项：指定列名
    name_double = DoubleField()
    name_bigint = BigIntegerField()
    name_int = IntegerField()
    name_smallint = SmallIntegerField()
    name_tinyint = TinyIntegerField()
    name_nchar = NCharField(max_length=59)
    name_binary = BinaryField(max_length=3)
    name_ = BooleanField()
    dd = PrimaryKeyField()
    birthday = DateTimeField()
    class Meta:
        database = db
        db_table = 'all_field'


db.create_database(safe=True)  #建库 safe：如果库存在，则跳过建库指令。
# db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115) #可选字段：建库时配置数据库参数，具体字段含义请参考tdengine文档。
# db.drop_database(safe=True) #删库 safe：如果库不存在，则跳过删库指令。

# db.alter_database(keep= 120,comp=1,replica=1,quorum=1,blocks=156) #同建库可选字段。


Meter1.create_table(safe=True) #建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
# db.create_table(Meter1,safe=True) #通过数据库对象建表，功能同上
print(Meter1.table_exists())
Meter1.drop_table(safe=True) #删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
# db.drop_table(Meter1,safe=True) #通过数据库对象删表，功能同上
print(Meter1.table_exists()) #查看表是否存在，存在返回True,不存在返回：False