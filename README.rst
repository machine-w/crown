crown
======

crown 是一个轻量级的针对时序数据（TSDB）TDengine的小型ORM库。 

* 需要python 3.0版本以上
* 在tdengine 2.0.8版本测试通过
* 旨在解决mac操作系统下目前没有原生python连接器的问题，也为更加方便的使用tdengine数据库。

安装
----------------------

大多数情况下，可以通过pip,轻松安装最新版本：

.. code-block:: console

    pip install crown


还可以通过git安装，项目地址： https://github.com/machine-w/crown

使用方法:

.. code-block:: console

    git clone https://github.com/machine-w/crown.git
    cd crowm
    python setup.py install


简单使用
-------

模型定义:

.. code-block:: python

    from crown import *
    import datetime


    DATABASENAME = 'taos_test'
    HOST = 'localhost'
    PORT = 6041
    # 默认端口 6041，默认用户名：root,默认密码：taosdata
    db = TdEngineDatabase(DATABASENAME,host=HOST)
    # 如不使用默认值，可以如下传入参数
    # db = TdEngineDatabase(DATABASENAME,host=HOST,port=PORT,user='yourusername',passwd='yourpassword')


    # 表模型类继承自Model类
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
        dd = PrimaryKeyField() # 如果定义了主键列，则使用主键列作为主键，如果没有定义，则默认“ts”为主键。
        birthday = DateTimeField()
        class Meta:
            database = db
            db_table = 'all_field'

建立数据库与删除数据库:

.. code-block:: python

    db.create_database(safe=True)  #建库 safe：如果库存在，则跳过建库指令。
    # db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115) #可选字段：建库时配置数据库参数，具体字段含义请参考tdengine文档。
    db.drop_database(safe=True) #删库 safe：如果库不存在，则跳过删库指令。


修改数据库参数:

.. code-block:: python

    db.alter_database(keep= 120,comp=1,replica=1,quorum=1,blocks=156) #同建库可选字段。


建表、删表、检查表是否存在：

.. code-block:: python

    Meter1.create_table(safe=True) #建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
    # db.create_table(Meter1,safe=True) #通过数据库对象建表，功能同上
    Meter1.drop_table(safe=True) #删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
    # db.drop_table(Meter1,safe=True) #通过数据库对象删表，功能同上
    Meter1.table_exists() #查看表是否存在，存在返回True,不存在返回：False

超级表定义：

.. code-block:: python
    # 超级表模型类继承自SuperModel类
    class Meters(SuperModel):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')
        class Meta:
            database = db
            db_table = 'meters'
            # Meta类中定义的Field，为超级表的标签
            location = BinaryField(max_length=30)
            groupid = IntegerField(db_column='gid')

超级表的建表、删表、检查表是否存在：

.. code-block:: python

    Meters.create_table(safe=True) #建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
    # db.create_table(Meters,safe=True) #通过数据库对象建表，功能同上
    Meters.drop_table(safe=True) #删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
    # db.drop_table(Meters,safe=True) #通过数据库对象删表，功能同上
    Meters.supertable_exists() #查看表是否存在，存在返回True,不存在返回：False

从超级表建立子表：

.. code-block:: python

    SonTable_d3 = Meters.create_son_table('d3',location='beijing',groupid=3) #生成字表模型类的同时，自动在数据库中建表。

    SonTable_d3.table_exists() # SonTable_d3的使用方法和继承自Modle类的模型类一样。可以进行插入与查询操作
    # m = SonTable_d3(cur = 65.8,curInt=10,curDouble=1.1,desc='g1',ts = datetime.datetime.now())
    # m.save()
