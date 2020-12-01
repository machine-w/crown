crown
======

crown 是一个轻量级的针对时序数据（TSDB）TDengine的ORM库。 

* 需要python 3.0版本以上
* 在tdengine 2.0.8版本测试通过
* 旨在解决mac操作系统下目前没有原生python连接器的问题，也为更加方便的使用tdengine数据库。
* 目前使用TDengine的restful接口连接数据库，以后将提供原生接口引擎可供选择（目前原生接口无法在mac系统上使用）。

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


使用文档
------------------------

建立数据库与删除数据库:

.. code-block:: python

    from crown import *

    DATABASENAME = 'taos_test'
    HOST = 'localhost'
    PORT = 6041
    # 默认端口 6041，默认用户名：root,默认密码：taosdata
    db = TdEngineDatabase(DATABASENAME,host=HOST) #新建数据库对象
    # db.connect()  # 尝试连接数据库，如果库不存在，则自动建库。
    # print(db.databases) #连接数据库db对象后会自动获取全部数据库信息，以字典的形式保存在属性databases中。
    # 如不使用默认值，可以如下传入参数
    # db = TdEngineDatabase(DATABASENAME,host=HOST,port=PORT,user='yourusername',passwd='yourpassword')
    db.create_database(safe=True)  #建库指令。 （safe：如果库存在，则跳过建库指令。）
    # db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115) #可选字段：建库时配置数据库参数，具体字段含义请参考tdengine文档。
    db.drop_database(safe=True) #删库指令 （safe：如果库不存在，则跳过删库指令。）

修改数据库参数:

.. code-block:: python

    db.alter_database(keep= 120,comp=1,replica=1,quorum=1,blocks=156) #同建库可选字段。

执行sql语句:

.. code-block:: python

    #可以通过数据库对象直接执行sql语句，语句规则与TDengine restful接口要求一致。
    res = db.raw_sql('select c1,c2 from taos_test.member1')
    print(res,res.head,res.rowcount) #返回的对象为二维数据。res.head属性为数组对象，保存每一行数据的代表的列名。res.rowcount属性保存返回行数。
    # res: [[1.2,2.2],[1.3,2.1],[1.5,2.0],[1.6,2.1]]
    # res.head: ['c1','c2']
    # res.rowcount: 4

模型定义:

.. code-block:: python

    from crown import *

    DATABASENAME = 'taos_test'
    HOST = 'localhost'
    db = TdEngineDatabase(DATABASENAME,host=HOST) #新建数据库对象
    db.connect()  #尝试连接数据库，如果库不存在，则自动建库。
    # print(db.databases) #连接数据库db对象后会自动获取全部数据库信息，以字典的形式保存在属性databases中。

    # 表模型类继承自Model类，每个模型类对应数据库中的一张表，模型类中定义的每个Field，对应表中的一列
    class Meter1(Model):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')

        class Meta: #Meta子类中定义模型类的配置信息
            database = db #指定表所使用的数据库
            db_table = 'meter1' #指定表名

    # 可选择的全部Field类型如下，类型与Tdengine支持的数据类型一一对应
    class AllField(Model):
        name_float = FloatField(column_name='nf1') #可选项：指定列名
        name_double = DoubleField()
        name_bigint = BigIntegerField()
        name_int = IntegerField()
        name_smallint = SmallIntegerField()
        name_tinyint = TinyIntegerField()
        name_nchar = NCharField(max_length=59,db_column='n1')
        name_binary = BinaryField(max_length=3)
        name_bool = BooleanField()
        dd = PrimaryKeyField() # 如果定义了主键列，则使用主键列作为主键，如果没有定义，则默认“ts”为主键。
        birthday = DateTimeField()
        class Meta:
            database = db
            db_table = 'all_field'

主键定义：

.. code-block:: python

    #定义主键方式1 
    #不定义主键，系统默认主键：“ts”
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        class Meta:
            database = db
    res = TestPri.describe_table() #获取表结构信息
    print(res[0][0]) # 结果: “ts”

    #定义主键方式2
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        timeline = PrimaryKeyField() #定义主键列，主键名设置为列名
        class Meta:
            database = db
    res = TestPri.describe_table()
    print(res[0][0]) # 结果: “timeline”

     #定义主键方式3
    class TestPri(Model):
        cur = FloatField(db_column='c1')
        class Meta:
            database = db
            primary_key = 'timeline' # Meta中定主键名称
    res = TestPri.describe_table()
    print(res[0][0]) # 结果: “timeline”
    


建表、删表、检查表是否存在：

.. code-block:: python

    Meter1.create_table(safe=True) #建表 safe：如果表存在，则跳过建表指令。命令运行成功放回True,失败raise错误
    # db.create_table(Meter1,safe=True) #通过数据库对象建表，功能同上
    Meter1.drop_table(safe=True) #删表 safe：如果表不存在，则跳过删表指令。命令运行成功放回True,失败raise错误
    # db.drop_table(Meter1,safe=True) #通过数据库对象删表，功能同上
    Meter1.table_exists() #查看表是否存在，存在返回True,不存在返回：False

插入数据：

.. code-block:: python

    #方法一
    for i in range(1,101):
        #使用模型类实例化的每个对象对应数据表中的每一行，可以通过传入属性参数的方式给每一列赋值
        m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(seconds=(102-i)))
        #使用对象的save方法将数据存入数据库
        m.save()
    print(Meter1.select().count()) # 结果：100
    #方法二
    for i in range(1,11):
        #也可以直接使用模型类的insert方法插入数据。
        Meter1.insert(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1',ts= datetime.datetime.now() - datetime.timedelta(seconds=(12-i)))
    print(Meter1.select().count()) # 结果：100
    #如果不传入时间属性，则会以当前时刻为默认值传入
    Meter1.insert(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1')
    m = Meter1(cur = 1/i,curInt=i,curDouble=1/i+10,desc='g1')
    m.save()

查询单条数据：

.. code-block:: python

    #获取一条数据
    #使用select()类方法获取查询字段（参数留空表示取全部字段），然后可以链式使用one方法获取第一条数据
    res = Meter1.select().one()
    print(res.desc,res.curDouble,res.curInt,res.cur,res.ts)

    #select函数中可以选择要读取的字段
    res = Meter1.select(Meter1.cur,Meter1.desc).one()
    print(res.desc,res.curDouble,res.curInt,res.cur,res.ts)

查询全部数据：

.. code-block:: python

    #获取一条数据
    #使用select()类方法获取查询字段（参数留空表示取全部字段），然后可以链式使用all方法获取全部数据
    res_all = Meter1.select().all()
    for res in res_all:
        print(res.desc,res.curDouble,res.curInt,res.cur,res.ts)

    #select函数中可以选择要读取的字段
    res_all = Meter1.select(Meter1.cur,Meter1.desc).all()
    for res in res_all:
        print(res.desc,res.curDouble,res.curInt,res.cur,res.ts)

选择列四则运算：

.. code-block:: python

    #使用select()类方法获取查询字段时，可以返回某列或多列间的值加、减、乘、除、取余计算结果（+ - * / %）
    res_all = Meter1.select((Meter1.curDouble+Meter1.cur),Meter1.ts).all()
    for res in res_all:
        print(res.get(Meter1.curDouble+Meter1.cur),res.ts) #返回的结果对象可以用get方法获取原始计算式结果

    #字段别名
    res_all = Meter1.select(((Meter1.curDouble+Meter1.cur)*Meter1.curDouble).alias('new_name'),Meter1.ts).all() #给运算式起别名（不仅运算式，其他放在select函数中的任何属性都可以使用别名）
    for res in res_all:
        print(res.new_name,res.ts) #使用别名获取运算结果

where查询条件：

.. code-block:: python

    #可以在select函数后链式调用where函数进行条件限
    one_time =datetime.datetime.now() - datetime.timedelta(hours=10)
    ress = Meter1.select().where(Meter1.ts > one_time).all()
    #限定条件可以使用 > < == >= <= != and or ! 等。字符类型的字段可以使用 % 作为模糊查询（相当于like）
    ress = Meter1.select().where(Meter1.cur > 0 or Meter1.desc % 'g%').all()
    #where函数可以接收任意多参数，每个参数为一个限定条件，参数条件之间为"与"的关系。
    ress = Meter1.select().where(Meter1.cur > 0, Meter1.ts > one_time, Meter1.desc % '%1').all()

排序（目前tdengine只支持主键排序）：

.. code-block:: python

    #可以在select函数后链式调用desc或者asc函数进行时间轴的正序或者倒序查询
    res = Meter1.select().desc().one()
    #定义模型类的时候定义默认排序方法
    class Meter1(Model):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')
        dd = PrimaryKeyField().desc() #可以在定义主键的时候调用field的desc或asc方法定义默认排序
        class Meta:
            # order_by= ['-dd'] #也可以在元数据类中定义‘-dd’代表倒序‘dd’ 代表正序
            database = db

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
