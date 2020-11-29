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

    pip install peewee


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


    db = SqliteDatabase('my_database.db')

    class BaseModel(Model):
        class Meta:
            database = db

    class User(BaseModel):
        username = CharField(unique=True)

    class Tweet(BaseModel):
        user = ForeignKeyField(User, backref='tweets')
        message = TextField()
        created_date = DateTimeField(default=datetime.datetime.now)
        is_published = BooleanField(default=True)

连接数据库并建立表:
