crown
======

crown is a simple and small ORM forTDengine(TSDB) 

* python 3.0 up
* tdengine 2.0.8 up


Installing
----------------------

Most users will want to simply install the latest version, hosted on PyPI：

.. code-block:: console

    pip install crown


The project is hosted at https://github.com/machine-w/crown and can be installed using git:

.. code-block:: console

    git clone https://github.com/machine-w/crown.git
    cd crowm
    python setup.py install


Quickstart
-------

Model Definition:

.. code-block:: python

    from crown import *
    import datetime


    DATABASENAME = 'taos_test'
    HOST = 'localhost'
    PORT = 6041
    db = TdEngineDatabase(DATABASENAME,host=HOST)
    # db = TdEngineDatabase(DATABASENAME,host=HOST,port=PORT,user='yourusername',passwd='yourpassword')

    

    class Meter1(Model):
        cur = FloatField(db_column='c1')
        curInt = IntegerField(db_column='c2')
        curDouble = DoubleField(db_column='c3')
        desc = BinaryField(db_column='des')
        class Meta:
            database = db 
            db_table = 'meter1'

    class AllField(Model):
        name_float = FloatField(column_name='n_float') 
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

creat database and delete database:

.. code-block:: python

    db.create_database(safe=True)  
    # db.create_database(safe=True,keep= 100,comp=0,replica=1,quorum=2,blocks=115) 
    db.drop_database(safe=True) 


alter database:

.. code-block:: python

    db.alter_database(keep= 120,comp=1,replica=1,quorum=1,blocks=156)


create drop and exist table：

.. code-block:: python

    Meter1.create_table(safe=True) 
    # db.create_table(Meter1,safe=True) 
    Meter1.drop_table(safe=True)
    # db.drop_table(Meter1,safe=True) 
    Meter1.table_exists()

