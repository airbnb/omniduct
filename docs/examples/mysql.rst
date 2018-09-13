Example: MySQL
=================

This is example for MySQL protocol

*Method 1: Via SQLAlchemyClient class*

.. code-block:: python

    from omniduct.databases.sqlalchemy import SQLAlchemyClient


    sa_con = SQLAlchemyClient(protocol='mysql', host='localhost', port=3306,
                              username='username', password='password')

    sa_con.query('use my_database')
    sa_con.query('show tables')

    #                      Tables_in_my_database
    # 0                               my_table_0
    # 1                               my_table_1
    # ...
    # ...


*Method 2: Via Duct subclass registry*

.. code-block:: python

    from omniduct import Duct

    mysql_con = Duct.for_protocol('mysql')(host='localhost', port=3306,
                                           username='username', password='password')

    mysql_con.query('use my_database')
    mysql_con.query('show tables')
    # ... And all of the rest from above.

