
from db import (enable_logging,enable_hstore,
                connect,shutdown,
                cursor,execute,
                query,query_one,query_dict,
                select,select_one,select_dict,
                join,join_one,join_dict,
                insert,delete,update,
                check_table,drop_table,create_table,
                init_db)

version = "0.4"
__doc__ = """

    pgwrap - simple PostgreSQL database wrapper
    -------------------------------------------

    The 'pgwrap' module provides a simple wrapper over psycopg2 supporting a
    Python API for common sql functions.

    This is not intended to provide an ORM-like functionality, just to make it
    easier to interact with PostgreSQL from python code for simple use-cases.
    For more complex operations direct SQL access is available.
    
    The module wraps the excellent 'psycopg2' library and most of the 
    functionality is provided by this behind the scenes.

    The module provides:

        * Simplified handling of connections/cursor
            * Connection pool (provided by psycopg2.pool)
            * Cursor context handler 
        * Python API to wrap basic SQL functionality 
            * Simple select,update,delete,join methods extending the cursor 
              context handler (also available as stand-alone methods which
              create an implicit cursor for simple queries)
        * Query results as dict (using psycopg2.extras.RealDictCursor)
        * Logging support

    Basic usage
    -----------

    >>> import pgwrap
    >>> pgwrap.connect(url='postgres://localhost')
    >>> with pgwrap.cursor() as c:
    ...     c.query('select version()')
    [{'version': 'PostgreSQL...'}]
    >>> pgwrap.query_one('select version()')
    {'version': 'PostgreSQL...'}

    Cursor
    ------

    The module provides a cursor context handler wrapping the psycopg2 cursor.

    Entering the cursor context handler will obtain a connection from the
    connection pool and create a cursor using this connection. When the context
    handler is exited the associated transaction will be committed, cursor
    closed, and the connection released back to the connection pool.

    The cursor object uses the psycopg2 'RealDictCursor' by default (which
    returns rows as a python dictionary) however this can be overridden by
    providing a 'cursor_factory' parameter to the constructor.

    >>> with pgwap.cursor() as c:
    ...     c.query('select version()')
    [{'version': 'PostgreSQL...'}]

    The cursor context provides the following basic methods:
    
        execute         - execute SQL query and return rowcount
        query           - execute SQL query and fetch results
        query_one       - execute SQL query and fetch first result
        query_dict      - execute SQL query and return results as dict
                          keyed on specified key (which should be unique)
        callproc        - Call stored procedure
        commit          - Commit transaction (called implicitly on exiting
                          context handler)
        rollback        - Rollback transaction

    In addition the cursor can use the SQL API methods described below or
    access the underlying psycopg2 cursor (via the self.cursor attribute).

    The cursor methods are also available as standalone functions which
    run inside an implicit cursor object.

    SQL API
    -------

    The cursor class also provides a simple Python API for common SQL
    operations.  The basic methods provides are:

        select          - single table select (with corresponding select_one,
                          select_dict methods)
        join            - two table join (with corresponding join_one,
                          join_dict methods)
        insert          - SQL insert
        update          - SQL update
        delete          - SQL delete

    The methods can be parameterised to customise the associated query 
    (see db module docs for detail): 

        where           - 'where' clause as dict (column operators can be 
                          specified using the colunm__operator format) 
                            
                          where = {'name':'abc','status__in':(1,2,3)}

        columns         - list of columns to be returned - these can 
                          be real columns or expressions. If spefified
                          as a tuple the column is explicitly named
                          using the AS operator

                          columns = ('name',('status > 1','updated'))

        order           - sort order as list (use 'column__desc' to
                          reverse order)

                          order = ('name__desc',)

        limit           - row limit (int)

        offset          - offset (int)

        on              - join columns (as tuple)

        values          - insert data as dict

        returning       - columns to return (string)

    The methods are also available as standalone functions which create an 
    implicit cursor object.

    Basic usage:

        >>> create_table('t1','id serial,name text,count int')
        >>> create_table('t2','id serial,t1_id int,value text')
        >>> enable_logging(sys.stdout)
        >>> insert('t1',{'name':'abc'},returning='id,name')
        INSERT INTO t1 (name) VALUES ('abc') RETURNING id,name
        {'id': 1, 'name': 'abc'}
        >>> insert('t2',{'t1_id':1,'value':'t2'})
        >>> select('t1')
        SELECT * FROM t1
        []
        >>> select_one('t1',where={'name':'abc'},columns=('name','value'))
        >>> join('t1','t2',where={'t1.id__in':(1,2,3)},columns=('t1.id','t2.name'))
        >>> insert('t1',{'name':'abc'},returning='id')
        >>> update('t1',{'name':'xyz'},where={'name':'abc'})
        >>> update('t1',{'count__func':'count + 1'},where={'count__lt':10},returning="id,count")



    Logging
    -------



                          


    Changelog
    ---------

        *   0.1     19-10-2012  Initial import
        *   0.2     20-10-2012  Remove psycopg2 dep in setup.py
        *   0.3     20-10-2012  Remove hstore default for cursor
        *   0.4     21-10-2012  Add logging support 

"""
