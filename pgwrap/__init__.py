
#from .db import connection

version = "0.8.1"
__doc__ = """

    pgwrap - simple PostgreSQL database wrapper
    -------------------------------------------

    The 'pgwrap' module provides a simple wrapper over psycopg2 supporting a
    Python API for common sql functions.

    This is not intended to provide ORM-like functionality, just to make it
    easier to interact with PostgreSQL from python code for simple use-cases
    and allow direct SQL access for more complex operations.
    
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
        * Query results as dict (using psycopg2.extras.DictCursor)
        * Callable prepared statements
        * Logging support
        * Supports Python 2/3

    Basic usage
    -----------

    >>> import pgwrap
    >>> db = pgwrap.connection(url='postgres://localhost')
    >>> with db.cursor() as c:
    ...     c.query('select version()')
    [['PostgreSQL...']]
    >>> v = db.query_one('select version()')
    >>> v
    ['PostgreSQL...']
    >>> v.items()
    [('version', 'PostgreSQL...')]
    >>> v['version']
    'PostgreSQL...'

    Connection
    ----------

    The connection class initialises an internal connection pool and provides
    methods to return a cursor object or execute SQL queries directly (using an
    implicit cursor).

    The intention is that a single instance of this class is created at
    application start up.

    Cursor
    ------

    The module provides a cursor context handler wrapping the psycopg2 cursor.

    Entering the cursor context handler will obtain a connection from the
    connection pool and create a cursor using this connection. When the context
    handler is exited the associated transaction will be committed, cursor
    closed, and the connection released back to the connection pool.

    The cursor object uses the psycopg2 'DictCursor' by default (which
    returns rows as a pseudo python dictionary) however this can be overridden
    by providing a 'cursor_factory' parameter to the constructor.

    >>> db = pgwrap.connection(url='postgres://localhost')
    >>> with db.cursor() as c:
    ...     c.query('select version()')
    [['PostgreSQL...']]

    The cursor context provides the following basic methods:
    
        execute         - execute SQL query and return rowcount
        query           - execute SQL query and fetch results
        query_one       - execute SQL query and fetch first result
        query_dict      - execute SQL query and return results as dict
                          keyed on specified key (which should be unique)
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
    (see db module for detail): 

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

        >>> db.create_table('t1','id serial,name text,count int')
        >>> db.create_table('t2','id serial,t1_id int,value text')
        >>> db.log = sys.stdout
        >>> db.insert('t1',{'name':'abc','count':0},returning='id,name')
        INSERT INTO t1 (name) VALUES ('abc') RETURNING id,name
        [1, 'abc']
        >>> db.insert('t2',{'t1_id':1,'value':'t2'})
        INSERT INTO t2 (t1_id,value) VALUES (1,'t2')
        1
        >>> db.select('t1')
        SELECT * FROM t1
        [[1, 'abc', 0]]
        >>> db.select_one('t1',where={'name':'abc'},columns=('name','count'))
        SELECT name, count FROM t1 WHERE name = 'abc'
        ['abc', 0]
        >>> db.join(('t1','t2'),columns=('t1.id','t2.value'))
        SELECT t1.id, t2.value FROM t1 JOIN t2 ON t1.id = t2.t1_id
        [[1, 't2']]
        >>> db.insert('t1',{'name':'abc'},returning='id')
        INSERT INTO t1 (name) VALUES ('abc') RETURNING id
        [2]
        >>> db.update('t1',{'name':'xyz'},where={'name':'abc'})
        UPDATE t1 SET name = 'xyz' WHERE name = 'abc'
        2
        >>> db.update('t1',{'count__func':'count + 1'},where={'count__lt':10},returning="id,count")
        UPDATE t1 SET count = count + 1 WHERE count < 10 RETURNING id,count
        [[1, 1]]

    Prepared Statements
    -------------------

        Prepared statements can be created using the

            connection.prepare(stmt,params,name,call_type) 

            stmt      : prepared statement (with parameters identified 
                        in the statement using the psql $1,$2... notation)
            params    : list of optional parameter types (usually not 
                        needed - infered by psql)
            name      : name for the prepared statement (usually
                        autogenerated)
            call_type : method used when instance called as method
                        (defaults to 'query')

        The constructor returns a PreparedStatement object which can be used
        instead of an sql statement in the connection.execute and
        connection.query_xxx methods.

        >>> p = db.prepare('UPDATE t1 SET name = $2 WHERE id = $1')
        PREPARE _pstmt_001  AS UPDATE t1 SET name = $2 WHERE id = $1
        >>> with db.cursor() as c:
        ...     c.execute(p,(1,'xxx'))
        EXECUTE _pstmt_001 (1,'xxx')

        The PreparedStatement object can also be called directly using the
        execute/query/query_one/query_dict methods. The instance is also
        directly callable using the method type identified in 'call_type'

        >>> p = db.prepare('UPDATE t1 SET name = $2 WHERE id = $1')
        PREPARE _pstmt_001  AS UPDATE t1 SET name = $2 WHERE id = $1
        >>> p.execute(1,'xxx')
        EXECUTE _pstmt_001 (1,'xxx')
        >>> p(1,'xxx')
        EXECUTE _pstmt_001 (1,'xxx')

    Logging
    -------

        To enable logging the connection.log attribute can be set to either an
        instance of logging.Logger or a file-like object (supporting the write
        method).

        The log message is generated using the self.logf function (called with 
        the cursor object as a parameter). By default this just returns the
        query string however can be customised as needed. A cursor.timestamp
        attribute is available to allow execution time to be tracked.

        >>> db.log = sys.stdout
        >>> db.logf = lambda c : '[%f] %s' % (time.time() - c.timestamp,c.query)
        >>> db.query('SELECT * FROM t1')
        [0.000536] SELECT * FROM t1

    Changelog
    ---------

        *   0.1     19-10-2012  Initial import
        *   0.2     20-10-2012  Remove psycopg2 dep in setup.py
        *   0.3     20-10-2012  Remove hstore default for cursor
        *   0.4     21-10-2012  Add logging support 
        *   0.5     22-12-2012  Refactor connection class / remove globals
        *   0.6     23-12-2012  Add support for prepared statements
        *   0.7     26-12-2012  Add callable prepared statements & named cursor
        *   0.8     02-02-2019  Support Python 3 (finally)

    Author
    ------

        *   Paul Chakravarti (paul.chakravarti@gmail.com)

    Master Repository/Issues
    ------------------------

        *   https://github.com/paulchakravarti/pgwrap

"""

