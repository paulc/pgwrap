
from db import (connect,cursor,execute,
                query,query_one,query_dict,
                select,select_one,select_dict,
                join,join_one,join_dict,
                insert,delete,update,
                check_table,drop_table,create_table,
                init_db)

version = "0.3"
description = """

    pgwrap - simple PostgreSQL database wrapper
    -------------------------------------------

    The 'pgwrap' module provides a simple wrapper over psycopg2 supporting a
    Python API for common sql functions.

    This is not intended to provide an ORM-like functionality, just some basic
    functionality to make it easier to interact with PostgreSQL from python
    code for simple use-cases. For more complex operations direct SQL
    access is available.
    
    These 'module' include:

        * Simplified handling of connections/cursor
            * Background connection pool (provided by psycopg2.pool)
            * Cursor context handler 
        * Python API to wrap basic SQL functionality 
            * select,update,delete,join)
            * These methods are implemented as extensions to the cursor
              context handler (allowing then to be used transactionally 
              where needed), however are also available as stand-alone 
              methods which create an implicit cursor for simple queries
        * Query results as dict (using psycopg2.extras.RealDictCursor)

    Basic usage:

    >>> from pgwrap import connect,query
    >>> connect(url="postgres://localhost")
    >>> query_one("select version()")
    {'version': 'PostgreSQL...'}

    The module wraps the excellent 'psycopg2' library and most of the 
    functionality is provided by this behind the scenes.

    Changelog:

        0.1     19-10-2012  Initial import
        0.2     10-10-2012  Remove psycopg2 dep in setup.py
        0.3     10-10-2012  Remove hstore default for cursor

"""
