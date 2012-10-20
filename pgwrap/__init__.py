
from db import (connect,cursor,execute,
                query,query_one,query_dict,
                select,select_one,select_dict,
                join,join_one,join_dict,
                insert,delete,update,
                check_table,drop_table,create_table,
                init_db)

version = "0.1"
description = """

    pgwrap - simple PostgreSQL database wrapper
    -------------------------------------------

    The 'pgwrap' module provides a simple wrapper over psycopg2 supporting a
    Python API for common sql functions.

    This is not intended to 

    >>> from pgwrap import connect,query
    >>> connect(url="postgres://localhost")
    >>> query("select version")
    ...


"""
