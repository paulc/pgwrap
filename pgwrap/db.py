
import os,urlparse
import psycopg2,psycopg2.extras,psycopg2.pool

_pool = None

_operators = { 'eq'     : '=',
               'lt'     : '<', 
               'gt'     : '>', 
               'ne'     : '!=',
               're'     : '~',
               'like'   : 'LIKE',
               'not_like': 'NOT LIKE',
              }

_update_operators = { ''        : "%(field)s = %%(%(key)s)s",
                      'add'     : "%(field)s = %(field)s + %%(%(key)s)s",
                      'sub'     : "%(field)s = %(field)s - %%(%(key)s)s",
                      'append'  : "%(field)s = %(field)s || %%(%(key)s)s",
                      'func'    : "%(field)s = %(val)s",
                    }


def connect(url=None,min=1,max=5):
    """
        Initialise connection pool
    """
    global _pool
    if not _pool:
        params = urlparse.urlparse(url or 
                                   os.environ.get('DATABASE_URL') or 
                                   'postgres://localhost/')
        _pool = psycopg2.pool.ThreadedConnectionPool(min,max,
                                                     database=params.path[1:],
                                                     user=params.username,
                                                     password=params.password,
                                                     host=params.hostname,
                                                     port=params.port)
    
def _where(where):
    """
        Construct where clause from dict in format:

        eg. { 'key1'     : 'value1',
              'key2__op' : 'value1' }

            'key1 = %s AND key2 op %s' % (value1,value2) 

        'op' is looked up in '_operators' hash table which
        maps common opeartors (eg '__lt' to '<'). If the 
        operator is not found it is passed through directly
        allowing other operators to be specified directly.
    """
    if where: 
        _where = []
        for f in where.keys():
            field,_,op = f.partition('__')
            _where.append('%s %s %%(%s)s' % (field,_operators.get(op,op) or '=',f))
        return ' WHERE ' + ' AND '.join(_where)
    else:
        return ''

def _order(order):
    if order:
        _order = []
        for f in order:
            field,_,direction = f.partition('__')
            _order.append(field + (' DESC' if direction == 'desc' else ''))
        return ' ORDER BY ' + ', '.join(_order)
    else:
        return ''

def _columns(columns):
    if columns:
        return ",".join([(c if isinstance(c,(str,unicode)) else "%s AS %s" % c) for c in columns])
    else:
        return '*'

def _on((t1,t2),on):
    if on:
        return "%s = %s" % on
    else:
        return "%s.id = %s.%s_id" % (t1,t2,t1)

def _limit(limit):
    if limit:
        return ' LIMIT %d' % limit
    else:
        return ''

class cursor(object):

    def __init__(self,hstore=False,cursor_factory=psycopg2.extras.RealDictCursor):
        self.hstore = hstore
        self.cursor_factory = cursor_factory
        if not _pool:
            raise ValueError("No database pool")

    def __enter__(self):
        self.connection = _pool.getconn()
        if self.hstore:
            psycopg2.extras.register_hstore(self.connection)
        self.cursor = self.connection.cursor(cursor_factory=self.cursor_factory)
        return self

    def __exit__(self,type,value,traceback):
        self.commit()
        self.cursor.close()
        _pool.putconn(self.connection)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def execute(self,sql,params=None):
        self.cursor.execute(sql,params)
        return self.cursor.rowcount

    def query(self,sql,params=None):
        self.cursor.execute(sql,params)
        return self.cursor.fetchall()

    def query_one(self,sql,params=None):
        self.cursor.execute(sql,params)
        return self.cursor.fetchone()

    def query_dict(self,sql,key,params=None):
        _d = {}
        for row in self.query(sql,params):
            _d[row[key]] = row
        return _d

    def select(self,table,where=None,order=None,columns=None,limit=None):
        sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
        return self.query(sql,where)

    def select_one(self,table,where=None,order=None,columns=None,limit=None):
        sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
        return self.query_one(sql,where)

    def select_dict(self,table,key,where=None,order=None,columns=None,limit=None):
        sql = 'SELECT %s FROM %s' % (_columns(columns),table) + _where(where) + _order(order) + _limit(limit)
        return self.query_dict(sql,key,where)

    def join(self,t1,t2,where=None,on=None,order=None,columns=None,limit=None):
        sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) \
                                + _where(where) + _order(order) + _limit(limit)
        return self.query(sql,where)

    def join_one(self,t1,t2,where=None,on=None,order=None,columns=None,limit=None):
        sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) \
                                + _where(where) + _order(order) + _limit(limit)
        return self.query_one(sql,where)

    def join_dict(self,t1,t2,key,where=None,on=None,order=None,columns=None,limit=None):
        sql = 'select %s from %s join %s on (%s)' % (_columns(columns),t1,t2,_on((t1,t2),on)) \
                                + _where(where) + _order(order) + _limit(limit)
        return self.query_dict(sql,key,where)

    def insert(self,table,values,returning=None):
        _values = [ '%%(%s)s' % v for v in values.keys() ]
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table,','.join(values.keys()),','.join(_values))
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query_one(sql,values)
        else:
            return self.execute(sql,values)

    def delete(self,table,where=None,returning=None):
        sql = 'DELETE FROM %s' % table + _where(where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql,where)
        else:
            return self.execute(sql,where)

    def update(self,table,values,where=None,returning=None):
        _update = []
        for k,v in values.items():
            f,_,op = k.partition('__')
            _update.append(_update_operators[op] % {'key':k,'val':v,'field':f,'op':op})
        sql = 'UPDATE %s SET %s' % (table,','.join(_update))
        sql = self.cursor.mogrify(sql,values)
        if where:
            sql += self.cursor.mogrify(_where(where),where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql)
        else:
            return self.execute(sql)

def execute(sql,params=None):
    """
        >>> execute("INSERT INTO doctest_t1 (name) VALUES ('xxx')")
        1
        >>> execute("DELETE FROM doctest_t1 WHERE name = 'xxx'")
        1
    """
    with cursor() as c:
        return c.execute(sql,params)

def query(sql,params=None):
    """
        >>> r = query('select name,active FROM doctest_t1 ORDER BY name')
        >>> r[0] == {'name':'aaaaa','active':True}
        True
        >>> len(r)
        10
    """
    with cursor() as c:
        return c.query(sql,params)

def query_one(sql,params=None):
    """
        >>> r = query_one('select name,active FROM doctest_t1 WHERE name = %s',('aaaaa',))
        >>> r == {'name':'aaaaa','active':True}
        True
    """
    with cursor() as c:
        return c.query_one(sql,params)

def query_dict(sql,key,params=None):
    """
        >>> r = query_dict('select name,active FROM doctest_t1 ORDER BY name','name')
        >>> r['aaaaa'] == {'name':'aaaaa','active':True}
        True
        >>> sorted(r.keys())
        ['aaaaa', 'bbbbb', 'ccccc', 'ddddd', 'eeeee', 'fffff', 'ggggg', 'hhhhh', 'iiiii', 'jjjjj']
    """
    with cursor() as c:
        return c.query_dict(sql,key,params)

def select(table,where=None,order=None,columns=None,limit=None):
    """
        >>> select('doctest_t1') == query('SELECT * FROM doctest_t1')
        True
        >>> select('doctest_t1',columns=('name',),order=('name',),limit=2)
        [{'name': 'aaaaa'}, {'name': 'bbbbb'}]
        >>> select('doctest_t1',where={'name__in':('aaaaa','bbbbb')},order=('name__desc',)) == \
                query("SELECT * FROM doctest_t1 WHERE name IN ('aaaaa','bbbbb') ORDER BY name DESC")
        True
        >>> select_one('doctest_t1',columns=('name',),where={'name__in':('bbbbb',)})
        {'name': 'bbbbb'}
    """
    with cursor() as c:
        return c.select(table,where,order,columns,limit)

def select_one(table,where=None,order=None,columns=None,limit=None):
    """
        >>> select_one('doctest_t1',order=('name',),columns=('name',))
        {'name': 'aaaaa'}
        >>> select_one('doctest_t1',order=('name',),columns=(('name','_name'),))
        {'_name': 'aaaaa'}
    """
    with cursor() as c:
        return c.select_one(table,where,order,columns,limit)

def select_dict(table,key,where=None,order=None,columns=None,limit=None):
    """
        >>> select_dict('doctest_t1','name',columns=('name',),order=('name',),limit=2)
        {'aaaaa': {'name': 'aaaaa'}, 'bbbbb': {'name': 'bbbbb'}}
    """
    with cursor() as c:
        return c.select_dict(table,key,where,order,columns,limit)

def join(t1,t2,where=None,on=None,order=None,columns=None,limit=None):
    """
        >>> join('doctest_t1','doctest_t2',columns=('name','value'),
        ...             where={'doctest_t1.name__in':('aaaaa','bbbbb','ccccc')},
        ...             order=('name',),limit=2)
        [{'name': 'aaaaa', 'value': 'aa'}, {'name': 'bbbbb', 'value': 'bb'}]
    """
    with cursor() as c:
        return c.join(t1,t2,where,on,order,columns,limit)

def join_one(t1,t2,where=None,on=None,order=None,columns=None,limit=None):
    """
        >>> join('doctest_t1','doctest_t2',columns=('name','value'),where={'name':'aaaaa'})
        [{'name': 'aaaaa', 'value': 'aa'}]
    """
    with cursor() as c:
        return c.join_one(t1,t2,where,on,order,columns,limit)

def join_dict(t1,t2,key,where=None,on=None,order=None,columns=None,limit=None):
    """
        >>> join_dict('doctest_t1','doctest_t2','name',columns=('name','value'),
        ...             where={'doctest_t1.name__in':('aaaaa','bbbbb','ccccc')},
        ...             order=('name',),limit=2)
        {'aaaaa': {'name': 'aaaaa', 'value': 'aa'}, 'bbbbb': {'name': 'bbbbb', 'value': 'bb'}}
    """
    with cursor() as c:
        return c.join_dict(t1,t2,key,where,on,order,columns,limit)

def insert(table,values,returning=None):
    """
        >>> insert('doctest_t1',{'name':'xxx'})
        1
        >>> insert('doctest_t1',{'name':'yyy'},'name')
        {'name': 'yyy'}
        >>> insert('doctest_t1',values={'name':'zzz'},returning='name')
        {'name': 'zzz'}
        >>> select('doctest_t1',where={'name__~':'[xyz]+'},order=('name',),columns=('name',))
        [{'name': 'xxx'}, {'name': 'yyy'}, {'name': 'zzz'}]
        >>> delete('doctest_t1',where={'name__in':('xxx','yyy','zzz')})
        3
    """
    with cursor() as c:
        return c.insert(table,values,returning)

def delete(table,where=None,returning=None):
    """
        >>> insert('doctest_t1',{'name':'xxx'})
        1
        >>> insert('doctest_t1',{'name':'xxx'})
        1
        >>> delete('doctest_t1',where={'name':'xxx'},returning='name')
        [{'name': 'xxx'}, {'name': 'xxx'}]
    """
    with cursor() as c:
        return c.delete(table,where,returning)

def update(table,values,where=None,returning=None):
    """
        >>> insert('doctest_t1',{'name':'xxx'})
        1
        >>> update('doctest_t1',{'name':'yyy','active':False},{'name':'xxx'})
        1
        >>> update('doctest_t1',values={'count__add':1},where={'name':'yyy'},returning='count')
        [{'count': 1}]
        >>> update('doctest_t1',values={'count__add':1},where={'name':'yyy'},returning='count')
        [{'count': 2}]
        >>> update('doctest_t1',values={'count__func':'floor(pi()*count)'},where={'name':'yyy'},returning='count')
        [{'count': 6}]
        >>> update('doctest_t1',values={'count__sub':6},where={'name':'yyy'},returning='count')
        [{'count': 0}]
        >>> delete('doctest_t1',{'name':'yyy'})
        1
    """
    with cursor() as c:
        return c.update(table,values,where,returning)

def check_table(t):
    """
        >>> check_table('doctest_t1')
        True
        >>> check_table('nonexistent')
        False
    """
    with cursor() as c:
        _sql = 'SELECT tablename FROM pg_tables WHERE schemaname=%s and tablename=%s'
        return c.query_one(_sql,('public',t)) is not None

def drop_table(t):
    """
        >>> create_table('doctest_t3','''id SERIAL PRIMARY KEY, name TEXT''')
        >>> check_table('doctest_t3')
        True
        >>> drop_table('doctest_t3');
        >>> check_table('doctest_t3')
        False
    """
    with cursor() as c:
        c.execute('DROP TABLE IF EXISTS %s CASCADE' % t)

def create_table(name,schema):
    """
        >>> create_table('doctest_t3','''id SERIAL PRIMARY KEY, name TEXT''')
        >>> check_table('doctest_t3')
        True
        >>> drop_table('doctest_t3');
        >>> check_table('doctest_t3')
        False
    """
    if not check_table(name):
        with cursor() as c:
            c.execute('CREATE TABLE %s (%s)' % (name,schema))

def init_db(tables):
    for (name,schema) in tables:
        create_table(name,schema)

if __name__ == '__main__':
    import doctest,sys
    tables = (('doctest_t1','''id SERIAL PRIMARY KEY,
                               name TEXT NOT NULL,
                               count INTEGER NOT NULL DEFAULT 0,
                               active BOOLEAN NOT NULL DEFAULT true'''),
              ('doctest_t2','''id SERIAL PRIMARY KEY,
                               value TEXT NOT NULL,
                               doctest_t1_id INTEGER NOT NULL REFERENCES doctest_t1(id)'''),
             )
    # Connect to database and create test tables
    connect('postgres://localhost/')
    drop_table('doctest_t1')
    drop_table('doctest_t2')
    init_db(tables)
    for i in range(10):
        id = insert('doctest_t1',{'name':chr(97+i)*5},returning='id')['id']
        _ = insert('doctest_t2',{'value':chr(97+i)*2,'doctest_t1_id':id})
    # Run tests
    doctest.testmod(optionflags=doctest.ELLIPSIS)
    # Drop tables
    drop_table('doctest_t1')
    drop_table('doctest_t2')

