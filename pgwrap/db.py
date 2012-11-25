
import logging,os,time,urlparse
import psycopg2
from psycopg2.extras import RealDictCursor,NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool

import sqlop

class connection(object):

    def __init__(self,url=None,hstore=False,log=None,logf=None,min=1,max=5,
                               default_cursor=RealDictCursor):
        params = urlparse.urlparse(url or 
                                   os.environ.get('DATABASE_URL') or 
                                   'postgres://localhost/')
        self.pool = ThreadedConnectionPool(min,max,
                                           database=params.path[1:],
                                           user=params.username,
                                           password=params.password,
                                           host=params.hostname,
                                           port=params.port,
                    )
        self.hstore = hstore
        self.log = log
        self.logf = logf or (lambda cursor : cursor.query)
        self.default_cursor = default_cursor

    def shutdown(self):
        if self.pool:
            self.pool.closeall()
            self.pool = None

    def cursor(self,cursor_factory=None):
        return cursor(self.pool,
                      cursor_factory or self.default_cursor,
                      self.hstore,
                      self.log,
                      self.logf)

    def __del__(self):
        self.shutdown()

    def __getattr__(self,name):
        def _wrapper(*args,**kwargs):
            with self.cursor() as c:
                return getattr(c,name)(*args,**kwargs)
        return _wrapper

class cursor(object):

    def __init__(self,pool,cursor_factory,hstore,log,logf):
        self.connection = None
        self.pool = pool
        if cursor_factory:
            self.cursor_factory = cursor_factory
        else:
            self.cursor_factory = psycopg2.extensions.cursor
        self.hstore = hstore
        self.log = log
        self.logf = logf

    def __enter__(self):
        self.connection = self.pool.getconn()
        if self.hstore:
            psycopg2.extras.register_hstore(self.connection)
        self.cursor = self.connection.cursor(cursor_factory=self.cursor_factory)
        return self

    def __exit__(self,type,value,traceback):
        self.commit()
        self.cursor.close()
        self.pool.putconn(self.connection)

    def _write_log(self,cursor):
        msg = self.logf(cursor)
        if msg:
            if isinstance(self.log,logging.Logger):
                self.log.debug(msg)
            else:
                self.log.write(msg + os.linesep)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def callproc(self,proc,params=None):
        if self.log and self.logf:
            try:
                return self.cursor.callproc(proc,params)
            finally:
                self._write_log(self.cursor)
        else:
            return self.cursor.callproc(proc,params)

    def execute(self,sql,params=None):
        if self.log and self.logf:
            try:
                self.cursor.execute(sql,params)
                return self.cursor.rowcount
            finally:
                self._write_log(self.cursor)
        else:
            self.cursor.execute(sql,params)
            return self.cursor.rowcount


    def query(self,sql,params=None):
        """
            >>> c = connection()
            >>> r = c.query('select name,active FROM doctest_t1 ORDER BY name')
            >>> r[0] == {'name':'aaaaa','active':True}
            True
            >>> len(r)
            10
        """
        self.execute(sql,params)
        return self.cursor.fetchall()

    def query_one(self,sql,params=None):
        """
            >>> c = connection()
            >>> r = c.query_one('select name,active FROM doctest_t1 WHERE name = %s',('aaaaa',))
            >>> r == {'name':'aaaaa','active':True}
            True
        """
        self.execute(sql,params)
        return self.cursor.fetchone()

    def query_dict(self,sql,key,params=None):
        """
            >>> c = connection()
            >>> r = c.query_dict('select name,active FROM doctest_t1 ORDER BY name','name')
            >>> r['aaaaa'] == {'name':'aaaaa','active':True}
            True
            >>> sorted(r.keys())
            ['aaaaa', 'bbbbb', 'ccccc', 'ddddd', 'eeeee', 'fffff', 'ggggg', 'hhhhh', 'iiiii', 'jjjjj']
        """
        _d = {}
        for row in self.query(sql,params):
            _d[row[key]] = row
        return _d

    def _build_select(self,table,where,order,columns,limit,offset,update):
        return 'SELECT %s FROM %s' % (sqlop.columns(columns),table) \
                + sqlop.where(where) + sqlop.order(order) + sqlop.limit(limit) \
                + sqlop.offset(offset) + sqlop.for_update(update)

    def select(self,table,where=None,order=None,columns=None,limit=None,offset=None,update=False):
        """
            >>> c = connection()
            >>> c.select('doctest_t1') == c.query('SELECT * FROM doctest_t1')
            True
            >>> c.select('doctest_t1',columns=('name',),order=('name',),limit=2)
            [{'name': 'aaaaa'}, {'name': 'bbbbb'}]
            >>> c.select('doctest_t1',where={'name__in':('aaaaa','bbbbb')},order=('name__desc',)) == \
                    c.query("SELECT * FROM doctest_t1 WHERE name IN ('aaaaa','bbbbb') ORDER BY name DESC")
            True
            >>> c.select_one('doctest_t1',columns=('name',),where={'name__in':('bbbbb',)})
            {'name': 'bbbbb'}
        """
        return self.query(self._build_select(table,where,order,columns,limit,offset,update),where)

    def select_one(self,table,where=None,order=None,columns=None,limit=None,offset=None,update=False):
        """
            >>> c = connection()
            >>> c.select_one('doctest_t1',order=('name',),columns=('name',))
            {'name': 'aaaaa'}
            >>> c.select_one('doctest_t1',order=('name',),columns=(('name','_name'),))
            {'_name': 'aaaaa'}
        """
        return self.query_one(self._build_select(table,where,order,columns,limit,offset,update),where)

    def select_dict(self,table,key,where=None,order=None,columns=None,limit=None,offset=None,update=False):
        """
            >>> c = connection()
            >>> c.select_dict('doctest_t1','name',columns=('name',),order=('name',),limit=2)
            {'aaaaa': {'name': 'aaaaa'}, 'bbbbb': {'name': 'bbbbb'}}
        """
        return self.query_dict(self._build_select(table,where,order,columns,limit,offset,update),key,where)

    def _build_join(self,tables,where,on,order,columns,limit,offset):
        on = on or [ None ] * len(tables)
        return 'SELECT %s FROM %s ' % (sqlop.columns(columns),tables[0]) + \
                                       " ".join([ 'JOIN %s ON %s' % (tables[i],sqlop.on((tables[0],tables[i]),on[i-1])) 
                                                        for i in range(1,len(tables)) ]) + \
                                        sqlop.where(where) + sqlop.order(order) + sqlop.limit(limit) + sqlop.offset(offset)

    def join(self,tables,where=None,on=None,order=None,columns=None,limit=None,offset=None):
        """
            >>> c = connection()
            >>> c.join(('doctest_t1','doctest_t2'),columns=('name','value'),
            ...             where={'doctest_t1.name__in':('aaaaa','bbbbb','ccccc')},
            ...             order=('name',),limit=2)
            [{'name': 'aaaaa', 'value': 'aa'}, {'name': 'bbbbb', 'value': 'bb'}]
            >>> c.join(('doctest_t1','doctest_t2'),on=[('doctest_t1.id','doctest_t2.doctest_t1_id')]) \
                            == c.join(('doctest_t1','doctest_t2'))
            True
        """
        return self.query(self._build_join(tables,where,on,order,columns,limit,offset),where)

    def join_one(self,tables,where=None,on=None,order=None,columns=None,limit=None,offset=None):
        """
            >>> c = connection()
            >>> c.join_one(('doctest_t1','doctest_t2'),columns=('name','value'),where={'name':'aaaaa'})
            {'name': 'aaaaa', 'value': 'aa'}
        """
        return self.query_one(self._build_join(tables,where,on,order,columns,limit,offset),where)

    def join_dict(self,tables,key,where=None,on=None,order=None,columns=None,limit=None,offset=None):
        """
            >>> c = connection()
            >>> c.join_dict(('doctest_t1','doctest_t2'),'name',columns=('name','value'),
            ...             where={'doctest_t1.name__in':('aaaaa','bbbbb','ccccc')},
            ...             order=('name',),limit=2)
            {'aaaaa': {'name': 'aaaaa', 'value': 'aa'}, 'bbbbb': {'name': 'bbbbb', 'value': 'bb'}}
        """
        return self.query_dict(self._build_join(tables,where,on,order,columns,limit,offset),key,where)

    def insert(self,table,values,returning=None):
        """
            >>> c = connection()
            >>> c.insert('doctest_t1',{'name':'xxx'})
            1
            >>> c.insert('doctest_t1',{'name':'yyy'},'name')
            {'name': 'yyy'}
            >>> c.insert('doctest_t1',values={'name':'zzz'},returning='name')
            {'name': 'zzz'}
            >>> c.select('doctest_t1',where={'name__~':'[xyz]+'},order=('name',),columns=('name',))
            [{'name': 'xxx'}, {'name': 'yyy'}, {'name': 'zzz'}]
            >>> c.delete('doctest_t1',where={'name__in':('xxx','yyy','zzz')})
            3
        """
        _values = [ '%%(%s)s' % v for v in values.keys() ]
        sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table,','.join(values.keys()),','.join(_values))
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query_one(sql,values)
        else:
            return self.execute(sql,values)

    def delete(self,table,where=None,returning=None):
        """
            >>> c = connection()
            >>> c.insert('doctest_t1',{'name':'xxx'})
            1
            >>> c.insert('doctest_t1',{'name':'xxx'})
            1
            >>> c.delete('doctest_t1',where={'name':'xxx'},returning='name')
            [{'name': 'xxx'}, {'name': 'xxx'}]
        """
        sql = 'DELETE FROM %s' % table + sqlop.where(where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql,where)
        else:
            return self.execute(sql,where)

    def update(self,table,values,where=None,returning=None):
        """
            >>> c = connection()
            >>> c.insert('doctest_t1',{'name':'xxx'})
            1
            >>> c.update('doctest_t1',{'name':'yyy','active':False},{'name':'xxx'})
            1
            >>> c.update('doctest_t1',values={'count__add':1},where={'name':'yyy'},returning='count')
            [{'count': 1}]
            >>> c.update('doctest_t1',values={'count__add':1},where={'name':'yyy'},returning='count')
            [{'count': 2}]
            >>> c.update('doctest_t1',values={'count__func':'floor(pi()*count)'},where={'name':'yyy'},returning='count')
            [{'count': 6}]
            >>> c.update('doctest_t1',values={'count__sub':6},where={'name':'yyy'},returning='count')
            [{'count': 0}]
            >>> c.delete('doctest_t1',{'name':'yyy'})
            1
        """
        sql = 'UPDATE %s SET %s' % (table,sqlop.update(values))
        sql = self.cursor.mogrify(sql,values)
        if where:
            sql += self.cursor.mogrify(sqlop.where(where),where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql)
        else:
            return self.execute(sql)

    def check_table(self,t):
        """
            >>> c = connection()
            >>> c.check_table('doctest_t1')
            True
            >>> c.check_table('nonexistent')
            False
        """
        _sql = 'SELECT tablename FROM pg_tables WHERE schemaname=%s and tablename=%s'
        return self.query_one(_sql,('public',t)) is not None

    def drop_table(self,t):
        """
            >>> c = connection()
            >>> c.create_table('doctest_t3','''id SERIAL PRIMARY KEY, name TEXT''')
            >>> c.check_table('doctest_t3')
            True
            >>> c.drop_table('doctest_t3');
            >>> c.check_table('doctest_t3')
            False
        """
        self.execute('DROP TABLE IF EXISTS %s CASCADE' % t)

    def create_table(self,name,schema):
        """
            >>> c = connection()
            >>> c.create_table('doctest_t3','''id SERIAL PRIMARY KEY, name TEXT''')
            >>> c.check_table('doctest_t3')
            True
            >>> c.drop_table('doctest_t3');
            >>> c.check_table('doctest_t3')
            False
        """
        if not self.check_table(name):
            self.execute('CREATE TABLE %s (%s)' % (name,schema))

if __name__ == '__main__':
    import code,doctest,sys
    tables = (('doctest_t1','''id SERIAL PRIMARY KEY,
                               name TEXT NOT NULL,
                               count INTEGER NOT NULL DEFAULT 0,
                               active BOOLEAN NOT NULL DEFAULT true'''),
              ('doctest_t2','''id SERIAL PRIMARY KEY,
                               value TEXT NOT NULL,
                               doctest_t1_id INTEGER NOT NULL REFERENCES doctest_t1(id)'''),
             )
    # Connect to database and create test tables
    c = connection()
    c.drop_table('doctest_t1')
    c.drop_table('doctest_t2')
    for (name,schema) in tables:
        c.create_table(name,schema)
    for i in range(10):
        id = c.insert('doctest_t1',{'name':chr(97+i)*5},returning='id')['id']
        _ = c.insert('doctest_t2',{'value':chr(97+i)*2,'doctest_t1_id':id})
    if sys.argv.count('--interact'):
        c.log = sys.stdout
        code.interact(local=locals())
    else:
        # Run tests
        doctest.testmod(optionflags=doctest.ELLIPSIS)
    # Drop tables
    c.drop_table('doctest_t1')
    c.drop_table('doctest_t2')

