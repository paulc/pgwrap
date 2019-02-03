
import logging,os,time
try:
    from urllib.parse import urlparse, parse_qs
except ImportError:
    from urlparse import urlparse, parse_qs
from collections import namedtuple
import psycopg2
from psycopg2.extras import DictCursor,DictRow,NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool

import pgwrap.sqlop as sqlop

class SafeNamedTupleCursor(NamedTupleCursor):
    def _make_nt(self,namedtuple=namedtuple):
        return namedtuple("Record", [d[0] for d in self.description or ()],rename=True)

class AttrDictRow(DictRow):
    def __init__(self, *args, **kwargs):
        super(AttrDictRow, self).__init__(*args, **kwargs)
    def __getattr__(self,attr):
        return self.__getitem__(attr)

class AttrDictCursor(DictCursor):
    def __init__(self, *args, **kwargs):
        kwargs['row_factory'] = AttrDictRow
        super(DictCursor, self).__init__(*args, **kwargs)
        self._prefetch = 1

class connection(object):

    def __init__(self,url=None,hstore=False,log=None,logf=None,min=1,max=5,
                               default_cursor=DictCursor):
        params = urlparse(url or 
                          os.environ.get('DATABASE_URL') or 
                          'postgres://localhost/')
        if params.scheme != 'postgres':
            raise ValueError("Invalid connection string (postgres://user@pass:host/db?param=value)")

        self.pool = ThreadedConnectionPool(min,max,
                                           database=params.path[1:] or parse_qs(params.query).get('dbname'),
                                           user=params.username or parse_qs(params.query).get('user'),
                                           password=params.password or parse_qs(params.query).get('password'),
                                           host=params.hostname or parse_qs(params.query).get('host'),
                                           port=params.port or parse_qs(params.query).get('port'),
                    )
        self.hstore = hstore
        self.log = log
        self.logf = logf or (lambda cursor : cursor.query.decode())
        self.default_cursor = default_cursor
        self.prepared_statement_id = 0

    def prepare(self,statement,params=None,name=None,call_type=None):
        """
            >>> db = connection()
            >>> p1 = db.prepare('SELECT name FROM doctest_t1 WHERE id = $1')
            >>> p2 = db.prepare('UPDATE doctest_t1 set name = $2 WHERE id = $1',('int','text'))
            >>> db.execute(p2,(1,'xxxxx'))
            1
            >>> db.query_one(p1,(1,))
            ['xxxxx']
            >>> db.execute(p2,(1,'aaaaa'))
            1
            >>> db.query_one(p1,(1,))
            ['aaaaa']
        """
        if not name:
            self.prepared_statement_id += 1
            name = '_pstmt_%03.3d' % self.prepared_statement_id
        if params:
            params = '(' + ','.join(params) + ')'
        else:
            params = ''
        with self.cursor() as c:
            c.execute('PREPARE %s %s AS %s' % (name,params,statement))
        if call_type is None:
            if statement.lower().startswith('select'):
                call_type = 'query'
            else:
                call_type = 'execute'
        return PreparedStatement(self,name,call_type)

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

    def _write_log(self,cursor):
        """
            >>> db = connection()
            >>> db.log = sys.stdout
            >>> _ = db.select('doctest_t1',columns=('name','count'),where={'active':True,'name__gt':'c'})
            SELECT name, count FROM doctest_t1 WHERE active = true AND name > 'c'
            >>> db.logf = lambda c : "--- %s ---" % c.query.decode()
            >>> _ = db.select('doctest_t1')
            --- SELECT * FROM doctest_t1 ---
        """
        msg = self.logf(cursor)
        if msg:
            if isinstance(self.log,logging.Logger):
                self.log.debug(msg)
            else:
                self.log.write(msg + os.linesep)

    def __enter__(self,name=None):
        """
            >>> db = connection()
            >>> with db.cursor() as c:
            ...     _ = c.insert('doctest_t1',values={'name':'xxx'})
            ...     _ = c.insert('doctest_t1',values={'name':'yyy'})
            ...     _ = c.insert('doctest_t1',values={'name':'zzz'})
            >>> db.select('doctest_t1',where={'name__~':'[xyz]+'},order=('name',),columns=('name',))
            [['xxx'], ['yyy'], ['zzz']]
            >>> db.delete('doctest_t1',where={'name__in':('xxx','yyy','zzz')},returning='name')
            [['xxx'], ['yyy'], ['zzz']]
            >>> with db.cursor() as c:
            ...     _ = c.insert('doctest_t1',values={'name':'xxx'})
            ...     c.commit()
            ...     _ = c.insert('doctest_t1',values={'name':'yyy'})
            ...     _ = c.insert('doctest_t1',values={'name':'zzz'})
            ...     c.rollback()
            >>> db.delete('doctest_t1',where={'name__in':('xxx','yyy','zzz')},returning='name')
            [['xxx']]
        """
        self.connection = self.pool.getconn()
        self.cursor = self.connection.cursor(name=name,cursor_factory=self.cursor_factory)
        if self.hstore:
            psycopg2.extras.register_hstore(self.cursor)
        return self

    def __exit__(self,type,value,traceback):
        self.commit()
        self.cursor.close()
        self.pool.putconn(self.connection)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def execute(self,sql,params=None):
        """
            >>> db = connection()
            >>> db.execute('select name,active FROM doctest_t1')
            10
        """
        if isinstance(sql,PreparedStatement):
            if params:
                sql = 'EXECUTE %s (%s)' % (sql.name,','.join(['%s']*len(params)))
            else:
                sql = 'EXECUTE %s' % sql.name
        if self.log and self.logf:
            try:
                self.cursor.timestamp = time.time()
                self.cursor.execute(sql,params)
                return self.cursor.rowcount
            finally:
                self._write_log(self.cursor)
        else:
            self.cursor.execute(sql,params)
            return self.cursor.rowcount

    def query(self,sql,params=None):
        """
            >>> db = connection()
            >>> r = db.query('select name,active FROM doctest_t1 ORDER BY name')
            >>> r[0]
            ['aaaaa', True]
            >>> len(r)
            10
        """
        self.execute(sql,params)
        return self.cursor.fetchall()

    def query_one(self,sql,params=None):
        """
            >>> db = connection()
            >>> db.query_one('select name,active FROM doctest_t1 WHERE name = %s',('aaaaa',))
            ['aaaaa', True]
        """
        self.execute(sql,params)
        return self.cursor.fetchone()

    def query_dict(self,sql,key,params=None):
        """
            >>> db = connection()
            >>> r = db.query_dict('select name,active FROM doctest_t1 ORDER BY name','name')
            >>> r['aaaaa']
            ['aaaaa', True]
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
            >>> db = connection()
            >>> db.select('doctest_t1') == db.query('SELECT * FROM doctest_t1')
            True
            >>> db.select('doctest_t1',columns=('name',),order=('name',),limit=2)
            [['aaaaa'], ['bbbbb']]
            >>> db.select('doctest_t1',where={'name__in':('aaaaa','bbbbb')},order=('name__desc',)) == \
                    db.query("SELECT * FROM doctest_t1 WHERE name IN ('aaaaa','bbbbb') ORDER BY name DESC")
            True
            >>> db.select_one('doctest_t1',columns=('name',),where={'name__in':('bbbbb',)})
            ['bbbbb']
        """
        return self.query(self._build_select(table,where,order,columns,limit,offset,update),where)

    def select_one(self,table,where=None,order=None,columns=None,limit=None,offset=None,update=False):
        """
            >>> db = connection()
            >>> db.select_one('doctest_t1',order=('name',),columns=('name',))
            ['aaaaa']
            >>> db.select_one('doctest_t1',order=('name',),columns=(('name','abcd'),))
            ['aaaaa']
        """
        return self.query_one(self._build_select(table,where,order,columns,limit,offset,update),where)

    def select_dict(self,table,key,where=None,order=None,columns=None,limit=None,offset=None,update=False):
        """
            >>> db = connection()
            >>> db.select_dict('doctest_t1','name',columns=('name',),order=('name',),limit=2)
            {'aaaaa': ['aaaaa'], 'bbbbb': ['bbbbb']}
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
            >>> db = connection()
            >>> db.join(('doctest_t1','doctest_t2'),columns=('name','value'),
            ...             where={'doctest_t1.name__in':('aaaaa','bbbbb','ccccc')},
            ...             order=('name',),limit=2)
            [['aaaaa', 'aa'], ['bbbbb', 'bb']]
            >>> db.join(('doctest_t1','doctest_t2'),on=[('doctest_t1.id','doctest_t2.doctest_t1_id')]) \
                            == db.join(('doctest_t1','doctest_t2'))
            True
        """
        return self.query(self._build_join(tables,where,on,order,columns,limit,offset),where)

    def join_one(self,tables,where=None,on=None,order=None,columns=None,limit=None,offset=None):
        """
            >>> db = connection()
            >>> db.join_one(('doctest_t1','doctest_t2'),columns=('name','value'),where={'name':'aaaaa'})
            ['aaaaa', 'aa']
        """
        return self.query_one(self._build_join(tables,where,on,order,columns,limit,offset),where)

    def join_dict(self,tables,key,where=None,on=None,order=None,columns=None,limit=None,offset=None):
        """
            >>> db = connection()
            >>> db.join_dict(('doctest_t1','doctest_t2'),'name',columns=('name','value'),
            ...               where={'doctest_t1.name__in':('aaaaa','bbbbb','ccccc')},
            ...               order=('name',),limit=2)
            {'aaaaa': ['aaaaa', 'aa'], 'bbbbb': ['bbbbb', 'bb']}
        """
        return self.query_dict(self._build_join(tables,where,on,order,columns,limit,offset),key,where)

    def insert(self,table,values,returning=None):
        """
            >>> db = connection()
            >>> db.insert('doctest_t1',{'name':'xxx'})
            1
            >>> db.insert('doctest_t1',values={'name':'yyy'},returning='name')
            ['yyy']
            >>> db.insert('doctest_t1',values={'name':'zzz'},returning='name')
            ['zzz']
            >>> db.select('doctest_t1',where={'name__~':'[xyz]+'},order=('name',),columns=('name',))
            [['xxx'], ['yyy'], ['zzz']]
            >>> db.delete('doctest_t1',where={'name__in':('xxx','yyy','zzz')})
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
            >>> db = connection()
            >>> db.insert('doctest_t1',{'name':'xxx'})
            1
            >>> db.insert('doctest_t1',{'name':'xxx'})
            1
            >>> db.delete('doctest_t1',where={'name':'xxx'},returning='name')
            [['xxx'], ['xxx']]
        """
        sql = 'DELETE FROM %s' % table + sqlop.where(where)
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql,where)
        else:
            return self.execute(sql,where)

    def update(self,table,values,where=None,returning=None):
        """
            >>> db = connection()
            >>> db.insert('doctest_t1',{'name':'xxx'})
            1
            >>> db.update('doctest_t1',{'name':'yyy','active':False},{'name':'xxx'})
            1
            >>> db.update('doctest_t1',values={'count__add':1},where={'name':'yyy'},returning='count')
            [[1]]
            >>> db.update('doctest_t1',values={'count__add':1},where={'name':'yyy'},returning='count')
            [[2]]
            >>> db.update('doctest_t1',values={'count__func':'floor(pi()*count)'},where={'name':'yyy'},returning='count')
            [[6]]
            >>> db.update('doctest_t1',values={'count__sub':6},where={'name':'yyy'},returning='count')
            [[0]]
            >>> db.delete('doctest_t1',{'name':'yyy'})
            1
        """
        sql = 'UPDATE %s SET %s' % (table,sqlop.update(values))
        sql = self.cursor.mogrify(sql,values).decode()
        if where:
            sql += self.cursor.mogrify(sqlop.where(where),where).decode()
        if returning:
            sql += ' RETURNING %s' % returning
            return self.query(sql)
        else:
            return self.execute(sql)

    def check_table(self,t):
        """
            >>> db = connection()
            >>> db.check_table('doctest_t1')
            True
            >>> db.check_table('nonexistent')
            False
        """
        _sql = 'SELECT tablename FROM pg_tables WHERE schemaname=%s and tablename=%s'
        return self.query_one(_sql,('public',t)) is not None

    def drop_table(self,t):
        """
            >>> db = connection()
            >>> db.create_table('doctest_t3','''id SERIAL PRIMARY KEY, name TEXT''')
            >>> db.check_table('doctest_t3')
            True
            >>> db.drop_table('doctest_t3');
            >>> db.check_table('doctest_t3')
            False
        """
        self.execute('DROP TABLE IF EXISTS %s CASCADE' % t)

    def create_table(self,name,schema):
        """
            >>> db = connection()
            >>> db.create_table('doctest_t3','''id SERIAL PRIMARY KEY, name TEXT''')
            >>> db.check_table('doctest_t3')
            True
            >>> db.drop_table('doctest_t3');
            >>> db.check_table('doctest_t3')
            False
        """
        if not self.check_table(name):
            self.execute('CREATE TABLE %s (%s)' % (name,schema))

class PreparedStatement(object):

    def __init__(self,connection,name,call_type='query'):
        self.connection = connection
        self.name = name
        self.call_type = call_type

    def deallocate(self):
        self.connection.execute('DEALLOCATE %s' % self.name)

    def execute(self,*params):
        return self.connection.execute(self,params)

    def query(self,*params):
        return self.connection.query(self,params)

    def query_one(self,*params):
        return self.connection.query_one(self,params)

    def query_dict(self,key,*params):
        return self.connection.query_dict(self,key,params)

    def __call__(self,*params):
        return getattr(self.connection,self.call_type)(self,params)

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
    db = connection()
    if sys.argv.count('--interact'):
        db.log = sys.stdout
        code.interact(local=locals())
    else:
        try:
            # Setup tables
            db.drop_table('doctest_t1')
            db.drop_table('doctest_t2')
            for (name,schema) in tables:
                db.create_table(name,schema)
            for i in range(10):
                id = db.insert('doctest_t1',{'name':chr(97+i)*5},returning='id')['id']
                _ = db.insert('doctest_t2',{'value':chr(97+i)*2,'doctest_t1_id':id})
            # Run tests
            doctest.testmod(optionflags=doctest.ELLIPSIS)
        finally:
            # Drop tables
            db.drop_table('doctest_t1')
            db.drop_table('doctest_t2')

