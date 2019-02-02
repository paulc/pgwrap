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

def where(where):
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
            _where.append('%s %s %%(%s)s' % (field,_operators.get(op,op) or 
                                             '=',f))
        return ' WHERE ' + ' AND '.join(_where)
    else:
        return ''

def update(values):
    _update = []
    for k,v in values.items():
        f,_,op = k.partition('__')
        _update.append(_update_operators[op] % 
                            {'key':k,'val':v,'field':f,'op':op})
    return ','.join(_update)

def order(order):
    if order:
        _order = []
        for f in order:
            field,_,direction = f.partition('__')
            _order.append(field + (' DESC' if direction == 'desc' else ''))
        return ' ORDER BY ' + ', '.join(_order)
    else:
        return ''

def columns(columns):
    if columns:
        return ", ".join([(c if isinstance(c,type("")) 
                                else "%s AS %s" % c) for c in columns])
    else:
        return '*'

def on(tables,on):
    t1,t2 = tables
    if on:
        return "%s = %s" % on
    else:
        return "%s.id = %s.%s_id" % (t1,t2,t1)

def limit(limit):
    if limit:
        return ' LIMIT %d' % limit
    else:
        return ''

def offset(offset):
    if offset:
        return ' OFFSET %d' % offset
    else:
        return ''

def for_update(update):
    if update:
        return ' FOR UPDATE'
    else:
        return ''

