#
# CC0 "No rights reserved" - feel free to use anywhere
#

import numpy
from sqlite3 import connect, Connection
from typing import List, Union, Tuple, Dict, Any
from csv import reader
from collections import defaultdict
import gzip

def _csv_datatype(value):
    try:
        value = int(value)
        return 'int'
    except:
        try:
            value = float(value)
            return 'real'
        except:
            return 'string'

_np_datatype = defaultdict(lambda: 'string')
_np_datatype[numpy.dtype('float32').str] = 'real'
_np_datatype[numpy.dtype('float64').str] = 'real'
_np_datatype[numpy.dtype('int32').str] = 'int'
_np_datatype[numpy.dtype('int64').str] = 'int'

_np_convert = defaultdict(lambda: str)
_np_convert[numpy.dtype('float32').str] = float
_np_convert[numpy.dtype('float64').str] = float
_np_convert[numpy.dtype('int32').str] = int
_np_convert[numpy.dtype('int64').str] = int

def _validate(conn: Connection,
    query: str,
    dtypes: Union[None,List[Union[numpy.dtype, None]]]) -> List[numpy.array]:

    cursor = conn.cursor()
    cursor.execute(query)
    row = cursor.fetchone()
    if row is None:
        raise ValueError('empty query result')
    if not (dtypes is None) and len(dtypes) != len(row):
        raise ValueError('the length of dtypes needs to be the same as the number of columns')
    return row, cursor, [i[0] for i in cursor.description]

def columnnames(conn: Connection, query: str) -> List[str]:
    row, cursor, names = _validate(conn, query, None)

    return names

def columnnamestypes(conn: Connection, query: str) -> List[Tuple[str,str]]:
    row, cursor, names = _validate(conn, query, None)

    return [(n, t) for n, t in \
        zip(names, [numpy.array([i]).dtype.str for i in row])]

def tableschema(conn: Connection, table: str) -> str:
    return \
      [(i[1], i[2].lower()) for i in \
       conn.execute("pragma table_info(%s)" % table).fetchall()]

def tablenames(conn: Connection) -> List[str]:
    return [i[0] for i in conn.execute(
    "select name from sqlite_schema where type='table' and name not like 'sqlite_%'")]

# indices are [column][row]
def query2colarr(conn: Connection,
    query: str,
    dtypes: Union[None,List[Union[numpy.dtype, None]]]=None) -> List[numpy.array]:

    row, cursor, names = _validate(conn, query, dtypes)
    if dtypes is None:
        dtypes = [None]*len(row)

    columns = [[i] for i in row]
    for row in cursor:
        for i, c in zip(row, columns):
            c.append(i)
    return [numpy.array(c, d) for c, d in zip(columns, dtypes)]

# indices are [column:str][row]
def query2coldict(conn: Connection,
    query: str,
    dtypes: Union[None,List[Union[numpy.dtype, None]]]=None) -> List[numpy.array]:

    row, cursor, names = _validate(conn, query, dtypes)
    if dtypes is None:
        dtypes = [None]*len(row)

    columns = [[i] for i in row]
    for row in cursor:
        for i, c in zip(row, columns):
            c.append(i)
    return {n: numpy.array(c, d) for c, n, d in zip(columns, names, dtypes)}

# indices are [row][column] or [row,column]
# can only be one data type
def query2array(conn: Connection,
    query: str, dtype: numpy.dtype=None) -> numpy.array:

    row, cursor, names = _validate(conn, query, None)

    rows = [row]
    for row in cursor:
        rows.append(row)
    return numpy.array(rows, dtype)

# indices are [row][column:int] or [column:str][row]
def query2struct(conn: Connection,
    query: str,
    dtypes: Union[None,List[Union[numpy.dtype, None]]]=None) -> numpy.array:

    row, cursor, names = _validate(conn, query, dtypes)
    if dtypes is None:
        dtypes = [None]*len(row)

    rows = [row]
    for row in cursor:
        rows.append(row)
    return numpy.array(rows, [(n, d) for n, d in zip(names, dtypes)])

# turns a csv into a sqlite table, columns of None reads the first line as a header for column names;
# if columns is not None and header_skip is True, it will skip the first line in the CSV
def csv2sqlite(conn: Connection, table: str, filename: str,
    columns: Union[None,List[str]]=None, header_skip=True,
    csv_options: Dict[str,Any]={}, gzipped: bool=False,
    encoding: str='utf-8') -> List[Tuple[str,str]]:

    if gzipped:
      f = gzip.open(filename, 'rt', encoding=encoding)
    else:
      f = open(filename, 'r', encoding=encoding)
    r = reader(f, **csv_options)

    if columns is None:
        columns = next(r)
    elif header_skip == True:
        next(r)
    types = [_csv_datatype(i) for i in next(r)]

    cursor = conn.cursor()
    columnstr = ""
    insertstr = "insert into '%s' values (?" % table
    columnstr = "%s %s" % (columns[0], types[0])
    for n, t in zip(columns[1:], types[1:]):
        columnstr = columnstr + ", %s %s" % (n, t)
        insertstr = insertstr + ", ?"
    insertstr = insertstr + ")"
    cursor.execute("create table '%s' (%s)" % (table, columnstr))
    for row in r:
        cursor.execute(insertstr, row)
    conn.commit()

    return list(zip(columns, types))

# turns the array data back into a sqlite table
def columns2sqlite(conn: Connection, table: str,
    columns: Union[Dict[str, numpy.array],List[numpy.array]],
    names: Union[List[Tuple[int,str]],List[str]]) -> List[Tuple[str,str]]:

    if isinstance(names[0], tuple):
        idx = [i[0] for i in names]
        names = [i[1] for i in names]
    else:
        idx = names
    types = [_np_datatype[columns[i].dtype.str] for i in idx]
    conv = [_np_convert[columns[i].dtype.str] for i in idx]

    cursor = conn.cursor()
    columnstr = ""
    insertstr = "insert into '%s' values (?" % table
    columnstr = "%s %s" % (names[0], types[0])
    for n, t in zip(names[1:], types[1:]):
        columnstr = columnstr + ", %s %s" % (n, t)
        insertstr = insertstr + ", ?"
    insertstr = insertstr + ")"
    cursor.execute("create table '%s' (%s)" % (table, columnstr))
    for row in zip(*[columns[i] for i in idx]):
        cursor.execute(insertstr, [c(v) for c, v in zip(conv, row)])
    conn.commit()

    return list(zip(columns, types))
