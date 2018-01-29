from collections import OrderedDict
import os
import numpy as np
import time

import pytest
import pandas as pd
import turbodbc
import intake_odbc as odbc
from intake.catalog import Catalog
from .util import start_mssql, stop_mssql, start_postgres, stop_postgres


here = os.path.dirname(__file__)
# os.environ['ODBCSYSINI'] = os.path.join(here, '..', 'examples')
N = 10000
df0 = pd.DataFrame(OrderedDict([('productname', np.random.choice(
    ['fridge', 'toaster', 'kettle', 'micro', 'mixer', 'oven'], size=N)),
                                ('price', np.random.rand(N) * 10),
                                ('productdescription', ["hi"] * N)]))
df0.index.name = 'productid'


def test_mssql_minimal(mssql):
    q = 'SELECT session_id, blocking_session_id FROM sys.dm_exec_requests'
    s = odbc.ODBCSource(
        uri=None, sql_expr=q,
        odbc_kwargs=mssql,
        metadata={})
    disc = s.discover()
    assert list(disc['dtype']) == ['session_id', 'blocking_session_id']
    data = s.read()
    assert len(data)


def test_mssql_part_minimal(mssql):
    args = mssql.copy()
    args.update(dict(index='session_id', npartitions=2))
    q = 'SELECT session_id, blocking_session_id FROM sys.dm_exec_requests'
    s = odbc.ODBCPartitionedSource(
        uri=None, sql_expr=q,
        odbc_kwargs=args,
        metadata={})
    disc = s.discover()
    assert list(disc['dtype']) == ['session_id', 'blocking_session_id']
    assert s.npartitions == 2
    data = s.read()
    assert len(data)
    part1, part2 = s.read_partition(0), s.read_partition(1)
    assert data.equals(pd.concat([part1, part2], ignore_index=True))
    assert data.equals(pd.concat(s.read_chunked(), ignore_index=True))


@pytest.fixture(scope='module')
def mssql():
    """Start docker container for MS SQL and cleanup connection afterward."""
    stop_mssql(let_fail=False)
    start_mssql()

    kwargs = dict(dsn="MSSQL", uid='sa', pwd='yourStrong(!)Password',
                  mssql=True)
    timeout = 5
    try:
        while True:
            try:
                conn = turbodbc.connect(**kwargs)
                break
            except Exception as e:
                print(e)
                time.sleep(0.2)
                timeout -= 0.2
                if timeout < 0:
                    raise
        curs = conn.cursor()
        curs.execute("""CREATE TABLE testtable
            (productid int PRIMARY KEY NOT NULL,  
             productname varchar(25) NOT NULL,  
             price float NULL,  
             productdescription text NULL)""")
        for i, row in df0.iterrows():
            curs.execute(
                "INSERT testtable (productid, productname, price, "
                "                  productdescription) "
                "VALUES ({}, '{}', {}, '{}')".format(*([i] + row.tolist())))
        conn.commit()
        conn.close()
        yield kwargs
    finally:
        stop_mssql()


@pytest.fixture(scope='module')
def pg():
    """Start docker container for MS SQL and cleanup connection afterward."""
    stop_postgres(let_fail=False)
    start_postgres()

    kwargs = dict(dsn="PG")
    timeout = 5
    try:
        while True:
            try:
                conn = turbodbc.connect(**kwargs)
                break
            except Exception as e:
                print(e)
                time.sleep(0.2)
                timeout -= 0.2
                if timeout < 0:
                    raise
        curs = conn.cursor()
        curs.execute("""CREATE TABLE testtable
            (productid int PRIMARY KEY NOT NULL,
             productname varchar(25) NOT NULL,
             price float NULL,
             productdescription text NULL)""")
        for i, row in df0.iterrows():
            curs.execute(
                "INSERT INTO testtable "
                "VALUES ({}, '{}', {}, '{}')".format(*([i] + row.tolist())))
        conn.commit()
        conn.close()
        yield kwargs
    finally:
        stop_mssql()


def test_engines(mssql, pg):
    for kwargs in [mssql, pg]:
        q = "SELECT * from testtable"
        with odbc.ODBCSource(uri=None, sql_expr=q, odbc_kwargs=kwargs,
                             metadata={}) as s:
            # needs auto-close if container might disappear on completion
            df = s.read()
            assert df.equals(df0.reset_index())


def test_pg_simple(pg):
    q = "SELECT * FROM pg_database"
    s = odbc.ODBCSource(uri=None, sql_expr=q, odbc_kwargs=pg,
                        metadata={})
    out = s.read()
    assert 'datname' in out.columns
