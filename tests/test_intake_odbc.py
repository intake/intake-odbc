from collections import OrderedDict
import os
import numpy as np
import time

import pytest
import pandas as pd
import turbodbc
import intake_odbc as odbc
from intake.catalog import Catalog
from .util import start_mssql, stop_mssql


here = os.path.dirname(__file__)
# os.environ['ODBCSYSINI'] = os.path.join(here, '..', 'examples')
N = 10000
df0 = pd.DataFrame(OrderedDict([('productname', np.random.choice(
    ['fridge', 'toaster', 'kettle', 'micro', 'mixer', 'oven'], size=N)),
                                ('price', np.random.rand(N) * 10),
                                ('productdescription', ["hi"] * N)]))
df0.index.name = 'productid'


def test_minimal(mssql):
    q = 'SELECT session_id, blocking_session_id FROM sys.dm_exec_requests'
    s = odbc.ODBCSource(
        uri=None, sql_expr=q,
        odbc_kwargs=mssql,
        metadata={})
    disc = s.discover()
    assert list(disc['dtype']) == ['session_id', 'blocking_session_id']
    data = s.read()
    assert len(data)


def test_part_minimal(mssql):
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
    """Start docker container for ES and cleanup connection afterward."""
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


@pytest.mark.parametrize('conn', [mssql])
def test_mssql(conn):
    for kwargs in conn():
        # here, fixture is ignored, use as regular generator
        q = "SELECT * from testtable"
        s = odbc.ODBCSource(uri=None, sql_expr=q, odbc_kwargs=kwargs,
                            metadata={})
        df = s.read()
        assert df.equals(df0.reset_index())

# @pytest.fixture(scope='module')
# def engine():
#     """Start docker container for ODBC database, yield a tuple (engine,
#     metadata), and cleanup connection afterward."""
#     # FIXME
#     from .util import start_odbc, stop_odbc
#     from sqlalchemy import create_engine
#     stop_odbc(let_fail=True)
#     local_port = start_odbc()
#
#     # FIXME
#     uri = 'postgresql://postgres@localhost:{}/postgres'.format(local_port)
#     engine = create_engine(uri)
#     for table_name, csv_fpath in TEST_DATA:
#         df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#         df.to_sql(table_name, engine, index=False)
#
#     try:
#         yield engine
#     finally:
#         stop_odbc()
#
#
# def test_odbc_plugin():
#     p = odbc.Plugin()
#     assert isinstance(p.version, str)
#     assert p.container == 'dataframe'
#     verify_plugin_interface(p)
#
#
# @pytest.mark.parametrize('table_name,_', TEST_DATA)
# def test_open(engine, table_name, _):
#     p = odbc.Plugin()
#     d = p.open(str(engine.url), 'select * from '+table_name)
#     assert d.container == 'dataframe'
#     assert d.description is None
#     verify_datasource_interface(d)
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_discover(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#     info = source.discover()
#     assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
#     assert info['shape'] == (None, 3)
#     assert info['npartitions'] == 1
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_read(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#     df = source.read()
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_discover_after_read(engine, table_name, csv_fpath):
#     """Assert that after reading the dataframe, discover() shows more accurate
#     information.
#     """
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#     info = source.discover()
#     assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
#     assert info['shape'] == (None, 3)
#     assert info['npartitions'] == 1
#
#     df = source.read()
#     assert expected_df.equals(df)
#
#     info = source.discover()
#     assert info['dtype'] == list(zip(expected_df.columns, expected_df.dtypes))
#     assert info['shape'] == (4, 3)
#     assert info['npartitions'] == 1
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.skip('Not implemented yet')
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_read_chunked(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#
#     parts = list(source.read_chunked())
#     df = pd.concat(parts)
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.skip('Partition support not planned')
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_read_partition(engine, table_name, csv_fpath):
#     expected_df1 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#     expected_df2 = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#
#     source.discover()
#     assert source.npartitions == 2
#
#     # Read partitions is opposite order
#     df2 = source.read_partition(1)
#     df1 = source.read_partition(0)
#
#     assert expected_df1.equals(df1)
#     assert expected_df2.equals(df2)
#
#
# @pytest.mark.skip('Not implemented yet')
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_to_dask(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#
#     dd = source.to_dask()
#     df = dd.compute()
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_close(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#
#     source.close()
#     # Can reopen after close
#     df = source.read()
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,csv_fpath', TEST_DATA)
# def test_pickle(engine, table_name, csv_fpath):
#     expected_df = pd.read_csv(os.path.join(TEST_DATA_DIR, csv_fpath))
#
#     p = odbc.Plugin()
#     source = p.open(str(engine.url), 'select * from '+table_name)
#
#     pickled_source = pickle.dumps(source)
#     source_clone = pickle.loads(pickled_source)
#
#     expected_df = source.read()
#     df = source_clone.read()
#
#     assert expected_df.equals(df)
#
#
# @pytest.mark.parametrize('table_name,_1', TEST_DATA)
# def test_catalog(engine, table_name, _1):
#     catalog_fpath = os.path.join(TEST_DATA_DIR, 'catalog1.yml')
#
#     catalog = Catalog(catalog_fpath)
#     ds_name = table_name.rsplit('_idx', 1)[0]
#     src = catalog[ds_name]
#     pgsrc = src.get()
#     pgsrc._uri = str(engine.url)
#
#     assert src.describe()['container'] == 'dataframe'
#     assert src.describe_open()['plugin'] == 'odbc'
#     assert src.describe_open()['args']['sql_expr'][:6] in ('select', 'SELECT')
#
#     metadata = pgsrc.discover()
#     assert metadata['npartitions'] == 1
#
#     expected_df = pd.read_sql_query(pgsrc._sql_expr, engine)
#     df = pgsrc.read()
#     assert expected_df.equals(df)
#
#     pgsrc.close()
#
#
# def test_catalog_join(engine):
#     catalog_fpath = os.path.join(TEST_DATA_DIR, 'catalog1.yml')
#
#     catalog = Catalog(catalog_fpath)
#     ds_name = 'sample2'
#     src = catalog[ds_name]
#     pgsrc = src.get()
#     pgsrc._uri = str(engine.url)
#
#     assert src.describe()['container'] == 'dataframe'
#     assert src.describe_open()['plugin'] == 'odbc'
#     assert src.describe_open()['args']['sql_expr'][:6] in ('select', 'SELECT')
#
#     metadata = pgsrc.discover()
#     assert metadata['npartitions'] == 1
#
#     expected_df = pd.read_sql_query(pgsrc._sql_expr, engine)
#     df = pgsrc.read()
#     assert expected_df.equals(df)
#
#     pgsrc.close()
