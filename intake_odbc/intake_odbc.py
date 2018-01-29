from intake.source import base
from turbodbc import connect
import numpy as np


class UPPlugin(base.Plugin):
    def __init__(self):
        super(UPPlugin, self).__init__(name='odbc',
                                       version='0.1',
                                       container='dataframe',
                                       partition_access=False)

    def open(self, uri, sql_expr, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                SQL query to be executed.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ODBCSource(uri=uri,
                          sql_expr=sql_expr,
                          odbc_kwargs=source_kwargs,
                          metadata=base_kwargs['metadata'])


class ODBCSource(base.DataSource):
    """
    Options [docs](http://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#advanced-usage)

    Connection string [docs](http://turbodbc.readthedocs.io/en/latest/pages/getting_started.html#establish-a-connection-with-your-database)

    Parameters
    ----------
    head_rows: int (10)
        Number of rows that are read from the start of the data to infer data
        types upon discovery
    mssql: bool (False)
        Whether to use MS SQL Server syntax - depends on the backend target of
        the connection
    """

    def __init__(self, uri, sql_expr, odbc_kwargs, metadata):
        odbc_kwargs = odbc_kwargs.copy()
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'odbc_kwargs': odbc_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr
        self._head_rows = odbc_kwargs.pop('head_rows', 10)
        self._ms = odbc_kwargs.pop('mssql', False)
        self._odbc_kwargs = odbc_kwargs
        self._dataframe = None
        self._connection = None
        self._cursor = None

        super(ODBCSource, self).__init__(container='dataframe',
                                         metadata=metadata)

    def _get_schema(self):
        if self._dataframe is None:
            self._connection = connect(**self._odbc_kwargs)
            cursor = self._connection.cursor()
            self._cursor = cursor
            if self._ms:
                q = ms_limit(self._sql_expr, self._head_rows)
            else:
                q = limit(self._sql_expr, self._head_rows)
            cursor.execute(q)
            head = cursor.fetchallarrow().to_pandas()
            dtype = head[:0]
            shape = (None, head.shape[1])
        else:
            dtype = self._dataframe[:0]
            shape = self._dataframe.shape
        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            self._cursor.execute(self._sql_expr)
            self._dataframe = self._cursor.fetchallarrow().to_pandas()
            self._schema = None
        return self._dataframe

    def _close(self):
        self._dataframe = None
        self._connection = None
        self._cursor = None


def ms_limit(q, lim):
    """MS SQL Server implementation of 'limit'"""
    return "SELECT TOP {} sq.* FROM ({}) sq".format(lim, q)


def limit(q, lim):
    """Non-MS SQL Server implementation of 'limit'"""
    return "SELECT sq.* FROM ({}) sq LIMIT {}".format(q, lim)


class PPlugin(base.Plugin):
    def __init__(self):
        super(PPlugin, self).__init__(name='odbc_partitioned',
                                      version='0.1',
                                      container='dataframe',
                                      partition_access=False)

    def open(self, uri, sql_expr, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                SQL query to be executed.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ODBCSource(uri=uri,
                          sql_expr=sql_expr,
                          odbc_kwargs=source_kwargs,
                          metadata=base_kwargs['metadata'])


class ODBCPartitionedSource(base.DataSource):
    """
    Options [docs](http://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#advanced-usage)

    Connection string [docs](http://turbodbc.readthedocs.io/en/latest/pages/getting_started.html#establish-a-connection-with-your-database)

    Parameters
    ----------
    head_rows: int (10)
        Number of rows that are read from the start of the data to infer data
        types upon discovery
    mssql: bool (False)
        Whether to use MS SQL Server syntax - depends on the backend target of
        the connection
    index: str
        Column to use for partitioning
    max, min: str
        Range of values in index to consider (will query DB if not given)
    npartitions: int
        Number of partitions to assume
    divisions: list of values
        If given, use these as partition boundaries - and therefore ignore max/
        min and npartitions
    """

    def __init__(self, uri, sql_expr, odbc_kwargs, metadata):
        odbc_kwargs = odbc_kwargs.copy()
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'odbc_kwargs': odbc_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr
        self._head_rows = odbc_kwargs.pop('head_rows', 10)
        self._ms = odbc_kwargs.pop('mssql', False)
        self._index = odbc_kwargs.pop('index')  # required
        self._max = odbc_kwargs.pop('max', None)
        self._min = odbc_kwargs.pop('min', None)
        self._npartitions = odbc_kwargs.pop('npartitions', None)
        self._divisions = odbc_kwargs.pop('divisions', None)
        self._odbc_kwargs = odbc_kwargs
        self._connection = None
        self._cursor = None

        super(ODBCPartitionedSource, self).__init__(container='dataframe',
                                                    metadata=metadata)

    def _get_schema(self):
        self._connection = connect(**self._odbc_kwargs)
        cursor = self._connection.cursor()
        self._cursor = cursor
        if self._ms:
            q = ms_limit(self._sql_expr, self._head_rows)
        else:
            q = limit(self._sql_expr, self._head_rows)
        cursor.execute(q)
        head = cursor.fetchallarrow().to_pandas()
        dtype = head[:0]
        shape = (None, head.shape[1])  # could have called COUNT()
        nparts = self._npartitions or len(self._divisions)
        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=shape,
                           npartitions=nparts,
                           extra_metadata={})

    def _get_partition(self, i):
        if self._divisions is None:
            # compute divisions
            if self._max is None:
                # get data boundaries from DB
                q = "SELECT MAX(sq.{ind}) as ma, MIN(sq.{ind}) as mi " \
                    "FROM ({exp}) sq".format(ind=self._index, exp=self._sql_expr)
                self._cursor.execute(q)
                self._max, self._min = self._cursor.fetchone()
                self._max += 0.001
            self._divisions = np.linspace(self._min, self._max,
                                          self._npartitions + 1)

        mi, ma = self._divisions[i:i+2]
        q = "SELECT sq.* FROM ({exp}) as sq WHERE " \
            "sq.{ind} >= {mi} AND sq.{ind} < {ma}".format(
                exp=self._sql_expr, ind=self._index, mi=mi, ma=ma)
        self._cursor.execute(q)
        df = self._cursor.fetchallarrow().to_pandas()
        return df

    def _close(self):
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self._cursor = None
