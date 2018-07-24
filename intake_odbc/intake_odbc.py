from intake.source import base
import numpy as np
from . import __version__


class ODBCSource(base.DataSource):
    """
    One-shot ODBC to dataframe reader

    Parameters
    ----------
    uri: str or None
        Full connection string for TurbODBC. If using keyword parameters, this
        should be ``None``
    sql_expr: str
        Query expression to pass to the DB backend

    Further connection arguments, such as username/password, and may also
    include the following:

        head_rows: int (10)
            Number of rows that are read from the start of the data to infer
            data types upon discovery
        mssql: bool (False)
            Whether to use MS SQL Server syntax - depends on the backend target
            of the connection
    """
    name = 'odbc'
    version = __version__
    container = 'dataframe'
    partition_access = False

    def __init__(self, uri, sql_expr, metadata=None, **odbc_kwargs):
        odbc_kwargs = odbc_kwargs.copy()
        self._uri = uri
        self._sql_expr = sql_expr
        self._head_rows = odbc_kwargs.pop('head_rows', 10)
        self._ms = odbc_kwargs.pop('mssql', False)
        self._odbc_kwargs = odbc_kwargs
        self._dataframe = None
        self._connection = None
        self._cursor = None

        super(ODBCSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from turbodbc import connect
        if self._dataframe is None:
            self._connection = connect(connection_string=self._uri,
                                       **self._odbc_kwargs)
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
            dtype = {k: str(v) for k, v
                     in self._dataframe.dtypes.to_dict().items()}
            shape = self._dataframe.shape
        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        self._get_schema()
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


class ODBCPartitionedSource(base.DataSource):
    """
    ODBC partitioned reader

    This source produces new queries for each partition, where an index column
    is used to select rows belonging to each partition

    Parameters
    ----------
    uri: str or None
        Full connection string for TurbODBC. If using keyword parameters, this
        should be ``None``
    sql_expr: str
        Query expression to pass to the DB backend

    Further connection arguments, such as username/password, and may also
    include the following:

        head_rows: int (10)
            Number of rows that are read from the start of the data to infer
            data types upon discovery
        mssql: bool (False)
            Whether to use MS SQL Server syntax - depends on the backend target
            of the connection
        index: str
            Column to use for partitioning
        max, min: str
            Range of values in index to consider (will query DB if not given)
        npartitions: int
            Number of partitions to assume
        divisions: list of values
            If given, use these as partition boundaries - and therefore ignore
            max/min and npartitions
    """
    name = 'odbc'
    version = __version__
    container = 'dataframe'
    partition_access = True

    def __init__(self, uri, sql_expr, metadata=None, **odbc_kwargs):
        odbc_kwargs = odbc_kwargs.copy()
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

        super(ODBCPartitionedSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        from turbodbc import connect
        self._connection = connect(connection_string=self._uri,
                                   **self._odbc_kwargs)
        cursor = self._connection.cursor()
        self._cursor = cursor
        if self._ms:
            q = ms_limit(self._sql_expr, self._head_rows)
        else:
            q = limit(self._sql_expr, self._head_rows)
        cursor.execute(q)
        head = cursor.fetchallarrow().to_pandas().set_index(self._index)
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
                    "FROM ({exp}) sq".format(ind=self._index,
                                             exp=self._sql_expr)
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
        return df.set_index(self._index)

    def _close(self):
        if self._connection is not None:
            self._connection.close()
        self._connection = None
        self._cursor = None
