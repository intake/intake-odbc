from intake.source import base
from turbodbc import connect


class Plugin(base.Plugin):
    def __init__(self):
        super(Plugin, self).__init__(name='odbc',
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


def ms_limit(q, lim):
    """MS SQL Server implementation of 'limit'"""
    return "SELECT TOP {} sq.* FROM ({}) sq".format(lim, q)


def limit(q, lim):
    """Non-MS SQL Server implementation of 'limit'"""
    return "SELECT sq.* FROM ({}) sq LIMIT {}".format(q, lim)
