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
            # This approach is not optimal; LIMIT is know to confuse the query
            # planner sometimes. If there is a faster approach to gleaning
            # dtypes from arbitrary SQL queries, we should use it instead.
            cursor.execute("SELECT * FROM ({}) LIMIT {}".format(
                self._sql_expr, self._head_rows))
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
