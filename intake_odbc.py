from intake.source import base


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
    def __init__(self, uri, sql_expr, odbc_kwargs, metadata):
        self._init_args = {
            'uri': uri,
            'sql_expr': sql_expr,
            'odbc_kwargs': odbc_kwargs,
            'metadata': metadata,
        }

        self._uri = uri
        self._sql_expr = sql_expr
        self._odbc_kwargs = odbc_kwargs
        self._dataframe = None

        super(ODBCSource, self).__init__(container='dataframe',
                                         metadata=metadata)

    def _get_schema(self):
        if self._dataframe is None:
            # This approach is not optimal; LIMIT is know to confuse the query
            # planner sometimes. If there is a faster approach to gleaning
            # dtypes from arbitrary SQL queries, we should use it instead.
            first_row = ODBC_FIXME(
                self._uri,
                dataframe=True,
                query=('({}) limit 1').format(self._sql_expr),
                **self._odbc_kwargs
            )._to_dataframe()
            dtype = list(zip(first_row.dtypes.index, first_row.dtypes))
            shape = (None, len(first_row.dtypes.index))
        else:
            dtype = list(zip(self._dataframe.dtypes.index, self._dataframe.dtypes))
            shape = self._dataframe.shape
        return base.Schema(datashape=None,
                           dtype=dtype,
                           shape=shape,
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, _):
        if self._dataframe is None:
            self._dataframe = ODBC_FIXME(
                self._uri,
                dataframe=True,
                query=self._sql_expr,
                **self._odbc_kwargs
            )._to_dataframe()
            # The schema should be corrected once the data is read.
            self._schema = None
        return self._dataframe

    def _close(self):
        self._dataframe = None
