from .intake_odbc import ODBCSource, ODBCPartitionedSource, base

__version__ = '0.0.1'


class ODBCPlugin(base.Plugin):
    def __init__(self):
        super(ODBCPlugin, self).__init__(name='odbc',
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


class ODBCPartPlugin(base.Plugin):
    def __init__(self):
        super(ODBCPartPlugin, self).__init__(name='odbc_partitioned',
                                             version='0.1',
                                             container='dataframe',
                                             partition_access=True)

    def open(self, uri, sql_expr, **kwargs):
        """
        Parameters:
            uri : str
                Full SQLAlchemy URI for the database connection.
            sql_expr : string or SQLAlchemy Selectable (select or text object):
                SQL query to be executed.
        """
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return ODBCPartitionedSource(uri=uri,
                                     sql_expr=sql_expr,
                                     odbc_kwargs=source_kwargs,
                                     metadata=base_kwargs['metadata'])
