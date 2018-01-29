# intake-odbc

Intake-ODBC: ODBC Plugin for Intake

## User Installation

[configuration instructions](http://turbodbc.readthedocs.io/en/latest/pages/databases/mysql.html)

## Developer Installation

- Create a development environment with `conda create`. Then install the dependencies:

```
# TDB: Fix these install instructions
conda install -c intake intake
conda install -n root conda-build
conda install pandas turbodbc
```

- Development installation:
```
python setup.py develop --no-deps
```

- Create a DB to connect to. You can do this by bootstrapping a MS SQL
    Server image,
    and follow the [configuration instructions](http://turbodbc.readthedocs.io/en/latest/pages/databases/mysql.html).
    You will generally need a driver.

```
# run docker MS SQL Server image
docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=Strong(!)Password' \
    -p 1433:1433 -d microsoft/mssql-server-linux:2017-CU3
# log in as system admin
docker exec -it <container> /opt/mssql-tools/bin/sqlcmd -S localhost \
    -U sa -P 'Strong(!)Password'
# create database
1> CREATE DATABASE test
2> GO
```

```
# install an ODBC driver on OSX
brew install freetds --with-unixodbc
```

- Set up odbc config, e.g., the two ``.ini`` files provided

```
    export ODBCSYSINI=/Users/mdurant/code/intake-odbc/examples
```