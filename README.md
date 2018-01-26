# intake-odbc

Intake-ODBC: ODBC Plugin for Intake

## User Installation

TBD

## Developer Installation

1. Create a development environment with `conda create`. Then install the dependencies:

    ```
    # TDB: Fix these install instructions
    conda install -c intake intake
    conda install -n root conda-build
    conda install pandas turbodbc
    ```

1. Development installation:
    ```
    python setup.py develop --no-deps
    ```

1. Create a DB to connect to. You can do this by bootstrapping a MySQL
    image in docker, e.g.,

    ```
    docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=Strong(!)Password' \
        -p 1433:1433 -d microsoft/mssql-server-linux:2017-CU3
    ```
    and follow the [configuration instructions](http://turbodbc.readthedocs.io/en/latest/pages/databases/mysql.html).
