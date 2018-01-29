import requests
import shlex
import subprocess
import time


def verify_plugin_interface(plugin):
    """Assert types of plugin attributes."""
    assert isinstance(plugin.version, str)
    assert isinstance(plugin.container, str)
    assert isinstance(plugin.partition_access, bool)


def verify_datasource_interface(source):
    """Assert presence of datasource attributes."""
    for attr in ['container', 'description', 'datashape', 'dtype', 'shape',
                 'npartitions', 'metadata']:
        assert hasattr(source, attr)

    for method in ['discover', 'read', 'read_chunked', 'read_partition',
                   'to_dask', 'close']:
        assert hasattr(source, method)


def start_mssql():
    print('Starting MS SQL Server...')

    cmd = shlex.split("docker run -e 'ACCEPT_EULA=Y' --name intake-mssql "
                      "-e 'SA_PASSWORD=yourStrong(!)Password' "
                      "-p 1433:1433 -d microsoft/mssql-server-linux:2017-CU3")
    cid = subprocess.check_output(cmd).strip().decode()

    time.sleep(5)

    cmd = shlex.split("docker exec {} /opt/mssql-tools/bin/sqlcmd"
                      " -S localhost -U sa -P 'yourStrong(!)Password' "
                      "-Q 'CREATE DATABASE test'".format(cid))
    subprocess.check_output(cmd)


def stop_mssql(let_fail=False):
    try:
        print('Stopping MS SQL Server...')
        cmd = shlex.split('docker ps -q --filter "name=intake-mssql"')
        cid = subprocess.check_output(cmd).strip().decode()
        if cid:
            subprocess.call(['docker', 'kill', cid])
            subprocess.call(['docker', 'rm', cid])
    except subprocess.CalledProcessError as e:
        print(e)
        if not let_fail:
            raise
