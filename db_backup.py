#!/usr/bin/env python

'''
Required modules:
    * pymongo (https://pypi.python.org/pypi/pymongo)
    * PyMySQL (https://pypi.python.org/pypi/PyMySQL)
'''

try:
    import pymongo
    import pymysql
except ImportError as e:
    import sys
    print("Please install all required modules")
    sys.exit()


import os
from collections import namedtuple
import traceback
import datetime
import subprocess
import shutil
import json

import pymysql, pymongo, plumbum
from pymysql import cursors
from plumbum import local


#TODO: load settings from file
settings = namedtuple('Settings', settings.keys())(**settings)


class InvalidServerTypeError(Exception):
    pass


class Server:

    def __init__(self, data):
        for k,v in data.items():
            setattr(self, k, v)
        self.data = data

    @property
    def key(self):
        name = self.host + ":{0}".format(self.port) if self.port else ''
        return "[{0}] - {1}".format(self.type, name)

    def __unicode__(self):
        return str(self.data)

    def __str__(self):
        return str(self.data)


# some magic here. function should be called <db type>_all_dbs
def mysql_all_dbs(server):
    link = pymysql.connect(host=server.host, port=server.port,
        user=server.user, password=server.password)
    try:
        with link.cursor(cursors.DictCursor) as cursor:
            cursor.execute('show databases')
            all_dbs = cursor.fetchall()
    except Exception as e:
        raise e
    finally:
        link.close()
    return [db['Database'] for db in all_dbs]


# some magic here. function should be called <db type>_all_dbs
def mongo_all_dbs(server):
    client = pymongo.MongoClient(server.host, server.port)
    dbs = client.database_names()
    client.close()
    return dbs


# some magic here. function should be called <db type>_backup
def mysql_backup(server, backup_dir, db_name):
    filename = os.path.join(backup_dir, db_name)
    backup_cmd = "mysqldump --host=%(host)s --port=%(port)d -u%(user)s -p%(password)s %(db)s | bzip2 > %(filename)s.sql.bz2" % {
        'host': server.host,
        'port': server.port,
        'user': server.user,
        'password': server.password,
        'db': db_name,
        'filename': filename
    }
    subprocess.call(backup_cmd, shell=True)
    return "{0}.sql.bz2".format(filename)


# some magic here. function should be called <db type>_backup
def mongo_backup(server, backup_dir, db_name):
    dir_name = os.path.join(backup_dir, db_name)
    backup_cmd = "mongodump --host=%(host)s --port=%(port)d --db=%(db)s --out=%(dir_name)s" % {
        'host': server.host,
        'port': server.port,
        'db': db_name,
        'dir_name': backup_dir,
    }
    subprocess.call(backup_cmd, shell=True)
    tar_cmd = "cd %(dir)s && tar -czf %(db)s.tar.gz %(db)s" % { 'dir': backup_dir, 'db': db_name }
    subprocess.call(tar_cmd, shell=True)
    filename = "{0}.tar.gz".format(dir_name)
    if os.path.exists(filename):
        shutil.rmtree(dir_name)
    return filename


def get_backup_dir(server, basedir):
    str_date = datetime.datetime.now().strftime("%d.%m.%Y")
    str_time = datetime.datetime.now().strftime("%H:%M:%S")
    return os.path.join(basedir, server.type,
        "{0}:{1}".format(server.host, server.port), str_date, str_time)

def invalid_server_type(*args, **kwargs):
    raise InvalidServerTypeError(*args)


def get_all_dbs(server):
    get_dbs = globals().get("{0}_all_dbs".format(server.type), invalid_server_type)
    return get_dbs(server)


def backup_db(server, backup_dir, db_name):
    backup = globals().get("{0}_backup".format(server.type), invalid_server_type)
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    return backup(server, backup_dir, db_name)


def copy_to_remote(server, remote_server):
    local_dir = os.path.join(settings.local_backup_dir, server.type)
    scp_cmd = "scp -r %(local_dir)s %(host)s:%(path)s" % {
        'local_dir': local_dir,
        'host': remote_server['host'],
        'path': remote_server['path'],
    }
    subprocess.call(scp_cmd, shell=True)


def backup_server(server):
    report = {}
    backup_dir = get_backup_dir(server, settings.local_backup_dir)
    for db in get_all_dbs(server):
        if db in server.ignore_dbs:
            continue
        try:
            dumpfile = backup_db(server, backup_dir, db)
        except Exception as e:
            report[db] = traceback.format_exc()
        else:
            report[db] = 'success'
    copy_to_remote(server, settings.remote_server)
    local_dir = os.path.join(settings.local_backup_dir, server.type)
    shutil.rmtree(local_dir)
    return report


def send_report(report):
    # TODO:
    pass


if __name__ == "__main__":
    report = {}
    for data in settings.servers:
        server = Server(data)
        try:
            r = backup_server(server)
        except Exception as e:
            report[server.key] = traceback.format_exc()
        else:
            report[server.key] = r

    send_report(report)



