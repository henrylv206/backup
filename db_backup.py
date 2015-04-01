#!/usr/bin/env python

'''
Required modules:
    * outbox (https://pypi.python.org/pypi/outbox)
    * pymongo (https://pypi.python.org/pypi/pymongo)
    * PyMySQL (https://pypi.python.org/pypi/PyMySQL)
'''

import sys
try:
    import pymongo
    import pymysql
    import outbox
except ImportError as e:
    print("Please install all required modules")
    print(__doc__)
    sys.exit()

import os
from collections import namedtuple
import traceback
import datetime
import subprocess
from subprocess import Popen, PIPE
import shutil
import json
import logging
import logging.handlers
import logging.config

import pymysql, pymongo
from pymysql import cursors

try:
    filename = sys.argv[1]
    import json
    dictionary = json.load(open(filename, 'r'))
    settings = namedtuple('Settings', dictionary.keys())(**dictionary)
except Exception as e:
    print("Can't load settings from file {0}".format(filename))
    print("See default.conf.template for example\n")
    sys.exit()


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "base": {
            "format": "%(asctime)s - %(levelname)s - %(message)s (%(pathname)s:%(lineno)s)",
            "datefmt": "%d.%m.%Y %H:%M:%S"
        }
    },
    "handlers": {
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "base",
            "filename": settings.logfile,
            "maxBytes": 1024 * 1024,
            "backupCount": 3,
        }
    },
    "loggers": {
        "backup": {
            "handlers": ["file"],
            "level": "DEBUG",
            "propagate": False,
        }
    }
}

logger = logging.getLogger('backup')
logging.config.dictConfig(LOGGING)


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

def get_server_key(server):
    return "[%(type)s] %(host)s:%(port)d" % {
            "type": server.type,
            "host": server.host,
            "port": server.port,
        }


def call_cmd(cmd):
    logger.info(cmd)
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = proc.communicate()
    if out:
        logger.info(out.decode("utf-8"))
        return out.decode('utf-8')
    if err:
        logger.error(err.decode("utf-8"))
        return err.decode('utf-8')


# some magic here. function should be named <server.type>_all_dbs
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


# some magic here. function should be named <server.type>_all_dbs
def mongo_all_dbs(server):
    client = pymongo.MongoClient(server.host, server.port)
    dbs = client.database_names()
    client.close()
    return dbs


# some magic here. function should be named <server.type>_backup
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
    call_cmd(backup_cmd)
    return "{0}.sql.bz2".format(filename)


# some magic here. function should be named <server.type>_backup
def mongo_backup(server, backup_dir, db_name):
    dir_name = os.path.join(backup_dir, db_name)
    backup_cmd = "mongodump --host=%(host)s --port=%(port)d --db=%(db)s --out=%(dir_name)s" % {
        'host': server.host,
        'port': server.port,
        'db': db_name,
        'dir_name': backup_dir,
    }
    call_cmd(backup_cmd)
    tar_cmd = "cd %(dir)s && tar -czf %(db)s.tar.gz %(db)s" % { 'dir': backup_dir, 'db': db_name }
    call_cmd(tar_cmd)
    filename = "{0}.tar.gz".format(dir_name)
    if os.path.exists(filename):
        logger.debug("remove directory %s" % dir_name)
        shutil.rmtree(dir_name)
    return filename


def get_backup_dir(server, basedir):
    dt = datetime.datetime.now()
    str_date = dt.strftime("%d.%m.%Y")
    str_time = dt.strftime("%H:%M:%S")
    return os.path.join(basedir, server.type,
        "{0}:{1}".format(server.host, server.port), str_date, str_time)

def invalid_server_type(*args, **kwargs):
    raise InvalidServerTypeError(*args)


def get_all_dbs(server):
    get_dbs = globals().get("{0}_all_dbs".format(server.type), invalid_server_type)
    return get_dbs(server)


def backup_db(server, backup_dir, db_name):
    logger.info(">>>> DB: %s" % db_name)
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
    return call_cmd(scp_cmd)


def ignore_db(db_name):
    try:
        if isinstance(server.ignore_dbs, list):
            return db_name in server.ignore_dbs
        dbs = json.load(open(server.ignore_dbs))
        return db_name in dbs
    except Exception as e:
        logger.exception(e)
    return false


def backup_server(server):
    logger.info("Backup server %s" % get_server_key(server).upper())
    report = {}
    backup_dir = get_backup_dir(server, settings.local_backup_dir)
    for db in get_all_dbs(server):
        if ignore_db(db):
            logger.debug("ignore db %s" % db)
            continue
        try:
            dumpfile = backup_db(server, backup_dir, db)
        except Exception as e:
            logger.error("backup %s FAILED" % db)
            logger.exception(e)
            report[db] = traceback.format_exc()
        else:
            logger.debug("backup %s finished" % db)
            report[db] = 'success'
    report["remote"] = copy_to_remote(server, settings.remote_server)
    local_dir = os.path.join(settings.local_backup_dir, server.type)
    logger.debug("remove directory %s" % local_dir)
    shutil.rmtree(local_dir)
    return report


def send_report(report):
    message = ''
    for server_key, server_report in report.items():
        message += "SERVER %s: \n\n" % server_key
        if isinstance(server_report, dict):
            remote_report = server_report.get("remote")
            del(server_report["remote"])
            for db_name, db_report in server_report.items():
                message += "\tDB %s: %s\n" % (db_name, db_report)
            if remote_report:
                message += "\n\t[Remote server (scp)]: %s\n" % remote_report
        else:
            message += "%s" % server_report
        message += "\n\n"
    print(message)
    try:
        import socket
        from outbox import Outbox, Email, Attachment
        hostname = socket.gethostname()
        smtp = namedtuple('smtp', settings.smtp.keys())(**settings.smtp)
        attachments = [Attachment('backup.log', fileobj=open(settings.logfile))]
        outbox = Outbox(username=smtp.user, password=smtp.password,
                server=smtp.host, port=smtp.port, mode='SSL')
        message = "HOST: %s\n\n" % hostname + message
        outbox.send(Email(subject='[%s] Daily backup report' % hostname,
            body=message, recipients=settings.emails), attachments=attachments)
        # if report sent, we can remove log file
        os.unlink(settings.logfile)
    except Exception as e:
        logger.error("Can't send report via email")
        logger.exception(e)


if __name__ == "__main__":
    logger.info('{:*^100}'.format('START BACKUP'))
    report = {}
    for data in settings.servers:
        server = Server(data)
        server_key = get_server_key(server)
        try:
            r = backup_server(server)
        except Exception as e:
            logger.error("Can't backup server %s" % server_key)
            logger.exception(e)
            report[server_key] = traceback.format_exc()
        else:
            report[server_key] = r
    logger.info('{:*^100}'.format('BACKUP FINISHED'))

    logger.info('sending report...')
    send_report(report)



