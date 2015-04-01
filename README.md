# Backup
Script for backup mysql/mongodb

Uses *mysqldump*, *mongodump*. After backing up copy files to remote server and send report email.


You can use **default.conf.template** as template to create config file for your
server.


###Example of usage
```
$ ./backup.py server.conf
```
