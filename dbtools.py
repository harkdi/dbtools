#!/usr/bin/env python
#coding:utf-8

import os
import json
import string
import subprocess
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


#--------------------- Load the configuration file ---------------------------------
sys.path.append('/oma/deploy/scripts/')
with open('/oma/deploy/scripts/robotConf.json') as f:
    robotConf = json.load(f)

#---Import and export mysql table --------------------------------------------------
def export_mysql(ENV, db_name, tab_name):
    "First lead to the proxy host, and then copy the sql file to the console host"
    proxyIP = robotConf[ENV]["proxyIP"]
    mysql_ip = robotConf[ENV]["mysql"][0]
    username=robotConf[ENV]["mysql_account"][0]
    passwd = robotConf[ENV]["mysql_account"][1]
    dir1="/tmp/"
    files="%s%s.sql" % (dir1, tab_name)
    export_cmd="/usr/bin/mysqldump -u%s -p%s -h%s --single-transaction  \
                                                  --set-gtid-purged=OFF \
                                              %s %s > %s 2> /dev/null"  \
              % (username, passwd, mysql_ip, db_name, tab_name, files )
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, export_cmd ), shell=True)
    ## If the host is not controlled,
    ## copy the exported file from the proxy host to the control host
    if ENV != 'pre':
        subprocess.call('%s%s:%s %s' % ('scp ', proxyIP, files, dir1), shell=True)
    print "\nexport %s ENV mysql tab %s to %s dir" % (ENV, tab_name, dir1)


def import_mysql(ENV, db_name, tab_name, rename=0):
    "First copy the sql file to the proxy host, and then import to mysql"
    proxyIP = robotConf[ENV]["proxyIP"]
    mysql_ip = robotConf[ENV]["mysql"][0]
    username=robotConf[ENV]["mysql_account"][0]
    passwd = robotConf[ENV]["mysql_account"][1]
    dir1="/tmp/"
    files="%s%s.sql" % (dir1, tab_name)
    import_cmd="/usr/bin/mysql -u%s -p%s -h%s  %s < %s 2> /dev/null" % \
                         (username, passwd, mysql_ip, db_name, files)
    if not os.path.exists(files):
        print "file %s is no exis ,exit!!!" % files
        sys.exit()
    
    #If you need to rename, modify the export SQL file, 
    #just modify the table name in the contents of the SQL file
    if rename != 0:
        subprocess.call("sed -i 's/`%s`/`%s`/g' %s" % (tab_name, rename, files), shell=True)
    ###If it is not a control host, copy files from the control host to the agent
    if ENV != 'pre':
        subprocess.call('scp %s %s:%s ' % (files, proxyIP, dir1), shell=True)
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, import_cmd ), shell=True)
    print "import %s dir mysql table %s to %s ENV" % (dir1, tab_name, ENV)


def mysql_env_to_env(source_env, target_env, db_name, tab_name, rename=0):
    export_mysql(source_env, db_name, tab_name)
    if rename == 0:
        import_mysql(target_env, db_name, tab_name)
    else:
        import_mysql(target_env, db_name, tab_name, rename)


#---Import and export MongoDB coll----------------------------------------------------------
def export_mongodb(ENV, db_name, tab_name):
    proxyIP = robotConf[ENV]["proxyIP"]
    mongo_ip=robotConf[ENV]["mongodb"][0]
    dir1="/tmp/"
    files="%s%s.json" % (dir1, tab_name)
    export_cmd="/usr/bin/mongoexport -h %s -d %s -c %s -o %s" % \
                (mongo_ip, db_name, tab_name, files)
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, export_cmd ), shell=True)
    if ENV != 'pre':
        subprocess.call('%s%s:%s %s' % ('scp ', proxyIP, files, dir1), shell=True)

    print "\nexport %s ENV MongoDB collection %s to %s dir" % (ENV, tab_name, dir1)


def import_mongodb(ENV, db_name, tab_name, rename=0):
    proxyIP = robotConf[ENV]["proxyIP"]
    mongo_ip=robotConf[ENV]["mongodb"][0]
    dir1="/tmp/"
    files="%s%s.json" % (dir1, tab_name)

    if rename == 0:
        import_cmd="/usr/bin/mongoimport -h %s -d %s -c %s  %s" % \
                   (mongo_ip, db_name, tab_name, files)
    else:
        import_cmd="/usr/bin/mongoimport -h %s -d %s -c %s  %s" % \
                   (mongo_ip, db_name, rename, files)

    if not os.path.exists(files):
        print "file %s is no exis ,exit!!!" % files
        sys.exit()

    if ENV != 'pre':
        subprocess.call('scp %s %s:%s ' % (files, proxyIP, dir1), shell=True)
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, import_cmd ), shell=True)
    
    print "import %s dir mongoDB coll %s to %s ENV" % (dir1, tab_name, ENV)


def mongodb_env_to_env(source_env, target_env, db_name, tab_name, rename=0):
    export_mongodb(source_env, db_name, tab_name)
    if rename == 0:
        import_mongodb(target_env, db_name, tab_name)
    else:
        import_mongodb(target_env, db_name, tab_name, rename)


_env={'1': 'test', '2': 'pre', '3': 'pro'}

menu_list={
"db_type":
"""
----------------------------------------------------
                     DBtools
----------------------------------------------------
  1 MySQL
  2 MongoDB
  q Exit

Please select the number: """,

"mysql_db_menu":
"""
----------------------------------------------------
  1 export mysql table
  2 import mysql table
  3 Cross-environment migration mysql table
  q exit

Please select the number: """,

"mongodb_menu":
"""
----------------------------------------------------
  1 export mongodb collection
  2 import mongodb collection
  3 Cross-environment migration mongodb collection
  q exit
  
Please select the number: """,

"env_menu": 
"""
---------------------------------------------------
  1 test     2 pre     3 pro
  
Please select the number: """,

"source_env_menu": 
"""
----------------------------------------------------
  1 test     2 pre     3 pro

Please select the "source" environment : """,

"target_env_menu": 
'Please select the "Target" environment: '
}

#id_dist = {"drj":"direnjie",
#            "wp":"wandapeng",
#            "lcj":"liangchangliang"
#            }

def quit_page():
    print '\n%s' % ('-' * 50)
    print string.center('Exit!', 50)
    print '%s\n' % ('-' * 50)
    sys.exit(0)


def home_page():
    try:
        choose = raw_input(menu_list["db_type"]).strip()
        if choose == '1':
            mysql_page()
        elif choose == '2':
            mongodb_page()
        else:
            quit_page()
    except (KeyboardInterrupt, EOFError):
        quit_page()


def mysql_page():
    try:
        choose=raw_input(menu_list["mysql_db_menu"]).strip()

        if choose == '1':
            env = _env[raw_input(menu_list["env_menu"]).strip()]
            db_name = raw_input("Please enter the MySQL database name: ").strip()
            tab_name = raw_input("Please enter a table name: ").strip()
            export_mysql(env, db_name, tab_name)
        
        elif choose == "2":
            env = _env[raw_input(menu_list["env_menu"]).strip()]
            db_name = raw_input("Please enter MySQL DBname: ").strip()
            tab_name = raw_input("Please enter a table name: ").strip()
            rename_status=raw_input("Whether you need to rename y/n: ").strip()
            if rename_status == 'y':
                rename = raw_input("Please enter a new name: ").strip()
                import_mysql(env, db_name, tab_name, rename)
            else:
                import_mysql(env, db_name, tab_name)

        elif choose == "3":
            #ID=getpass.getpass("Please enter certification ID:").strip()
            #if ID in id_dist:
            #    print "\nHello %s\n" % id_dist[ID]
            #else:
            #    sys.exit()
            while True:
                source_env = _env[raw_input(menu_list["source_env_menu"]).strip()]
                target_env = _env[raw_input(menu_list["target_env_menu"]).strip()]
                db_name = raw_input("Please enter MySQL DBname: ").strip()
                tab_name = raw_input("Please enter a table name: ").strip()
                rename_status=raw_input("Whether you need to rename y/n: ").strip()
                if rename_status == 'y':
                    rename = raw_input("Please enter a new name: ").strip()
                else:
                    rename = 0
                print "\nfrom \033[0;31m%s\033[0m ENV ====> \033[0;31m%s\033[0m ENV" % (source_env, target_env)
                print "DB    name   ======= \033[0;31m%s\033[0m" % (db_name)
                print "table name   ======= \033[0;31m%s\033[0m" % (tab_name)
                if rename != 0:print "RENAME       ======= \033[0;31m%s\033[0m" % (rename)
                
                _choose=raw_input("Enter y to continue, n re-select\nPlease select(y/n): ")
                if _choose != 'y':continue
                mysql_env_to_env(source_env, target_env, db_name, tab_name, rename)
                break
        elif choose == 'r':
            home_page()
    except (KeyboardInterrupt, EOFError):
        quit_page()


def mongodb_page():
    try:
        choose=raw_input(menu_list["mongodb_menu"]).strip()
        if choose == '1':
            env = _env[raw_input(menu_list["env_menu"]).strip()]
            db_name = raw_input("Please enter database name: ").strip()
            tab_name = raw_input("Please enter collection name: ").strip()
            export_mongodb(env, db_name, tab_name)
        
        elif choose == "2":
            env = _env[raw_input(menu_list["env_menu"]).strip()]
            db_name = raw_input("Please enter database name: ").strip()
            tab_name = raw_input("Please enter collection name: ").strip()
            rename_status=raw_input("Whether you need to rename y/n: ").strip()
            if rename_status == 'y':
                rename = raw_input("Please enter new name: ").strip()
                import_mongodb(env, db_name, tab_name, rename)
            else:
                import_mongodb(env, db_name, tab_name)

        elif choose == "3":
            while True:
                source_env = _env[raw_input(menu_list["source_env_menu"]).strip()]
                target_env = _env[raw_input(menu_list["target_env_menu"]).strip()]
                db_name = raw_input("Please enter database name: ").strip()
                tab_name = raw_input("Please enter collection name: ").strip()
                rename_status=raw_input("Whether you need to rename y/n: ").strip()
                if rename_status == 'y':
                    rename = raw_input("Please enter new name: ").strip()
                else:
                    rename = 0

                print "\nfrom \033[0;31m%s\033[0m ENV ====> \033[0;31m%s\033[0m ENV" % (source_env, target_env)
                print "DB   name ======= \033[0;31m%s\033[0m" % (db_name)
                print "coll name ======= \033[0;31m%s\033[0m" % (tab_name)
                if rename != 0:print "RENAME    ======= \033[0;31m%s\033[0m" % (rename)
                
                _choose=raw_input("Enter y to continue, n re-select\nPlease select(y/n): ")
                if _choose != 'y':continue
                mongodb_env_to_env(source_env, target_env, db_name, tab_name, rename)
                break
        elif choose == 'r':
            home_page()
    except (KeyboardInterrupt, EOFError):
        quit_page()

#-----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    home_page()

