#!/usr/bin/env python
#coding:utf-8
# mysql and mongodb table import export

import os
import json
import string
#import getpass
import subprocess
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


#--------------------- Load the configuration file ---------------------------------
sys.path.append('/oma/deploy/scripts/')
with open('/oma/deploy/scripts/robotConf.json') as f:
    robotConf = json.load(f)

#---导入导出mysql 表------------------------------------------------------------------
def export_mysql(ENV, db_name, tab_name):
    "先导出到代理主机, 再把sql文件复制到控制端主机"
    proxyIP = robotConf[ENV]["proxyIP"]
    mysql_ip = robotConf[ENV]["mysql"][0]
    username=robotConf[ENV]["mysql_account"][0]
    passwd = robotConf[ENV]["mysql_account"][1]
    dir1="/home/developer/"
    files="%s%s.sql" % (dir1, tab_name)
    export_cmd="/usr/bin/mysqldump -u%s -p%s -h%s --single-transaction  \
                                                  --set-gtid-purged=OFF \
                                              %s %s > %s 2> /dev/null"  \
                                        % (username, passwd, mysql_ip, 
                                            db_name, tab_name, files )
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, export_cmd ), shell=True)
    ##如果不是控制主机,则从代理主机复制文件到控制主机
    if ENV != 'pre':
        subprocess.call('%s%s:%s %s' % ('scp ', proxyIP, files, dir1), shell=True)
    print "\n导出 %s 环境mysql表 %s 到 %s 目录" % (ENV, tab_name, dir1)

    #ssh root@172.16.0.3 'mysqldump -umysqloma -pptb-oma-IndefeuUMDOGDNFQZ  \
    #                           -h 172.16.0.211 zeus user > /opt/user.sql'
    #scp root@172.16.0.3:/opt/user.sql /opt/


def import_mysql(ENV, db_name, tab_name, rename=0):
    "先把sql文件复制到代理主机, 再导入到mysql"
    proxyIP = robotConf[ENV]["proxyIP"]
    mysql_ip = robotConf[ENV]["mysql"][0]
    username=robotConf[ENV]["mysql_account"][0]
    passwd = robotConf[ENV]["mysql_account"][1]
    dir1="/home/developer/"
    files="%s%s.sql" % (dir1, tab_name)
    import_cmd="/usr/bin/mysql -u%s -p%s -h%s  %s < %s 2> /dev/null" % \
                         (username, passwd, mysql_ip, db_name, files)
    if not os.path.exists(files):
        print "file %s is no exis ,exit!!!" % files
        sys.exit()
    
    #如果需要重命名,则修改导出的sql文件,  只是修改sql文件内容中的table name
    #文件名称不变
    if rename != 0:
        subprocess.call("sed -i 's/`%s`/`%s`/g' %s" % (tab_name, rename, files), shell=True)
    ###如果不是控制主机,则从控制主机复制文件到代理机
    if ENV != 'pre':
        subprocess.call('scp %s %s:%s ' % (files, proxyIP, dir1), shell=True)
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, import_cmd ), shell=True)
    print "导入 %s 目录mysql表 %s 到 %s 环境" % (dir1, tab_name, ENV)


def mysql_env_to_env(source_env, target_env, db_name, tab_name, rename=0):
    export_mysql(source_env, db_name, tab_name)
    if rename == 0:
        import_mysql(target_env, db_name, tab_name)
    else:
        import_mysql(target_env, db_name, tab_name, rename)



#---导入导出mongoDB------------------------------------------------------------------
def export_mongodb(ENV, db_name, tab_name):
    proxyIP = robotConf[ENV]["proxyIP"]
    mongo_ip=robotConf[ENV]["mongodb"][0]
    dir1="/home/developer/"
    files="%s%s.json" % (dir1, tab_name)
    export_cmd="/usr/bin/mongoexport -h %s -d %s -c %s -o %s" % \
                (mongo_ip, db_name, tab_name, files)
    subprocess.call('%s%s "%s"'  % ('ssh ', proxyIP, export_cmd ), shell=True)
    ##如果是控制主机,则跳过复制
    if ENV != 'pre':
        subprocess.call('%s%s:%s %s' % ('scp ', proxyIP, files, dir1), shell=True)

    print "\n导出 %s 环境mongoDB集合 %s 到 %s 目录" % (ENV, tab_name, dir1)


def import_mongodb(ENV, db_name, tab_name, rename=0):
    "先把json文件复制到代理主机, 再导入到mongodb"
    proxyIP = robotConf[ENV]["proxyIP"]
    mongo_ip=robotConf[ENV]["mongodb"][0]
    dir1="/home/developer/"
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
    
    print "导入 %s 目录mongoDB集合 %s 到 %s 环境" % (dir1, tab_name, ENV)


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
-----------------------------------------
          数据库导入导出工具
-----------------------------------------
  1 MySQL
  2 MongoDB
  q Exit

请选择编号: """,

"mysql_db_menu":
"""
-----------------------------------------
  1 导出mysql表
  2 导入mysql表
  3 跨环境迁移mysql表
  q exit

请选择编号: """,

"mongodb_menu":
"""
-----------------------------------------
  1 导出mongodb集合
  2 导入mongodb集合
  3 跨环境迁移mongodb集合
  q exit
  
请选择编号: """,

"env_menu": 
"""----------------------------------------
  1 测试环境    2 预生产环境    3 丰台晓月
  
请选择编号: """,
  
"source_env_menu": 
"""-----------------------------------------------------
  1 测试环境(test)  2 预生产环境(pre)   3 丰台晓月(pro)

请选择 "源" 环境: """,

"target_env_menu": 
"""请选择 "目标" 环境: """

}

#id_dist = {"drj":"狄仁杰",
#            "wp":"万大鹏",
#            "lcj":"亮亮",
#            "mc":"Big Data engineer",
#            "zx":"Automatic test engineer",
#            "wgh":"王冠华",
#            "fzk":"傅作奎",
#            "hw":"胡伟"
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
            db_name = raw_input("请输入MySQL数据库名称: ").strip()
            tab_name = raw_input("请输入MySQL表名称: ").strip()
            export_mysql(env, db_name, tab_name)
        
        elif choose == "2":
            env = _env[raw_input(menu_list["env_menu"]).strip()]
            db_name = raw_input("请输入MySQL数据库名称: ").strip()
            tab_name = raw_input("请输入MySQL表名称: ").strip()
            rename_status=raw_input("是否重命名要导入的表 y/n: ").strip()
            if rename_status == 'y':
                rename = raw_input("请输入新名称: ").strip()
                import_mysql(env, db_name, tab_name, rename)
            else:
                import_mysql(env, db_name, tab_name)

        elif choose == "3":
            #ID=getpass.getpass("请输入认证ID:").strip()
            #if ID in id_dist:
            #    print "\nHello %s\n" % id_dist[ID]
            #else:
            #    sys.exit()
            while True:
                source_env = _env[raw_input(menu_list["source_env_menu"]).strip()]
                target_env = _env[raw_input(menu_list["target_env_menu"]).strip()]
                db_name = raw_input("请输入MySQL数据库名称: ").strip()
                tab_name = raw_input("请输入MySQL表名称: ").strip()
                rename_status=raw_input("是否重命名要导入的表 y/n: ").strip()
                if rename_status == 'y':
                    rename = raw_input("请输入新名称: ").strip()
                else:
                    rename = 0
                print "\n从\033[0;31m%s\033[0m环境 ====> \033[0;31m%s\033[0m环境" % (source_env, target_env)
                print "数据库   ======= \033[0;31m%s\033[0m" % (db_name)
                print "表名称   ======= \033[0;31m%s\033[0m" % (tab_name)
                if rename != 0:print "重命名后名称 === \033[0;31m%s\033[0m" % (rename)
                
                _choose=raw_input("输入y继续, n重新选择\n请选择(y/n): ")
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
            db_name = raw_input("请输入数据库名称: ").strip()
            tab_name = raw_input("请输入集合名称: ").strip()
            export_mongodb(env, db_name, tab_name)
        
        elif choose == "2":
            env = _env[raw_input(menu_list["env_menu"]).strip()]
            db_name = raw_input("请输入数据库名称: ").strip()
            tab_name = raw_input("请输入集合名称: ").strip()
            rename_status=raw_input("是否重命名要导入的集合 y/n: ").strip()
            if rename_status == 'y':
                rename = raw_input("请输入新名称: ").strip()
                import_mongodb(env, db_name, tab_name, rename)
            else:
                import_mongodb(env, db_name, tab_name)

        elif choose == "3":
            while True:
                source_env = _env[raw_input(menu_list["source_env_menu"]).strip()]
                target_env = _env[raw_input(menu_list["target_env_menu"]).strip()]
                db_name = raw_input("请输入数据库名称: ").strip()
                tab_name = raw_input("请输入集合名称: ").strip()
                rename_status=raw_input("是否重命名要导入的集合 y/n: ").strip()
                if rename_status == 'y':
                    rename = raw_input("请输入新名称: ").strip()
                else:
                    rename = 0

                print "\n从\033[0;31m%s\033[0m环境 ====> \033[0;31m%s\033[0m环境" % (source_env, target_env)
                print "数据库   ======= \033[0;31m%s\033[0m" % (db_name)
                print "集合名称 ======= \033[0;31m%s\033[0m" % (tab_name)
                if rename != 0:print "重命名后名称 === \033[0;31m%s\033[0m" % (rename)
                
                _choose=raw_input("输入y继续, n重新选择\n请选择(y/n): ")
                if _choose != 'y':continue
                mongodb_env_to_env(source_env, target_env, db_name, tab_name, rename)
                break
        elif choose == 'r':
            home_page()
    except (KeyboardInterrupt, EOFError):
        quit_page()

#-----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    #export_mysql('pro', 'zeus', 'user')
    #import_mysql('pro', 'ze', 'user', 'user9')
    #mysql_env_to_env('pro','pre','zeus', 'Internal_users')
    
    home_page()
    #import_mongo("pre","uranus","schedule","schedule2")

# mysql -umysqloma -pptb-oma-IndefeuUMDOGDNFQZ -h 192.168.200.145 zeus1 <user.sql
# ssh 172.16.0.3 'mysql -umysqloma -pptb-oma-IndefeuUMDOGDNFQZ -h 172.16.0.211 zeus user  > /opt/user.sql'
# scp 172.16.0.3:/opt/user.sql /opt/


