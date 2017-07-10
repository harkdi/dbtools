#!/usr/bin/env python
# coding=utf-8

import time
import json
import string
import subprocess
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#--------------------- Load the configuration file ------------------------------------------------------
sys.path.append('/oma/deploy/scripts/')
with open('/oma/deploy/scripts/robotConf.json') as f:
    robotConf = json.load(f)

#----------------全局配置文件
mongodbPY = "/oma/deploy/scripts/mongodb_check.py"
mysqlPY   = "/oma/deploy/scripts/mysql_check.py"

#----------------同步配置文件到pro 和test 环境---------------------------------
def sync_config_file():
    for ip in [ robotConf['test']["proxyIP"], robotConf['pro']["proxyIP"] ]:
        subprocess.call('scp -q /oma/deploy/scripts/mongodb_check.py \
                                /oma/deploy/scripts/mysql_check.py \
                                /oma/deploy/scripts/robot.py \
                                /oma/deploy/scripts/robotConf.json \
                                %s:/oma/deploy/scripts/' % ip, shell=True) 
sync_config_file()
## 定义页面菜单 --------------------------------------------------------

subprocess.call('clear', shell=True)

menu_list = { 'home_page_menu':
"""
--------------------------
 数据库监控及状态查看工具
--------------------------
 1 测试环境
 2 预生产环境
 3 生产环境
 q Quit

请选择编号: """ ,


'db_menu':
"""
1 MongoDB状态查看
2 Mongodb监控
3 mysql状态查看
4 mysql监控
r 返回主菜单
q Quit

请选择编号: """ ,

'mongodb_status_menu':
"""
-------------------
mongodb_status_menu
-------------------
1 进程CPU内存状态
2 网络流量和连接数
3 数据库增删查改
4 所有状态查看
5 mongodb数据报告
r 返回主菜单
q Quit

请选择编号: """ ,

'mongodb_monitor_menu':
"""
---------------------
mongodb_monitor_menu
---------------------
1 慢查询大于500ms数量
2 长时间查询进程
3 oplog信息
4 主从同步延时
r 返回主菜单
q Quit

请选择编号: """ ,


'mysql_status_menu':
"""
-----------------
mysql_status_menu
-----------------
1 进程CPU内存状态
2 网络流量和连接数
3 数据库增删查改
4 所有状态查看
r 返回主菜单
q Quit

请选择编号: """ ,

'mysql_monitor_menu':
"""
-------------------
mysql_monitor_menu
-------------------
1 慢查询大于1000ms数量
2 长时间查询进程
3 主从同步状态
4 大表碎片化程度
r 返回主菜单
q Quit

请选择编号: """ ,
}


#------------- 定义页面函数 -------------------
def quit_page():
    print '\n%s' % ('-' * 50)
    print string.center('感谢您的使用，再见!', 50)
    print '%s\n' % ('-' * 50)
    sys.exit(0)


def invalid_input_output():
    print '\n\033[1;31m%s\033[0m' % ('!' * 50)
    print '\033[1;31m输入有误，请重新输入:\033[0m'
    print '\033[1;31m%s\033[0m\n' % ('!' * 50)

#---------------------------------------------------------------------------------------------
def home_page():
    global ENV, proxyIP
    subprocess.call('clear', shell=True)
    _env={'1':'test', '2': 'pre', '3': 'pro'}
    all_choices = {'1': db_select_page , '2': db_select_page, '3': db_select_page, 'q': quit_page}
    while True:
        try:
            choice = raw_input(menu_list['home_page_menu']).strip()
            if    choice == 'q':
                quit_page()
            elif  choice  in [m for m in all_choices]:
                break
        except (KeyboardInterrupt, EOFError):
            quit_page()
    
    #根据数字选择环境名称 创建全局变量
    ENV = _env[choice]
    proxyIP = robotConf[ENV]["proxyIP"]
    all_choices[choice]()

#---------------------------------------------------------------------------------------------
def db_select_page():
    subprocess.call('clear', shell=True)
    all_choices = {'1': mongodb_status_page, '2': mongodb_monitor_page, 
                   '3': mysql_status_page,   '4': mysql_monitor_page,
                   'r': home_page,           'q': quit_page}
    try:
        choice = raw_input(menu_list['db_menu']).strip()
        if choice not in [m for m in all_choices]: db_select_page()
    except (KeyboardInterrupt, EOFError):
        quit_page()

    all_choices[choice]()

#---------------------------------------------------------------------------------------------
def mongodb_execution(ARG):
    '''ENV 环境变量在全局已生成'''
    subprocess.call('%s%s %s' % ('ssh root@', proxyIP, '%s %s %s' % (mongodbPY, ARG, ENV) ), shell=True)

def mongodb_status_page():
    #subprocess.call('clear', shell=True)
    all_choices = {'1': 'proc', '2': 'net', '3':'opcounters', '4':'all', '5':'report'}
    while 1:
        try:
            choice = raw_input(menu_list['mongodb_status_menu']).strip()
        except (KeyboardInterrupt, EOFError):
            quit_page()
        
        if   choice == '5' and ENV != 'pro':
            print '\n!!!!!只有生产环境有MongoDB数据报告!!!!!,请重新选择'
            mongodb_status_page()
        elif choice in [m for m in all_choices]:
            mongodb_execution(all_choices[choice])
        elif choice == "r": home_page()
        elif choice == "q": quit_page()


def mongodb_monitor_page():
    subprocess.call('clear', shell=True)
    all_choices = {'1': 'slow_query', '2': 'long_query', '3':'oplog', '4':'sync_delay'}
    while 1:
        try:
            choice = raw_input(menu_list['mongodb_monitor_menu']).strip()
        except (KeyboardInterrupt, EOFError):
            quit_page()
        if   choice in [m for m in all_choices]:
            mongodb_execution(all_choices[choice])
        elif choice == "r": home_page()
        elif choice == "q": quit_page()


def mysql_execution(CMD):
    '''ENV 环境变量在全局已生成'''
    subprocess.call('%s%s %s' % ('ssh root@', proxyIP, '%s %s %s' % (mysqlPY, CMD, ENV) ), shell=True)

def mysql_status_page():
    #subprocess.call('clear', shell=True)
    all_choices = {'1': 'proc', '2': 'net', '3':'opcounters', '4':'all'}
    while 1:
        try:
            choice = raw_input(menu_list['mysql_status_menu']).strip()
        except (KeyboardInterrupt, EOFError):
            quit_page()
        if   choice in [m for m in all_choices]:
            mysql_execution(all_choices[choice])
        elif choice == "r": home_page()
        elif choice == "q": quit_page()

def mysql_monitor_page():
    #subprocess.call('clear', shell=True)
    all_choices = {'1': 'slow_query', '2': 'long_query', '3':'sync_status', '4':'debris'}
    while 1:
        try:
            choice = raw_input(menu_list['mysql_monitor_menu']).strip()
        except (KeyboardInterrupt, EOFError):
            quit_page()
        if   choice in [m for m in all_choices]:
            mysql_execution(all_choices[choice])
        elif choice == "r": home_page()
        elif choice == "q": quit_page()


# -------------------------- #
if __name__ == '__main__':
    home_page()
