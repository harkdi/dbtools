#!/usr/bin/env python
# coding: utf-8

from __future__ import unicode_literals
import os
import re
import time
import json
import datetime
import operator
import subprocess
import mysql.connector
from pymongo import MongoClient
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#import robotConf
#robotConf = robotConf.robotConf

#--------------------- Load the configuration file ------------------------------------------------------
sys.path.append('/oma/deploy/scripts/')
with open('/oma/deploy/scripts/robotConf.json') as f:
    robotConf = json.load(f)


def exeTime1(func):
    def _wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print  "%s cost %s second" % (func.__name__, time.time() - start)
        return result
    return _wrapper
#--------------------- mysql_resource_use START ------------------------------------------------------
#@exeTime1
def mysql_resource_use(IP,*ResourceName):
    """资源使用查看  参数 IP  , 资源名称"""
    #由于用户名密码相同,在此暂时使用 pro
    dbconfig = [ IP,   robotConf['pro']["mysql_account"][0], robotConf['pro']["mysql_account"][1] ]
    # 增删查改 查询结果按字母大小排序
    sql1 = "show global status where Variable_name in \
           ( 'com_select','com_insert','com_delete','com_update','Queries', 'Threads_connected')"
    # mysql net in out
    sql2 = "show global status like 'Bytes_%'"
    m=MySQL(*dbconfig)
    RS1 = m.query(sql1)
    mysql_net1 = m.query(sql2)

    cpu_num=os.popen("ssh root@%s 'grep -c processor /proc/cpuinfo'" % IP).read().strip()
    #PID = subprocess.call("ssh root@%s 'pidof mysqld'" % IP, shell=True).read().strip()
    PID = os.popen("ssh root@%s 'pidof mysqld'" % IP).read().strip()
    if PID is None:
        PROC_STAT ="off"
    else:
        PROC_STAT = "on"
    if PID == 0: PID = 1
    CPU,MEM =  os.popen("ssh root@%s 'top -b -n 1|grep -v grep|grep mysqld'" % IP).read().strip().split()[8:10]
    CPU=int(float(str(CPU))) / int(cpu_num)
    portConn = os.popen("ssh root@%s 'netstat -an|grep :3306|wc -l '" % (IP)).read().strip()
    io_r1, io_w1 = os.popen("ssh root@%s 'cat /proc/%s/io '" % (IP, PID)).read().split()[9:12:2]
    time.sleep(1)

    io_r2, io_w2 = os.popen("ssh root@%s 'cat /proc/%s/io '" % (IP, PID)).read().split()[9:12:2]
    ## 单位为MB
    io_r=(int(io_r2)-int(io_r1)) / 1024 / 1024
    io_w=(int(io_w2)-int(io_w1)) / 1024 / 1024

    
    # mysql opcounters
    RS2 = m.query(sql1)
    mysql_net2 = m.query(sql2)
    m.close()
    #-----------------------结果2转为int型-------------结果1转为int型--------
    RS = map(operator.sub, [int(n) for m, n in RS2], [int(n) for m, n in RS1])
    mysql_net_in  = int(mysql_net2[0][1]) - int(mysql_net1[0][1])
    mysql_net_out = int(mysql_net2[1][1]) - int(mysql_net1[1][1])

    if io_r is None:
        io_r = 0
        io_w = 0
    return PROC_STAT, CPU, MEM, io_r, io_w, RS[0], RS[1], RS[2], RS[3],RS[4], RS[5], portConn, mysql_net_in, mysql_net_out

#--------------------- MySQL class -------------------------------------------------
class MySQL:  
    def __init__(self,host,user,password,charset="utf8"):  
        self.host=host  
        self.user=user  
        self.password=password  
        self.charset=charset  
        try:  
            self.conn=mysql.connector.connect(host=self.host,        user = self.user, 
                                              password=self.password,charset=self.charset)   
            self.cur=self.conn.cursor()  
        except mysql.connector.Error as e:  
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))  

    def query(self,sql):
        self.cur.execute(sql)
        return self.cur.fetchall()
    
    def close(self):  
        self.cur.close()  
        self.conn.close()

#--------------- 独立mysql查询统计 qps query insert delete update ---------------

def mysql_opcounters(ENV):
    #### -------------|----------MYSQL IP -----------|------------user --------------------|-----------  password -----------
    master_dbconfig = [ robotConf[ENV]["mysql"][0], robotConf[ENV]["mysql_account"][0], robotConf[ENV]["mysql_account"][1] ]
    slave_dbconfig  = [ robotConf[ENV]["mysql"][1], robotConf[ENV]["mysql_account"][0], robotConf[ENV]["mysql_account"][1] ]
    mysql_config= [master_dbconfig, slave_dbconfig]
    mysql_cluster_status=['Master','Slave','Slave']
    # 增删查改 查询结果按字母大小排序
    sql1 = "show global status where Variable_name in \
           ( 'com_select','com_insert','com_delete','com_update','Queries', 'Threads_connected')"
    #mysql net
    sql2 = "show global status like 'Bytes_%'"

    print '-' * 90, "\n%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s" % \
    ('select', 'insert', 'update', 'delete', 'Queries', 'portConn', "status", "IP")
    for conf,status in zip(mysql_config, mysql_cluster_status):
        m=MySQL(*conf)
        RS1 = m.query(sql1)
        mysql_net1 = m.query(sql2)
    
        time.sleep(1)
        RS2 = m.query(sql1)
        mysql_net2 = m.query(sql2)
        m.close()
        #-----------------------结果2转为int型-------------结果1转为int型--------
        RS = map(operator.sub, [int(n) for m, n in RS2], [int(n) for m, n in RS1])
        mysql_net_in  = int(mysql_net2[0][1]) - int(mysql_net1[0][1])
        mysql_net_out = int(mysql_net2[1][1]) - int(mysql_net1[1][1])
        print "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s" % \
            (RS[0], RS[1], RS[2], RS[3],RS[4], RS[5], status, conf[0])



def mysql_long_query_proc(ENV):
    #### -------------|----------MYSQL IP -----------|------------user --------------------|-----------  password -----------
    master_dbconfig = [ robotConf[ENV]["mysql"][0], robotConf[ENV]["mysql_account"][0], robotConf[ENV]["mysql_account"][1] ]
    sql = "show full processlist"
    master=MySQL(*master_dbconfig)
    result = master.query(sql)
    master.close()

    print "%-10s %-10s %-25s %-12s %-17.17s %-8s %-50.50s" % \
           ("Id", "User", "Host", "DB", "Command", "Time", "State")
    #不是睡眠进程且查询时间大于10秒  则打印
    for i in result:
        if i[4] != 'Sleep' and i[5] > 10:
            print "%-10s %-10s %-25s %-12s %-17.17s %-8s %-50.50s" % \
            (i[0]  ,i[1],  i[2],  i[3],  i[4],  i[5],  i[6])


def mysql_sync_status(ENV):
    # mysql集群数量小于2
    if len(robotConf[ENV]['mysql']) < 2:
        print "mysql cluster only one, exit"
        return
    #### -------------|----------MYSQL IP -----------|------------user --------------------|-----------  password -----------
    slave_dbconfig = [ robotConf[ENV]["mysql"][1], robotConf[ENV]["mysql_account"][0], robotConf[ENV]["mysql_account"][1] ]
    sql = "show slave status"
    slave=MySQL(*slave_dbconfig)
    result = slave.query(sql)
    slave.close()
    s = result[0]

    ##查询结果没有标题, 需要自定义
    Title = ["Master_Host","Master_User","Master_Port","Master_Log_File","Relay_Log_File", \
             "Relay_Master_Log_File", "Slave_IO_Running","Slave_SQL_Running",\
             "Seconds_Behind_Master","Last_IO_Error"]
    Value = [s[1], s[2], s[3], s[5], s[7], s[9], s[10], s[11], s[32], s[37]]
    print '-'*60
    for i,j in zip(Title, Value):
        print  "%25s: %-50s" % (i, j)


def difference_gt(ENV, NUM):
    '''媒体报价差大于20'''
    #从配置文件中取出mysql ip user password
    #### -------------|----------MYSQL IP -----------|------------user --------------------|-----------  password -----------
    master_dbconfig = [ robotConf["pro"]["mysql"][0], robotConf["pro"]["mysql_account"][0], robotConf["pro"]["mysql_account"][1] ]

    sql="select  p.product_name, p.pmid,group_concat( p.price) \
         from payment.ptb_product as p join zeus.user as z     \
         on z.id = p.owner_id \
         where p.status=1 and p.price>0 \
         group by p.pmid  \
         order by count(p.price) desc ;"
    master=MySQL(*master_dbconfig)
    result=master.query(sql)

    print "%-15s %-15s %-15s %-20s %-15s" % ('max_price','min_price','percentage', 'pmid', 'product_name')
    for i,j,k in result:
        p = k.split(',')
        price = [int(e) for e in p]
        #print price
        max_price=float(max(price))
        min_price=float(min(price))
        diff = ( max_price - min_price ) / max_price * 100
        if diff > int(NUM):
            print "%-15s %-15s %-15.2f %-20s %-15s" %  (max_price, min_price, diff, j, i)

#@exeTime1
def mysql_debris(ENV,num=6):
    # mysql集群数量小于2
    if len(robotConf[ENV]['mysql']) < 2:
        print "mysql cluster only one, exit"
        return
    """ 检测mysql master大表碎片化程度 
    计算公式 (master表大小 - slave表大小) / master表大小
    show table status from  zeus;
    use information_schema;
    select TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH ,(DATA_LENGTH + INDEX_LENGTH) as ALL_LENGTH  
    from tables  order by DATA_LENGTH desc limit 10 ;
    """
    #从配置文件中取出mysql ip user password
    #### -------------|----------MYSQL IP -----------|------------user --------------------|-----------  password -----------
    master_dbconfig = [ robotConf[ENV]["mysql"][0], robotConf[ENV]["mysql_account"][0], robotConf[ENV]["mysql_account"][1] ]
    slave_dbconfig  = [ robotConf[ENV]["mysql"][1], robotConf[ENV]["mysql_account"][0], robotConf[ENV]["mysql_account"][1] ]

    # 查排名前5的大表
    sql1="select TABLE_NAME, (DATA_LENGTH + INDEX_LENGTH) from information_schema.TABLES order by DATA_LENGTH desc limit %s"
    sql2="select TABLE_NAME, (DATA_LENGTH + INDEX_LENGTH) from information_schema.TABLES where \
                                            table_name in ('%s', '%s', '%s', '%s', '%s','%s')"

    master=MySQL(*master_dbconfig)
    master_tablename_result = master.query(sql1 % num)
    # 从结果中取大表名称 
    tabName =  [i for i,j in master_tablename_result]
    
    #master_result = master.query(sql2 % (tabName[0], tabName[1], tabName[2], tabName[3], tabName[4], tabName[5]))
    master_result = master.query(sql2 % (tuple(tabName)))
    master.close()

    slave=MySQL(*slave_dbconfig)
    slave_result = slave.query(sql2 % (tuple(tabName)))
    slave.close()
    #print "bigTableNam=" , tabName
    #print "master_tablename_result=",master_tablename_result
    #print "master_result=",master_result
    #print "slave_result=",slave_result
    #查询完成 开始计算
    #由于查询没有按tabName 列表中的顺序进行,是按字母进行排序,重新取tablename
    tableName = [i for i,j in master_result]
    masterTableLen = [float(j) for i,j in master_result]
    slaveTableLen  = [j for i,j in slave_result]
    #master表大小 - slave表大小
    difference = map(operator.sub, masterTableLen, slaveTableLen)
    #碎片化百分比
    precent = map(operator.div, difference, masterTableLen)

    print "%-30s %-14s %-20s" % ('TableName', u'碎片化百分比', 'TableSize(GB)')
    for i,j,k in zip(tableName, precent, masterTableLen):
        # 如果百分比为负数 , 则修改为0
        if j < 0:
            j = 0
        print "%-30s %-20.1f %-20.3f" % (i, j*100, k/1024/1024/1024)


#--------------------- 查询大于1000ms数量 ------------------------------------------------------

def mysql_slow_query(ENV):
    master_ip = robotConf[ENV]["mysql"][0]
    mysql_slowquery_num=os.popen("ssh root@%s 'grep -Poc 'Query_time' /var/log/mysql/slowquery.log'" \
                                 % master_ip).read().strip()
    print "%-17s %-12s" % ('IP', 'slow_query_number')
    print "%-22s %-12s" % (master_ip, mysql_slowquery_num)

#--------------------- print mysql status ------------------------------------------------------
#@exeTime1
def mysql_status(TYPE,ENV):
    """print mysql_status"""
    #print 'mysql_status_ENV =',ENV
    # 从配置文件中取集群所有IP
    mysqlClusIP = robotConf[ENV]['mysql']
    mysql_cluster_num = len(mysqlClusIP)
    mysql_cluster_status=['Master','Slave','Slave']

    if TYPE == 'proc':
        print '-' * 70 , "\n%-9s %-4s %-6s %-8s %-8s %-11s %-8s" % \
        ('proc-stat', 'cpu', 'mem', 'io_read', 'io_write', 'status', 'IP')
        pool = ThreadPool(3)
        result = pool.map(mysql_resource_use, mysqlClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mysql_cluster_status, mysqlClusIP):
            print "%-9s %-4s %-6s %-8s %-8s %-11s %-8s" % \
            (i[0], i[1], i[2], i[3], i[4], status, ip)

    elif TYPE == 'net':
        print '-' * 60, "\n%-10s %-10s %-10s %-11s %-8s" % \
              ('portConn', 'netIn', 'netOut', 'status', 'IP')
        pool = ThreadPool(3)
        result = pool.map(mysql_resource_use, mysqlClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mysql_cluster_status, mysqlClusIP):
            print "%-10s %-10s %-10s %-11s %-8s" % \
            (i[0], i[1], i[2], status, ip)


    elif TYPE == 'opcounters':
        print '-' * 90, "\n%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s" % \
        ('select', 'insert', 'update', 'delete', 'Queries', 'portConn', "status", "IP")
        pool = ThreadPool(3)
        result = pool.map(mysql_resource_use, mysqlClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mysql_cluster_status, mysqlClusIP):
            print "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s" % \
            (i[0], i[1], i[2], i[3], i[4], i[5],  status, ip)

    elif TYPE == 'all':
        print "%-4s %-4s %-6s %-8s %-8s %-6s %-6s %-6s %-6s %-10s %-10s %-10s %-10s %-10s %-10s" % \
        ("stat", "cpu", "mem", "io_read", "io_write", \
         "select", "insert", "update", "delete", "Queries", \
         "portConn", "netIn", "netOut", "status", "IP")

        pool = ThreadPool(3)
        result = pool.map(mysql_resource_use, mysqlClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mysql_cluster_status, mysqlClusIP):
            print "%-4s %-4s %-6s %-8s %-8s %-6s %-6s %-6s %-6s %-10s %-10s %-10s %-10s %-10s %-10s" % \
            (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], \
             i[10],  i[11],  i[12],  status, ip)


    elif TYPE == 'long_query':
        mysql_long_query_proc(ENV)

    elif TYPE == 'slow_query':
        mysql_slow_query(ENV)

    elif TYPE == 'sync_status':
        mysql_sync_status(ENV)

    elif TYPE == 'debris':
        mysql_debris(ENV)

if __name__ == '__main__':
    #--------------------- TYPE -------- ENV ---
    exec "mysql_status(sys.argv[1], sys.argv[2])"
    #mysql_status('proc','pre')
    #mysql_status('net','pre')
    #mysql_status('opcounters','pre')
    #mysql_status('all','pre')
    #mysql_long_query_prec('pre')
    #mysql_sync_status('pre')
    #mysql_debris('pre')
    #difference_gt('pre','50')
    #mysql_opcounters('pre')
    #mysql_slow_query('pre')

