#!/usr/bin/env python
# coding: utf-8
#from __future__ import unicode_literals
import os
import re
import time
import json
import pprint
import datetime
import operator
import mysql.connector
from pymongo import MongoClient
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


#------------------------------------Load the configuration file-------------------------------------
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
#-------------------------------------------输出MongoDB数据报告--------------------------------------------
def mongodb_check_report(day1=1, day2=1):
    # 创建连接
    config = {
              'user':'oma', 
              'password':'ptb-dc-oma', 
              'host':'172.16.0.3', 
              'port':3306,  
              'database':'oma'}
    
    conn = mysql.connector.connect(**config)
    # 创建游标
    cur = conn.cursor()
    # 一天前信息
    d1 = "select * from system_storage_info order by statistics_time desc limit %s"   % day1
    # 两天前信息
    d2 = "select * from system_storage_info order by statistics_time desc limit %s,1" % day2

    cur.execute(d1)
    result1 = cur.fetchall()
    cur.execute(d2)
    result2 = cur.fetchall()
    
    # 关闭游标和连接
    cur.close()
    conn.close()

    a1 = result1[0][2:8]    ##取出文章媒体数量
    a2 = result2[0][2:8]
    b1 = result1[0][8:]     ##取出集合 库大小信息
    b2 = result2[0][8:]    
    
    ##方法一
    #c = numpy.array(a1) - numpy.array(a2)          ## 文章媒体数量差 列表相减  一天前 - 两天前
    #d = numpy.array(b1) - numpy.array(b2)          ## 集合大小差
    
    ##方法二
    c = map(operator.sub, a1, a2)
    d = map(operator.sub, b1, b2)

    gb = 1024 ** 3.0
    
    ## 时间格式转换  datetime.datetime(2017, 3, 30, 23, 59, 1)  ==>  2017-03-30 23:59:01
    t1=result1[0][1].strftime('%Y-%m-%d %X')
    t2=result2[0][1].strftime('%Y-%m-%d %X')
    time_range = '\n时间范围     ' + t2 + ' - ' + t1

    print  time_range
    print '-' * 87
    print "类型         所有文章     微信文章     微博文章     所有媒体     微信媒体     微博媒体"
    print '-' * 87
    print "%-14s %-12s %-12s %-12s %-12s %-12s %-12s"      %  \
    ('总数',   a1[0], a1[1], a1[2], a1[3], a1[4], a1[5])
    print "%-14s %-12s %-12s %-12s %-12s %-12s %-12s\n"    %  \
    ('增减',    c[0],  c[1],  c[2],  c[3],  c[4],  c[5])
    
    print '-' * 73
    print "名称         GAIA2库      wxAtricle    wbArticle    wxMedia      wbMedia"
    print '-' * 73
    print "%-14s %-12.3f %-12.3f %-12.3f %-12.3f %-12.3f"    %  \
    ('大小 (GB)', b1[0]/gb, b1[1]/gb, b1[2]/gb, b1[3]/gb, b1[4]/gb)
    print "%-14s %-12.3f %-12.3f %-12.3f %-12.3f %-12.3f\n"  %  \
    ('增减 (GB)',  d[0]/gb,  d[1]/gb,  d[2]/gb,  d[3]/gb,  d[4]/gb)

#--------------- print MongoDB status cpu mem network io 增删查改 ------------------------------
#--------------------- mongo_resource_use START ------------------------------------------------------
#@exeTime1
def mongo_resource_use(IP,*ResourceName):
    """资源使用查看  参数 IP  , 资源名称"""
    cpu_num=os.popen("ssh root@%s 'grep -c processor /proc/cpuinfo'" % IP).read().strip()
    PID = os.popen("ssh root@%s 'pidof mongod'" % IP).read().strip()
    if PID > 0:
        PROC_STAT ="on"
    else:
        PROC_STAT="off"
    CPU,MEM =  os.popen("ssh root@%s 'top -b -n 1|grep -v grep|grep mongod'" % IP).read().strip().split()[8:10]
    CPU=int(float(str(CPU))) / int(cpu_num)
    portConn = os.popen("ssh root@%s 'netstat -an|grep :27017|wc -l '" % (IP)).read().strip()
    io_r1, io_w1 = os.popen("ssh root@%s 'cat /proc/%s/io '" % (IP, PID)).read().split()[9:12:2]
    opcounters1 = eval(os.popen("mongo --quiet %s --eval 'db.serverStatus().opcounters'" % (IP))\
                 .read().replace('NumberLong',''))
    network1 = eval(os.popen("mongo --quiet %s --eval 'db.serverStatus().network'" % (IP))\
                  .read().replace('NumberLong',''))
    time.sleep(1)

    io_r2, io_w2 = os.popen("ssh root@%s 'cat /proc/%s/io '" % (IP, PID)).read().split()[9:12:2]
    ## 单位为MB
    io_r=(int(io_r2)-int(io_r1)) / 1024 / 1024
    io_w=(int(io_w2)-int(io_w1)) / 1024 / 1024
    opcounters2 = eval(os.popen("mongo --quiet %s --eval 'db.serverStatus().opcounters'" % (IP))\
                                                           .read().replace('NumberLong',''))
    # 取字典VALUE 用列表控制字典查询顺序
    KEY = ['query','insert','update','delete','getmore','command']
    RS = map(operator.sub, [opcounters2[i] for i in KEY], [opcounters1[i] for i in KEY])
    network2 = eval(os.popen("mongo --quiet %s --eval 'db.serverStatus().network'" % (IP))\
               .read().replace('NumberLong',''))
    network_in    = int(network2['bytesIn'])  - int(network1['bytesIn'])
    network_out   = int(network2['bytesOut']) - int(network1['bytesOut'])
    return PROC_STAT, CPU, MEM, io_r, io_w, RS[0], RS[1], RS[2], RS[3],RS[4], RS[5], portConn, network_in, network_out

#--------------------- mongo_resource_use END --------------------------------------------------


#--------------------- mongodb_monitor ------------------------------------------------------
#@exeTime1
def mongodb_slow_query(ENV):
    mongoClusIP = robotConf[ENV]['mongodb']
    print "%-17s %-12s" % ('IP', 'slow_query_number')
    for ip in  mongoClusIP:
        mongo_slow_num=os.popen("mongo --quiet %s --eval 'rs.slaveOk();db.system.profile.find\
        ({millis:{$gt:500}}).count()'" % (ip)).read().strip()
        print "%-22s %-12s" % (ip, mongo_slow_num)

#@exeTime1
def mongodb_long_query_proc(ENV):
    mongoClusIP = robotConf[ENV]['mongodb']
    KEY=["opid", "secs_running", "ns", "desc"]
    print "MongoDB长时间查询进程","\n",'-'*90
    print  "%-17s %-12s %-15s %-25s %-15s" % ("IP", KEY[0], KEY[1], KEY[2], KEY[3] ),"\n",'-'*90
    for ip in  mongoClusIP:
        mongo_long_query = os.popen("mongo --quiet %s --eval 'db.currentOp({secs_running \
                            : {$gt : 1 }})'" % (ip)).read().strip()
        mongo_long_query1=eval(mongo_long_query.replace('\n','').replace('true','True')\
                            .replace('false','False').replace('NumberLong',''))
        for i in mongo_long_query1['inprog']:
            print "%-17s %-12s %-15s %-25s %-15s" %  (ip, i[KEY[0]], i[KEY[1]], i[KEY[2]], i[KEY[3]])

#@exeTime1
def mongodb_sync_delay(ENV):
    mongoClusIP = robotConf[ENV]['mongodb']
    sync_info = os.popen("mongo --quiet %s --eval 'db.printSlaveReplicationInfo()'" % \
                (mongoClusIP[1])).read().strip()
    mongoIP=re.findall(r'\d+\.\d+\.\d+\.\d+', sync_info)
    sync_delay = re.findall(r'\t\d+ secs', sync_info)
    print "%-17s %-12s" % ('MongoDB_IP', 'sync_delay(s)')
    for i,j in zip(mongoIP, sync_delay):
        sec = re.search(r'\d+',j).group()  
        print "%-22s  %-12s" % (i,sec)  

#@exeTime1
def mongodb_oplog(ENV):
    mongoClusIP = robotConf[ENV]['mongodb']
    print "%-16s %10s %16s" % ("MongoDB_IP", "oplog_size", "oplog_len_time")
    for ip in  mongoClusIP:
        oplog_info = os.popen("mongo --quiet %s --eval 'db.printReplicationInfo()'" % (ip)).read().strip()
        oplog_size = re.search(r'\d+MB', oplog_info).group()
        oplog_len_time = re.search(r'[\d\.]+hrs', oplog_info).group()
        print "%-16s %10s %14s" % (ip, oplog_size, oplog_len_time)

#--------------------- print mongodb_status ------------------------------------------------------
#@exeTime1
def mongodb_status(TYPE, ENV):
    """print mongodb_status"""
    print 'mongo_status_ENV =',ENV

    # 探测mongodb集群ip 取集群ip并转为列表
    #mongoClusIP=os.popen("mongo --quiet %s --eval 'rs.status()'| grep  name|grep  -Eo \
    #                    '[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}\.[0-9]{1,}'" % (mongodb_ip)).read()\
    #                    .replace('\n',' ').strip().split()
    # 从配置文件中取集群所有IP
    mongoClusIP = robotConf[ENV]['mongodb']
    mongo_cluster_num = len(mongoClusIP)
    # 用集群中第一个IP探测整个集群所有IP mongodb 状态
    mongo_cluster_status=os.popen("mongo --quiet %s --eval 'rs.status()' | grep  stateStr" % \
                         (mongoClusIP[0])).read().replace('"stateStr" :','').replace('\n','')\
                         .replace('\t','').replace(',','').replace('"','').split()

    if   TYPE == 'proc':
        print '-' * 70 , "\n%-9s %-4s %-6s %-8s %-8s %-11s %-8s" % \
        ('proc-stat', 'cpu', 'mem', 'io_read', 'io_write', 'status', 'IP')
        pool = ThreadPool(3)
        result = pool.map(mongo_resource_use, mongoClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mongo_cluster_status, mongoClusIP):
            print "%-9s %-4s %-6s %-8s %-8s %-11s %-8s" % \
            (i[0], i[1], i[2], i[3], i[4],status, ip)

    elif TYPE == 'net':
        print '-' * 60, "\n%-10s %-10s %-10s %-11s %-8s" % \
              ('portConn', 'netIn', 'netOut', 'status', 'IP')
        pool = ThreadPool(3)
        result = pool.map(mongo_resource_use, mongoClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mongo_cluster_status, mongoClusIP):
            print "%-10s %-10s %-10s %-11s %-8s" % \
            (i[11],  i[12],  i[13], status, ip)


    elif TYPE == 'opcounters':
        print '-' * 70, "\n%-6s %-6s %-6s %-6s %-8s %-8s %-11s %-8s" % \
        ("query", "insert", "update", "delete", "getmore", "command", "status", "IP")
        pool = ThreadPool(3)
        result = pool.map(mongo_resource_use, mongoClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mongo_cluster_status, mongoClusIP):
            print "%-6s %-6s %-6s %-6s %-8s %-8s %-11s %-8s" % \
            (i[5], i[6],  i[7],  i[8],  i[9],  i[10], status,ip)


    elif TYPE == 'all':
        print "%-4s %-4s %-6s %-8s %-8s %-6s %-6s %-6s %-6s %-7s %-7s %-8s %-8s %-8s %-11s %-8s" % \
        ("stat", "cpu", "mem", "io_read", "io_write", "query", "insert", "update", "delete",\
         "getmore", "command", "portConn", "netIn", "netOut", "status", "IP")

        pool = ThreadPool(3)
        result = pool.map(mongo_resource_use, mongoClusIP )
        pool.close()
        pool.join()
        for i, status, ip in zip(result,  mongo_cluster_status, mongoClusIP):
            print "%-4s %-4s %-6s %-8s %-8s %-6s %-6s %-6s %-6s %-7s %-7s %-8s %-8s %-8s %-11s %-8s" \
            % (i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], \
               i[10],  i[11],  i[12],  i[13], status, ip)


    elif TYPE == 'report':
        mongodb_check_report()

    elif TYPE == 'long_query':
        mongodb_long_query_proc(ENV)

    elif TYPE == 'slow_query':
        mongodb_slow_query(ENV)

    elif TYPE == 'sync_delay':
        mongodb_sync_delay(ENV)

    elif TYPE == 'oplog':
        mongodb_oplog(ENV)


if __name__ == '__main__':
    #----------------------- TYPE -------- ENV ---
    exec "mongodb_status(sys.argv[1], sys.argv[2])"
    
    #mongodb_check_report()
    #mongodb_status('proc','pre')
    #mongodb_status('net','pre')
    #mongodb_status('opcounters','pre')
    #mongodb_status('all','pre')
    #mongodb_long_query_proc('pre')
    #mongodb_slow_query('pre')
    #mongodb_sync_delay('pre')
    #mongodb_oplog('pre')

