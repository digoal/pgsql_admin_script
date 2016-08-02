# PostgreSQL TPC-C极限优化玩法  
## digoal  
## 2016-01-19  
# 简介  
本文以工业界测试模型TPC-C为测试模型，介绍PostgreSQL数据库从系统层面的优化到数据库层面的优化方法。    
测试仓库4000个，数据量400GB。   
TPmC从 **256195.32** 提升到 **606466.31** 是如何做到的。   
  
# 测试环境介绍  
16核开HT共32线程，  
256G 1600MHz 内存，  
万兆网卡，  
3 块 6.4TB AliFlash PCI-E SSD，  
逻辑卷条带，  
XFS，  
数据块对齐。  
  
# XFS文件系统优化  
主要分3块，  
1. 逻辑卷优化部分  
2. XFS mkfs 优化部分  
3. XFS mount 优化部分  
以上几个部分都可以通过man手册查看，了解原理和应用场景后着手优化。  
man lvcreate  
man xfs  
man mkfs.xfs  
man mount  
  
## 逻辑卷优化部分  
对于不在lvm列表的设备，可能要先修改lvm.conf，添加设备号才行。否则不能创建PV。    
```
# cat /proc/devices
252 shannon

[root@localhost ~]# vi /etc/lvm/lvm.conf
    # types = [ "fd", 16 ]
    types = [ "shannon", 252 ]
```

1.1 创建PV前，将块设备对齐（对齐的目的是避免双写，因为SSD有最小写入单元，如果没有对齐，可能出现SSD写多个块），前面1MB最好不要分配，从2048 sector开始分配。  
（使用pvcreate的--dataalignment参数也可以达到同样的目的。）  
fdisk -c -u /dev/dfa  
start  2048  
end    + (2048*n) - 1  
或者使用parted创建分区。  
  
LVM的layout  
https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/7/html/Logical_Volume_Manager_Administration/LVM_components.html#pv_illustration  
创建PV时，也需要对齐DATA的数据。  
从4MB处开始分配DATA EXTENSION：  
`# pvcreate --dataalignment 4M /dev/sdc`  
  
1st PE 即数据开始位置。  
```  
[root@digoal ~]# pvs -o+pe_start  
  PV         VG     Fmt  Attr PSize  PFree  1st PE   
  /dev/sda2  centos lvm2 a--  19.51g 40.00m   1.00m  
  /dev/sdc          lvm2 ---  20.00g 20.00g   4.00m  
```  
列出所有可以查看的flag  
`pvs -o+`  
  
1.2 创建lv主要指定2个参数，  
条带数量，和pv数量一致即可，如果PV本身是一个RAID设备，根据RAID的块设备个数来定条带数。    
例如RAID5 5块盘，去除1个校验数据，取4作为条带数。RAID10 10块盘，取5作为条带数。RAID0 10块盘，取10作为条带数。    
```  
       -i, --stripes Stripes  
  Gives the number of stripes.  This is equal to the number of physical volumes to scatter the logical volume.  
```  
条带大小，和数据库块大小一致，例如postgresql默认为 8KB。  
```  
       -I, --stripesize StripeSize  
  Gives the number of kilobytes for the granularity of the stripes.  
  StripeSize must be 2^n (n = 2 to 9) for metadata in LVM1 format.  For metadata in LVM2 format, the stripe size may be a larger power of 2 but must not exceed the physical extent size.  
```  
创建快照时，指定的参数    
chunksize, 最好和数据库的块大小一致, 例如postgresql默认为 8KB。    
```  
       -c, --chunksize ChunkSize  
  Power of 2 chunk size for the snapshot logical volume between 4k and 512k.  
```  
例如：      
预留2GB给xfs的LOG DEV     
```  
#lvcreate -i 3 -I 8 -n lv02 -L 2G vgdata01  
  Logical volume "lv02" created  
#lvcreate -i 3 -I 8 -n lv01 -l 100%FREE vgdata01  
  Logical volume "lv01" created  
#lvs  
  LV   VG       Attr   LSize   Origin Snap%  Move Log Copy%  Convert  
  lv01 vgdata01 -wi-a-  17.29t    
  lv02 vgdata01 -wi-a-  2g   
```  
  
## XFS mkfs 优化部分  
### 首先要搞清楚XFS的layout。  
xfs包含3个section，data, log, realtime files。    
默认情况下 log存在data里面，没有realtime。所有的section都是由最小单位block组成，初始化xfs是-b指定block size。    
  
2.1 data    
包含 metadata(inode, 目录, 间接块), user file data, non-realtime files    
data被拆分成多个allocation group，mkfs.xfs时可以指定group的个数，以及单个group的SIZE。    
group越多，可以并行进行的文件和块的allocation就越多。你可以认为单个组的操作是串行的，多个组是并行的。    
但是组越多，消耗的CPU会越多，需要权衡。对于并发写很高的场景，可以多一些组，（例如一台主机跑了很多小的数据库，每个数据库都很繁忙的场景下）    
  
2.2 log    
存储metadata的log，修改metadata前，必须先记录log，然后才能修改data section中的metadata。  
也用于crash后的恢复。    
  
2.3 realtime    
被划分为很多个小的extents, 要将文件写入到realtime   section中，必须使用xfsctl改一下文件描述符的bit位，并且一定要在数据写入前完成。在realtime中的文件大小是realtime    extents的倍数关系。        

### mkfs.xfs优化  
对于data section：  
allocation group count数量和AGSIZE相乘等于块设备大小。  
AG count数量多少和用户需求的并行度相关。  
同时AG SIZE的取值范围是16M到1TB，PostgreSQL 建议1GB左右。  
-b size=8192  与数据库块大小一致 （但不是所有的xfs版本都支持大于4K的block   size，所以如果你发现mount失败并且告知只支持4K以下的BLOCK，那么请重新格式化）  
-d agcount=9000,sunit=16,swidth=48  
   假设有9000个并发写操作，使用9000个allocation groups  
   (单位512 bytes) 与lvm或RAID块设备的条带大小对齐  
    与lvm或RAID块设备条带跨度大小对齐，以上对应3*8 例如 -i 3 -I 8。  

log section：  
最好放在SSD上，速度越快越好。最好不要使用cgroup限制LOG块设备的iops操作。  

realtime section:  
不需要的话，不需要创建。  

agsize绝对不能是条带宽度的倍数。(假设条带数为3，条带大小为8K，则宽度为24K。)  
如果根据指定agcount算出的agsize是swidth的倍数，会弹出警告：  
例如下面的例子，  
agsize=156234 blks 是 swidth=6 blks 的倍数 26039。  
给出的建议是减掉一个stripe unit即8K，即156234 blks -  sunit 2 blks = 156232 blks。  
156232 blks换算成字节数= 156232*4096 = 639926272 bytes 或 156232*4 = 624928K  
```
#mkfs.xfs -f -b size=4096 -l logdev=/dev/mapper/vgdata01-lv01,size=2136997888,sunit=16 -d agcount=30000,sunit=16,swidth=48 /dev/mapper/vgdata01-lv02
Warning: AG size is a multiple of stripe width.  This can cause performance
problems by aligning all AGs on the same disk.  To avoid this, run mkfs with
an AG size that is one stripe unit smaller, for example 156232.
meta-data=/dev/mapper/vgdata01-lv02 isize=256    agcount=30000, agsize=156234 blks
         =                       sectsz=4096  attr=2, projid32bit=1
         =                       crc=0        finobt=0
data     =                       bsize=4096   blocks=4686971904, imaxpct=5
         =                       sunit=2      swidth=6 blks
naming   =version 2              bsize=4096   ascii-ci=0 ftype=0
log      =/dev/mapper/vgdata01-lv01 bsize=4096   blocks=521728, version=2
         =                       sectsz=512   sunit=2 blks, lazy-count=1
realtime =none                   extsz=4096   blocks=0, rtextents=0
```
对于上面这个mkfs.xfs操作，改成以下  
```
#mkfs.xfs -f -b size=4096 -l logdev=/dev/mapper/vgdata01-lv01,size=2136997888,sunit=16 -d agsize=639926272,sunit=16,swidth=48 /dev/mapper/vgdata01-lv02
```
或  
```
#mkfs.xfs -f -b size=4096 -l logdev=/dev/mapper/vgdata01-lv01,size=2136997888,sunit=16 -d agsize=624928k,sunit=16,swidth=48 /dev/mapper/vgdata01-lv02
```
输出如下  
```
meta-data=/dev/mapper/vgdata01-lv02 isize=256    agcount=30001, agsize=156232 blks  (约600MB)
         =                       sectsz=4096  attr=2, projid32bit=1
         =                       crc=0        finobt=0
data     =                       bsize=4096   blocks=4686971904, imaxpct=5
         =                       sunit=2      swidth=6 blks
naming   =version 2              bsize=4096   ascii-ci=0 ftype=0
log      =/dev/mapper/vgdata01-lv01 bsize=4096   blocks=521728, version=2
         =                       sectsz=512   sunit=2 blks, lazy-count=1
realtime =none                   extsz=4096   blocks=0, rtextents=0
```

## XFS mount 优化部分  
nobarrier  
largeio     针对数据仓库，流媒体这种大量连续读的应用    
nolargeio   针对OLTP    
logbsize=262144   指定 log buffer    
logdev=           指定log section对应的块设备，用最快的SSD。    
noatime,nodiratime    
swalloc           条带对齐    
allocsize=16M     delayed allocation writeout的buffer io大小  
inode64           Indicates that XFS is allowed to create inodes at any location in the filesystem  
  
### mount.xfs 例子  
```  
#mount -t xfs -o allocsize=16M,inode64,nobarrier,nolargeio,logbsize=262144,noatime,nodiratime,swalloc,logdev=/dev/mapper/vgdata01-lv02 /dev/mapper/vgdata01-lv01 /data01  
```  
  
## xfsctl 优化部分  
略  
  
##排错  
```  
#mount -o noatime,swalloc /dev/mapper/vgdata01-lv01 /data01  
mount: Function not implemented  
```  
原因是用了不支持的块大小    
```  
[ 5736.642924] XFS (dm-0): File system with blocksize 8192 bytes. Only pagesize (4096) or less will currently work.  
[ 5736.695146] XFS (dm-0): SB validate failed with error -38.  
```  
排除    
```  
# mkfs.xfs -f -b size=4096 -l logdev=/dev/mapper/vgdata01-lv02,size=2136997888,sunit=16 -d agcount=9000,sunit=16,swidth=48 /dev/mapper/vgdata01-lv01   
  
meta-data=/dev/mapper/vgdata01-lv01 isize=256    agcount=9000, agsize=515626 blks  
         =           sectsz=512   attr=2  
data     =           bsize=4096   blocks=4640621568, imaxpct=5  
         =           sunit=2      swidth=6 blks  
naming   =version 2  bsize=4096   ascii-ci=0  
log      =/dev/mapper/vgdata01-lv02 bsize=4096   blocks=521728, version=2  
         =           sectsz=512   sunit=2 blks, lazy-count=1  
realtime =none       extsz=4096   blocks=0, rtextents=0  
```  
mount时指定logdev  
```  
#mount -t xfs -o allocsize=16M,inode64,nobarrier,nolargeio,logbsize=262144,noatime,nodiratime,swalloc,logdev=/dev/mapper/vgdata01-lv02 /dev/mapper/vgdata01-lv01 /data01  
```  
  
# 安装benchmarksql  
http://sourceforge.net/projects/benchmarksql/    
  
下载安装 JDK7    
```  
http://www.oracle.com/technetwork/cn/java/javase/downloads/jdk7-downloads-1880260.html  
wget http://download.oracle.com/otn-pub/java/jdk/7u79-b15/jdk-7u79-linux-x64.rpm  
rpm -ivh jdk-7u79-linux-x64.rpm    
```  
  
检查包安装位置(使用rpm安装时也可以直接指定位置)    
```  
rpm -ql jdk  
...  
/usr/java/jdk1.7.0_79/bin/java  
...  
```  
  
配置JAVA环境变量    
```  
$  export JAVA_HOME=/usr/java/jdk1.7.0_79    
$  export PATH=$JAVA_HOME/bin:$PATH    
$  export CLASSPATH=.:$CLASSPATH    
```  
  
下载最新java版本对应的postgresql jdbc jar    
```  
wget https://jdbc.postgresql.org/download/postgresql-9.4.1207.jre7.jar  
mv postgresql-9.4.1207.jre7.jar benchmarksql-4.1.0/lib/  
```  
  
配置benchmarksql，使用新的postgresql java驱动    
```  
$ vi runBenchmark.sh   
java -cp .:../lib/postgresql-9.4.1207.jre7.jar:../lib/log4j-1.2.17.jar:../lib/apache-log4j-extras-1.1.jar:../dist/BenchmarkSQL-4.1.jar -Dprop=$1 jTPCC  
  
$ vi runLoader.sh  
java -cp .:../lib/postgresql-9.4.1207.jre7.jar:../dist/BenchmarkSQL-4.1.jar -Dprop=$1 LoadData $2 $3 $4 $5  
  
$ vi runSQL.sh   
myCP="../lib/postgresql-9.4.1207.jre7.jar"  
myCP="$myCP:../dist/BenchmarkSQL-4.1.jar"  
  
myOPTS="-Dprop=$1"  
myOPTS="$myOPTS -DcommandFile=$2"  
  
java -cp .:$myCP $myOPTS ExecJDBC  
```  
  
修改log4j，减少日志打印量。priority改成info，只输出最终结果，不输出产生订单的日志。    
```  
$ vi log4j.xml  
<?xml version="1.0" encoding="UTF-8" ?>  
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">  
<log4j:configuration xmlns:log4j='http://jakarta.apache.org/log4j/'>  
  
<appender name="console" class="org.apache.log4j.ConsoleAppender">  
<param name="Threshold" value="info"/>  
<layout class="org.apache.log4j.PatternLayout">  
<param name="ConversionPattern" value="%d %5p - %m%n"/>  
</layout>  
</appender>  
  
<appender name="R" class="org.apache.log4j.rolling.RollingFileAppender">  
<param name="Append" value="True" />  
<rollingPolicy class="org.apache.log4j.rolling.TimeBasedRollingPolicy">  
<param name="FileNamePattern" value="log/archive/benchmarksql.%d{yyyyMMddHHmm}.log"/>  
<param name="ActiveFileName" value="log/benchmarksql.log"/>  
</rollingPolicy>  
<triggeringPolicy class="org.apache.log4j.rolling.SizeBasedTriggeringPolicy">  
<param name="MaxFileSize" value="1"/>  
</triggeringPolicy>  
<layout class="org.apache.log4j.PatternLayout">  
<param name="ConversionPattern" value="%5p\t[%d{yyyy-MM-dd HH:mm:ss.SSS}]\t%t \t%m%n"/>  
</layout>  
<filter class="org.apache.log4j.filter.StringMatchFilter">  
<param name="StringToMatch" value ="\n" />  
<param name="AcceptOnMatch" value="false" />  
</filter>  
</appender>  
  
<appender name="E" class="org.apache.log4j.rolling.RollingFileAppender">  
<param name="Append" value="True" />  
<param name="Threshold" value="warn"/>  
<rollingPolicy class="org.apache.log4j.rolling.TimeBasedRollingPolicy">  
<param name="FileNamePattern" value="log/BenchmarkSQLError.%d.log"/>  
<param name="ActiveFileName" value="log/BenchmarkSQLError.log"/>  
</rollingPolicy>  
<layout class="org.apache.log4j.PatternLayout">  
<param name="ConversionPattern" value="%5p\t[%d{yyyy-MM-dd HH:mm:ss.SSS}]\t%t \t%m%n"/>  
</layout>  
</appender>  
  
<root>  
<priority value="info"/>  
<appender-ref ref="R"/>  
<appender-ref ref="E"/>  
</root>  
  
</log4j:configuration>  
```  
  
# 系统配置优化  
```  
内核配置  
/etc/grub.conf  
numa=off  
elevator=deadline  
  
编译器版本  
gcc version 4.4.6 20110731 (Red Hat 4.4.6-3) (GCC)   
  
/etc/sysctl.conf  
vm.swappiness = 0  
kernel.shmmax=135497418752  
net.core.rmem_max = 4194304  
net.core.wmem_max = 4194304  
net.core.rmem_default = 262144  
net.core.wmem_default = 262144  
net.ipv4.ip_local_port_range = 9000 65535  
kernel.sem = 50100 64128000 50100 51200  
vm.dirty_background_bytes = 102400000  
vm.dirty_ratio = 80  
vm.nr_hugepages = 102352  
  
/etc/security/limits.conf  
* soft nofile 655360  
* hard nofile 655360  
* soft nproc 655360  
* hard nproc 655360  
* soft stack unlimited  
* hard stack unlimited  
* soft   memlock    250000000  
* hard   memlock    250000000  
  
块设备预读  
blockdev --setra 16384 /dev/dfa  
blockdev --setra 16384 /dev/dfb  
blockdev --setra 16384 /dev/dfc  
blockdev --setra 16384 /dev/dm-0  
```  
  
# 安装PostgreSQL  
PostgreSQL编译项    
```  
./configure --prefix=/u02/digoal/soft_bak/pgsql9.5 --with-blocksize=8 --with-pgport=1921 --with-perl --with-python --with-tcl --with-openssl --with-pam --with-ldap --with-libxml --with-libxslt --enable-thread-safety  
gmake world -j32  
gmake install-world -j32  
```  
  
配置postgres环境变量    
```  
$ vi env_pg.sh   
export PS1="$USER@`/bin/hostname -s`-> "  
export PGPORT=1921  
export PGDATA=/data01/pgdata/pg_root  
export LANG=en_US.utf8  
export PGHOME=/u02/digoal/soft_bak/pgsql9.5  
export LD_LIBRARY_PATH=$PGHOME/lib:/lib64:/usr/lib64:/usr/local/lib64:/lib:/usr/lib:/usr/local/lib:$LD_LIBRARY_PATH  
export DATE=`date +"%Y%m%d%H%M"`  
export PATH=$PGHOME/bin:$PATH:.  
export MANPATH=$PGHOME/share/man:$MANPATH  
export PGHOST=$PGDATA  
export PGDATABASE=postgres  
export PGUSER=postgres  
alias rm='rm -i'  
alias ll='ls -lh'  
unalias vi  
```  
  
配置postgresql.conf    
```  
$ vi $PGDATA/postgresql.conf  
port = 1921     # (change requires restart)  
max_connections = 300       # (change requires restart)  
unix_socket_directories = '.'   # comma-separated list of directories  
shared_buffers = 32GB       # min 128kB  
huge_pages = try           # on, off, or try  
maintenance_work_mem = 2GB  # min 1MB  
dynamic_shared_memory_type = posix      # the default is the first option  
bgwriter_delay = 10ms       # 10-10000ms between rounds  
wal_level = minimal  # minimal, archive, hot_standby, or logical  
synchronous_commit = off    # synchronization level;  
full_page_writes = off      # recover from partial page writes, 有备份和归档就可以关闭它, crash后从备份恢复, 放partial write    
wal_buffers = 16MB           # min 32kB, -1 sets based on shared_buffers  
wal_writer_delay = 10ms         # 1-10000 milliseconds  
max_wal_size = 32GB  
effective_cache_size = 240GB  
log_destination = 'csvlog'  # Valid values are combinations of  
logging_collector = on          # Enable capturing of stderr and csvlog  
log_truncate_on_rotation = on           # If on, an existing log file with the  
```  
  
## 编辑benchmarksql连接配置和压测配置     
1000 个仓库，约5亿数据量。    
```  
$ vi props.pg   
driver=org.postgresql.Driver  
conn=jdbc:postgresql://localhost:1921/postgres  
user=postgres  
password=123  
  
warehouses=1000  
terminals=96  
//To run specified transactions per terminal- runMins must equal zero  
runTxnsPerTerminal=0  
//To run for specified minutes- runTxnsPerTerminal must equal zero  
runMins=1  
//Number of total transactions per minute  
limitTxnsPerMin=0  
  
//The following five values must add up to 100  
//The default percentages of 45, 43, 4, 4 & 4 match the TPC-C spec  
newOrderWeight=40  
paymentWeight=36  
orderStatusWeight=8  
deliveryWeight=8  
stockLevelWeight=8  
```  
  
## 生成测试数据  
配置postgres用户默认搜索路径    
```  
$ psql  
psql (9.5.0)  
Type "help" for help.  
postgres=# alter role postgres set search_path='benchmarksql','public';  
```  
  
创建用于存放生成CSV的目录    
```  
$ mkdir /u02/digoal/soft_bak/benchcsv  
```  
  
修改benchmarksql sqlTableCopies，指定目录    
```  
$ vi sqlTableCopies   
  
copy benchmarksql.warehouse  
  (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip)    
  from '/u02/digoal/soft_bak/benchcsv/warehouse.csv' WITH CSV;  
  
copy benchmarksql.item  
  (i_id, i_name, i_price, i_data, i_im_id)   
  from '/u02/digoal/soft_bak/benchcsv/item.csv' WITH CSV;  
  
copy benchmarksql.stock  
  (s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,  
   s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,  
   s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10)  
  from '/u02/digoal/soft_bak/benchcsv/stock.csv' WITH CSV;  
  
copy benchmarksql.district  
  (d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1,  
   d_street_2, d_city, d_state, d_zip)   
  from '/u02/digoal/soft_bak/benchcsv/district.csv' WITH CSV;  
  
copy benchmarksql.customer  
  (c_id, c_d_id, c_w_id, c_discount, c_credit, c_last, c_first, c_credit_lim,   
   c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_street_1,   
   c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_middle, c_data)   
  from '/u02/digoal/soft_bak/benchcsv/customer.csv' WITH CSV;  
  
copy benchmarksql.history  
  (hist_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)   
  from '/u02/digoal/soft_bak/benchcsv/cust-hist.csv' WITH CSV;  
  
copy benchmarksql.oorder  
  (o_id, o_w_id, o_d_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)   
  from '/u02/digoal/soft_bak/benchcsv/order.csv' WITH CSV;  
  
copy benchmarksql.order_line  
  (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,   
   ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)   
  from '/u02/digoal/soft_bak/benchcsv/order-line.csv' WITH CSV;  
  
copy benchmarksql.new_order  
  (no_w_id, no_d_id, no_o_id)    
  from '/u02/digoal/soft_bak/benchcsv/new-order.csv' WITH CSV;  
```  
  
建立表结构  
```  
$ cd benchmarksql-4.1.0/run  
$ ./runSQL.sh props.pg sqlTableCreates  
```  
  
生成CSV  
```  
$ ./runLoader.sh props.pg numWarehouses 1000 fileLocation /u02/digoal/soft_bak/benchcsv/   
```  
1000个仓库的数据量：  
```  
total 69G  
-rw-r--r-- 1 digoal users 2.0G Jan  9 15:53 cust-hist.csv  
-rw-r--r-- 1 digoal users  16G Jan  9 15:53 customer.csv  
-rw-r--r-- 1 digoal users 898K Jan  9 15:12 district.csv  
-rw-r--r-- 1 digoal users 7.0M Jan  9 14:22 item.csv  
-rw-r--r-- 1 digoal users  95M Jan  9 16:14 new-order.csv  
-rw-r--r-- 1 digoal users 1.3G Jan  9 16:14 order.csv  
-rw-r--r-- 1 digoal users  22G Jan  9 16:14 order-line.csv  
-rw-r--r-- 1 digoal users  28G Jan  9 15:12 stock.csv  
-rw-r--r-- 1 digoal users  84K Jan  9 14:22 warehouse.csv  
```  
  
导入数据库    
```  
$ ./runSQL.sh props.pg sqlTableCopies  
```  
  
创建约束和索引    
```  
$ ./runSQL.sh props.pg sqlIndexCreates   
```  
备份    
```  
$ pg_dump -f /u02/digoal/soft_bak/benchmarksql.dmp -F c -n benchmarksql postgres  
```  
# 阶段1 TPC-C 压测  
  
```  
nohup ./runBenchmark.sh props.pg >/dev/null 2>./errrun.log &  
```  
测试结果  
```  
 INFO   [2016-01-09 22:03:39.961]       Thread-7        Term-00,   
 INFO   [2016-01-09 22:03:39.963]       Thread-7        Term-00,   
 INFO   [2016-01-09 22:03:39.963]       Thread-7        Term-00, Measured tpmC (NewOrders) = 102494.46  
 INFO   [2016-01-09 22:03:39.963]       Thread-7        Term-00, Measured tpmTOTAL = 256195.32  
 INFO   [2016-01-09 22:03:39.964]       Thread-7        Term-00, Session Start     = 2016-01-09 21:53:39  
 INFO   [2016-01-09 22:03:39.964]       Thread-7        Term-00, Session End       = 2016-01-09 22:03:39  
 INFO   [2016-01-09 22:03:39.964]       Thread-7        Term-00, Transaction Count = 2563088  
```  
主机信息，截取压测第9分钟的数据。    
```  
TOP  
top - 22:02:09 up 3 days, 12:55,  3 users,  load average: 19.23, 15.97, 8.37  
Tasks: 619 total,  10 running, 609 sleeping,   0 stopped,   0 zombie  
Cpu(s): 35.0%us,  9.4%sy,  0.0%ni, 52.6%id,  0.1%wa,  0.0%hi,  2.9%si,  0.0%st  
Mem:  264643396k total, 241719372k used, 22924024k free,    36672k buffers  
Swap: 18825200k total,        0k used, 18825200k free, 196557376k cached  
  
iostat -x  
avg-cpu:  %user   %nice %system %iowait  %steal   %idle  
          35.07    0.00   12.30    0.12    0.00   52.51  
Device:         rrqm/s   wrqm/s     r/s     w/s   rsec/s   wsec/s avgrq-sz avgqu-sz   await  svctm  %util  
dfa   0.00     0.00   57.40  743.40   918.40 11849.00    15.94     0.02    0.03   0.03   2.08  
dfb   0.00     0.00   57.20  740.40   915.20 11829.00    15.98     0.02    0.03   0.03   2.04  
dfc   0.00     0.00   58.40  730.80   934.40 11675.80    15.98     0.03    0.03   0.03   2.52  
dm-0  0.00     0.00  173.00 2213.20  2768.00 35331.40    15.97     0.08    0.03   0.03   7.02  
```  
  
PostgreSQL可以使用oprofile或perf top跟踪统计    
参考    
http://blog.163.com/digoal@126/blog/static/163877040201549115140794/  
找到需要优化的代码就靠它了    
```  
CPU: Intel Ivy Bridge microarchitecture, speed 2600 MHz (estimated)  
Counted CPU_CLK_UNHALTED events (Clock cycles when not halted) with a unit mask of 0x00 (No unit mask) count 100000  
vma      samples  %        app name     symbol name  
007a7780 751274    5.1565  /soft/digoal/soft_bak/pgsql9.5/bin/postgres hash_search_with_hash_value  
004a92f0 574315    3.9419  /soft/digoal/soft_bak/pgsql9.5/bin/postgres _bt_compare  
006a4bd0 514473    3.5312  /soft/digoal/soft_bak/pgsql9.5/bin/postgres LWLockAcquire  
0078a090 510962    3.5071  /soft/digoal/soft_bak/pgsql9.5/bin/postgres SearchCatCache  
007bc3a0 484601    3.3262  /soft/digoal/soft_bak/pgsql9.5/bin/postgres AllocSetAlloc  
006969c0 442341    3.0361  /soft/digoal/soft_bak/pgsql9.5/bin/postgres GetSnapshotData  
00498930 352134    2.4170  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_hot_search_buffer  
005b8f70 279718    1.9199  /soft/digoal/soft_bak/pgsql9.5/bin/postgres ExecInitExpr  
006895d0 249377    1.7117  /soft/digoal/soft_bak/pgsql9.5/bin/postgres PinBuffer  
006a4220 168770    1.1584  /soft/digoal/soft_bak/pgsql9.5/bin/postgres LWLockRelease  
007ac620 161861    1.1110  /soft/digoal/soft_bak/pgsql9.5/bin/postgres pg_encoding_mbcliplen  
007a2180 161090    1.1057  /soft/digoal/soft_bak/pgsql9.5/bin/postgres FunctionCall2Coll  
004aaa80 153079    1.0507  /soft/digoal/soft_bak/pgsql9.5/bin/postgres _bt_checkkeys  
007a3950 147078    1.0095  /soft/digoal/soft_bak/pgsql9.5/bin/postgres fmgr_info_cxt_security  
0049bce0 136680    0.9381  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_page_prune_opt  
0048c8f0 130807    0.8978  /soft/digoal/soft_bak/pgsql9.5/bin/postgres hash_any  
006b2e50 130564    0.8962  /soft/digoal/soft_bak/pgsql9.5/bin/postgres PostgresMain  
0046c790 121776    0.8358  /soft/digoal/soft_bak/pgsql9.5/bin/postgres slot_deform_tuple  
......  
```  
  
## 阶段1 PostgreSQL 9.5.0 TPmC : 256195.32  
## 阶段1 性能瓶颈分析  
系统还有大量空闲CPU，IO资源，所以性能应该不止于此。预计PostgreSQL可到50W tpm。  
  
# 阶段2 TPC-C 优化  
benchmarksql放到另一台主机，主机间万兆网同一交换机下互联。    
  
为了突破测试程序的极限，开4个schema，每个schema负责1000个仓库，数据量总共20亿左右，数据量400GB。    
每个测试程序对付一个schema。    
终端数保持一致，每个测试程序开24个终端，一共96个终端。    
  
## 让benchmarksql支持多个schema  
benchmarksql 默认编译好的，还有配置都是用的benchmarksql 这个schema，如果我们想对一个数据库用多个schema来压性能，就需要开多个benchmarksql终端来压。    
这里就涉及到benchmarksql需要支持多个schema，每个benchmarksql连一个schema。    
目录结构：    
```  
drwxr-xr-x 2 digoal users 4096 Jan 10 13:24 build  
-rwxr-xr-x 1 digoal users 1112 Jan 10 13:24 build.xml  
drwxr-xr-x 2 digoal users 4096 Jan 10 13:24 dist  
-rw-r--r-- 1 digoal users  128 Jan 10 13:24 env_java.sh  
-rwxr-xr-x 1 digoal users 1927 Jan 10 13:24 HOW-TO-RUN.txt  
drwxr-xr-x 2 digoal users 4096 Jan 10 13:24 lib  
-rwxr-xr-x 1 digoal users 2825 Jan 10 13:24 README.txt  
drwxr-xr-x 3 digoal users 4096 Jan 10 13:24 run  
drwxr-xr-x 6 digoal users 4096 Jan 10 13:24 src  
```  
  
需要修改的地方：    
```  
src/LoadData/LoadData.java  
src/client/jTPCCTerminal.java  
run/props.ora  
run/props.pg  
run/sqlIndexCreates  
run/sqlIndexDrops  
run/sqlTableCopies  
run/sqlTableCreates  
run/sqlTableDrops  
run/sqlTableTruncates  
```  
  
把所有的benchmarksql替换成新的schema name，例如 test01    
```  
sed -i "s/benchmarksql/test01/g" src/LoadData/LoadData.java  
sed -i "s/benchmarksql/test01/g" src/client/jTPCCTerminal.java  
sed -i "s/benchmarksql/test01/g" run/props.ora  
sed -i "s/benchmarksql/test01/g" run/props.pg  
sed -i "s/benchmarksql/test01/g" run/sqlIndexCreates  
sed -i "s/BENCHMARKSQL/TEST01/g" run/sqlIndexCreates  
sed -i "s/benchmarksql/test01/g" run/sqlIndexDrops  
sed -i "s/benchmarksql/test01/g" run/sqlTableCopies  
sed -i "s/benchmarksql/test01/g" run/sqlTableCreates  
sed -i "s/benchmarksql/test01/g" run/sqlTableDrops  
sed -i "s/benchmarksql/test01/g" run/sqlTableTruncates  
```  
  
然后使用ant重新打包工程，如果没有安装ant，可以用yum install -y ant安装它。    
  
使用ant重新打包benchmarksql.jar  
```  
$ant -buildfile ./build.xml   
Buildfile: ./build.xml  
Trying to override old definition of task javac  
  
init:  
  
compile:  
    [javac] Compiling 16 source files to /soft/digoal/soft_bak/benchmarksql-4.1.0_oracle01/build  
    [javac] Note: /soft/digoal/soft_bak/benchmarksql-4.1.0_oracle01/src/client/jTPCCTerminal.java uses unchecked or unsafe operations.  
    [javac] Note: Recompile with -Xlint:unchecked for details.  
  
dist:  
      [jar] Building jar: /soft/digoal/soft_bak/benchmarksql-4.1.0_oracle01/dist/BenchmarkSQL-4.1.jar  
  
BUILD SUCCESSFUL  
Total time: 2 seconds  
```  
现在benchmarksql使用的是test01这个schema。    
使用同样的方法，生成支持test02,test03,test04 schema的benchmarksql版本。  
  
创建4个数据库，分别为test01,test02,test03,test04    
将阶段1 pg_dump导出的数据导入到这4个数据库, 并将schema重命名为对应的test01,test02,test03,test04    
测试数据量    
```  
postgres=# \l+  
   List of databases  
   Name    |  Owner   | Encoding | Collate | Ctype |   Access privileges   |  Size   | Tablespace |    Description       
-----------+----------+----------+---------+-------+-----------------------+---------+------------+--------------------------------------------  
 test01    | test01   | UTF8     | C       | C     |           | 100 GB  | pg_default |   
 test02    | test02   | UTF8     | C       | C     |           | 100 GB  | pg_default |   
 test03    | test03   | UTF8     | C       | C     |           | 100 GB  | pg_default |   
 test04    | test04   | UTF8     | C       | C     |           | 100 GB  | pg_default |   
```  
  
benchmarksql软件目录  
```  
$ ll  
drwxr-xr-x 7 digoal users 4.0K Jan 10 14:41 benchmarksql-4.1.0_pg01  
drwxr-xr-x 7 digoal users 4.0K Jan 10 14:41 benchmarksql-4.1.0_pg02  
drwxr-xr-x 7 digoal users 4.0K Jan 10 14:41 benchmarksql-4.1.0_pg03  
drwxr-xr-x 7 digoal users 4.0K Jan 10 14:41 benchmarksql-4.1.0_pg04  
```  
  
配置每个benchmarksql的props.pg，修改对应的连接。  
例如：  
```  
$cat run/props.pg  
driver=org.postgresql.Driver  
conn=jdbc:postgresql://xxx.xxx.xxx.xxx:1921/test01?preparedStatementCacheSizeMiB=10  
user=test01  
password=123  
  
warehouses=1000  
terminals=20  
//To run specified transactions per terminal- runMins must equal zero  
runTxnsPerTerminal=0  
//To run for specified minutes- runTxnsPerTerminal must equal zero  
runMins=10  
//Number of total transactions per minute  
limitTxnsPerMin=0  
  
//The following five values must add up to 100  
//The default percentages of 45, 43, 4, 4 & 4 match the TPC-C spec  
newOrderWeight=40  
paymentWeight=36  
orderStatusWeight=8  
deliveryWeight=8  
stockLevelWeight=8  
```  
  
配置数据库pg_hba.conf，允许测试机连接。  
```  
vi $PGDATA/pg_hba.conf  
host all all 0.0.0.0/0 md5  
pg_ctl reload  
```  
  
## 阶段2 TPC-C 压测  
```  
cd benchmarksql-4.1.0_pg01/run  
nohup ./runBenchmark.sh props.pg >/dev/null 2>./errrun.log &  
cd ../../benchmarksql-4.1.0_pg02/run  
nohup ./runBenchmark.sh props.pg >/dev/null 2>./errrun.log &  
cd ../../benchmarksql-4.1.0_pg03/run  
nohup ./runBenchmark.sh props.pg >/dev/null 2>./errrun.log &  
cd ../../benchmarksql-4.1.0_pg04/run  
nohup ./runBenchmark.sh props.pg >/dev/null 2>./errrun.log &  
cd ../..  
```  
  
## 阶段2 PostgreSQL 9.5.0 TPmC : 453058.64  
```  
$ cat benchmarksql-4.1.0_pg01/run/log/benchmarksql.log   
 INFO   [2016-01-10 17:54:04.925]       Thread-22       Term-00, Measured tpmC (NewOrders) = 45416.28  
 INFO   [2016-01-10 17:54:04.925]       Thread-22       Term-00, Measured tpmTOTAL = 113487.61  
 INFO   [2016-01-10 17:54:04.925]       Thread-22       Term-00, Session Start     = 2016-01-10 17:44:04  
 INFO   [2016-01-10 17:54:04.925]       Thread-22       Term-00, Session End       = 2016-01-10 17:54:04  
 INFO   [2016-01-10 17:54:04.925]       Thread-22       Term-00, Transaction Count = 1134913  
$ cat benchmarksql-4.1.0_pg02/run/log/benchmarksql.log   
 INFO   [2016-01-10 17:54:04.943]       Thread-12       Term-00, Measured tpmC (NewOrders) = 45292.48  
 INFO   [2016-01-10 17:54:04.943]       Thread-12       Term-00, Measured tpmTOTAL = 113269.54  
 INFO   [2016-01-10 17:54:04.943]       Thread-12       Term-00, Session Start     = 2016-01-10 17:44:04  
 INFO   [2016-01-10 17:54:04.944]       Thread-12       Term-00, Session End       = 2016-01-10 17:54:04  
 INFO   [2016-01-10 17:54:04.944]       Thread-12       Term-00, Transaction Count = 1132770  
$ cat benchmarksql-4.1.0_pg03/run/log/benchmarksql.log   
 INFO   [2016-01-10 17:54:04.955]       Thread-12       Term-00, Measured tpmC (NewOrders) = 45336.15  
 INFO   [2016-01-10 17:54:04.955]       Thread-12       Term-00, Measured tpmTOTAL = 113247.19  
 INFO   [2016-01-10 17:54:04.956]       Thread-12       Term-00, Session Start     = 2016-01-10 17:44:04  
 INFO   [2016-01-10 17:54:04.956]       Thread-12       Term-00, Session End       = 2016-01-10 17:54:04  
 INFO   [2016-01-10 17:54:04.956]       Thread-12       Term-00, Transaction Count = 1132537  
$ cat benchmarksql-4.1.0_pg04/run/log/benchmarksql.log   
 INFO   [2016-01-10 17:54:04.986]       Thread-23       Term-00, Measured tpmC (NewOrders) = 45231.67  
 INFO   [2016-01-10 17:54:04.987]       Thread-23       Term-00, Measured tpmTOTAL = 113054.3  
 INFO   [2016-01-10 17:54:04.987]       Thread-23       Term-00, Session Start     = 2016-01-10 17:44:04  
 INFO   [2016-01-10 17:54:04.987]       Thread-23       Term-00, Session End       = 2016-01-10 17:54:04  
 INFO   [2016-01-10 17:54:04.987]       Thread-23       Term-00, Transaction Count = 1130640  
  
TPM ：   
113487.61 + 113269.54 + 113247.19 + 113054.3 =  453058.64  
```  
  
第9分钟操作系统统计信息    
```  
TOP  
top - 17:38:27 up 4 days,  8:32,  4 users,  load average: 78.54, 68.64, 37.22  
Tasks: 658 total,  34 running, 624 sleeping,   0 stopped,   0 zombie  
Cpu(s): 70.2%us, 15.7%sy,  0.0%ni,  5.5%id,  1.5%wa,  0.0%hi,  7.1%si,  0.0%st  
Mem:  264643396k total, 229866068k used, 34777328k free,    59652k buffers  
Swap: 18825200k total,        0k used, 18825200k free, 183529592k cached  
  
iostat -x  
avg-cpu:  %user   %nice %system %iowait  %steal   %idle  
          71.39    0.00   22.47    1.26    0.00    4.88  
Device:         rrqm/s   wrqm/s     r/s     w/s   rsec/s   wsec/s avgrq-sz avgqu-sz   await  svctm  %util  
dfa   0.00     0.00 3659.33 7008.67 58538.67 112050.67    15.99     5.85    0.55   0.06  68.17  
dfb   0.00     0.00 3714.67 6888.67 59418.67 110173.33    15.99     5.98    0.56   0.06  67.87  
dfc   0.00     0.00 3709.00 6974.33 59328.00 111504.00    15.99     5.63    0.52   0.07  71.60  
dm-0  0.00     0.00 11083.00 20870.33 177285.33 333706.67    15.99    17.60    0.55   0.03  92.10  
```  
  
测试过程oprofile报告  
```  
#/home/digoal/oprof/bin/opreport -l -f -w -x -t 0.5  
Using /soft/digoal/soft_bak/oprof_test/oprofile_data/samples/ for samples directory.  
  
WARNING! Some of the events were throttled. Throttling occurs when  
the initial sample rate is too high, causing an excessive number of  
interrupts.  Decrease the sampling frequency. Check the directory  
/soft/digoal/soft_bak/oprof_test/oprofile_data/samples/current/stats/throttled  
for the throttled event names.  
  
CPU: Intel Ivy Bridge microarchitecture, speed 2600 MHz (estimated)  
Counted CPU_CLK_UNHALTED events (Clock cycles when not halted) with a unit mask of 0x00 (No unit mask) count 100000  
vma      samples  %        app name     symbol name  
007a7780 2632700   5.2511  /soft/digoal/soft_bak/pgsql9.5/bin/postgres hash_search_with_hash_value  
004a92f0 1895924   3.7816  /soft/digoal/soft_bak/pgsql9.5/bin/postgres _bt_compare  
006969c0 1844371   3.6787  /soft/digoal/soft_bak/pgsql9.5/bin/postgres GetSnapshotData  
0078a090 1775031   3.5404  /soft/digoal/soft_bak/pgsql9.5/bin/postgres SearchCatCache  
006a4bd0 1725350   3.4413  /soft/digoal/soft_bak/pgsql9.5/bin/postgres LWLockAcquire  
007bc3a0 1565190   3.1219  /soft/digoal/soft_bak/pgsql9.5/bin/postgres AllocSetAlloc  
00498930 1406694   2.8058  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_hot_search_buffer  
005b8f70 965646    1.9261  /soft/digoal/soft_bak/pgsql9.5/bin/postgres ExecInitExpr  
006895d0 767078    1.5300  /soft/digoal/soft_bak/pgsql9.5/bin/postgres PinBuffer  
004aaa80 617741    1.2321  /soft/digoal/soft_bak/pgsql9.5/bin/postgres _bt_checkkeys  
007a2180 588043    1.1729  /soft/digoal/soft_bak/pgsql9.5/bin/postgres FunctionCall2Coll  
006a4220 575864    1.1486  /soft/digoal/soft_bak/pgsql9.5/bin/postgres LWLockRelease  
007ac620 485162    0.9677  /soft/digoal/soft_bak/pgsql9.5/bin/postgres pg_encoding_mbcliplen  
007a3950 471102    0.9396  /soft/digoal/soft_bak/pgsql9.5/bin/postgres fmgr_info_cxt_security  
0046c790 441548    0.8807  /soft/digoal/soft_bak/pgsql9.5/bin/postgres slot_deform_tuple  
0048c8f0 425867    0.8494  /soft/digoal/soft_bak/pgsql9.5/bin/postgres hash_any  
006b2e50 404548    0.8069  /soft/digoal/soft_bak/pgsql9.5/bin/postgres PostgresMain  
007bd0f0 396510    0.7909  /soft/digoal/soft_bak/pgsql9.5/bin/postgres palloc  
0049bce0 394201    0.7863  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_page_prune_opt  
007bce00 353243    0.7046  /soft/digoal/soft_bak/pgsql9.5/bin/postgres pfree  
0049b300 335896    0.6700  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_page_prune  
0046c580 313145    0.6246  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_getsysattr  
006b14a0 311776    0.6219  /soft/digoal/soft_bak/pgsql9.5/bin/postgres exec_bind_message  
007cb070 292106    0.5826  /soft/digoal/soft_bak/pgsql9.5/bin/postgres HeapTupleSatisfiesMVCC  
007bd210 275282    0.5491  /soft/digoal/soft_bak/pgsql9.5/bin/postgres MemoryContextAllocZeroAligned  
005b8530 273199    0.5449  /soft/digoal/soft_bak/pgsql9.5/bin/postgres ExecProject  
00494ba0 266495    0.5315  /soft/digoal/soft_bak/pgsql9.5/bin/postgres heap_update  
007bca10 265556    0.5297  /soft/digoal/soft_bak/pgsql9.5/bin/postgres AllocSetFree  
```  
  
## 阶段2 性能瓶颈分析  
1. 单次IO请求响应较高，在0.06毫秒  
2. 系统调用占用的CPU百分比较高  
3. 数据库获取快照占用CPU较高，需要代码层优化  
  
  
# 阶段3 TPC-C 优化  
1. 开启PostgreSQL 预读, 预读数(n-1), n是条带数, 所以本例case effective_io_concurrency = 2  
   (这个使用xfs的largeio参数效果是类似的，还有块设备的预读功能)  
   (开启预读可能存在IO浪费的情况，例如全BUFFER命中的情况下。预读对于OLAP非常有效)  
2. 开启大页支持,  开到168G;    
```  
/etc/sysctl.conf  
  vm.nr_hugepages = 102352  
sysctl -p  
/etc/security/limits.conf  
  * soft   memlock    250000000  
  * hard   memlock    250000000  
  #memlock    大于  nr_hugepages   大于  shared_buffers   
```  
  
3. 使用数据块分组提交, commit_delay = 10, commit_siblings = 16   
   平滑检查点到0.8个周期，减少fsync dirty page IO影响。  
```  
http://blog.163.com/digoal@126/blog/static/1638770402016011115141697/  
shared_buffers = 164GB       # min 128kB  
huge_pages = on           # on, off, or try  
maintenance_work_mem = 2GB  # min 1MB  
wal_buffers = 16MB           # min 32kB, -1 sets based on shared_buffers  
wal_writer_delay = 10ms         # 1-10000 milliseconds  
commit_delay = 10           # range 0-100000, in microseconds  
commit_siblings = 16        # range 1-1000  
checkpoint_timeout = 35min  # range 30s-1h  
max_wal_size = 320GB  
checkpoint_completion_target = 0.8     # checkpoint target duration, 0.0 - 1.0  
effective_cache_size = 240GB  
log_destination = 'csvlog'  # Valid values are combinations of  
logging_collector = on          # Enable capturing of stderr and csvlog  
log_truncate_on_rotation = on           # If on, an existing log file with the  
```  
  
## 阶段3 TPC-C 压测  
```  
$tail -n 5 benchmarksql-4.1.0_pg01/run/log/benchmarksql.log   
 INFO   [2016-01-11 13:33:55.917]       Thread-14       Term-00, Measured tpmC (NewOrders) = 48151.07  
 INFO   [2016-01-11 13:33:55.917]       Thread-14       Term-00, Measured tpmTOTAL = 120215.48  
 INFO   [2016-01-11 13:33:55.917]       Thread-14       Term-00, Session Start     = 2016-01-11 13:23:55  
 INFO   [2016-01-11 13:33:55.917]       Thread-14       Term-00, Session End       = 2016-01-11 13:33:55  
 INFO   [2016-01-11 13:33:55.917]       Thread-14       Term-00, Transaction Count = 1202222  
  
$tail -n 5 benchmarksql-4.1.0_pg02/run/log/benchmarksql.log   
 INFO   [2016-01-11 13:33:55.971]       Thread-16       Term-00, Measured tpmC (NewOrders) = 48505.54  
 INFO   [2016-01-11 13:33:55.971]       Thread-16       Term-00, Measured tpmTOTAL = 121182.26  
 INFO   [2016-01-11 13:33:55.971]       Thread-16       Term-00, Session Start     = 2016-01-11 13:23:55  
 INFO   [2016-01-11 13:33:55.972]       Thread-16       Term-00, Session End       = 2016-01-11 13:33:55  
 INFO   [2016-01-11 13:33:55.972]       Thread-16       Term-00, Transaction Count = 1211858  
  
$tail -n 5 benchmarksql-4.1.0_pg03/run/log/benchmarksql.log   
 INFO   [2016-01-11 13:33:55.985]       Thread-4        Term-00, Measured tpmC (NewOrders) = 48119.61  
 INFO   [2016-01-11 13:33:55.985]       Thread-4        Term-00, Measured tpmTOTAL = 120523.98  
 INFO   [2016-01-11 13:33:55.985]       Thread-4        Term-00, Session Start     = 2016-01-11 13:23:55  
 INFO   [2016-01-11 13:33:55.985]       Thread-4        Term-00, Session End       = 2016-01-11 13:33:55  
 INFO   [2016-01-11 13:33:55.985]       Thread-4        Term-00, Transaction Count = 1205271  
  
$tail -n 5 benchmarksql-4.1.0_pg04/run/log/benchmarksql.log   
 INFO   [2016-01-11 13:33:55.958]       Thread-21       Term-00, Measured tpmC (NewOrders) = 48087.55  
 INFO   [2016-01-11 13:33:55.958]       Thread-21       Term-00, Measured tpmTOTAL = 120461.29  
 INFO   [2016-01-11 13:33:55.958]       Thread-21       Term-00, Session Start     = 2016-01-11 13:23:55  
 INFO   [2016-01-11 13:33:55.958]       Thread-21       Term-00, Session End       = 2016-01-11 13:33:55  
 INFO   [2016-01-11 13:33:55.958]       Thread-21       Term-00, Transaction Count = 1204638  
TPM:  
120215.48 + 121182.26 + 120523.98 + 120461.29 = 482383.01  
```  
  
## 阶段3 PostgreSQL 9.5.0 TPmC : 482383.01  
## 阶段3 性能瓶颈分析  
1. 操作系统后台刷脏页的数据量太大，容易带来抖动  
2. 优化并发数，减少事务快照CPU开销  
3. 优化work_mem，减少文件排序  
4. 优化分组提交阈值  
  
# 阶段4 TPC-C 优化  
优化分组提交的时延，最小结束点并发事务数量，work_mem等。     
操作系统内核参数优化，    
优化老化脏页刷新间隔    
vm.dirty_writeback_centisecs=10    
优化老化脏页阈值    
vm.dirty_expire_centisecs=6000    
优化用户进程刷脏页阈值    
vm.dirty_ratio=80    
优化内核进程刷脏页阈值    
vm.dirty_background_bytes=102400000    
优化终端数，每个benchmarksql 20个终端，一共80个终端。    
  
参数    
```  
listen_addresses = '0.0.0.0'         # what IP address(es) to listen on;  
port = 1921     # (change requires restart)  
max_connections = 300       # (change requires restart)  
unix_socket_directories = '.'   # comma-separated list of directories  
shared_buffers = 164GB       # min 128kB  
huge_pages = on           # on, off, or try  
work_mem = 256MB # min 64kB  
maintenance_work_mem = 2GB  # min 1MB  
autovacuum_work_mem = 2GB   # min 1MB, or -1 to use maintenance_work_mem  
dynamic_shared_memory_type = mmap      # the default is the first option  
vacuum_cost_delay = 10      # 0-100 milliseconds  
vacuum_cost_limit = 10000    # 1-10000 credits  
bgwriter_delay = 10ms       # 10-10000ms between rounds  
bgwriter_lru_maxpages = 1000# 0-1000 max buffers written/round  
bgwriter_lru_multiplier = 10.0          # 0-10.0 multipler on buffers scanned/round  
effective_io_concurrency = 2           # 1-1000; 0 disables prefetching  
wal_level = minimal  # minimal, archive, hot_standby, or logical  
synchronous_commit = off    # synchronization level;  
full_page_writes = off      # recover from partial page writes  
wal_buffers = 1GB           # min 32kB, -1 sets based on shared_buffers  
wal_writer_delay = 10ms         # 1-10000 milliseconds  
commit_delay = 10           # range 0-100000, in microseconds  
commit_siblings = 6        # range 1-1000  
checkpoint_timeout = 55min  # range 30s-1h  
max_wal_size = 320GB  
checkpoint_completion_target = 0.99     # checkpoint target duration, 0.0 - 1.0  
random_page_cost = 1.0     # same scale as above  
effective_cache_size = 240GB  
log_destination = 'csvlog'  # Valid values are combinations of  
logging_collector = on          # Enable capturing of stderr and csvlog  
log_truncate_on_rotation = on           # If on, an existing log file with the  
log_timezone = 'PRC'  
update_process_title = off  
track_activities = off  
autovacuum = on# Enable autovacuum subprocess?  'on'  
```  
重启数据库  
```  
pg_ctl restart  
```  
将数据加载到shared buffer    
```  
psql  
\c test01 test01  
explain analyze select * from customer; explain analyze select * from stock;  
\c test02 test02  
explain analyze select * from customer; explain analyze select * from stock;  
\c test03 test03  
explain analyze select * from customer; explain analyze select * from stock;  
\c test04 test04  
explain analyze select * from customer; explain analyze select * from stock;  
```  
  
## 阶段4 TPC-C 压测  
```  
$ tail -n 5 benchmarksql-4.1.0_pg01/run/log/benchmarksql.log   
 INFO   [2016-01-12 11:55:09.461]       Thread-12       Term-00, Measured tpmC (NewOrders) = 57995.55  
 INFO   [2016-01-12 11:55:09.461]       Thread-12       Term-00, Measured tpmTOTAL = 144975.59  
 INFO   [2016-01-12 11:55:09.461]       Thread-12       Term-00, Session Start     = 2016-01-12 11:45:09  
 INFO   [2016-01-12 11:55:09.461]       Thread-12       Term-00, Session End       = 2016-01-12 11:55:09  
 INFO   [2016-01-12 11:55:09.462]       Thread-12       Term-00, Transaction Count = 1449796  
$ tail -n 5 benchmarksql-4.1.0_pg02/run/log/benchmarksql.log   
 INFO   [2016-01-12 11:55:09.499]       Thread-0        Term-00, Measured tpmC (NewOrders) = 58013.75  
 INFO   [2016-01-12 11:55:09.499]       Thread-0        Term-00, Measured tpmTOTAL = 145006.74  
 INFO   [2016-01-12 11:55:09.499]       Thread-0        Term-00, Session Start     = 2016-01-12 11:45:09  
 INFO   [2016-01-12 11:55:09.500]       Thread-0        Term-00, Session End       = 2016-01-12 11:55:09  
 INFO   [2016-01-12 11:55:09.500]       Thread-0        Term-00, Transaction Count = 1450110  
$ tail -n 5 benchmarksql-4.1.0_pg03/run/log/benchmarksql.log   
 INFO   [2016-01-12 11:55:09.541]       Thread-14       Term-00, Measured tpmC (NewOrders) = 57322.05  
 INFO   [2016-01-12 11:55:09.541]       Thread-14       Term-00, Measured tpmTOTAL = 143227.03  
 INFO   [2016-01-12 11:55:09.542]       Thread-14       Term-00, Session Start     = 2016-01-12 11:45:09  
 INFO   [2016-01-12 11:55:09.542]       Thread-14       Term-00, Session End       = 2016-01-12 11:55:09  
 INFO   [2016-01-12 11:55:09.542]       Thread-14       Term-00, Transaction Count = 1432298  
$ tail -n 5 benchmarksql-4.1.0_pg04/run/log/benchmarksql.log   
 INFO   [2016-01-12 11:55:09.574]       Thread-7        Term-00, Measured tpmC (NewOrders) = 57863.92  
 INFO   [2016-01-12 11:55:09.574]       Thread-7        Term-00, Measured tpmTOTAL = 144596.45  
 INFO   [2016-01-12 11:55:09.575]       Thread-7        Term-00, Session Start     = 2016-01-12 11:45:09  
 INFO   [2016-01-12 11:55:09.575]       Thread-7        Term-00, Session End       = 2016-01-12 11:55:09  
 INFO   [2016-01-12 11:55:09.575]       Thread-7        Term-00, Transaction Count = 1445978  
TPM：  
144975.59 + 145006.74 + 143227.03 + 144596.45 = 577805.81  
```  
  
## 阶段4 PostgreSQL 9.5.0 TPmC : 577805.81  
## 阶段4 性能瓶颈分析  
无明显瓶颈，需要从编译器，代码方面入手优化。  
  
# 阶段5 TPC-C 优化  
gcc编译器版本更新  
http://blog.163.com/digoal@126/blog/static/163877040201601313814429/  
INTEL编译器  
https://software.intel.com/en-us/intel-compilers  
CLANG编译器  
http://blog.163.com/digoal@126/blog/static/163877040201601382640309/  
使用gcc 4.9.3版本，更新CFLAGS，重新编译  
```  
$ export LD_LIBRARY_PATH=/u02/digoal/gcc4.9.3/lib:/u02/digoal/cloog/lib:/u02/digoal/gmp/lib:/u02/digoal/isl/lib:/u02/digoal/mpc/lib:/u02/digoal/mpfr/lib:$LD_LIBRARY_PATH  
$ export PATH=/u02/digoal/gcc4.9.3/bin:$PATH  
  
$ CFLAGS="-O3 -march=native -flto" CC=/u02/digoal/gcc4.9.3/bin/gcc ./configure --prefix=/u02/digoal/soft_bak/pgsql9.5 --with-blocksize=8 --with-pgport=1921 --with-perl --with-python --with-tcl --with-openssl --with-pam --with-ldap --with-libxml --with-libxslt --enable-thread-safety --with-wal-segsize=64  
  
$ make world -j 32  
$ make install-world -j 32  
```  
  
## 阶段5 TPC-C 压测  
```  
digoal tail -n 5 benchmarksql-4.1.0_pg01/run/log/benchmarksql.log   
 INFO   [2016-01-13 02:00:49.699]       Thread-15       Term-00, Measured tpmC (NewOrders) = 59092.33  
 INFO   [2016-01-13 02:00:49.699]       Thread-15       Term-00, Measured tpmTOTAL = 147832.44  
 INFO   [2016-01-13 02:00:49.699]       Thread-15       Term-00, Session Start     = 2016-01-13 01:50:49  
 INFO   [2016-01-13 02:00:49.699]       Thread-15       Term-00, Session End       = 2016-01-13 02:00:49  
 INFO   [2016-01-13 02:00:49.699]       Thread-15       Term-00, Transaction Count = 1478385  
digoal tail -n 5 benchmarksql-4.1.0_pg02/run/log/benchmarksql.log   
 INFO   [2016-01-13 02:00:49.704]       Thread-0        Term-00, Measured tpmC (NewOrders) = 60051.49  
 INFO   [2016-01-13 02:00:49.704]       Thread-0        Term-00, Measured tpmTOTAL = 150231.54  
 INFO   [2016-01-13 02:00:49.704]       Thread-0        Term-00, Session Start     = 2016-01-13 01:50:49  
 INFO   [2016-01-13 02:00:49.704]       Thread-0        Term-00, Session End       = 2016-01-13 02:00:49  
 INFO   [2016-01-13 02:00:49.704]       Thread-0        Term-00, Transaction Count = 1502367  
digoal tail -n 5 benchmarksql-4.1.0_pg03/run/log/benchmarksql.log   
 INFO   [2016-01-13 02:00:49.693]       Thread-16       Term-00, Measured tpmC (NewOrders) = 60273.99  
 INFO   [2016-01-13 02:00:49.694]       Thread-16       Term-00, Measured tpmTOTAL = 150601.93  
 INFO   [2016-01-13 02:00:49.694]       Thread-16       Term-00, Session Start     = 2016-01-13 01:50:49  
 INFO   [2016-01-13 02:00:49.694]       Thread-16       Term-00, Session End       = 2016-01-13 02:00:49  
 INFO   [2016-01-13 02:00:49.694]       Thread-16       Term-00, Transaction Count = 1506066  
digoal tail -n 5 benchmarksql-4.1.0_pg04/run/log/benchmarksql.log   
 INFO   [2016-01-13 02:00:49.715]       Thread-18       Term-00, Measured tpmC (NewOrders) = 60180.69  
 INFO   [2016-01-13 02:00:49.715]       Thread-18       Term-00, Measured tpmTOTAL = 150591.78  
 INFO   [2016-01-13 02:00:49.716]       Thread-18       Term-00, Session Start     = 2016-01-13 01:50:49  
 INFO   [2016-01-13 02:00:49.716]       Thread-18       Term-00, Session End       = 2016-01-13 02:00:49  
 INFO   [2016-01-13 02:00:49.716]       Thread-18       Term-00, Transaction Count = 1505962  
  
TPM  
599257.69  
```  
  
## 阶段5 PostgreSQL 9.5.0 TPmC : 599257.69  
## 阶段5 性能瓶颈分析  
更换CLANG编译器。  
  
# 阶段6 TPC-C 优化  
CLANG编译  
http://blog.163.com/digoal@126/blog/static/163877040201601421045406/  
使用clang编译  
```  
CC=/u02/digoal/llvm/bin/clang CFLAGS="-O2 -fstrict-enums" ./configure --prefix=/u02/digoal/soft_bak/pgsql9.5  --with-pgport=1921 --with-perl --with-python --with-tcl --with-openssl --with-pam --with-ldap --with-libxml --with-libxslt --enable-thread-safety  
make world -j 32  
make install-world -j 32  
```  
## 阶段6 TPC-C 压测  
```  
$ tail -n 5 benchmarksql-4.1.0_pg01/run/log/benchmarksql.log   
 INFO   [2016-01-16 07:21:58.070]       Thread-12       Term-00, Measured tpmC (NewOrders) = 60519.19  
 INFO   [2016-01-16 07:21:58.070]       Thread-12       Term-00, Measured tpmTOTAL = 151235.02  
 INFO   [2016-01-16 07:21:58.070]       Thread-12       Term-00, Session Start     = 2016-01-16 07:11:58  
 INFO   [2016-01-16 07:21:58.071]       Thread-12       Term-00, Session End       = 2016-01-16 07:21:58  
 INFO   [2016-01-16 07:21:58.071]       Thread-12       Term-00, Transaction Count = 1512377  
$ tail -n 5 benchmarksql-4.1.0_pg02/run/log/benchmarksql.log   
 INFO   [2016-01-16 07:21:58.180]       Thread-15       Term-00, Measured tpmC (NewOrders) = 60924.87  
 INFO   [2016-01-16 07:21:58.180]       Thread-15       Term-00, Measured tpmTOTAL = 152126.73  
 INFO   [2016-01-16 07:21:58.180]       Thread-15       Term-00, Session Start     = 2016-01-16 07:11:58  
 INFO   [2016-01-16 07:21:58.180]       Thread-15       Term-00, Session End       = 2016-01-16 07:21:58  
 INFO   [2016-01-16 07:21:58.180]       Thread-15       Term-00, Transaction Count = 1521312  
$ tail -n 5 benchmarksql-4.1.0_pg03/run/log/benchmarksql.log   
 INFO   [2016-01-16 07:21:58.198]       Thread-0        Term-00, Measured tpmC (NewOrders) = 60481.19  
 INFO   [2016-01-16 07:21:58.198]       Thread-0        Term-00, Measured tpmTOTAL = 151294.63  
 INFO   [2016-01-16 07:21:58.199]       Thread-0        Term-00, Session Start     = 2016-01-16 07:11:58  
 INFO   [2016-01-16 07:21:58.199]       Thread-0        Term-00, Session End       = 2016-01-16 07:21:58  
 INFO   [2016-01-16 07:21:58.199]       Thread-0        Term-00, Transaction Count = 1512968  
$ tail -n 5 benchmarksql-4.1.0_pg04/run/log/benchmarksql.log   
 INFO   [2016-01-16 07:21:58.200]       Thread-5        Term-00, Measured tpmC (NewOrders) = 60715.57  
 INFO   [2016-01-16 07:21:58.200]       Thread-5        Term-00, Measured tpmTOTAL = 151809.93  
 INFO   [2016-01-16 07:21:58.200]       Thread-5        Term-00, Session Start     = 2016-01-16 07:11:58  
 INFO   [2016-01-16 07:21:58.200]       Thread-5        Term-00, Session End       = 2016-01-16 07:21:58  
 INFO   [2016-01-16 07:21:58.200]       Thread-5        Term-00, Transaction Count = 1518149  
TPM:  
606466.31  
```  
## 阶段6 PostgreSQL 9.5.0 TPmC : 606466.31  
  
当前perf top    
```  
 samples  pcnt function  DSO  
 _______ _____ _________________________________ __________________________________________  
  
15900.00  3.2% hash_search_with_hash_value       /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
13970.00  2.8% _bt_compare           /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
13215.00  2.6% AllocSetAlloc         /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
10678.00  2.1% LWLockAcquire         /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
10298.00  2.1% memcpy    /lib64/libc-2.12.so             
 9016.00  1.8% SearchCatCache        /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 8577.00  1.7% heap_hot_search_buffer/u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 8059.00  1.6% GetSnapshotData       /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 6975.00  1.4% ExecInitExpr          /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 6517.00  1.3% fmgr_info_cxt_security/u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 5232.00  1.0% PostgresMain          /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 4328.00  0.9% LWLockRelease         /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 4044.00  0.8% PinBuffer /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 4037.00  0.8% _int_malloc           /lib64/libc-2.12.so             
 4026.00  0.8% StrategyGetBuffer     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 3777.00  0.8% slot_deform_tuple     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 3755.00  0.7% FunctionCall2Coll     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 3741.00  0.7% __GI_vfprintf         /lib64/libc-2.12.so             
 3403.00  0.7% __strncpy_ssse3       /lib64/libc-2.12.so             
 3305.00  0.7% aliflash_reconfig_task[aliflash]          
 3090.00  0.6% _bt_checkkeys         /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 3012.00  0.6% __memset_sse2         /lib64/libc-2.12.so             
 2881.00  0.6% palloc    /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2698.00  0.5% __strlen_sse42        /lib64/libc-2.12.so             
 2585.00  0.5% _int_free /lib64/libc-2.12.so             
 2505.00  0.5% heap_page_prune       /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2495.00  0.5% hash_any  /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2442.00  0.5% heap_page_prune_opt   /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2437.00  0.5% __schedule[kernel.kallsyms]   
 2210.00  0.4% MemoryContextAllocZeroAligned     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2111.00  0.4% pfree     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2048.00  0.4% heap_update           /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 2012.00  0.4% update_blocked_averages           [kernel.kallsyms]   
 1937.00  0.4% __switch_to           [kernel.kallsyms]   
 1925.00  0.4% heap_getsysattr       /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1916.00  0.4% TupleDescInitEntry    /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1905.00  0.4% irq_entries_start     [kernel.kallsyms]   
 1863.00  0.4% AllocSetFree          /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1821.00  0.4% _wordcopy_bwd_aligned /lib64/libc-2.12.so             
 1761.00  0.4% _raw_spin_lock        [kernel.kallsyms]   
 1758.00  0.4% check_stack_depth     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1749.00  0.3% _bt_binsrch           /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1748.00  0.3% ReadBuffer_common     /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1747.00  0.3% expression_tree_walker/u02/digoal/soft_bak/pgsql9.5/bin/postgres  
 1651.00  0.3% __GI___libc_malloc    /lib64/libc-2.12.so             
 1608.00  0.3% __memcmp_sse4_1       /lib64/libc-2.12.so             
 1586.00  0.3% LockAcquireExtended   /u02/digoal/soft_bak/pgsql9.5/bin/postgres  
------------------------------------------------------------------------------------------------------------  
```  
## 阶段6 性能瓶颈分析  
### 其他本文未尝试的优化手段  
有兴趣的朋友可以试试：  
1. 使用interl的icc编译一下，看看性能还能不能提升。   
2. 关闭表的自动analyze, 关闭日志表的autovacuum和auto analyze.    
3. PostgreSQL jdbc有一些参数可以优化，本文还未处理。例如防止类型转换，QUERY plan CACHE size。  
http://www.postgresql.org/docs/9.2/interactive/libpq-connect.html  
4. PostgreSQL 代码层也有优化的空间，例如分区表的代码，快照的优化。  
  
# 总结  
内核参数优化总结    
以及每项配置的原理  
```  
vi /etc/sysctl.conf
# add by digoal.zhou
fs.aio-max-nr = 1048576
fs.file-max = 76724600
kernel.core_pattern= /data01/corefiles/core_%e_%u_%t_%s.%p   # /data01/corefiles事先建好，权限777
kernel.sem = 4096 2147483647 2147483646 512000    # 信号量, ipcs -l 或 -u 查看，每16个进程一组，每组信号量需要17个信号量。
kernel.shmall = 107374182      # 所有共享内存段相加大小限制(建议内存的80%)
kernel.shmmax = 274877906944   # 最大单个共享内存段大小(建议为内存一半), >9.2的版本已大幅降低共享内存的使用
kernel.shmmni = 819200         # 一共能生成多少共享内存段，每个PG数据库集群至少2个共享内存段
net.core.netdev_max_backlog = 10000
net.core.rmem_default = 262144       # The default setting of the socket receive buffer in bytes.
net.core.rmem_max = 4194304          # The maximum receive socket buffer size in bytes
net.core.wmem_default = 262144       # The default setting (in bytes) of the socket send buffer.
net.core.wmem_max = 4194304          # The maximum send socket buffer size in bytes.
net.core.somaxconn = 4096
net.ipv4.tcp_max_syn_backlog = 4096
net.ipv4.tcp_keepalive_intvl = 20
net.ipv4.tcp_keepalive_probes = 3
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_mem = 8388608 12582912 16777216
net.ipv4.tcp_fin_timeout = 5
net.ipv4.tcp_synack_retries = 2
net.ipv4.tcp_syncookies = 1    # 开启SYN Cookies。当出现SYN等待队列溢出时，启用cookie来处理，可防范少量的SYN攻击
net.ipv4.tcp_timestamps = 1    # 减少time_wait
net.ipv4.tcp_tw_recycle = 0    # 如果=1则开启TCP连接中TIME-WAIT套接字的快速回收，但是NAT环境可能导致连接失败，建议服务端关闭它
net.ipv4.tcp_tw_reuse = 1      # 开启重用。允许将TIME-WAIT套接字重新用于新的TCP连接
net.ipv4.tcp_max_tw_buckets = 262144
net.ipv4.tcp_rmem = 8192 87380 16777216
net.ipv4.tcp_wmem = 8192 65536 16777216
net.nf_conntrack_max = 1200000
net.netfilter.nf_conntrack_max = 1200000
vm.dirty_background_bytes = 4096000000       #  系统脏页到达这个值，系统后台刷脏页调度进程 pdflush（或其他） 自动将(dirty_expire_centisecs/100）秒前的脏页刷到磁盘
vm.dirty_expire_centisecs = 6000             #  比这个值老的脏页，将被刷到磁盘。6000表示60秒。
vm.dirty_ratio = 80                          #  如果系统进程刷脏页太慢，使得系统脏页超过内存 80 % 时，则用户进程如果有写磁盘的操作（如fsync, fdatasync等调用），则需要主动把系统脏页刷出。
vm.dirty_writeback_centisecs = 50            #  pdflush（或其他）后台刷脏页进程的唤醒间隔， 50表示0.5秒。
vm.extra_free_kbytes = 4096000
vm.min_free_kbytes = 2097152
vm.mmap_min_addr = 65536
vm.overcommit_memory = 0     #  在分配内存时，允许少量over malloc
vm.overcommit_ratio = 90     #  当overcommit_memory = 2 时，用于参与计算允许指派的内存大小。
vm.swappiness = 0            #  关闭交换分区
vm.zone_reclaim_mode = 0     # 禁用 numa, 或者在vmlinux中禁止.
net.ipv4.ip_local_port_range = 40000 65535    # 本地自动分配的TCP, UDP端口号范围
#  vm.nr_hugepages = 102352    #  建议shared buffer设置超过64GB时 使用大页，页大小 /proc/meminfo Hugepagesize
```
内存分配策略解释  
参考   
http://blog.163.com/digoal@126/blog/static/163877040201563044143325/  
```
当vm.overcommit_memory=0时，允许用户轻微的overcommit。  
当vm.overcommit_memory=1时，任何情况下都允许申请内存overcommit, 比较危险，常用于一些科学计算应用。  
当vm.overcommit_memory=2时，Committed_AS不能大于CommitLimit。
申请内存的限制 计算方法
              The CommitLimit is calculated with the following formula:
              CommitLimit = ([total RAM pages] - [total huge TLB pages]) *
              overcommit_ratio / 100 + [total swap pages]
              For example, on a system with 1G of physical RAM and 7G
              of swap with a `vm.overcommit_ratio` of 30 it would
              yield a CommitLimit of 7.3G.
[root@digoal postgresql-9.4.4]# free
             total       used       free     shared    buffers     cached
Mem:       1914436     713976    1200460      72588      32384     529364
-/+ buffers/cache:     152228    1762208
Swap:      1048572     542080     506492
[root@digoal ~]# cat /proc/meminfo |grep Commit
CommitLimit:     2005788 kB
Committed_AS:     132384 kB
这个例子的2G就是以上公式计算得来。  

overcommit限制的初衷是malloc后，内存并不是立即使用掉，所以如果多个进程同时申请一批内存的话，不允许OVERCOMMIT可能导致某些进程申请内存失败，但实际上内存是还有的。   
所以Linux内核给出了几种选择，
2是比较靠谱或者温柔的做法。   
1的话风险有点大，虽然可以申请内存，但是实际上可能已经没有足够的内存给程序使用，最终可能会导致OOM。  
0是最常见的，允许少量的overcommit，但是对于需要超很多内存的情况，不允许。  
还可以参考代码 : 
security/commoncap.c::cap_vm_enough_memory()

所以当数据库无法启动时，要么你降低一下数据库申请内存的大小（例如降低shared_buffer或者max conn），要么就是修改一下overcommit的风格。
```  

vi /etc/security/limits.conf   
```
# add by digoal.zhou
* soft    nofile  655360
* hard    nofile  655360
* soft    nproc   655360
* hard    nproc   655360
* soft    memlock unlimited
* hard    memlock unlimited
* soft    core    unlimited
* hard    core    unlimited
```

内核启动参数优化总结    
关闭numa  
使用deadline调度IO  
```  
kernel /vmlinuz-3.18.24 numa=off elevator=deadline intel_idle.max_cstate=0 scsi_mod.scan=sync  
```  

块设备优化总结，预读(适合greenplum, 不建议OLTP使用)  
```  
blockdev --setra 16384 /dev/dfa  
blockdev --setra 16384 /dev/dfb  
blockdev --setra 16384 /dev/dfc  
blockdev --setra 16384 /dev/dm-0  
```  

数据库参数优化总结   
```  
max_connections = 300       # (change requires restart)  
unix_socket_directories = '.'   # comma-separated list of directories  
shared_buffers = 194GB       # 尽量用数据库管理内存，减少双重缓存，提高使用效率  
huge_pages = on           # on, off, or try  ，使用大页
work_mem = 256MB # min 64kB  ， 减少外部文件排序的可能，提高效率
maintenance_work_mem = 2GB  # min 1MB  ， 加速建立索引
autovacuum_work_mem = 2GB   # min 1MB, or -1 to use maintenance_work_mem  ， 加速垃圾回收
dynamic_shared_memory_type = mmap      # the default is the first option  
vacuum_cost_delay = 0      # 0-100 milliseconds   ， 垃圾回收不妥协，极限压力下，减少膨胀可能性
bgwriter_delay = 10ms       # 10-10000ms between rounds    ， 刷shared buffer脏页的进程调度间隔，尽量高频调度，减少用户进程申请不到内存而需要主动刷脏页的可能（导致RT升高）。
bgwriter_lru_maxpages = 1000   # 0-1000 max buffers written/round ,  一次最多刷多少脏页
bgwriter_lru_multiplier = 10.0          # 0-10.0 multipler on buffers scanned/round  一次扫描多少个块，上次刷出脏页数量的倍数
effective_io_concurrency = 2           # 1-1000; 0 disables prefetching ， 执行节点为bitmap heap scan时，预读的块数。从而
wal_level = minimal         # minimal, archive, hot_standby, or logical ， 如果现实环境，建议开启归档。  
synchronous_commit = off    # synchronization level;    ， 异步提交  
wal_sync_method = open_sync    # the default is the first option  ， 因为没有standby，所以写xlog选择一个支持O_DIRECT的fsync方法。  
full_page_writes = off      # recover from partial page writes  ， 生产中，如果有增量备份和归档，可以关闭，提高性能。  
wal_buffers = 1GB           # min 32kB, -1 sets based on shared_buffers  ，wal buffer大小，如果大量写wal buffer等待，则可以加大。
wal_writer_delay = 10ms         # 1-10000 milliseconds  wal buffer调度间隔，和bg writer delay类似。
commit_delay = 20           # range 0-100000, in microseconds  ，分组提交的等待时间
commit_siblings = 9        # range 1-1000  , 有多少个事务同时进入提交阶段时，就触发分组提交。
checkpoint_timeout = 55min  # range 30s-1h  时间控制的检查点间隔。
max_wal_size = 320GB    #   2个检查点之间最多允许产生多少个XLOG文件
checkpoint_completion_target = 0.99     # checkpoint target duration, 0.0 - 1.0  ，平滑调度间隔，假设上一个检查点到现在这个检查点之间产生了100个XLOG，则这次检查点需要在产生100*checkpoint_completion_target个XLOG文件的过程中完成。PG会根据这些值来调度平滑检查点。
random_page_cost = 1.0     # same scale as above  , 离散扫描的成本因子，本例使用的SSD IO能力足够好
effective_cache_size = 240GB  # 可用的OS CACHE
log_destination = 'csvlog'  # Valid values are combinations of  
logging_collector = on          # Enable capturing of stderr and csvlog  
log_truncate_on_rotation = on           # If on, an existing log file with the  
update_process_title = off  
track_activities = off  
autovacuum = on    # Enable autovacuum subprocess?  'on'  
autovacuum_max_workers = 4 # max number of autovacuum subprocesses    ，允许同时有多少个垃圾回收工作进程。
autovacuum_naptime = 6s  # time between autovacuum runs   ， 自动垃圾回收探测进程的唤醒间隔
autovacuum_vacuum_cost_delay = 0    # default vacuum cost delay for  ， 垃圾回收不妥协
```  
其他优化总结：  
1. 尽量减少费的IO请求，所以本文从块设备，到逻辑卷，到文件系统的块大小都尽量和数据库块大小靠齐。  
2. 通过对齐，减少IO覆盖写。  
3. 通过大页减少内存管理开销。  
4. 通过多个客户端将数据库硬件资源充分利用起来。    
5. 减少客户端输出日志的开销，降低客户端性能干扰。   
6. 使用新的编译器，优化编译后的可执行程序质量。   
