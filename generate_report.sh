#!/bin/bash

export PGHOST=127.0.0.1
export PGPORT=1921
export PGDATABASE=postgres
export PGUSER=postgres
export PGDATA=/data01/pg_root_1921
export PGHOME=/opt/pgsql
export LD_LIBRARY_PATH=$PGHOME/lib:/lib64:/usr/lib64:/usr/local/lib64:/lib:/usr/lib:/usr/local/lib:$LD_LIBRARY_PATH
export PATH=$PGHOME/bin:$PATH:.
export DATE=`date +"%Y%m%d%H%M"`

# 记住当前目录
PWD=`pwd`

echo "----->>>---->>>  当前时间: "
echo "$DATE"
echo -e "\n"

echo "----->>>---->>>  主机名: "
hostname -s
echo -e "\n"

echo "----->>>---->>>  主机网络信息: "
ifconfig
echo -e "\n"

# 获取postgresql日志目录
pg_log_dir=`grep '^\ *[a-z]' $PGDATA/postgresql.conf|awk -F "#" '{print $1}'|grep log_directory|awk -F "=" '{print $2}'`

# 检查是否standby
is_standby=`psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -q -A -t -c 'select pg_is_in_recovery()'`

common() {
# 进入pg_log工作目录
cd $PGDATA
eval cd $pg_log_dir

echo "----->>>---->>>  获取postgresql.conf md5值: "
md5sum $PGDATA/postgresql.conf
echo "建议: "
echo "    主备md5值一致."
echo -e "\n"

echo "----->>>---->>>  获取postgresql.conf配置: "
grep '^\ *[a-z]' $PGDATA/postgresql.conf|awk -F "#" '{print $1}'
echo "建议: "
echo "    主备配置一致, 配置合理的参数值."
echo -e "\n"

echo "----->>>---->>>  获取pg_hba.conf md5值: "
md5sum $PGDATA/pg_hba.conf
echo "建议: "
echo "    主备md5值一致."
echo -e "\n"

echo "----->>>---->>>  获取pg_hba.conf配置: "
grep '^\ *[a-z]' $PGDATA/pg_hba.conf
echo "建议: "
echo "    主备配置一致, 注意trust和password认证方法的危害(password方法 验证时网络传输密码明文, 建议改为md5), 建议使用md5或者LDAP认证方法."
echo -e "\n"

echo "----->>>---->>>  获取错误日志信息: "
awk -F "," '{print $12" "$13}' *.csv |grep -E "WARNING|ERROR|FATAL|PANIC"|sort|uniq -c|sort -rn
echo "建议: "
echo "    参考 http://www.postgresql.org/docs/current/static/errcodes-appendix.html ."
echo -e "\n"

echo "----->>>---->>>  获取连接请求情况: "
find . -name "*.csv" -type f -mtime -28 -exec grep "connection authorized" {} +|awk -F "," '{print $2,$3,$5}'|sed 's/\:[0-9]*//g'|sort|uniq -c|sort -n -r
echo "建议: "
echo "    连接请求非常多时, 请考虑应用层使用连接池, 或者使用pgbouncer连接池. "
echo -e "\n"

echo "----->>>---->>>  获取认证失败情况: "
find . -name "*.csv" -type f -mtime -28 -exec grep "password authentication failed" {} +|awk -F "," '{print $2,$3,$5}'|sed 's/\:[0-9]*//g'|sort|uniq -c|sort -n -r
echo "建议: "
echo "    认证失败次数很多时, 可能是有用户在暴力破解, 建议使用auth_delay插件防止暴力破解. "
echo -e "\n"

echo "----->>>---->>>  输出定时任务: "
crontab -l
echo "建议: "
echo "    仔细检查定时任务的必要性, 以及定时任务的成功与否的评判标准, 以及监控措施. "
echo -e "\n"

echo "----->>>---->>>  输出文件系统剩余空间: "
df -m
echo "建议: "
echo "    注意预留足够的空间给数据库. "
echo -e "\n"

echo "----->>>---->>>  输出表空间对应目录: "
echo $PGDATA
ls -la $PGDATA/pg_tblspc/
echo "建议: "
echo "    注意表空间如果不是软链接, 注意是否刻意所为, 正常情况下应该是软链接. "
echo -e "\n"

echo "----->>>---->>>  输出表空间使用情况: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select spcname,pg_tablespace_location(oid),pg_size_pretty(pg_tablespace_size(oid)) from pg_tablespace order by pg_tablespace_size(oid) desc'
echo "建议: "
echo "    注意检查表空间所在文件系统的剩余空间, IOPS分配是否均匀, OS的sysstat包可以观察IO使用率. "
echo -e "\n"

echo "----->>>---->>>  输出数据库使用情况: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select datname,pg_size_pretty(pg_database_size(oid)) from pg_database order by pg_database_size(oid) desc'
echo "建议: "
echo "    注意检查数据库的大小, 是否需要清理历史数据. "
echo -e "\n"

echo "----->>>---->>>  TOP 5 SQL : total_cpu_time "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -x -c 'select c.rolname,b.datname,a.total_time/a.calls per_call_time,a.* from pg_stat_statements a,pg_database b,pg_authid c where a.userid=c.oid and a.dbid=b.oid order by a.total_time desc limit 5'
echo "建议: "
echo "    检查SQL是否有优化空间. "
echo -e "\n"

# 重置pg_stat_statements统计信息
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -A -c 'select pg_stat_statements_reset()'

echo "----->>>---->>>  活跃度: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select now(),state,count(*) from pg_stat_activity group by 1,2'
echo "建议: "
echo "    如果active状态很多, 说明数据库比较繁忙. 如果idle in transaction很多, 说明业务逻辑设计可能有问题. 如果idle很多, 说明使用了连接池, 并且可能没有自动回收连接到连接池的最小连接数. "
echo -e "\n"

echo "----->>>---->>>  用户连接数限制: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select a.rolname,a.rolconnlimit,b.connects from pg_authid a,(select usename,count(*) connects from pg_stat_activity group by usename) b where a.rolname=b.usename order by b.connects desc'
echo "建议: "
echo "    给用户保留足够的连接数, alter role ... CONNECTION LIMIT . "
echo -e "\n"

echo "----->>>---->>>  用户密码到期时间: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select rolname,rolvaliduntil from pg_authid order by rolvaliduntil'
echo "建议: "
echo "    到期后, 用户将无法登陆, 记得修改密码, 同时将密码到期时间延长到某个时间或无限时间, alter role ... VALID UNTIL 'timestamp' . "
echo -e "\n"

echo "----->>>---->>>  数据库连接限制: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select a.datname, a.datconnlimit, b.connects from pg_database a,(select datname,count(*) connects from pg_stat_activity group by datname) b where a.datname=b.datname order by b.connects desc'
echo "建议: "
echo "    给数据库保留足够的连接数, alter database ... CONNECTION LIMIT . "
echo -e "\n"

echo "----->>>---->>>  总剩余连接数: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select max_conn,used,res_super,max_conn-used-res_super res_normal from (select count(*) used from pg_stat_activity) t1,(select setting::int res_super from pg_settings where name=$$superuser_reserved_connections$$) t2,(select setting::int max_conn from pg_settings where name=$$max_connections$$) t3'
echo "建议: "
echo "    给超级用户和普通用户预留足够的连接, 以免不能登录数据库. "
echo -e "\n"


echo "----->>>---->>>  TOP 10 size对象: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),b.nspname,c.relname,c.relkind,pg_size_pretty(pg_relation_size(c.oid)),a.seq_scan,a.seq_tup_read,a.idx_scan,a.idx_tup_fetch,a.n_tup_ins,a.n_tup_upd,a.n_tup_del,a.n_tup_hot_upd,a.n_live_tup,a.n_dead_tup from pg_stat_all_tables a, pg_class c,pg_namespace b where c.relnamespace=b.oid and c.relkind=$$r$$ and a.relid=c.oid order by pg_relation_size(c.oid) desc limit 10'
done
echo "建议: "
echo "    单表超过8GB的情况下, 如果这个表需要频繁更新 或 删除+插入的话, 建议拆分后获得更好的性能, 以及便于对膨胀索引进行维护; 如果是只读的表, 建议适当结合SQL进行优化. "
echo -e "\n"

echo "----->>>---->>>  索引数超过4并且SIZE大于10MB的表: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(), t2.nspname, t1.relname, pg_size_pretty(pg_relation_size(t1.oid)), t3.idx_cnt from pg_class t1, pg_namespace t2, (select indrelid,count(*) idx_cnt from pg_index group by 1 having count(*)>4) t3 where t1.oid=t3.indrelid and t1.relnamespace=t2.oid and pg_relation_size(t1.oid)/1024/1024.0>10 order by t3.idx_cnt desc'
done
echo "建议: "
echo "    索引数量太多, 影响表的增删改性能, 建议检查是否有不需要的索引. "
echo -e "\n"

echo "----->>>---->>>  上次巡检以来未使用或使用较少的索引: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select t2.schemaname,t2.relname,t2.indexrelname,t2.idx_scan,t2.idx_tup_read,t2.idx_tup_fetch,pg_size_pretty(pg_relation_size(indexrelid)) from pg_stat_all_tables t1,pg_stat_all_indexes t2 where t1.relid=t2.relid and t2.idx_scan<10 and t2.schemaname not in ($$pg_toast$$,$$pg_catalog$$) and indexrelid not in (select conindid from pg_constraint where contype in ($$p$$,$$u$$,$$f$$)) and pg_relation_size(indexrelid)>65536 order by pg_relation_size(indexrelid) desc'
done
echo "建议: "
echo "    索引数量太多, 影响表的增删改性能, 建议删除不需要的索引. "
echo -e "\n"

echo "----->>>---->>>  表引膨胀检查: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -x -c 'SELECT
  current_database() AS db, schemaname, tablename, reltuples::bigint AS tups, relpages::bigint AS pages, otta,
  ROUND(CASE WHEN otta=0 OR sml.relpages=0 OR sml.relpages=otta THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
  CASE WHEN relpages < otta THEN $$0 bytes$$::text ELSE (bs*(relpages-otta))::bigint || $$ bytes$$ END AS wastedsize,
  iname, ituples::bigint AS itups, ipages::bigint AS ipages, iotta,
  ROUND(CASE WHEN iotta=0 OR ipages=0 OR ipages=iotta THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
  CASE WHEN ipages < iotta THEN $$0 bytes$$ ELSE (bs*(ipages-iotta))::bigint || $$ bytes$$ END AS wastedisize,
  CASE WHEN relpages < otta THEN
    CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta::bigint) END
    ELSE CASE WHEN ipages < iotta THEN bs*(relpages-otta::bigint)
      ELSE bs*(relpages-otta::bigint + ipages-iotta::bigint) END
  END AS totalwastedbytes
FROM (
  SELECT
    nn.nspname AS schemaname,
    cc.relname AS tablename,
    COALESCE(cc.reltuples,0) AS reltuples,
    COALESCE(cc.relpages,0) AS relpages,
    COALESCE(bs,0) AS bs,
    COALESCE(CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)),0) AS otta,
    COALESCE(c2.relname,$$?$$) AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM
     pg_class cc
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname <> $$information_schema$$
  LEFT JOIN
  (
    SELECT
      ma,bs,foo.nspname,foo.relname,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        ns.nspname, tbl.relname, hdr, ma, bs,
        SUM((1-coalesce(null_frac,0))*coalesce(avg_width, 2048)) AS datawidth,
        MAX(coalesce(null_frac,0)) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = ns.nspname AND s2.tablename = tbl.relname
        ) AS nullhdr
      FROM pg_attribute att 
      JOIN pg_class tbl ON att.attrelid = tbl.oid
      JOIN pg_namespace ns ON ns.oid = tbl.relnamespace 
      LEFT JOIN pg_stats s ON s.schemaname=ns.nspname
      AND s.tablename = tbl.relname
      AND s.inherited=false
      AND s.attname=att.attname,
      (
        SELECT
          (SELECT current_setting($$block_size$$)::numeric) AS bs,
            CASE WHEN SUBSTRING(SPLIT_PART(v, $$ $$, 2) FROM $$#"[0-9]+.[0-9]+#"%$$ for $$#$$)
              IN ($$8.0$$,$$8.1$$,$$8.2$$) THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ $$mingw32$$ OR v ~ $$64-bit$$ THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      WHERE att.attnum > 0 AND tbl.relkind=$$r$$
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  ON cc.relname = rs.relname AND nn.nspname = rs.nspname
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml order by wastedbytes desc limit 5'
done
echo "建议: "
echo "    根据浪费的字节数, 设置合适的autovacuum_vacuum_scale_factor, 大表如果频繁的有更新或删除和插入操作, 建议设置较小的autovacuum_vacuum_scale_factor来降低浪费空间. 同时还需要打开autovacuum, 根据服务器的内存大小, CPU核数, 设置足够大的autovacuum_work_mem或autovacuum_max_workers或maintenance_work_mem, 以及足够小的autovacuum_naptime . "
echo "    同时还需要检查是否对大数据库使用了逻辑备份, 系统中是否经常有长SQL, 长事务. 这些都有可能导致膨胀. "
echo "    使用pg_reorg或者vacuum full可以回收空间. "
echo "    参考: http://blog.163.com/digoal@126/blog/static/1638770402015329115636287/ "
echo -e "\n"


echo "----->>>---->>>  索引膨胀检查: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -x -c 'SELECT
  current_database() AS db, schemaname, tablename, reltuples::bigint AS tups, relpages::bigint AS pages, otta,
  ROUND(CASE WHEN otta=0 OR sml.relpages=0 OR sml.relpages=otta THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
  CASE WHEN relpages < otta THEN 0 ELSE relpages::bigint - otta END AS wastedpages,
  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::bigint END AS wastedbytes,
  CASE WHEN relpages < otta THEN $$0 bytes$$::text ELSE (bs*(relpages-otta))::bigint || $$ bytes$$ END AS wastedsize,
  iname, ituples::bigint AS itups, ipages::bigint AS ipages, iotta,
  ROUND(CASE WHEN iotta=0 OR ipages=0 OR ipages=iotta THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
  CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
  CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
  CASE WHEN ipages < iotta THEN $$0 bytes$$ ELSE (bs*(ipages-iotta))::bigint || $$ bytes$$ END AS wastedisize,
  CASE WHEN relpages < otta THEN
    CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta::bigint) END
    ELSE CASE WHEN ipages < iotta THEN bs*(relpages-otta::bigint)
      ELSE bs*(relpages-otta::bigint + ipages-iotta::bigint) END
  END AS totalwastedbytes
FROM (
  SELECT
    nn.nspname AS schemaname,
    cc.relname AS tablename,
    COALESCE(cc.reltuples,0) AS reltuples,
    COALESCE(cc.relpages,0) AS relpages,
    COALESCE(bs,0) AS bs,
    COALESCE(CEIL((cc.reltuples*((datahdr+ma-
      (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)),0) AS otta,
    COALESCE(c2.relname,$$?$$) AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
    COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
  FROM
     pg_class cc
  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname <> $$information_schema$$
  LEFT JOIN
  (
    SELECT
      ma,bs,foo.nspname,foo.relname,
      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM (
      SELECT
        ns.nspname, tbl.relname, hdr, ma, bs,
        SUM((1-coalesce(null_frac,0))*coalesce(avg_width, 2048)) AS datawidth,
        MAX(coalesce(null_frac,0)) AS maxfracsum,
        hdr+(
          SELECT 1+count(*)/8
          FROM pg_stats s2
          WHERE null_frac<>0 AND s2.schemaname = ns.nspname AND s2.tablename = tbl.relname
        ) AS nullhdr
      FROM pg_attribute att 
      JOIN pg_class tbl ON att.attrelid = tbl.oid
      JOIN pg_namespace ns ON ns.oid = tbl.relnamespace 
      LEFT JOIN pg_stats s ON s.schemaname=ns.nspname
      AND s.tablename = tbl.relname
      AND s.inherited=false
      AND s.attname=att.attname,
      (
        SELECT
          (SELECT current_setting($$block_size$$)::numeric) AS bs,
            CASE WHEN SUBSTRING(SPLIT_PART(v, $$ $$, 2) FROM $$#"[0-9]+.[0-9]+#"%$$ for $$#$$)
              IN ($$8.0$$,$$8.1$$,$$8.2$$) THEN 27 ELSE 23 END AS hdr,
          CASE WHEN v ~ $$mingw32$$ OR v ~ $$64-bit$$ THEN 8 ELSE 4 END AS ma
        FROM (SELECT version() AS v) AS foo
      ) AS constants
      WHERE att.attnum > 0 AND tbl.relkind=$$r$$
      GROUP BY 1,2,3,4,5
    ) AS foo
  ) AS rs
  ON cc.relname = rs.relname AND nn.nspname = rs.nspname
  LEFT JOIN pg_index i ON indrelid = cc.oid
  LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
) AS sml order by wastedibytes desc limit 5'
done
echo "建议: "
echo "    如果索引膨胀太大, 会影响性能, 建议重建索引, create index CONCURRENTLY ... . "
echo -e "\n"

echo "----->>>---->>>  垃圾数据: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),schemaname,relname,n_dead_tup from pg_stat_all_tables where n_live_tup>0 and n_dead_tup/n_live_tup>0.2 and schemaname not in ($$pg_toast$$,$$pg_catalog$$) order by n_dead_tup desc limit 5'
done
echo "建议: "
echo "    通常垃圾过多, 可能是因为无法回收垃圾, 或者回收垃圾的进程繁忙或没有及时唤醒, 或者没有开启autovacuum, 或在短时间内产生了大量的垃圾 . "
echo -e "\n"

echo "----->>>---->>>  数据库年龄: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select datname,age(datfrozenxid),2^31-age(datfrozenxid) age_remain from pg_database order by age(datfrozenxid) desc'
echo "建议: "
echo "    数据库的年龄正常情况下应该小于vacuum_freeze_table_age, 如果剩余年龄小于5亿, 建议人为干预, 将LONG SQL或事务杀掉后, 执行vacuum freeze . "
echo -e "\n"

echo "----->>>---->>>  表年龄: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER --pset=pager=off -d $PGDATABASE -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),rolname,nspname,relkind,relname,age(relfrozenxid),2^31-age(relfrozenxid) age_remain from pg_authid t1 join pg_class t2 on t1.oid=t2.relowner join pg_namespace t3 on t2.relnamespace=t3.oid where t2.relkind in ($$t$$,$$r$$) order by age(relfrozenxid) desc limit 5'
done
echo "建议: "
echo "    表的年龄正常情况下应该小于vacuum_freeze_table_age, 如果剩余年龄小于5亿, 建议人为干预, 将LONG SQL或事务杀掉后, 执行vacuum freeze . "
echo -e "\n"

echo "----->>>---->>>  继承关系检查: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select inhrelid::regclass,inhparent::regclass,inhseqno from pg_inherits order by 2,3'
done
echo "建议: "
echo "    如果使用继承来实现分区表, 注意分区表的触发器中逻辑是否正常, 对于时间模式的分区表是否需要及时加分区, 修改触发器函数 . "
echo "    关注继承表的权限统一性. "
echo -e "\n"

echo "----->>>---->>>  用户或数据库级别定制参数: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select * from pg_db_role_setting'
echo "建议: "
echo "    定制参数需要关注, 优先级高于数据库的启动参数和配置文件中的参数, 利于排错. "
echo -e "\n"

echo "----->>>---->>>  是否开启归档, 自动垃圾回收: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select name,setting from pg_settings where name in ($$archive_mode$$,$$autovacuum$$,$$archive_command$$)'
echo "建议: "
echo "    建议开启自动垃圾回收, 开启归档. "
echo -e "\n"

echo "----->>>---->>>  归档统计信息: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select pg_xlogfile_name(pg_current_xlog_location()) now_xlog, * from pg_stat_archiver'
echo "建议: "
echo "    如果当前的XLOG文件和最后一个归档失败的XLOG文件之间相差很多个文件, 建议尽快排查归档失败的原因, 以及修复, 否则pg_xlog目录可能会撑爆. "
echo -e "\n"

echo "----->>>---->>>  流复制统计信息: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -x -c 'select pg_xlog_location_diff(pg_current_xlog_location(),flush_location), * from pg_stat_replication'
echo "建议: "
echo "    关注流复制的延迟, 如果延迟非常大, 建议排查网络带宽, 以及本地读xlog的性能和远程写xlog的性能. "
echo -e "\n"

echo "----->>>---->>>  流复制插槽: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select pg_xlog_location_diff(pg_current_xlog_location(),restart_lsn), * from pg_replication_slots'
echo "建议: "
echo "    如果restart_lsn和当前XLOG相差非常大的字节数, 需要排查slot的订阅者是否能正常接收XLOG, 或者订阅者是否正常. 长时间不将slot的数据取走, pg_xlog目录可能会撑爆. "
echo -e "\n"

echo "----->>>---->>>  数据库统计信息, 回滚比例, 命中比例, 数据块读写时间, 死锁, 复制冲突: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select datname,round(100*(xact_rollback::numeric/(case when xact_commit > 0 then xact_commit else 1 end + xact_rollback)),2)||$$ %$$ rollback_ratio, round(100*(blks_hit::numeric/(case when blks_read>0 then blks_read else 1 end + blks_hit)),2)||$$ %$$ hit_ratio, blk_read_time, blk_write_time, conflicts, deadlocks from pg_stat_database'
echo "建议: "
echo "    回滚比例大说明业务逻辑可能有问题, 命中率小说明shared_buffer要加大, 数据块读写时间长说明块设备的IO能力要提升, 死锁次数多说明业务逻辑有问题, 复制冲突次数多说明备库可能在跑LONG SQL. "
echo -e "\n"

echo "----->>>---->>>  检查点, bgwriter 统计信息: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -x -c 'select * from pg_stat_bgwriter'
echo "建议: "
echo "    checkpoint_write_time多说明检查点持续时间长, 检查点过程中产生了较多的脏页. checkpoint_sync_time代表检查点开始时的shared buffer中的脏页被同步到磁盘的时间, 如果时间过长, 并且数据库在检查点时性能较差, 考虑一下提升块设备的IOPS能力. buffers_backend_fsync太多说明需要加大shared buffer或者减小bgwriter_delay参数. "
echo -e "\n"

echo "----->>>---->>>  长事务, 2PC: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -x -c 'select datname,usename,query,xact_start,now()-xact_start xact_duration,query_start,now()-query_start query_duration,state from pg_stat_activity where state<>$$idle$$ and (backend_xid is not null or backend_xmin is not null) order by xact_start'
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -x -c 'select name,statement,prepare_time,now()-prepare_time,parameter_types,from_sql from pg_prepared_statements order by prepare_time'
echo "建议: "
echo "    长事务过程中产生的垃圾, 无法回收, 建议不要在数据库中运行LONG SQL, 或者错开DML高峰时间去运行LONG SQL. 2PC事务一定要记得快速的结束掉. "
echo -e "\n"

echo "----->>>---->>>  锁等待: "
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -x --pset=pager=off <<EOF
create or replace function f_lock_level(i_mode text) returns int as \$\$
declare
begin
  case i_mode
    when 'INVALID' then return 0;
    when 'AccessShareLock' then return 1;
    when 'RowShareLock' then return 2;
    when 'RowExclusiveLock' then return 3;
    when 'ShareUpdateExclusiveLock' then return 4;
    when 'ShareLock' then return 5;
    when 'ShareRowExclusiveLock' then return 6;
    when 'ExclusiveLock' then return 7;
    when 'AccessExclusiveLock' then return 8;
    else return 0;
  end case;
end; 
\$\$ language plpgsql strict;

with t_wait as                     
(select a.mode,a.locktype,a.database,a.relation,a.page,a.tuple,a.classid,a.objid,a.objsubid,a.pid,a.virtualtransaction,a.virtualxid,a,transactionid,b.query,b.xact_start,b.query_start,b.usename,b.datname from pg_locks a,pg_stat_activity b where a.pid=b.pid and not a.granted),
t_run as 
(select a.mode,a.locktype,a.database,a.relation,a.page,a.tuple,a.classid,a.objid,a.objsubid,a.pid,a.virtualtransaction,a.virtualxid,a,transactionid,b.query,b.xact_start,b.query_start,b.usename,b.datname from pg_locks a,pg_stat_activity b where a.pid=b.pid and a.granted) 
select r.locktype,r.mode r_mode,r.usename r_user,r.datname r_db,r.relation::regclass,r.pid r_pid,r.xact_start r_xact_start,r.query_start r_query_start,now()-r.query_start r_locktime,r.query r_query,
w.mode w_mode,w.pid w_pid,w.xact_start w_xact_start,w.query_start w_query_start,now()-w.query_start w_locktime,w.query w_query  
from t_wait w,t_run r where
  r.locktype is not distinct from w.locktype and
  r.database is not distinct from w.database and
  r.relation is not distinct from w.relation and
  r.page is not distinct from w.page and
  r.tuple is not distinct from w.tuple and
  r.classid is not distinct from w.classid and
  r.objid is not distinct from w.objid and
  r.objsubid is not distinct from w.objsubid and
  r.transactionid is not distinct from w.transactionid and
  r.pid <> w.pid
  order by f_lock_level(w.mode)+f_lock_level(r.mode) desc,r.xact_start;
EOF
echo "建议: "
echo "    锁等待状态, 反映业务逻辑的问题或者性能问题. "
echo -e "\n"

echo "----->>>---->>>  unlogged table 和 哈希索引: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),t3.rolname,t2.nspname,t1.relname from pg_class t1,pg_namespace t2,pg_authid t3 where t1.relnamespace=t2.oid and t1.relowner=t3.oid and t1.relpersistence=$$u$$'
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),pg_get_indexdef(oid) from pg_class where relkind=$$i$$ and pg_get_indexdef(oid) ~ $$USING hash$$'
done
echo "建议: "
echo "    unlogged table和hash index不记录XLOG, 在standby节点没有数据, 同时在数据库CRASH后无法修复, 不建议使用. "
echo -e "\n"

echo "----->>>---->>>  触发器, 事件触发器: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),relname,tgname,proname,tgenabled from pg_trigger t1,pg_class t2,pg_proc t3 where t1.tgfoid=t3.oid and t1.tgrelid=t2.oid'
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -q -c 'select current_database(),rolname,proname,evtname,evtevent,evtenabled,evttags from pg_event_trigger t1,pg_proc t2,pg_authid t3 where t1.evtfoid=t2.oid and t1.evtowner=t3.oid'
done
echo "建议: "
echo "    请管理员注意触发器和事件触发器的必要性. "
echo -e "\n"

echo "----->>>---->>>  序列剩余使用次数: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off <<EOF
create or replace function f(OUT v_datname name, OUT v_role name, OUT v_nspname name, OUT v_relname name, OUT v_times_remain int8) returns setof record as \$\$
declare
begin
  v_datname := current_database();
  for v_role,v_nspname,v_relname in select rolname,nspname,relname from pg_authid t1 , pg_class t2 , pg_namespace t3 where t1.oid=t2.relowner and t2.relnamespace=t3.oid and t2.relkind='S' 
  LOOP
    execute 'select (max_value-last_value)/increment_by from "'||v_nspname||'"."'||v_relname||'" where not is_cycled' into v_times_remain;
    return next;
  end loop;
end;
\$\$ language plpgsql;

select * from f() where v_times_remain is not null order by v_times_remain limit 10;
EOF
done
echo "建议: "
echo "    序列剩余使用次数到了之后, 将无法使用, 报错. "
echo -e "\n"

echo "----->>>---->>>  密码泄露检查: "
grep -i "password" ~/.psql_history|grep -i -E "role|group|user"
grep -i -r -E "role|group|user" *.csv|grep -i "password"|grep -i -E "create|alter"
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -c 'select query from pg_stat_statements where (query ~* $$group$$ or query ~* $$user$$ or query ~* $$role$$) and query ~* $$password$$'
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -q -c 'select * from pg_authid where rolpassword !~ $$^md5$$ or length(rolpassword)<>35'
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -c 'select current_database(),* from pg_user_mappings'
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -c 'select current_database(),* from pg_views where definition ~* $$password$$ and definition ~* $$dblink$$'
done
echo "建议: "
echo "    如果以上输出显示密码已泄露, 尽快修改, 并通过参数避免密码又被记录到以上文件中(psql -n) (set log_statement='none'; set log_min_duration_statement=-1; set log_duration=off; set pg_stat_statements.track_utility=off;) . "
echo "    明文密码不安全, 建议使用create|alter role ... encrypted password. "
echo "    在fdw, dblink based view中不建议使用密码明文. "
echo -e "\n"

echo "----->>>---->>>  普通用户对象上的规则安全检查: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -c 'select current_database(),a.schemaname,a.tablename,a.rulename,a.definition from pg_rules a,pg_namespace b,pg_class c,pg_authid d where a.schemaname=b.nspname and a.tablename=c.relname and d.oid=c.relowner and not d.rolsuper union all select current_database(),a.schemaname,a.viewname,a.viewowner,a.definition from pg_views a,pg_namespace b,pg_class c,pg_authid d where a.schemaname=b.nspname and a.viewname=c.relname and d.oid=c.relowner and not d.rolsuper'
done
echo "建议: "
echo "    防止普通用户在规则中设陷阱, 注意有危险的security invoker的函数调用. "
echo -e "\n"

echo "----->>>---->>>  普通用户自定义函数安全检查: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -c 'select current_database(),b.rolname,c.nspname,a.proname from pg_proc a,pg_authid b,pg_namespace c where a.proowner=b.oid and a.pronamespace=c.oid and not b.rolsuper and not a.prosecdef'
done
echo "建议: "
echo "    防止普通用户在函数中设陷阱, 注意有危险的security invoker的函数调用. "
echo -e "\n"

echo "----->>>---->>>  重置统计信息: "
for db in `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -t -A -q -c 'select datname from pg_database where datname not in ($$template0$$, $$template1$$)'`
do
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $db --pset=pager=off -c 'select pg_stat_reset()'
done
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -c 'select pg_stat_reset_shared($$bgwriter$$)'
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE --pset=pager=off -c 'select pg_stat_reset_shared($$archiver$$)'

# HA 状态是否正常

# sar io, load, ......

}

standby() {
echo "----->>>---->>>  获取recovery.conf md5值: "
md5sum $PGDATA/recovery.conf
echo -e "\n"
echo "----->>>---->>>  获取recovery.conf配置: "
grep '^\ *[a-z]' $PGDATA/recovery.conf|awk -F "#" '{print $1}'
echo -e "\n"
}

primary() {
echo "----->>>---->>>  获取recovery.done md5值: "
md5sum $PGDATA/recovery.done
echo -e "\n"
echo "----->>>---->>>  获取recovery.done配置: "
grep '^\ *[a-z]' $PGDATA/recovery.done|awk -F "#" '{print $1}'
echo -e "\n"
}


if [ $is_standby == 't' ]; then
echo "This is standby node: "
echo -e "\n"
standby
common
cd $pwd
return 0

else
echo "This is primary node: "
echo -e "\n"
primary
common
cd $pwd
return 0

fi
