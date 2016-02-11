pgq的实际应用案例, 在线增量复制的实施案例.  
  
创建源库  
```  
postgres=# create database src;  
CREATE DATABASE  
```  
创建目标库  
```  
postgres=# create database dest;  
CREATE DATABASE  
```  
连接到源库  
```  
\c src  
```
创建测试表  
组1, 这两个表有外键关联, 在一个事务中操作, 在事务中的所有表的跟踪记录必须插入同一个记录表.   
```  
create table grp1_tbl1 (id int8 primary key, info text, crt_time timestamp);  
create table grp1_tbl2 ( id int8 primary key, tbl1_id int8 REFERENCES grp1_tbl1(id) DEFERRABLE INITIALLY DEFERRED, info text, crt_time timestamp );  
```  
组2, 这两个表有外键关联, 在一个事务中操作, 在事务中的所有表的跟踪记录必须插入同一个记录表.   
```  
create table grp2_tbl1 (id int8 primary key, info text, crt_time timestamp);  
create table grp2_tbl2 ( id int8 primary key, tbl1_id int8 REFERENCES grp2_tbl1(id) DEFERRABLE INITIALLY DEFERRED, info text, crt_time timestamp );  
```  
组3, 这两个表有外键关联, 在一个事务中操作, 在事务中的所有表的跟踪记录必须插入同一个记录表.   
```  
create table grp3_tbl1 (id int8 primary key, info text, crt_time timestamp);  
create table grp3_tbl2 ( id int8 primary key, tbl1_id int8 REFERENCES grp3_tbl1(id) DEFERRABLE INITIALLY DEFERRED, info text, crt_time timestamp );  
```  
创建pgbench测试脚本, 三组测试表分别使用3个事务, 每个事务包含对组内2个表的更新或插入操作2次, 删除操作1次.  
```  
vi test.sql  
\setrandom grp1_tbl1_id 1 1000000  
\setrandom grp1_tbl2_id 1 2000000  
\setrandom grp2_tbl1_id 1 1000000  
\setrandom grp2_tbl2_id 1 2000000  
\setrandom grp3_tbl1_id 1 1000000  
\setrandom grp3_tbl2_id 1 2000000  
  
begin;  
insert into grp1_tbl1 (id,info,crt_time) values (:grp1_tbl1_id, md5(random()::text), now()) on conflict ON CONSTRAINT grp1_tbl1_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp1_tbl2 (id,tbl1_id,info,crt_time) values (:grp1_tbl2_id, :grp1_tbl1_id, md5(random()::text), now()) on conflict ON CONSTRAINT grp1_tbl2_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp1_tbl1 (id,info,crt_time) values (:grp1_tbl1_id+1, md5(random()::text), now()) on conflict ON CONSTRAINT grp1_tbl1_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp1_tbl2 (id,tbl1_id,info,crt_time) values (:grp1_tbl2_id+1, :grp1_tbl1_id+1, md5(random()::text), now()) on conflict ON CONSTRAINT grp1_tbl2_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
delete from grp1_tbl2 where id = (:grp1_tbl2_id+100);  
end;  
  
begin;  
insert into grp2_tbl1 (id,info,crt_time) values (:grp2_tbl1_id, md5(random()::text), now()) on conflict ON CONSTRAINT grp2_tbl1_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp2_tbl2 (id,tbl1_id,info,crt_time) values (:grp2_tbl2_id, :grp2_tbl1_id, md5(random()::text), now()) on conflict ON CONSTRAINT grp2_tbl2_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp2_tbl1 (id,info,crt_time) values (:grp2_tbl1_id+1, md5(random()::text), now()) on conflict ON CONSTRAINT grp2_tbl1_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp2_tbl2 (id,tbl1_id,info,crt_time) values (:grp2_tbl2_id+1, :grp2_tbl1_id+1, md5(random()::text), now()) on conflict ON CONSTRAINT grp2_tbl2_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
delete from grp2_tbl2 where id = (:grp2_tbl2_id+100);  
end;  
  
begin;  
insert into grp3_tbl1 (id,info,crt_time) values (:grp3_tbl1_id, md5(random()::text), now()) on conflict ON CONSTRAINT grp3_tbl1_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp3_tbl2 (id,tbl1_id,info,crt_time) values (:grp3_tbl2_id, :grp3_tbl1_id, md5(random()::text), now()) on conflict ON CONSTRAINT grp3_tbl2_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp3_tbl1 (id,info,crt_time) values (:grp3_tbl1_id+1, md5(random()::text), now()) on conflict ON CONSTRAINT grp3_tbl1_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
insert into grp3_tbl2 (id,tbl1_id,info,crt_time) values (:grp3_tbl2_id+1, :grp3_tbl1_id+1, md5(random()::text), now()) on conflict ON CONSTRAINT grp3_tbl2_pkey do update set info=excluded.info,crt_time=excluded.crt_time;  
delete from grp3_tbl2 where id = (:grp3_tbl2_id+100);  
end;  
```  
生成一部分数据  
```  
pgbench -M prepared -n -r -P 1 -f ./test.sql -c 64 -j 64 -T 20 src  
```  
  
创建hstore扩展  
```  
create extension hstore;  
```  
创建mq schema  
```  
CREATE SCHEMA IF NOT EXISTS mq;  
```  
创建获取事务结束时间的函数  
```  
create or replace function mq.get_commit_time() returns timestamp without time zone as $$  
declare  
  res timestamp without time zone;  
begin  
  show commit_time.realval into res;  
  return res;  
exception when others then  -- 如果未设置, 则使用以下SQL设置.  
  res := clock_timestamp();  
  execute 'set local commit_time.realval = '''||res||'''';  -- 设置事务级变量  
  return res;  
end;  
$$ language plpgsql;  
```  
创建三组跟踪记录表, 实际生产中可以根据需要创建多组记录表, 多组记录表的好处是, 不同的记录表, 在目标端可以并行回放.   
在同一个事务要操作多个表的话, 这些表必须的跟踪记录必须记录到同一个记录表, 回放时以达到事务一致性.   
  
第1组跟踪记录表  
```  
CREATE TABLE mq.table_change_rec_grp1 (  
  id serial8 primary key,  
  x_id int8 default txid_current(),  -- 事务号  
  consumed boolean not null default false,  --  是否已消费  
  relid oid,  --  pg_class.oid  
  table_schema name,  -- schema name  
  table_name name,  --  table name  
  when_tg text,  --  after or before  
  level text,  -- statement or row  
  op text,  --  delete, update, or insert or truncate  
  old_rec hstore,  
  new_rec hstore,  
  crt_time timestamp without time zone  not null,  -- 时间  
  dbname name,  --  数据库名  
  username name,  --   用户名  
  client_addr inet,  --  客户端地址  
  client_port int    --  客户端端口  
);  
  
create index x_id_table_change_rec_grp1 on mq.table_change_rec_grp1(x_id) where consumed=false;  
create index crt_time_id_table_change_rec_grp1 on mq.table_change_rec_grp1(crt_time,id) where consumed=false;  
  
create table mq.table_change_rec_grp1_0 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
create table mq.table_change_rec_grp1_1 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
create table mq.table_change_rec_grp1_2 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
create table mq.table_change_rec_grp1_3 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
create table mq.table_change_rec_grp1_4 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
create table mq.table_change_rec_grp1_5 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
create table mq.table_change_rec_grp1_6 (like mq.table_change_rec_grp1 including all) inherits(mq.table_change_rec_grp1);  
```  
第1组跟踪记录表对应的触发器函数  
```  
CREATE OR REPLACE FUNCTION mq.dml_trace_grp1()  
RETURNS trigger  
LANGUAGE plpgsql  
AS $BODY$  
DECLARE  
  v_new_rec hstore;  
  v_old_rec hstore;  
  v_username name := session_user;  
  v_dbname name := current_database();  
  v_client_addr inet := inet_client_addr();  
  v_client_port int := inet_client_port();  
  v_crt_time timestamp without time zone := mq.get_commit_time();  
  v_xid int8 := txid_current();  
  v_dofweek int := EXTRACT(DOW FROM v_crt_time);  
BEGIN  
  
case TG_OP  
when 'DELETE' then   
  v_old_rec := hstore(OLD.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp1_0 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp1_1 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp1_2 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp1_3 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp1_4 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp1_5 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp1_6 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
when 'INSERT' then   
  v_new_rec := hstore(NEW.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp1_0 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp1_1 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp1_2 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp1_3 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp1_4 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp1_5 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp1_6 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
when 'UPDATE' then   
  v_old_rec := hstore(OLD.*);  
  v_new_rec := hstore(NEW.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp1_0 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp1_1 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp1_2 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp1_3 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp1_4 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp1_5 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp1_6 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
else  
  RETURN null;  
end case;  
  
  RETURN null;  
END;  
$BODY$ strict;  
```  
第2组跟踪记录表  
```  
CREATE TABLE mq.table_change_rec_grp2 (  
  id serial8 primary key,  
  x_id int8 default txid_current(),  -- 事务号  
  consumed boolean not null default false,  --  是否已消费  
  relid oid,  --  pg_class.oid  
  table_schema name,  -- schema name  
  table_name name,  --  table name  
  when_tg text,  --  after or before  
  level text,  -- statement or row  
  op text,  --  delete, update, or insert or truncate  
  old_rec hstore,  
  new_rec hstore,  
  crt_time timestamp without time zone  not null,  -- 时间  
  dbname name,  --  数据库名  
  username name,  --   用户名  
  client_addr inet,  --  客户端地址  
  client_port int    --  客户端端口  
);  
  
create index x_id_table_change_rec_grp2 on mq.table_change_rec_grp2(x_id) where consumed=false;  
create index crt_time_id_table_change_rec_grp2 on mq.table_change_rec_grp2(crt_time,id) where consumed=false;  
  
create table mq.table_change_rec_grp2_0 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
create table mq.table_change_rec_grp2_1 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
create table mq.table_change_rec_grp2_2 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
create table mq.table_change_rec_grp2_3 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
create table mq.table_change_rec_grp2_4 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
create table mq.table_change_rec_grp2_5 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
create table mq.table_change_rec_grp2_6 (like mq.table_change_rec_grp2 including all) inherits(mq.table_change_rec_grp2);  
```  
  
第2组跟踪记录表对应的触发器函数  
```  
CREATE OR REPLACE FUNCTION mq.dml_trace_grp2()  
RETURNS trigger  
LANGUAGE plpgsql  
AS $BODY$  
DECLARE  
  v_new_rec hstore;  
  v_old_rec hstore;  
  v_username name := session_user;  
  v_dbname name := current_database();  
  v_client_addr inet := inet_client_addr();  
  v_client_port int := inet_client_port();  
  v_crt_time timestamp without time zone := mq.get_commit_time();  
  v_xid int8 := txid_current();  
  v_dofweek int := EXTRACT(DOW FROM v_crt_time);  
BEGIN  
  
case TG_OP  
when 'DELETE' then   
  v_old_rec := hstore(OLD.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp2_0 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp2_1 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp2_2 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp2_3 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp2_4 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp2_5 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp2_6 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
when 'INSERT' then   
  v_new_rec := hstore(NEW.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp2_0 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp2_1 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp2_2 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp2_3 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp2_4 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp2_5 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp2_6 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
when 'UPDATE' then   
  v_old_rec := hstore(OLD.*);  
  v_new_rec := hstore(NEW.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp2_0 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp2_1 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp2_2 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp2_3 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp2_4 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp2_5 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp2_6 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
else  
  RETURN null;  
end case;  
  
  RETURN null;  
END;  
$BODY$ strict;  
```  
  
第3组跟踪记录表  
```  
CREATE TABLE mq.table_change_rec_grp3 (  
  id serial8 primary key,  
  x_id int8 default txid_current(),  -- 事务号  
  consumed boolean not null default false,  --  是否已消费  
  relid oid,  --  pg_class.oid  
  table_schema name,  -- schema name  
  table_name name,  --  table name  
  when_tg text,  --  after or before  
  level text,  -- statement or row  
  op text,  --  delete, update, or insert or truncate  
  old_rec hstore,  
  new_rec hstore,  
  crt_time timestamp without time zone  not null,  -- 时间  
  dbname name,  --  数据库名  
  username name,  --   用户名  
  client_addr inet,  --  客户端地址  
  client_port int    --  客户端端口  
);  
  
create index x_id_table_change_rec_grp3 on mq.table_change_rec_grp3(x_id) where consumed=false;  
create index crt_time_id_table_change_rec_grp3 on mq.table_change_rec_grp3(crt_time,id) where consumed=false;  
  
create table mq.table_change_rec_grp3_0 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
create table mq.table_change_rec_grp3_1 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
create table mq.table_change_rec_grp3_2 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
create table mq.table_change_rec_grp3_3 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
create table mq.table_change_rec_grp3_4 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
create table mq.table_change_rec_grp3_5 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
create table mq.table_change_rec_grp3_6 (like mq.table_change_rec_grp3 including all) inherits(mq.table_change_rec_grp3);  
```  
  
第3组跟踪记录表对应的触发器函数  
```  
CREATE OR REPLACE FUNCTION mq.dml_trace_grp3()  
RETURNS trigger  
LANGUAGE plpgsql  
AS $BODY$  
DECLARE  
  v_new_rec hstore;  
  v_old_rec hstore;  
  v_username name := session_user;  
  v_dbname name := current_database();  
  v_client_addr inet := inet_client_addr();  
  v_client_port int := inet_client_port();  
  v_crt_time timestamp without time zone := mq.get_commit_time();  
  v_xid int8 := txid_current();  
  v_dofweek int := EXTRACT(DOW FROM v_crt_time);  
BEGIN  
  
case TG_OP  
when 'DELETE' then   
  v_old_rec := hstore(OLD.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp3_0 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp3_1 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp3_2 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp3_3 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp3_4 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp3_5 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp3_6 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
when 'INSERT' then   
  v_new_rec := hstore(NEW.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp3_0 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp3_1 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp3_2 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp3_3 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp3_4 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp3_5 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp3_6 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
when 'UPDATE' then   
  v_old_rec := hstore(OLD.*);  
  v_new_rec := hstore(NEW.*);  
  case v_dofweek  
  when 0 then  
    insert into mq.table_change_rec_grp3_0 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 1 then  
    insert into mq.table_change_rec_grp3_1 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 2 then  
    insert into mq.table_change_rec_grp3_2 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 3 then  
    insert into mq.table_change_rec_grp3_3 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 4 then  
    insert into mq.table_change_rec_grp3_4 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 5 then  
    insert into mq.table_change_rec_grp3_5 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  when 6 then  
    insert into mq.table_change_rec_grp3_6 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)  
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);  
  end case;  
  
else  
  RETURN null;  
end case;  
  
  RETURN null;  
END;  
$BODY$ strict;  
```  
  
为第1组测试表创建触发器函数  
```  
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON grp1_tbl1 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace_grp1();  
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON grp1_tbl2 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace_grp1();  
```  
为第2组测试表创建触发器函数  
```  
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON grp2_tbl1 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace_grp2();  
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON grp2_tbl2 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace_grp2();  
```  
为第3组测试表创建触发器函数  
```  
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON grp3_tbl1 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace_grp3();  
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON grp3_tbl2 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace_grp3();  
```  
  
创建组1消费函数  
```  
create or replace function mq.build_sql_grp1(n int) returns setof text as $$  
declare  
  m int := 0;  
  v_table_change_rec_grp1 mq.table_change_rec_grp1;  
  v_tablename name;  
  v_crt_time timestamp without time zone;  
  curs1 refcursor;  
  v_sql text := '';  
  v_cols text := '';  
  v_vals text := '';  
  v_upd_set text := '';  
  v_upd_del_where text :='';  
  v_x_id int8;  
  v_max_crt_time timestamp without time zone;  
begin  
  if n <=0 then  
    -- raise notice 'n must be > 0.';  
    return;  
  end if;  
  
  return next 'BEGIN;';  
  
  -- 取一个最小的队列表  
  select tablename,crt_time into v_tablename,v_crt_time from   
  (  
  select 'table_change_rec_grp1_0' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_0 where consumed=false  
    union all  
  select 'table_change_rec_grp1_1' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_1 where consumed=false  
    union all  
  select 'table_change_rec_grp1_2' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_2 where consumed=false  
    union all  
  select 'table_change_rec_grp1_3' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_3 where consumed=false  
    union all  
  select 'table_change_rec_grp1_4' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_4 where consumed=false  
    union all  
  select 'table_change_rec_grp1_5' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_5 where consumed=false  
    union all  
  select 'table_change_rec_grp1_6' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp1_6 where consumed=false  
  ) t   
  order by crt_time limit 1;  
  
case v_tablename  
  
when 'table_change_rec_grp1_0' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_0 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_0 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_0 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_0 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_0 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_0 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_0 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_0 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
  
when 'table_change_rec_grp1_1' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_1 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_1 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_1 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_1 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_1 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_1 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_1 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_1 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
  
when 'table_change_rec_grp1_2' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_2 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_2 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_2 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_2 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_2 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_2 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_2 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_2 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
  
when 'table_change_rec_grp1_3' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_3 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_3 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_3 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_3 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_3 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_3 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_3 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_3 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
  
when 'table_change_rec_grp1_4' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_4 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_4 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_4 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_4 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_4 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_4 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_4 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_4 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
  
when 'table_change_rec_grp1_5' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_5 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_5 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_5 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_5 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_5 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_5 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_5 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_5 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
  
when 'table_change_rec_grp1_6' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp1_6 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp1_6 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp1_6 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp1_6 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp1_6 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp1;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp1;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp1.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_6 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_6 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp1.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp1.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp1.table_schema)||'.'||quote_ident(v_table_change_rec_grp1.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp1_6 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp1.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp1;  
END LOOP;  
  
else  
  -- raise notice 'no % queue table deal code in this function.', v_tablename;  
  return;  
  
end case;  
  
end;  
$$ language plpgsql strict ;  
```  
  
创建组2消费函数  
```  
create or replace function mq.build_sql_grp2(n int) returns setof text as $$  
declare  
  m int := 0;  
  v_table_change_rec_grp2 mq.table_change_rec_grp2;  
  v_tablename name;  
  v_crt_time timestamp without time zone;  
  curs1 refcursor;  
  v_sql text := '';  
  v_cols text := '';  
  v_vals text := '';  
  v_upd_set text := '';  
  v_upd_del_where text :='';  
  v_x_id int8 ;  
  v_max_crt_time timestamp without time zone;  
begin  
  if n <=0 then  
    -- raise notice 'n must be > 0.';  
    return;  
  end if;  
  
  return next 'BEGIN;';  
  
  -- 取一个最小的队列表  
  select tablename,crt_time into v_tablename,v_crt_time from   
  (  
  select 'table_change_rec_grp2_0' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_0 where consumed=false  
    union all  
  select 'table_change_rec_grp2_1' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_1 where consumed=false  
    union all  
  select 'table_change_rec_grp2_2' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_2 where consumed=false  
    union all  
  select 'table_change_rec_grp2_3' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_3 where consumed=false  
    union all  
  select 'table_change_rec_grp2_4' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_4 where consumed=false  
    union all  
  select 'table_change_rec_grp2_5' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_5 where consumed=false  
    union all  
  select 'table_change_rec_grp2_6' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp2_6 where consumed=false  
  ) t   
  order by crt_time limit 1;  
  
case v_tablename  
  
when 'table_change_rec_grp2_0' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_0 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_0 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_0 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_0 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_0 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_0 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_0 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_0 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
  
when 'table_change_rec_grp2_1' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_1 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_1 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_1 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_1 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_1 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_1 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_1 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_1 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
  
when 'table_change_rec_grp2_2' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_2 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_2 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_2 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_2 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_2 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_2 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_2 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_2 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
  
when 'table_change_rec_grp2_3' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_3 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_3 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_3 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_3 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_3 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_3 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_3 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_3 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
  
when 'table_change_rec_grp2_4' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_4 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_4 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_4 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_4 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_4 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_4 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_4 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_4 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
  
when 'table_change_rec_grp2_5' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_5 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_5 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_5 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_5 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_5 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_5 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_5 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_5 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
  
when 'table_change_rec_grp2_6' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp2_6 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp2_6 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp2_6 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp2_6 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp2_6 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp2;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp2;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp2.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_6 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_6 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp2.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp2.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp2.table_schema)||'.'||quote_ident(v_table_change_rec_grp2.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp2_6 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp2.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp2;  
END LOOP;  
  
else  
  -- raise notice 'no % queue table deal code in this function.', v_tablename;  
  return;  
  
end case;  
  
end;  
$$ language plpgsql strict ;  
```  
  
创建组3消费函数  
```  
create or replace function mq.build_sql_grp3(n int) returns setof text as $$  
declare  
  m int := 0;  
  v_table_change_rec_grp3 mq.table_change_rec_grp3;  
  v_tablename name;  
  v_crt_time timestamp without time zone;  
  curs1 refcursor;  
  v_sql text := '';  
  v_cols text := '';  
  v_vals text := '';  
  v_upd_set text := '';  
  v_upd_del_where text :='';  
  v_x_id int8 ;  
  v_max_crt_time timestamp without time zone;  
begin  
  if n <=0 then  
    -- raise notice 'n must be > 0.';  
    return;  
  end if;  
  
  return next 'BEGIN;';  
  
  -- 取一个最小的队列表  
  select tablename,crt_time into v_tablename,v_crt_time from   
  (  
  select 'table_change_rec_grp3_0' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_0 where consumed=false  
    union all  
  select 'table_change_rec_grp3_1' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_1 where consumed=false  
    union all  
  select 'table_change_rec_grp3_2' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_2 where consumed=false  
    union all  
  select 'table_change_rec_grp3_3' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_3 where consumed=false  
    union all  
  select 'table_change_rec_grp3_4' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_4 where consumed=false  
    union all  
  select 'table_change_rec_grp3_5' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_5 where consumed=false  
    union all  
  select 'table_change_rec_grp3_6' as tablename,min(crt_time) as crt_time from mq.table_change_rec_grp3_6 where consumed=false  
  ) t   
  order by crt_time limit 1;  
  
case v_tablename  
  
when 'table_change_rec_grp3_0' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_0 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_0 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_0 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_0 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_0 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_0 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_0 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_0 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
  
when 'table_change_rec_grp3_1' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_1 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_1 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_1 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_1 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_1 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_1 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_1 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_1 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
  
when 'table_change_rec_grp3_2' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_2 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_2 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_2 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_2 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_2 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_2 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_2 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_2 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
  
when 'table_change_rec_grp3_3' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_3 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_3 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_3 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_3 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_3 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_3 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_3 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_3 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
  
when 'table_change_rec_grp3_4' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_4 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_4 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_4 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_4 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_4 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_4 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_4 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_4 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
  
when 'table_change_rec_grp3_5' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_5 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_5 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_5 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_5 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_5 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_5 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_5 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_5 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
  
when 'table_change_rec_grp3_6' then  
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec_grp3_6 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec_grp3_6 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec_grp3_6 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec_grp3_6 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec_grp3_6 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  
  
fetch curs1 into v_table_change_rec_grp3;  
LOOP  
if found then  
-- raise notice '%', v_table_change_rec_grp3;  
-- build sql  
-- case tg insert,update,delete,ddl  
-- quote_ident 封装schema,tablename,column  
-- quote_nullable 封装value  
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)  
case v_table_change_rec_grp3.op  
when 'INSERT' then  
-- 组装COLUMNS, VALUES  
v_cols := '' ;  
v_vals := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || ',' ;  
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
end loop;  
v_cols := rtrim(v_cols, ',') ;  
v_vals := rtrim(v_vals, ',') ;  
  
-- 组装SQL  
v_sql := 'insert into '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_6 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'UPDATE' then  
-- 组装COLUMNS, VALUES  
v_upd_set := '' ;  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.new_rec),1) loop  
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.new_rec))[i][2]) || ',' ;  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_set := rtrim(v_upd_set, ',') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'update '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_6 set consumed=true where current of curs1;  
return next v_sql;  
  
when 'DELETE' then  
-- 组装COLUMNS, VALUES  
v_upd_del_where := '' ;  
for i in 1..array_length(hstore_to_matrix(v_table_change_rec_grp3.old_rec),1) loop  
  if quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) = 'NULL' then  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || ' is null ' || ' and';  
  else  
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec_grp3.old_rec))[i][2]) || ' and';  
  end if;  
end loop;  
  
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;  
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;  
  
-- 组装SQL  
v_sql := 'delete from '||quote_ident(v_table_change_rec_grp3.table_schema)||'.'||quote_ident(v_table_change_rec_grp3.table_name)||' where '|| v_upd_del_where ||';' ;  
-- raise notice '%', v_sql;  
update mq.table_change_rec_grp3_6 set consumed=true where current of curs1;  
return next v_sql;  
  
else  
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec_grp3.op;  
end case;  
  
else  
close curs1;  
return next 'END;';  
return;  
end if;  
fetch curs1 into v_table_change_rec_grp3;  
END LOOP;  
  
else  
  -- raise notice 'no % queue table deal code in this function.', v_tablename;  
  return;  
  
end case;  
  
end;  
$$ language plpgsql strict ;  
```  
  
验证消息队列取数据的事务一致性  
```  
pgbench -M prepared -n -r -P 1 -f ./test.sql -c 64 -j 64 -T 10 src  
```  
  
psql src  
```  
select mq.build_sql_grp1(1);  -- min(xid)=max(xid) 取1个xid  
 BEGIN;  
 update public.grp1_tbl1 set id='131056',info='c0d5e77b1a25e9895579d54abf5a1fe1',crt_time='2016-02-10 22:23:25.327334' where  id='131056' and info='65f46b8b12e5cdd35f1d95d51dbe0d96' and crt_time='2016-02-10 22:05:11.067228' ;  
 update public.grp1_tbl2 set id='1487543',info='988bddbd620b6ebae7bb12ff1498be09',tbl1_id='235631',crt_time='2016-02-10 22:23:25.327334' where  id='1487543' and info='4dcdeddd73928541f3f3991a5bb92239' and tbl1_id='235631' and crt_time='2  
016-02-10 21:36:37.314308' ;  
 update public.grp1_tbl1 set id='131057',info='8a7f1010881b8c6593022d11635219d9',crt_time='2016-02-10 22:23:25.327334' where  id='131057' and info='c799ebdd5cc46fa8e889c5e7541d7d00' and crt_time='2016-02-10 22:04:57.90855' ;  
 insert into public.grp1_tbl2(id,info,tbl1_id,crt_time) values('1487544','c519842ff4a0d6e8e7369e23bddef451','131057','2016-02-10 22:23:25.327334');  
 END;  
  
select mq.build_sql_grp1(4);  -- min(xid)=max(xid) 取1个xid  
 BEGIN;  
 update public.grp1_tbl1 set id='548793',info='32e70c3483fe65c4cf178e14e7ff4c28',crt_time='2016-02-10 22:23:25.327503' where  id='548793' and info='b68e03e7219b05e8f0b721772a665da8' and crt_time='2016-02-10 21:36:40.8851' ;  
 update public.grp1_tbl2 set id='665971',info='e2163b17b141901cd755436393b065f6',tbl1_id='328299',crt_time='2016-02-10 22:23:25.327503' where  id='665971' and info='911f8c54164052aa756a2d0ddcff5303' and tbl1_id='328299' and crt_time='201  
6-02-10 22:04:09.349097' ;  
 update public.grp1_tbl1 set id='548794',info='92c9e6cf5bff1c0f8bb343045a99aca5',crt_time='2016-02-10 22:23:25.327503' where  id='548794' and info='09b25e5bf65703fc8036b9c11fc851c5' and crt_time='2016-02-10 21:36:40.8851' ;  
 update public.grp1_tbl2 set id='665972',info='fd988478cc4a2ef639231cdb8c12bd87',tbl1_id='328300',crt_time='2016-02-10 22:23:25.327503' where  id='665972' and info='9c536cd4a631ab1bf819c2ab28351f4a' and tbl1_id='328300' and crt_time='201  
6-02-10 22:04:09.349097' ;  
 END;  
  
select mq.build_sql_grp1(5);  -- min(xid) <> max(xid) 不取最后一个xid  
 BEGIN;  
 update public.grp1_tbl1 set id='89941',info='41da03886622b5617ca640804edec45a',crt_time='2016-02-10 22:23:25.327111' where  id='89941' and info='d2f3972c3a254cc740d48ff10521fc34' and crt_time='2016-02-10 22:10:41.274318' ;  
 update public.grp1_tbl2 set id='930464',info='35d7e72cab05a5b77361b98605ce8a1f',tbl1_id='655832',crt_time='2016-02-10 22:23:25.327111' where  id='930464' and info='ee54ab83230c477bf504cc9db772adbe' and tbl1_id='655832' and crt_time='201  
6-02-10 21:36:21.791644' ;  
 update public.grp1_tbl1 set id='89942',info='70cdce347ab4c7e4fe9e07548e7031ca',crt_time='2016-02-10 22:23:25.327111' where  id='89942' and info='403cff6b29b47adaa3177051f2503d86' and crt_time='2016-02-10 22:10:41.274318' ;  
 update public.grp1_tbl2 set id='930465',info='4d5a312fe01ae145c6eeea4f3e5af046',tbl1_id='68321',crt_time='2016-02-10 22:23:25.327111' where  id='930465' and info='61d352aa53dbb00b04bc036130daaff2' and tbl1_id='68321' and crt_time='2016-  
02-10 21:36:21.791644' ;  
 END;  
  
select mq.build_sql_grp1(8);  -- min(xid) <> max(xid) 不取最后一个xid  
 BEGIN;  
 insert into public.grp1_tbl1(id,info,crt_time) values('194416','bba5f43fce8b54a956429ac1b7ef7669','2016-02-10 22:23:25.327116');  
 update public.grp1_tbl2 set id='227485',info='4d2619f67c62c4e3ead797b89691273f',tbl1_id='280404',crt_time='2016-02-10 22:23:25.327116' where  id='227485' and info='2e55602bb09999d3cdd807aa7316b88c' and tbl1_id='280404' and crt_time='201  
6-02-10 22:10:47.235621' ;  
 insert into public.grp1_tbl1(id,info,crt_time) values('194417','50479922a568fa84a18886965df92248','2016-02-10 22:23:25.327116');  
 update public.grp1_tbl2 set id='227486',info='11d2d48473c0d0c61a7d377bfd7d8b24',tbl1_id='280405',crt_time='2016-02-10 22:23:25.327116' where  id='227486' and info='041971b9a3ad5cacb68d6df4b4ef5b94' and tbl1_id='280405' and crt_time='201  
6-02-10 22:10:47.235621' ;  
 END;  
  
select mq.build_sql_grp1(9);  -- min(xid) <> max(xid) 不取最后一个xid  
 BEGIN;  
 update public.grp1_tbl1 set id='731671',info='da56e7cbf49a1d4dfd635be9413f8976',crt_time='2016-02-10 22:23:25.32763' where  id='731671' and info='02b4badf7cc1e49ee08d31c0de7a5ac9' and crt_time='2016-02-10 22:04:43.545489' ;  
 update public.grp1_tbl2 set id='1673381',info='43a208d276a24b3cf274acf81997d8e3',tbl1_id='338174',crt_time='2016-02-10 22:23:25.32763' where  id='1673381' and info='b6a1343706c49c760f226e9280c27185' and tbl1_id='338174' and crt_time='20  
16-02-10 21:35:58.04798' ;  
 update public.grp1_tbl1 set id='731672',info='d931c737f8032d246b9d7013857fea7b',crt_time='2016-02-10 22:23:25.32763' where  id='731672' and info='aaad1256f701bb98c0d2c6ecc9ca5035' and crt_time='2016-02-10 22:10:47.520137' ;  
 update public.grp1_tbl2 set id='1673382',info='55c07d91df8856b837e9791613e23c46',tbl1_id='338175',crt_time='2016-02-10 22:23:25.32763' where  id='1673382' and info='905a030545fb9912b95e6a06e9952945' and tbl1_id='338175' and crt_time='20  
16-02-10 21:35:58.04798' ;  
 update public.grp1_tbl1 set id='656384',info='4a859cebf311b9efba3f2eb9a6f09d9d',crt_time='2016-02-10 22:23:25.32711' where  id='656384' and info='acc22461e1179993e56558bb0b43d5e5' and crt_time='2016-02-10 22:04:26.242072' ;  
 update public.grp1_tbl2 set id='1447281',info='c9ec438cf276c3baf4f440633bc9a63f',tbl1_id='446266',crt_time='2016-02-10 22:23:25.32711' where  id='1447281' and info='3050833049ae73ebc6aaa43d7c74e854' and tbl1_id='446266' and crt_time='20  
16-02-10 21:24:59.28356' ;  
 update public.grp1_tbl1 set id='656385',info='72662c014906a6f513e1caa605f94fab',crt_time='2016-02-10 22:23:25.32711' where  id='656385' and info='dfa471e9a5818cd070df6f2593c07dff' and crt_time='2016-02-10 21:36:20.892822' ;  
 update public.grp1_tbl2 set id='1447282',info='e9bd5c2ba44cbfffad67b91ef761abbc',tbl1_id='446267',crt_time='2016-02-10 22:23:25.32711' where  id='1447282' and info='64f3242b7fd3949a07fa74bc342d9bc6' and tbl1_id='446267' and crt_time='20  
16-02-10 21:24:59.28356' ;  
 END;  
  
select mq.build_sql_grp1(13);  -- min(xid) <> max(xid) 不取最后一个xid  
 BEGIN;  
 update public.grp1_tbl1 set id='782912',info='c7909a2201657277731746397d237ef9',crt_time='2016-02-10 22:23:25.327133' where  id='782912' and info='d783a9796860b1316411f5b258051858' and crt_time='2016-02-10 21:24:44.640954' ;  
 update public.grp1_tbl1 set id='379581',info='f72224a3ba4cb557ea49994b16a3138b',crt_time='2016-02-10 22:23:25.327133' where  id='379581' and info='7a7b1817f4aac88af359ba594ddb5924' and crt_time='2016-02-10 21:36:25.240159' ;  
 update public.grp1_tbl2 set id='647936',info='ffb15cdf80106bb3593adf2ee60ccf30',tbl1_id='77106',crt_time='2016-02-10 22:23:25.327133' where  id='647936' and info='8599d26d938651dfcfde78b4f86ffb9d' and tbl1_id='77106' and crt_time='2016-  
02-10 21:35:39.811541' ;  
 insert into public.grp1_tbl2(id,info,tbl1_id,crt_time) values('1310498','57c717a8be1aaf2f45eaad8826233e83','782912','2016-02-10 22:23:25.327133');  
 insert into public.grp1_tbl1(id,info,crt_time) values('379582','d7ed641cee4485a94a6496a18de86c44','2016-02-10 22:23:25.327133');  
 update public.grp1_tbl1 set id='782913',info='d02f8d22d0f6c24d58ab13df5b3c9fba',crt_time='2016-02-10 22:23:25.327133' where  id='782913' and info='5d6844b7d2dc5ec3143c932eae2ad36e' and crt_time='2016-02-10 21:24:44.640954' ;  
 update public.grp1_tbl2 set id='647937',info='2e43cfcd9abf35d7e35ba94130555f61',tbl1_id='77107',crt_time='2016-02-10 22:23:25.327133' where  id='647937' and info='60f4e42343aa30120afa3033c1905ed2' and tbl1_id='77107' and crt_time='2016-  
02-10 21:35:39.811541' ;  
 update public.grp1_tbl2 set id='1310499',info='496e0a9ecc1a5d49592005eafdd5e68f',tbl1_id='106818',crt_time='2016-02-10 22:23:25.327133' where  id='1310499' and info='aca3cb5217031c58db8640ec376e66d6' and tbl1_id='106818' and crt_time='2  
016-02-10 22:05:16.69726' ;  
 update public.grp1_tbl1 set id='411867',info='17e6c6b8251f25e19c8cd234bceb1e06',crt_time='2016-02-10 22:23:25.327571' where  id='411867' and info='99ae15ee891be0d2561d7068d1502570' and crt_time='2016-02-10 22:04:28.532467' ;  
 update public.grp1_tbl2 set id='1358340',info='6975e14b065d06c78166fd84bd413919',tbl1_id='443296',crt_time='2016-02-10 22:23:25.327571' where  id='1358340' and info='a7e98e8f6edc4c98b2a933e9dcfb1ee8' and tbl1_id='443296' and crt_time='2  
016-02-10 22:04:30.559512' ;  
 update public.grp1_tbl1 set id='411868',info='4945ec0218c9c85406ab1e14dda51588',crt_time='2016-02-10 22:23:25.327571' where  id='411868' and info='aa89bbdd62ee5821976ee3bffd8866ad' and crt_time='2016-02-10 22:10:39.064562' ;  
 update public.grp1_tbl2 set id='1358341',info='4d99962abf04a45a85f6f729ff63cd25',tbl1_id='443297',crt_time='2016-02-10 22:23:25.327571' where  id='1358341' and info='6a29504b044349981c1309842cd8cb8a' and tbl1_id='443297' and crt_time='2  
016-02-10 22:04:30.559512' ;  
 END;  
```  
连接到目标库, 创建复制表的结构.  
```  
\c dest  
```  
组1, 这两个表有外键关联, 在一个事务中操作, 在事务中的所有表的跟踪记录必须插入同一个记录表.   
```  
create table grp1_tbl1 (id int8 primary key, info text, crt_time timestamp);  
create table grp1_tbl2 ( id int8 primary key, tbl1_id int8 REFERENCES grp1_tbl1(id) DEFERRABLE INITIALLY DEFERRED, info text, crt_time timestamp );  
```  
组2, 这两个表有外键关联, 在一个事务中操作, 在事务中的所有表的跟踪记录必须插入同一个记录表.   
```  
create table grp2_tbl1 (id int8 primary key, info text, crt_time timestamp);  
create table grp2_tbl2 ( id int8 primary key, tbl1_id int8 REFERENCES grp2_tbl1(id) DEFERRABLE INITIALLY DEFERRED, info text, crt_time timestamp );  
```  
组3, 这两个表有外键关联, 在一个事务中操作, 在事务中的所有表的跟踪记录必须插入同一个记录表.   
```  
create table grp3_tbl1 (id int8 primary key, info text, crt_time timestamp);  
create table grp3_tbl2 ( id int8 primary key, tbl1_id int8 REFERENCES grp3_tbl1(id) DEFERRABLE INITIALLY DEFERRED, info text, crt_time timestamp );  
```  
例子1, 暴力同步 :   
开始压测  
```  
pgbench -M prepared -n -r -P 1 -f ./test.sql -c 64 -j 64 -T 100000 src  
```  
触发器都创建好之后, 就可以导出数据了.  
压测的同时, 将数据dump出来, 恢复到dest  
```  
pg_dump -F p -a -t grp1_tbl1 -t grp1_tbl2 -t grp2_tbl1 -t grp2_tbl2 -t grp3_tbl1 -t grp3_tbl2 -x src | psql dest -f -  
  
COPY 158048  
COPY 164730  
COPY 158068  
COPY 165006  
COPY 158147  
COPY 164808  
```  
继续压测不要停  
  
  
开始增量恢复, 首先使用单个事务复制的方式, 跳过重复部分(因为消费函数不管目标是否执行成功, 只要数据被取出即更新consumed=true)  
```  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp1(1)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp2(1)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp3(1)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
```  
确认跳过重复部分后, 使用批量增量复制  
```  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp1(1000)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp2(1000)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp3(1000)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
```  
  
停止压测, 等待增量同步完成  
  
校验数据  
```  
psql src  
select sum(hashtext(t.*::text)) from grp1_tbl1 t;  
-163788004315  
select sum(hashtext(t.*::text)) from grp1_tbl2 t;  
311855736266  
select sum(hashtext(t.*::text)) from grp2_tbl1 t;  
-1605268316207  
select sum(hashtext(t.*::text)) from grp2_tbl2 t;  
-136992258088  
select sum(hashtext(t.*::text)) from grp3_tbl1 t;  
2375761278075  
select sum(hashtext(t.*::text)) from grp3_tbl2 t;  
-388257824197  
  
psql dest  
select sum(hashtext(t.*::text)) from grp1_tbl1 t;  
select sum(hashtext(t.*::text)) from grp1_tbl2 t;  
select sum(hashtext(t.*::text)) from grp2_tbl1 t;  
select sum(hashtext(t.*::text)) from grp2_tbl2 t;  
select sum(hashtext(t.*::text)) from grp3_tbl1 t;  
select sum(hashtext(t.*::text)) from grp3_tbl2 t;  
```  
结果一致  
  
例子2, 可以用于跳过重复的温柔例子, 利用快照功能导出 :   
(先清除dest端的数据)  
  
开始压测  
```  
pgbench -M prepared -n -r -P 1 -f ./test.sql -c 64 -j 64 -T 100000 src  
```  
连接到源库, 创建一个快照, 记录当前的事务状态, 不要退出事务  
```  
psql src  
src=# begin transaction isolation level repeatable read ;  
BEGIN  
src=# select txid_current_snapshot();  
               txid_current_snapshot      
----------------------------------------------------------------------  
 31004443:31004517:31004443,31004446,31004449,31004457,31004466,31004469,31004480,31004487,31004489,31004493,31004495,31004498,31004500,31004501,31004502,31004503,31004505,31004507,31004508,31004509,31004510,31004511,31004512,31004513,31004514,31004515  
(1 row)  
```  
最小未提交事务:最小未分配事务:未提交事务(s)  
```  
src=# select pg_export_snapshot();  
 pg_export_snapshot   
--------------------  
 01DA7E30-1  
(1 row)  
```  
使用这个快照, 将数据dump出来, 恢复到dest  
```  
pg_dump --snapshot=01DA7E30-1 -F p -a -t grp1_tbl1 -t grp1_tbl2 -t grp2_tbl1 -t grp2_tbl2 -t grp3_tbl1 -t grp3_tbl2 -x src | psql dest -f -  
  
COPY 678854  
COPY 865425  
COPY 679293  
COPY 866652  
COPY 678734  
COPY 865728  
```  
结束快照事务  
```  
src=# end;  
COMMIT  
```  
继续压测不要停  
  
开始增量恢复, 首先务必等待确认pg_dump时的未提交事务已提交  
```  
postgres=# select * from txid_snapshot_xip(txid_current_snapshot()) t(xid) where t.xid in (31004443,31004446,31004449,31004457,31004466,31004469,31004480,31004487,31004489,31004493,31004495,31004498,31004500,31004501,31004502,31004503,31004505,31004507,31004508,31004509,31004510,31004511,31004512,31004513,31004514,31004515);  
 xid   
-----  
(0 rows)  
```  
清除不需要恢复的跟踪记录  
```  
psql src  
src=# update mq.table_change_rec_grp1 set consumed =true where consumed=false and (x_id<31004443 or (x_id>=31004443 and x_id<31004517 and x_id not in (31004443,31004446,31004449,31004457,31004466,31004469,31004480,31004487,31004489,31004493,31004495,31004498,31004500,31004501,31004502,31004503,31004505,31004507,31004508,31004509,31004510,31004511,31004512,31004513,31004514,31004515)));  
UPDATE 699488  

src=# update mq.table_change_rec_grp2 set consumed =true where consumed=false and (x_id<31004443 or (x_id>=31004443 and x_id<31004517 and x_id not in (31004443,31004446,31004449,31004457,31004466,31004469,31004480,31004487,31004489,31004493,31004495,31004498,31004500,31004501,31004502,31004503,31004505,31004507,31004508,31004509,31004510,31004511,31004512,31004513,31004514,31004515)));  
UPDATE 699404  

src=# update mq.table_change_rec_grp3 set consumed =true where consumed=false and (x_id<31004443 or (x_id>=31004443 and x_id<31004517 and x_id not in (31004443,31004446,31004449,31004457,31004466,31004469,31004480,31004487,31004489,31004493,31004495,31004498,31004500,31004501,31004502,31004503,31004505,31004507,31004508,31004509,31004510,31004511,31004512,31004513,31004514,31004515)));  
UPDATE 699328  
```  
批量增量复制  
```  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp1(1000)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp2(1000)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql_grp3(1000)) to stdout;commit;' | psql dest -f - >/dev/null ; done  
```  
停止压测, 等待增量同步完成  
  
校验数据  
```  
psql src  
select sum(hashtext(t.*::text)) from grp1_tbl1 t;  
566782435274  
select sum(hashtext(t.*::text)) from grp1_tbl2 t;  
119298584431  
select sum(hashtext(t.*::text)) from grp2_tbl1 t;  
-794442717174  
select sum(hashtext(t.*::text)) from grp2_tbl2 t;  
-390984534106  
select sum(hashtext(t.*::text)) from grp3_tbl1 t;  
2937942086023  
select sum(hashtext(t.*::text)) from grp3_tbl2 t;  
302638200204  
  
psql dest  
select sum(hashtext(t.*::text)) from grp1_tbl1 t;  
select sum(hashtext(t.*::text)) from grp1_tbl2 t;  
select sum(hashtext(t.*::text)) from grp2_tbl1 t;  
select sum(hashtext(t.*::text)) from grp2_tbl2 t;  
select sum(hashtext(t.*::text)) from grp3_tbl1 t;  
select sum(hashtext(t.*::text)) from grp3_tbl2 t;  
```  
结果一致  
