skytools的pgq把PostgreSQL当消息队列来使用，londiste3再此基础上实现了表级逻辑复制，同时skytools还开放了API，允许用户自定义消息订阅功能。  

本文将介绍一下PGQ的触发器实现，因为有些情况下，你可能不方便安装PGQ。对于这种场景，其实我们可以使用自定义函数来实现同样的功能。  

本文的最终效果和使用PostgreSQL logical decode非常类似，只是对性能影响更大，因为用到了触发器，对于不支持logical decode的版本，使用本文提供的复制方法是不错的选择。  

触发器记录了所有需要用于回放的信息，包括事务号，schema，table，OP，OLD REC，NEW REC，事务提交时间。回放时，严格按照事务提交顺序进行回放。  

DDL需要其他方式来实现，比如事件触发器。  

设计思路，还有很多可以改进的点，本文只是一个演示。  
1. 使用HSTORE存储所有跟踪对象的记录。  
2. 记录事务号，如果要按事务来回放，这个是比较有效的。  
如果不按事务来回放，就是最终一致性。  
但是不按事务来同步的话，有一个问题，延迟检测的主外键关系，SQL在目标端操作可能导致失败。  
3. 在数据中加入消费标记，还有更好的方法（例如对记录订阅者的已读状态），使用标记这种方法只能有一个订阅者。  
4. 使用quote_nullable给文本逃逸  
5. 使用quote_ident给关键字逃逸  
6. 使用7张表来记录消息，一周中每天一张表。（如果量大，这个可以改进，比如每个小时一张表）  
(同样可以效仿PGQ，使用另一个进程ticker来切分消息，将消息分组)  
7. 事件触发器，记录DDL语句，并在队列表中记录状态，遇到DDL时，停止往下取消息，修改DDL标记位后，允许继续取消息。  
8. 打开事务提交时间戳track_commit_timestamp，按事务提交顺序取出，rebuild SQL.  
(理论上这样的回放顺序可以确保一致性)  
(track_commit_timestamp是9.5才有的特性，本文暂不支持。BDR也需要依赖事务提交时间戳)  
或者使用clock_timestamp，完全按照SQL执行顺序来回访。本文使用这种方式。  
9. 跨天事务的问题（如果事务跨天，可能导致先执行后半截，再执行前半截，有问题，调用mq.build_sql时，用repeatable read可以解决这个问题。）   

创建测试表  
```
CREATE TABLE test (id int primary key, info text, crt_time timestamp(0));
```
创建hstore extension  
```
create extension hstore;
```
创建队列schema  
```
CREATE SCHEMA IF NOT EXISTS mq;
```

创建7个消息队列记录表  
```
CREATE TABLE mq.table_change_rec (
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
```
在时间上创建索引  
```
create index x_id_table_change_rec on mq.table_change_rec(x_id) where consumed=false;
create index crt_time_id_table_change_rec on mq.table_change_rec(crt_time,id) where consumed=false;
```
创建子表  
```
create table mq.table_change_rec0 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
create table mq.table_change_rec1 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
create table mq.table_change_rec2 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
create table mq.table_change_rec3 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
create table mq.table_change_rec4 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
create table mq.table_change_rec5 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
create table mq.table_change_rec6 (like mq.table_change_rec including all) inherits(mq.table_change_rec);
```

创建取事务结束时间的函数，结合事务本地变量来实现。
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

创建触发器函数，记录队列  
```
CREATE OR REPLACE FUNCTION mq.dml_trace()
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
    insert into mq.table_change_rec0 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 1 then
    insert into mq.table_change_rec1 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 2 then
    insert into mq.table_change_rec2 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 3 then
    insert into mq.table_change_rec3 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 4 then
    insert into mq.table_change_rec4 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 5 then
    insert into mq.table_change_rec5 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 6 then
    insert into mq.table_change_rec6 (relid, table_schema, table_name, when_tg, level, op, old_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  end case;

when 'INSERT' then 
  v_new_rec := hstore(NEW.*);
  case v_dofweek
  when 0 then
    insert into mq.table_change_rec0 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 1 then
    insert into mq.table_change_rec1 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 2 then
    insert into mq.table_change_rec2 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 3 then
    insert into mq.table_change_rec3 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 4 then
    insert into mq.table_change_rec4 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 5 then
    insert into mq.table_change_rec5 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 6 then
    insert into mq.table_change_rec6 (relid, table_schema, table_name, when_tg, level, op, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  end case;

when 'UPDATE' then 
  v_old_rec := hstore(OLD.*);
  v_new_rec := hstore(NEW.*);
  case v_dofweek
  when 0 then
    insert into mq.table_change_rec0 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 1 then
    insert into mq.table_change_rec1 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 2 then
    insert into mq.table_change_rec2 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 3 then
    insert into mq.table_change_rec3 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 4 then
    insert into mq.table_change_rec4 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 5 then
    insert into mq.table_change_rec5 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  when 6 then
    insert into mq.table_change_rec6 (relid, table_schema, table_name, when_tg, level, op, old_rec, new_rec, crt_time, dbname, username, client_addr, client_port)
      values (tg_relid, tg_table_schema, tg_table_name, tg_when, tg_level, tg_op, v_old_rec, v_new_rec, v_crt_time, v_dbname, v_username, v_client_addr, v_client_port);
  end case;

else
  return null;
end case;

  RETURN null;
END;
$BODY$ strict;
```
创建触发器，跟踪DML，使用deferred，在事务结束时触发，从而获得事务的结束时间。    
所以事务不要使用immediate，否则时间会变成第一条触发的时间。    
一个事务有多行触发时，都记录同一个时间，即事务的结束时间。   
回放时，也使用事务结束时间顺序回放。   
```
CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON test DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace();
```
DDL跟踪使用事件触发器，本文未涉及，下一个版本完善。  

测试  
```
insert into test values (1,'test',now());
insert into test values (2,'你好\a\\''',now());
update test set info='new' where id=1;
delete from test where id=2;
```

查看跟踪信息  
```
postgres=# select tableoid::regclass,* from mq.table_change_rec;
-[ RECORD 1 ]+---------------------------------------------------------------------
tableoid     | table_change_rec2
x_id         | 283001935
consumed     | f
relid        | 24960
table_schema | public
table_name   | test
when_tg      | AFTER
level        | ROW
op           | INSERT
old_rec      | 
new_rec      | "id"=>"1", "info"=>"test", "crt_time"=>"2016-01-05 10:29:10"
crt_time     | 2016-01-05 10:29:09.755149
dbname       | postgres
username     | postgres
client_addr  | 
client_port  | 
-[ RECORD 2 ]+---------------------------------------------------------------------
tableoid     | table_change_rec2
x_id         | 283001936
consumed     | f
relid        | 24960
table_schema | public
table_name   | test
when_tg      | AFTER
level        | ROW
op           | INSERT
old_rec      | 
new_rec      | "id"=>"2", "info"=>"你好\\a\\\\'", "crt_time"=>"2016-01-05 10:29:10"
crt_time     | 2016-01-05 10:29:09.762116
dbname       | postgres
username     | postgres
client_addr  | 
client_port  | 
-[ RECORD 3 ]+---------------------------------------------------------------------
tableoid     | table_change_rec2
x_id         | 283001937
consumed     | f
relid        | 24960
table_schema | public
table_name   | test
when_tg      | AFTER
level        | ROW
op           | UPDATE
old_rec      | "id"=>"1", "info"=>"test", "crt_time"=>"2016-01-05 10:29:10"
new_rec      | "id"=>"1", "info"=>"new", "crt_time"=>"2016-01-05 10:29:10"
crt_time     | 2016-01-05 10:29:09.776981
dbname       | postgres
username     | postgres
client_addr  | 
client_port  | 
-[ RECORD 4 ]+---------------------------------------------------------------------
tableoid     | table_change_rec2
x_id         | 283001938
consumed     | f
relid        | 24960
table_schema | public
table_name   | test
when_tg      | AFTER
level        | ROW
op           | DELETE
old_rec      | "id"=>"2", "info"=>"你好\\a\\\\'", "crt_time"=>"2016-01-05 10:29:10"
new_rec      | 
crt_time     | 2016-01-05 10:29:10.06243
dbname       | postgres
username     | postgres
client_addr  | 
client_port  | 
```
消费者如何订阅消息  
分解步骤，你可以写成函数来实现。  
获得表名  
根据触发类型封装SQL  
```
1. insert
取new_rec长度，
array_length(hstore_to_matrix(new_rec),1) = 3
列名，
quote_ident(hstore_to_matrix(new_rec))[1][1]) = 'id'
quote_ident(hstore_to_matrix(new_rec))[2][1]) = 'info'
quote_ident(hstore_to_matrix(new_rec))[3][1]) = 'crt_time'
值
quote_nullable(hstore_to_matrix(new_rec))[1][2]) = '1'
quote_nullable(hstore_to_matrix(new_rec))[2][2]) = 'test'
quote_nullalbe(hstore_to_matrix(new_rec))[3][2]) = '2016-01-04 19:42:36'
......

封装SQL
insert into 表名(列名) values (值);

2. update
取new_rec和old_rec长度，列名，值

3. delete
取old_rec长度，列名，值
```

写成消费函数如下：  
（批量取N行，并保证事务完整性。）

```
create or replace function mq.build_sql(n int) returns setof text as $$
declare
  m int := 0;
  v_table_change_rec mq.table_change_rec;
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
  select 'table_change_rec0' as tablename,min(crt_time) as crt_time from mq.table_change_rec0 where consumed=false
    union all
  select 'table_change_rec1' as tablename,min(crt_time) as crt_time from mq.table_change_rec1 where consumed=false
    union all
  select 'table_change_rec2' as tablename,min(crt_time) as crt_time from mq.table_change_rec2 where consumed=false
    union all
  select 'table_change_rec3' as tablename,min(crt_time) as crt_time from mq.table_change_rec3 where consumed=false
    union all
  select 'table_change_rec4' as tablename,min(crt_time) as crt_time from mq.table_change_rec4 where consumed=false
    union all
  select 'table_change_rec5' as tablename,min(crt_time) as crt_time from mq.table_change_rec5 where consumed=false
    union all
  select 'table_change_rec6' as tablename,min(crt_time) as crt_time from mq.table_change_rec6 where consumed=false
  ) t 
  order by crt_time limit 1;

case v_tablename

when 'table_change_rec0' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec0 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec0 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec0 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec0 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec0 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec0 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec0 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec0 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;


when 'table_change_rec1' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec1 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec1 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec1 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec1 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec1 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec1 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec1 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec1 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;


when 'table_change_rec2' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec2 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec2 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec2 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec2 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec2 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec2 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec2 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec2 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;


when 'table_change_rec3' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec3 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec3 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec3 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec3 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec3 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec3 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec3 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec3 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;


when 'table_change_rec4' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec4 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec4 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec4 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec4 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec4 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec4 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec4 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec4 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;


when 'table_change_rec5' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec5 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec5 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec5 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec5 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec5 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec5 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec5 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec5 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;


when 'table_change_rec6' then
  -- 获取提交时间( 每个事务的结束时间获取原理, 通过延迟触发器, 在事务结束时触发行触发器, 通过mq.get_commit_time()函数获取时间, 可以确保事务内所有row的时间戳一致. )
  -- 回放顺序, 和事务提交顺序一致. 最小原子单位为事务.
  -- 单个事务包含多个SQL时, 可以通过command id来区分先后顺序, 或者通过序列来区分先后顺序.
  -- 多个事务同一时刻提交, 如果时间戳一致, 如果每个事务都包含多ROW, 则可能会混合顺序执行. 批量回放时合并成一个事务回放, 不影响一致性. 单一事务回放时, 随机选取哪个事务先执行.
  if n=1 then  
    select x_id into v_x_id from mq.table_change_rec6 where consumed=false order by crt_time,id limit 1;  
    open curs1 for select * from mq.table_change_rec6 where consumed=false and x_id=v_x_id order by crt_time,id for update;  
  else  
    select crt_time into v_crt_time from mq.table_change_rec6 where consumed=false order by crt_time,id limit 1 offset n-1;  
    if found then
      open curs1 for select * from mq.table_change_rec6 where consumed=false and crt_time<=v_crt_time order by crt_time,id for update;  
    else  
      -- n超出所剩跟踪记录  
      open curs1 for select * from mq.table_change_rec6 where consumed=false order by crt_time,id for update;  
    end if;  
  end if;  

fetch curs1 into v_table_change_rec;
LOOP
if found then
-- raise notice '%', v_table_change_rec;
-- build sql
-- case tg insert,update,delete,ddl
-- quote_ident 封装schema,tablename,column
-- quote_nullable 封装value
-- 不带主键的表, 如果有重复行, 使用ctid在源端操作单行, 会导致目标端不一致(避免使用ctid, 或者强制要求主键或非空唯一)
case v_table_change_rec.op
when 'INSERT' then
-- 组装COLUMNS, VALUES
v_cols := '' ;
v_vals := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_cols := v_cols || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || ',' ;
  v_vals := v_vals || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
end loop;
v_cols := rtrim(v_cols, ',') ;
v_vals := rtrim(v_vals, ',') ;

-- 组装SQL
v_sql := 'insert into '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||'('||v_cols||')'||' values('||v_vals||');' ;
-- raise notice '%', v_sql;
update mq.table_change_rec6 set consumed=true where current of curs1;
return next v_sql;

when 'UPDATE' then
-- 组装COLUMNS, VALUES
v_upd_set := '' ;
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.new_rec),1) loop
  v_upd_set := v_upd_set || quote_ident((hstore_to_matrix(v_table_change_rec.new_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.new_rec))[i][2]) || ',' ;
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_set := rtrim(v_upd_set, ',') ;
v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'update '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' set '||v_upd_set||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec6 set consumed=true where current of curs1;
return next v_sql;

when 'DELETE' then
-- 组装COLUMNS, VALUES
v_upd_del_where := '' ;
for i in 1..array_length(hstore_to_matrix(v_table_change_rec.old_rec),1) loop
  if quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) = 'NULL' then
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || ' is null ' || ' and';
  else
    v_upd_del_where := v_upd_del_where || ' ' || quote_ident((hstore_to_matrix(v_table_change_rec.old_rec))[i][1]) || '=' || quote_nullable((hstore_to_matrix(v_table_change_rec.old_rec))[i][2]) || ' and';
  end if;
end loop;

v_upd_del_where := rtrim(v_upd_del_where, 'd') ;
v_upd_del_where := rtrim(v_upd_del_where, 'n') ;
v_upd_del_where := rtrim(v_upd_del_where, 'a') ;

-- 组装SQL
v_sql := 'delete from '||quote_ident(v_table_change_rec.table_schema)||'.'||quote_ident(v_table_change_rec.table_name)||' where '|| v_upd_del_where ||';' ;
-- raise notice '%', v_sql;
update mq.table_change_rec6 set consumed=true where current of curs1;
return next v_sql;

else
  -- raise notice 'I do not known how to deal this op: %', v_table_change_rec.op;
end case;

else
close curs1;
return next 'END;';
return;
end if;
fetch curs1 into v_table_change_rec;
END LOOP;

else
  -- raise notice 'no % queue table deal code in this function.', v_tablename;
  return;

end case;

end;
$$ language plpgsql strict ;
```

使用这个消费函数，进行数据测试：  
```
postgres=# create database src;
CREATE DATABASE
postgres=# create database dest;
CREATE DATABASE
```
源端创建hstore，队列表，触发器函数，  
(略)  

创建测试表，以及触发器  
```
src=# create table test(id int primary key,info text,crt_time timestamp);
CREATE TABLE
src=# CREATE CONSTRAINT TRIGGER tg AFTER INSERT OR DELETE OR UPDATE ON test DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE mq.dml_trace();
CREATE TRIGGER
```
目标端创建测试表  
```
dest=# create table test(id int primary key,info text,crt_time timestamp);
CREATE TABLE
```
创建压测脚本  
```
vi test.sql
\setrandom id 1 5000000
insert into test values (:id, md5(random()::text), now()) on conflict ON CONSTRAINT test_pkey do update set info=excluded.info,crt_time=excluded.crt_time;
```
压测  
```
pgbench -M prepared -n -r -P 1 -f ./test.sql -c 48 -j 48 -T 120 src
```
同时另外开一个会话数据同步到dest库，每次同步1万条循环  
```
while true; do psql src -q -A -n -t -c 'begin work isolation level repeatable read; copy (select mq.build_sql(10000)) to stdout;commit;' | psql dest -f - >/dev/null ; done
```
同步结束之后，查看两边的HASH值是否一致。  
```
dest=# \c src
src=# select count(*),now() from test;
  count  |              now              
---------+-------------------------------
 3910319 | 2016-01-05 15:26:24.046004+08
(1 row)
src=# select sum(hashtext(test.*::text)) from test;
      sum       
----------------
 -1327225009705
(1 row)

src=# \c dest
dest=# select count(*),now() from test;
  count  |              now              
---------+-------------------------------
 3910319 | 2016-01-05 15:27:27.210017+08
(1 row)
dest=# select sum(hashtext(test.*::text)) from test;
      sum       
----------------
 -1327225009705
(1 row)
```
已上线业务（已经有数据积累了的）的数据同步的例子：  
```
1. 源端
首先配置以上队列跟踪。(同本文上面的过程)
然后，在一个rr隔离级别的事务中，导出数据。
# begin work isolation level REPEATABLE READ;
BEGIN
-- 记录当前事务快照，最大已提交事务号，最小未分配事务号，未提交事务数组。
# select * from txid_current_snapshot();
 txid_current_snapshot 
-----------------------
 339148965:339148965:
-- 将数据拷贝到文件
# copy table to '';
# 将mq记录表，不需要复制的记录标记为consumed=true，不需要的记录包括 
  schema+tablename匹配被复制表 并且：
  事务 <= 最大已提交事务号  
  事务 in ( >最大已提交事务 and <最小未分配事务 and not in (为提交事务数组) )

2. 目标端
导入数据
# copy table from '';
然后就可以使用以上消费函数进行数据复制了。
```
