set grammar to oracle;
--单列索引
create table tt(id integer,name char(10));
create index idx on tt(name);
drop table tt;
--联合唯一索引，插入重复数据
create table tt(id integer,name varchar(10));
create unique index idx on tt(id,name);
insert into tt values(1,'a');
insert into tt values(1,'b');
insert into tt values(1,'a');
drop table tt;
--插入重复数据后，创建唯一索引
create table tt(id integer,name varchar(10));
insert into tt values(1,'a');
insert into tt values(2,'a');
insert into tt values(1,'a');
create unique index idx on tt(id,name);
create unique index idx on tt(name);
drop index idx;
drop table tt;
--不支持，在不同字段分别创建唯一索引
create table tt(id integer,num binary_float);
create unique index idx on tt(id);
create unique index numx on tt(num);
drop table tt;
--不支持，在非主键字段创建唯一索引
create table tt(id integer,num binary_float);
create unique index numx on tt(num);
drop table tt;
--desc参数
create table tt(id integer,priority int);
create index px on tt(id desc);
drop table tt;
--重命名索引
create table tt(id integer,name char(10));
create index idx on tt(name);
alter index idx rename to tdx;
drop table tt;