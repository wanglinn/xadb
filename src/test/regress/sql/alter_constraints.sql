set grammar to oracle;
--删除检查约束
create table tt(id integer unique, name char(10), constraint cons_name check( name in('a','b','c')));
alter table tt drop constraint cons_name;
drop table tt;
--删除外键约束
create table tt(id integer primary key,name char(10));
create table aa(id int primary key, job varchar(10), constraint fk FOREIGN KEY(id) REFERENCES tt(id));
alter table aa drop constraint fk;
drop table tt;
drop table aa;
--增加检查约束
create table tt(id integer unique,name char(10));
alter table tt add constraint cons_name check( name in('a','b','c'));
drop table tt;
--删除主键约束
create table tt(id integer primary key,name char(10));
alter table tt drop primary key;
drop table tt;
create table tt(id integer,name char(10),constraint pkk primary key(id));
alter table tt drop constraint pkk;
drop table tt;
