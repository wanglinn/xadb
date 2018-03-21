set grammar to oracle;
create table tt(id int unique,name varchar(2));
insert into tt values(1,'a');
drop table tt;
--cascade constraints删除主表数据数据和drop主表
create table tt(id integer primary key,name char(10));
insert into tt values(1,'a');
insert into tt values(2,'b');
create table aa(id int primary key, job varchar(10), constraint fk FOREIGN KEY(id) REFERENCES tt(id));
insert into aa values(1,'teacher');
insert into aa values(2,'doctor');
delete from tt cascade constraints;
drop table tt cascade constraints;
drop table aa;
drop table tt;
--truncate数据
create table tt(id int unique,name varchar(2));
insert into tt values(1,'a');
select * from tt;
truncate table tt;
select * from tt;
drop table tt;
--truncate主表数据
create table tt(id integer primary key,name char(10));
insert into tt values(1,'a');
insert into tt values(2,'b');
create table aa(id int primary key, job varchar(10), constraint fk FOREIGN KEY(id) REFERENCES tt(id));
insert into aa values(1,'teacher');
insert into aa values(2,'doctor');
truncate table tt cascade constraints;
drop table aa;
drop table tt;
--delete删除主表被依赖数据
create table tt(id integer primary key,name char(10));
insert into tt values(1,'a');
insert into tt values(2,'b');
create table aa(id int primary key, job varchar(10), constraint fk FOREIGN KEY(id) REFERENCES tt(id));
insert into aa values(1,'teacher');
delete from tt where id=2;
select * from tt;
select * from aa;
drop table aa;
drop table tt;
--delete 清空表格和按条件删除
create table tt(id int,name varchar(10));
insert into tt(id,name) values(1, 'mike');
insert into tt(id,name) values(2, 'Jack');
delete from tt;
select * from tt;
insert into tt(id,name) values(1, 'mike');
insert into tt(id,name) values(2, 'Jack');
delete  from tt where id =2;
select * from tt;
drop table tt;
--从视图删除数据
create table tt(tid int,name varchar(5),sal int);
create view vv as select name,sal from tt;
insert into vv(name,sal) values('Mike',5999);
delete vv  where sal=5999;
select * from tt;
drop view vv;
drop table tt;
