set grammar to oracle;
--1、创建会话级临时表
create global temporary table tt(id integer, name varchar(10)) on commit preserve rows;
insert into tt values(1,'test');
select * from tt;
drop table tt;
--2、创建事务级临时表
create global temporary table tt(id integer, name varchar(10)) on commit delete rows;
insert into tt values(1,'test');
select * from tt;
drop table tt;
--3、普通表和临时表同名
create global temporary table tt(id integer, name varchar(10)) on commit preserve rows;
drop table tt;
create  table tt(id integer primary key, name varchar(10));
drop table tt;
--4、表格命名
create table t1(id int);
drop table t1;
create table a_#(id int);
drop table a_#;
create table tt(a#b int);
drop table tt;
create table 员工(id int);
drop table 员工;
create table a_$(id int);
drop table a_$;
create table tt(a_b int);
drop table tt;
--重命名表
create table tt(tid int,name varchar(2));
alter table tt rename to na;
drop table na;
--5、子查询创建表
create table tt(id integer,name char(10));
insert into tt values(1,'a');
insert into tt values(1,'b');
create table rr as select * from tt;
select * from rr;
drop table tt;
drop table rr;
--为表取别名
create table tt(id int,sal binary_float);
insert into tt values(1,1880.23);
insert into tt values(2,17000);
select sal money from tt;
select sal as money from tt;
select sal "money" from tt;
select id  as "my money" from tt;
select sal from tt employee;
select sal from tt employee where id>1;
select sal from tt as employee where id>1;
drop table tt;
