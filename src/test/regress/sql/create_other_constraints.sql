set grammar to oracle;
--1、非空约束
create table tt(id integer,name varchar2(10) not null);
insert into tt values(1,'');
insert into tt values(2,null);
insert into tt values(3);
drop table tt;
--2、默认值
create table tt(id integer,name varchar2(10) default 'no');
insert into tt values(1);
select * from tt;
drop table tt;
--3、唯一性
create table tt(id integer,name varchar2(10) unique);
insert into tt values(1,null);
insert into tt values(2,null);
insert into tt values(3,'a');
insert into tt values(4,'a');
insert into tt values(5,'');
insert into tt values(6,'');
select * from tt order by id;
drop table tt;
create table tt(id integer,name varchar2(10), constraint unq unique(name));
drop table tt;
create table tt(id integer unique,name varchar2(10) unique);
drop table tt;
create table tt(tid int primary key,name varchar(2) unique);
drop table tt;
create table tt(id integer,name varchar2(10), unique(id,name));
insert into tt values(1,'a');
insert into tt values(1,'b');
insert into tt values(2,'a');
insert into tt values(2,'a');
drop table tt;
--4、检查约束
create table tt(id integer,rate binary_float check(rate>1024) );
insert into tt values(1,500);
insert into tt values(1,1024.1);
select * from tt;
drop table tt;
create table tt(id integer check(id>0), rate binary_float check(rate>1024) );
insert into tt values(0,1024.1);
drop table tt;
create table tt(id integer,rate binary_float check(rate is not null) );
drop table tt;
create table tt(id integer,rate binary_float, constraint chk check(rate > 1000));
insert into tt values(1,500);
insert into tt values(1,1001);
select * from tt;
drop table tt;
create table tt(id integer,rate binary_float, constraint chk check(rate > 1000 and id >100));
insert into tt values(101,1001);
insert into tt values(1,1001);
drop table tt;

