set grammar to oracle;
--1、非空约束
create table coctbl(id integer,name varchar2(10) not null);
insert into coctbl values(1,'');
insert into coctbl values(2,null);
insert into coctbl values(3);
drop table coctbl;
--2、默认值
create table coctbl(id integer,name varchar2(10) default 'no');
insert into coctbl values(1);
select * from coctbl;
drop table coctbl;
--3、唯一性
create table coctbl(id integer,name varchar2(10) unique);
insert into coctbl values(1,null);
insert into coctbl values(2,null);
insert into coctbl values(3,'a');
insert into coctbl values(4,'a');
insert into coctbl values(5,'');
insert into coctbl values(6,'');
select * from coctbl order by id;
drop table coctbl;
create table coctbl(id integer,name varchar2(10), constraint unq unique(name));
drop table coctbl;
create table coctbl(id integer unique,name varchar2(10) unique);
drop table coctbl;
create table coctbl(tid int primary key,name varchar(2) unique);
drop table coctbl;
create table coctbl(id integer,name varchar2(10), unique(id,name));
insert into coctbl values(1,'a');
insert into coctbl values(1,'b');
insert into coctbl values(2,'a');
insert into coctbl values(2,'a');
drop table coctbl;
--4、检查约束
create table coctbl(id integer,rate binary_float check(rate>1024) );
insert into coctbl values(1,500);
insert into coctbl values(1,1024.1);
select * from coctbl;
drop table coctbl;
create table coctbl(id integer check(id>0), rate binary_float check(rate>1024) );
insert into coctbl values(0,1024.1);
drop table coctbl;
create table coctbl(id integer,rate binary_float check(rate is not null) );
drop table coctbl;
create table coctbl(id integer,rate binary_float, constraint chk check(rate > 1000));
insert into coctbl values(1,500);
insert into coctbl values(1,1001);
select * from coctbl;
drop table coctbl;
create table coctbl(id integer,rate binary_float, constraint chk check(rate > 1000 and id >100));
insert into coctbl values(101,1001);
insert into coctbl values(1,1001);
drop table coctbl;

