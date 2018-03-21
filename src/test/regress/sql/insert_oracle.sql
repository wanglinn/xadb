set grammar to oracle;
set datestyle='ISO,YMD';
--数据类型不同，全字段插入
create table tt(id number(2,1),name varchar(10));
insert into tt values('1',11);
insert into tt values('2','11');
insert into tt values(to_char(3),33);
insert into tt values(to_char(4),'Tom');
drop table tt;
create table tt(id number(8));
insert into tt values('1');
insert into tt values(to_char(3));
drop table tt;
create table tt(id float,name varchar(10));
insert into tt values(1,'a');
insert into tt values('2','b');
insert into tt values(3,1.0);
insert into tt values(to_char(4),1.0);
drop table tt;
create table tt(id int,name varchar(10));
insert into tt values(1,'a');
insert into tt values(to_char(4),1.0);
drop table tt;
--插入部分字段数据
create table tt(id integer,name char(10), addr varchar(20),sal number(8,2));
insert into tt values(1, 'mike');
insert into tt values(2, 1900);
insert into tt(id,sal) values(3, 1900);
insert into tt(id,name,addr,sal) values(4,'Ann','NY', 1900);
insert into tt(id,name) values(5,'Ann','NY', 1900);
insert into tt(id,name,addr,sal) values(6,'Ann','NY');
select * from tt order by id;
drop table tt;
--一次性插入多条数据
create table tt(id integer,name char(10), addr varchar(20),sal number(8,2));
insert into tt(id,name) select 1, 'mike' from dual union all select 2,'Ann' from dual union all select 3,'Jhon' from dual;
select * from tt order by id;
drop table tt;
--将查询结果插入表格
create table tt(id integer,name char(10), addr varchar(20),sal number(8,2));
create table aa(id integer,name char(10));
insert into tt(id,name) values(1, 'mike');
insert into tt(id,name) values(2, 1900);
insert into aa select id,name from tt;
select * from aa order by id;
drop table tt;
drop table aa;
--类型不同的查询结果插入到表格
--char-int
create table tt(id char(2));
create table aa(id int);
insert into tt(id) values(2);
insert into aa values('1');
insert into aa select id from tt;
select * from tt order by id;
select * from aa order by id;
drop table tt;
drop table aa;
--number-char
create table tt(id int,sal char(10));
create table aa(id int,sal number(6,2));
insert into tt(id,sal) values(2, '1000');
insert into aa values(1,'2000');
insert into aa select id,sal from tt;
select * from tt;
select * from aa order by id;
drop table tt;
drop table aa;
--date-varchar
create table tt(id int,dt date);
create table aa(id int,dt varchar(20));
insert into tt values(1, to_date('2015-05-05','YYYY-mm-dd'));
insert into aa values(2, to_date('2016-06-06','YYYY-MM-DD'));
insert into aa select * from tt;
select * from tt;
select * from aa order by id;
drop table tt;
drop table aa;
--char>>date
create table tt(id int,dt varchar(30));
create table aa(id int,dt date);
insert into tt values(1,to_date('2015-05-05','YYYY-mm-dd'));
insert into aa values(2,to_date('2016-06-06','YYYY-mm-dd'));
insert into aa select * from tt;
select * from tt;
select * from aa order by id;
drop table tt;
drop table aa;
--int>>>char
create table tt(id int,name char(10));
create table aa(id char(2),name varchar(10));
insert into tt(id,name) values(2, 'mike');
insert into aa values(1,'Jack');
insert into aa select id,name from tt;
select * from tt;
select * from aa order by id;
drop table tt;
drop table aa;
--number>>>int
create table tt(id number(2,1),name char(10));
create table aa(id int,name varchar(10));
insert into tt(id,name) values(2, 'mike');
insert into aa values(to_number(1),'Jack');
insert into aa select id,name from tt;
select * from tt;
select * from aa order by id;
drop table tt;
drop table aa;
--通过子查询插入的同时修改部分数据
create table tt(id int,name varchar(10));
create table aa(id int,name varchar(10));
insert into tt(id,name) values(1, 'mike');
insert into aa(id,name) select id+1,name from tt;
insert into aa(id,name) select 3,name from tt where id=1;
select * from aa order by id;
drop table tt;
drop table aa;
--通过别名插入
create table tt(id int,name varchar(10));
create table aa(id int,name varchar(10));
insert into tt as t values(1,'Marissa');
insert into tt t values(2,'Sabstian');
insert into tt(id,name) as t values(3, 'Mike');
insert into aa(id,name) as t select id+1,name from tt;
select * from tt order by id;
drop table tt;
drop table aa;
--insert view
create table tt(tid int,name varchar(5),sal int not null);
create view vv as select name,sal from tt;
insert into vv(name,sal) values('Mike','5999');
insert into vv(name,sal) select name, sal from tt;
insert into vv(name,sal) select name, sal from vv;
insert into vv(name) values('Jhon');
select * from vv order by name;
select * from tt order by tid;
drop view vv;
drop table tt;