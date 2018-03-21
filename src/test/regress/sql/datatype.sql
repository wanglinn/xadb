set grammar to oracle;
set datestyle='ISO,YMD';
set timezone to 'PRC';
--varchar2		
create table tt(id number, name varchar2(4100));
drop table tt;
create table tt(id number, name varchar2(4000));
drop table tt;
--查询出的varchar2类型插入到varchar中
create table tt(id number, name varchar2(4));
insert into tt values(1,'abad');
create table aa(id number, name varchar(4));
insert into aa select * from tt;
drop table tt;
drop table aa;
--查询出的varchar2类型插入到varchar中
create table tt(id number, name char(50));
insert into tt values(1,'abad   ');
insert into tt values(2,'  abad');
create table aa(id number, name varchar(4));
insert into aa select * from tt where id=1;
insert into aa select * from tt where id=2;
insert into aa values(3,'aabb    ');
select * from aa order by id;
drop table aa;
drop table tt;
--char
create table tt(id number, name char(50));
insert into tt values(1,'abad');
select * from tt;
drop table tt;
--nvarchar2	
create table tt(id number, name nvarchar2(3));
insert into tt values(1,'你好吗');
drop table tt;
create table tt(id number, name varchar2(3));
insert into tt values(1,'你好吗');
insert into tt values(1,'你');
drop table tt;
--clob	char类型的数据插入clob时
create table aa(id int, info char(20));
insert into aa values(3,'abc');
create table tt(id int, info clob);
insert into tt values(1,'aga');
insert into tt values(2,'aga      ');
insert into tt select * from aa;
select * from tt order by id;
select * from aa order by id;
select id, length(info) from tt order by id;
drop table tt;
drop table aa;
--varchar类型的数据插入clob类型
create table aa(id int, info varchar(20));
insert into aa values(3,'abc  ');
create table tt(id int, info clob);
insert into tt values(1,'aga');
insert into tt values(2,'aga      ');
insert into tt select * from aa;
select * from tt order by id;
select * from aa order by id;
select id,length(info) from tt order by id;
drop table tt;
drop table aa;
--clob类型的数据插入varchar类型
create table aa(id int, info varchar(20));
insert into aa values(3,'abc  ');
create table tt(id int, info clob);
insert into tt values(1,'aga');
insert into tt values(2,'aga      ');
insert into aa select * from tt;
select * from tt order by id;
select * from aa order by id;
select id,length(info) from aa order by id;
drop table tt;
drop table aa;
--timestamp[(p)]精度测试
create table tt(dt timestamp(2));
insert into tt values(to_timestamp('2016-01-02 10:30:00.34','yyyy-mm-dd hh24:mi:ssxff'));
select * from tt;
drop table tt;
create table tt(dt timestamp(9));
insert into tt values(to_timestamp('2016-01-02 10:30:00.34','yyyy-mm-dd hh24:mi:ssxff'));
select * from tt;
drop table tt;
create table tt(dt timestamp(10));
drop table tt;
--timestamp[(p)] WITH TIME ZONE
set timezone=7;
select dbtimezone from dual;
select sessiontimezone from dual;
create table tt(id integer,dt timestamp(4) with time zone);
insert into tt values(1,to_timestamp('2016-01-02 10:30:00','yyyy-mm-dd hh24:mi:ss'));
select * from tt;
set timezone=8;
insert into tt values(2, to_timestamp('2016-01-02 11:31:00','yyyy-mm-dd hh24:mi:ss'));
select * from tt;
drop table tt;
--timestamp[(p)] WITH LOCAL TIME ZONE
set timezone=7;
select dbtimezone from dual;
select sessiontimezone from dual;
create table tt(id integer,dt timestamp(4) with local time zone);
insert into tt values(1,to_timestamp('2016-01-02 10:30:00','yyyy-mm-dd hh24:mi:ss'));
select * from tt;
set timezone=8;
insert into tt values(2, to_timestamp('2016-01-02 11:31:00','yyyy-mm-dd hh24:mi:ss'));
select * from tt;
drop table tt;
--double类型	精度测试
create table aa(id number,sal number(38,37));
insert into aa values(2,sinh(0.7));
create table tt(id number,job varchar(50));
insert into tt values(1,exp(2));
insert into tt select * from aa;
select length(job) from tt;
select * from tt;
drop table tt;
drop table aa;
--其他数据类型只测试极限值
--int
create table tt(id int);
insert into tt values(power(2,31)-1);
insert into tt values(power(2,31));
insert into tt values(-power(2,31));
insert into tt values(-power(2,31)-1);
insert into tt values(null);
insert into tt values('');
select * from tt order by id;
drop table tt;
--smallint
create table tt(id smallint);
insert into tt values(-32768);
insert into tt values(-32769);
insert into tt values(32767);
insert into tt values(32768);
insert into tt values(65536);
insert into tt values(null);
insert into tt values('');
select * from tt order by id;
drop table tt;
--bigint
create table tt(id bigint);
insert into tt values(power(2,63)-1);
insert into tt values(power(2,63));
insert into tt values(-power(2,63));
insert into tt values(-power(2,63)-1);
insert into tt values(null);
insert into tt values('');
select * from tt order by id;
drop table tt;
--real
create table tt(id real);
insert into tt values(power(2,63)-1);
insert into tt values(power(2,63));
insert into tt values(-power(2,63));
insert into tt values(-power(2,63)-1);
insert into tt values(null);
insert into tt values('');
select * from tt order by id;
drop table tt;
set timezone to 'PRC';
