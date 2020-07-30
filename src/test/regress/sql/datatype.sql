set grammar to oracle;
set datestyle='ISO,YMD';
set timezone to 'PRC';
--varchar2		
create table datatype_tbl1(id number, name varchar2(4100));
drop table datatype_tbl1;
create table datatype_tbl1(id number, name varchar2(4000));
drop table datatype_tbl1;
--查询出的varchar2类型插入到varchar中
create table datatype_tbl1(id number, name varchar2(4));
insert into datatype_tbl1 values(1,'abad');
create table datatype_tbl2(id number, name varchar(4));
insert into datatype_tbl2 select * from datatype_tbl1;
drop table datatype_tbl1;
drop table datatype_tbl2;
--查询出的varchar2类型插入到varchar中
create table datatype_tbl1(id number, name char(50));
insert into datatype_tbl1 values(1,'abad   ');
insert into datatype_tbl1 values(2,'  abad');
create table datatype_tbl2(id number, name varchar(4));
insert into datatype_tbl2 select * from datatype_tbl1 where id=1;
insert into datatype_tbl2 select * from datatype_tbl1 where id=2;
insert into datatype_tbl2 values(3,'aabb    ');
select * from datatype_tbl2 order by id;
drop table datatype_tbl2;
drop table datatype_tbl1;
--char
create table datatype_tbl1(id number, name char(50));
insert into datatype_tbl1 values(1,'abad');
select * from datatype_tbl1;
drop table datatype_tbl1;
--nvarchar2	
create table datatype_tbl1(id number, name nvarchar2(3));
insert into datatype_tbl1 values(1,'你好吗');
drop table datatype_tbl1;
create table datatype_tbl1(id number, name varchar2(3));
insert into datatype_tbl1 values(1,'你好吗');
insert into datatype_tbl1 values(1,'你');
drop table datatype_tbl1;
--clob	char类型的数据插入clob时
create table datatype_tbl2(id int, info char(20));
insert into datatype_tbl2 values(3,'abc');
create table datatype_tbl1(id int, info clob);
insert into datatype_tbl1 values(1,'aga');
insert into datatype_tbl1 values(2,'aga      ');
insert into datatype_tbl1 select * from datatype_tbl2;
select * from datatype_tbl1 order by id;
select * from datatype_tbl2 order by id;
select id, length(info) from datatype_tbl1 order by id;
drop table datatype_tbl1;
drop table datatype_tbl2;
--varchar类型的数据插入clob类型
create table datatype_tbl2(id int, info varchar(20));
insert into datatype_tbl2 values(3,'abc  ');
create table datatype_tbl1(id int, info clob);
insert into datatype_tbl1 values(1,'aga');
insert into datatype_tbl1 values(2,'aga      ');
insert into datatype_tbl1 select * from datatype_tbl2;
select * from datatype_tbl1 order by id;
select * from datatype_tbl2 order by id;
select id,length(info) from datatype_tbl1 order by id;
drop table datatype_tbl1;
drop table datatype_tbl2;
--clob类型的数据插入varchar类型
create table datatype_tbl2(id int, info varchar(20));
insert into datatype_tbl2 values(3,'abc  ');
create table datatype_tbl1(id int, info clob);
insert into datatype_tbl1 values(1,'aga');
insert into datatype_tbl1 values(2,'aga      ');
insert into datatype_tbl2 select * from datatype_tbl1;
select * from datatype_tbl1 order by id;
select * from datatype_tbl2 order by id;
select id,length(info) from datatype_tbl2 order by id;
drop table datatype_tbl1;
drop table datatype_tbl2;
--timestamp[(p)]精度测试
create table datatype_tbl1(dt timestamp(2));
insert into datatype_tbl1 values(to_timestamp('2016-01-02 10:30:00.34','yyyy-mm-dd hh24:mi:ssxff'));
select * from datatype_tbl1;
drop table datatype_tbl1;
create table datatype_tbl1(dt timestamp(9));
insert into datatype_tbl1 values(to_timestamp('2016-01-02 10:30:00.34','yyyy-mm-dd hh24:mi:ssxff'));
select * from datatype_tbl1;
drop table datatype_tbl1;
create table datatype_tbl1(dt timestamp(10));
drop table datatype_tbl1;
--timestamp[(p)] WITH TIME ZONE
set timezone=7;
select dbtimezone from dual;
select sessiontimezone from dual;
create table datatype_tbl1(id integer,dt timestamp(4) with time zone);
insert into datatype_tbl1 values(1,to_timestamp('2016-01-02 10:30:00','yyyy-mm-dd hh24:mi:ss'));
select * from datatype_tbl1;
set timezone=8;
insert into datatype_tbl1 values(2, to_timestamp('2016-01-02 11:31:00','yyyy-mm-dd hh24:mi:ss'));
select * from datatype_tbl1;
drop table datatype_tbl1;
--timestamp[(p)] WITH LOCAL TIME ZONE
set timezone=7;
select dbtimezone from dual;
select sessiontimezone from dual;
create table datatype_tbl1(id integer,dt timestamp(4) with local time zone);
insert into datatype_tbl1 values(1,to_timestamp('2016-01-02 10:30:00','yyyy-mm-dd hh24:mi:ss'));
select * from datatype_tbl1;
set timezone=8;
insert into datatype_tbl1 values(2, to_timestamp('2016-01-02 11:31:00','yyyy-mm-dd hh24:mi:ss'));
select * from datatype_tbl1;
drop table datatype_tbl1;
--double类型	精度测试
create table datatype_tbl2(id number,sal number(38,37));
insert into datatype_tbl2 values(2,sinh(0.7));
create table datatype_tbl1(id number,job varchar(50));
insert into datatype_tbl1 values(1,exp(2));
insert into datatype_tbl1 select * from datatype_tbl2;
select length(job) from datatype_tbl1;
select * from datatype_tbl1;
drop table datatype_tbl1;
drop table datatype_tbl2;
--其他数据类型只测试极限值
--int
create table datatype_tbl1(id int);
insert into datatype_tbl1 values(power(2,31)-1);
insert into datatype_tbl1 values(power(2,31));
insert into datatype_tbl1 values(-power(2,31));
insert into datatype_tbl1 values(-power(2,31)-1);
insert into datatype_tbl1 values(null);
insert into datatype_tbl1 values('');
select * from datatype_tbl1 order by id;
drop table datatype_tbl1;
--smallint
create table datatype_tbl1(id smallint);
insert into datatype_tbl1 values(-32768);
insert into datatype_tbl1 values(-32769);
insert into datatype_tbl1 values(32767);
insert into datatype_tbl1 values(32768);
insert into datatype_tbl1 values(65536);
insert into datatype_tbl1 values(null);
insert into datatype_tbl1 values('');
select * from datatype_tbl1 order by id;
drop table datatype_tbl1;
--bigint
create table datatype_tbl1(id bigint);
insert into datatype_tbl1 values(power(2,63)-1);
insert into datatype_tbl1 values(power(2,63));
insert into datatype_tbl1 values(-power(2,63));
insert into datatype_tbl1 values(-power(2,63)-1);
insert into datatype_tbl1 values(null);
insert into datatype_tbl1 values('');
select * from datatype_tbl1 order by id;
drop table datatype_tbl1;
--real
create table datatype_tbl1(id real);
insert into datatype_tbl1 values(power(2,63)-1);
insert into datatype_tbl1 values(power(2,63));
insert into datatype_tbl1 values(-power(2,63));
insert into datatype_tbl1 values(-power(2,63)-1);
insert into datatype_tbl1 values(null);
insert into datatype_tbl1 values('');
select * from datatype_tbl1 order by id;
drop table datatype_tbl1;
set timezone to 'PRC';
