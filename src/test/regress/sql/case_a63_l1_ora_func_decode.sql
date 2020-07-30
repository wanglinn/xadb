
set grammar to oracle;
set datestyle='ISO,YMD';
select decode(to_char(45),'3','9','45','3') from dual;
select decode(1.5*3,3.0,4,4.5,4.5) from dual;
select decode(3,4,4,3,6) from dual;
select decode('a','a','b','a') from dual;
select decode('你好','你好','b','a') as str from dual;
select decode(to_number('2'),2,3,'2',4) from dual;
select decode(to_number('2.4'),2.3,4,'2.4',4) from dual;
select decode(4,to_char('a'),4,'b',5) from dual;
select decode(to_char('5'),to_char('5'),to_char('6'),to_char('6'),to_char('7')) from dual;
select decode(3,exp(2),4,5) from dual;
select decode(exp(2),7,4,5) from dual;
select decode('3',tanh(3),4,5) from dual;
select decode('3',3,4,5) from dual;
select decode(3,'3',4,5) from dual;
select decode('3',3.0,4,5) from dual;
select decode('a','a','2',1,3) from dual;
select decode(to_date('2007-1-1','yyyy-mm-dd'),to_date('2007-1-1','yyyy-mm-dd'),1,0) from dual;
select decode(to_number('NaN'),'NaN',4,5) from dual;
select decode(exp(2),tanh(2),'4',exp(2),3) from dual;
select decode(to_date('2007-1-1','yyyy-mm-dd'),'2007-1-1',1,0) from dual;
select decode(to_date('2007-1-1','yyyy-mm-dd'),to_date('2007-1-1','yyyy-mm-dd'),1,0) from dual;
select decode(3,null,4,5) from dual;
select decode(3,'',4,5) from dual;
select decode(null,null,4,5) from dual;
select decode(null,3,4,5) from dual;
select decode(3,0,1,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,33,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,33,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,33,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,33,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,33,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3) from dual;
select to_char(trunc(to_char(decode(20, 0,0,10/20)),4) * 100,'fm9999990.00')  from dual;

--http://10.20.16.216:9090/ADB/AntDB/issues/777
create table t_decode( name varchar(100));
insert into t_decode values('1');
insert into t_decode values('1212');
select decode(name,null,2,name) from t_decode;
create or replace view test_view as select decode(name,null,2,name) nn from t_decode ;
select * from test_view;
drop view test_view;
drop table t_decode;


create table t_decode (id integer,d1 date,d2 date,t varchar(1024),v varchar2(40),nv nvarchar2(40),c char,n numeric,si smallint,bi number(20,0),c2 char,i integer,vc varchar(20));

insert into t_decode values(1,to_date('2020-02-13 12:12:12' , 'yyyy-mm-dd hh24:mi:ss'),to_date('2019-2-26 11:14:25' , 'yyyy-mm-dd hh24:mi:ss'),'2020-02-13 12:12:12','2020-02-13 12:12:12','2020-02-13 12:12:12','1',1,2,3,'1',1,'2020-02-13 12:12:12');

insert into t_decode values(2,to_date('2020-02-13 12:12:12' , 'yyyy-mm-dd hh24:mi:ss'),to_date('2019-2-26 11:14:25' , 'yyyy-mm-dd hh24:mi:ss'),'1','0','1','0',1,0,1,'1',0,'1');


--character varying | oracle.date       | oracle.date
select vc,d1,decode(vc,d1,1,0) from t_decode where id =1;
--text              | oracle.date       | oracle.date
select t,d2,decode(t,d2,1,0) from t_decode where id =1;
--oracle.varchar2   | oracle.date       | oracle.date
select v,d2,decode(v,d2,1,0) from t_decode where id =1;
--oracle.nvarchar2  | oracle.date       | oracle.date
select nv,d2,decode(nv,d2,1,0) from t_decode where id =1;
--oracle.date       | character varying | oracle.date
select d2,v,decode(d2,v,1,0) from t_decode where id =1;
--oracle.date       | text              | oracle.date
select d1,t,decode(d1,t,1,0) from t_decode where id =1;
--oracle.date       | oracle.varchar2   | oracle.date
select d2,v,decode(d2,v,1,0) from t_decode where id =1;
--oracle.date       | oracle.nvarchar2  | oracle.date
select d1,nv,decode(d1,nv,1,0) from t_decode where id =1;
--text              | numeric           | numeric
 select t,n,decode(t,n,1,0) from t_decode where id =2;
--"char"            | numeric           | numeric
 select c,n,decode(c,n,1,0) from t_decode where id =2;
--character varying | numeric           | numeric
 select c2,n,decode(c2,n,1,0) from t_decode where id =2;
--oracle.varchar2   | numeric           | numeric
 select vc,n,decode(vc,n,1,0) from t_decode where id =2;
--oracle.nvarchar2  | numeric           | numeric
 select nv,n,decode(nv,n,1,0) from t_decode where id =2;
--numeric           | text              | numeric
 select n,t,decode(n,t,1,0) from t_decode where id =2;
--numeric           | "char"            | numeric
 select n,c,decode(n,c,1,0) from t_decode where id =2;
--numeric           | character varying | numeric
select n,vc,decode(n,vc,1,0) from t_decode where id =2;
--numeric           | oracle.varchar2   | numeric
select n,v,decode(n,v,1,0) from t_decode where id =2;
--numeric           | oracle.nvarchar2  | numeric
select n,nv,decode(n,nv,1,0) from t_decode where id =2;
--numeric           | smallint          | numeric
select n,si,decode(n,si,1,0) from t_decode where id =2;
--numeric           | integer           | numeric
select n,i,decode(n,i,1,0) from t_decode where id =2;
--numeric           | bigint            | numeric
select n,bi,decode(n,bi,1,0) from t_decode where id =2;
--smallint          | numeric           | numeric
select si,n,decode(si,n,1,0) from t_decode where id =2;
--integer           | numeric           | numeric
select i,n,decode(i,n,1,0) from t_decode where id =2;
--bigint            | numeric           | numeric
select bi,n,decode(bi,n,1,0) from t_decode where id =2;
--character         | numeric           | numeric
select c,n,decode(c,n,1,0) from t_decode where id =2;
--numeric           | character         | numeric
select n,c,decode(n,c,1,0) from t_decode where id =2;
drop table t_decode;