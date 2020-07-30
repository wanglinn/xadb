set grammar to oracle;
set datestyle='ISO,YMD';

--create table
create table todatetbl(id int, dt varchar2(30));
insert into todatetbl values(1,'1-1-1');
insert into todatetbl values(2,'9999-12-1');
insert into todatetbl values(3,'2015-10-18');
select id,to_date(dt,'yyyy-mm-dd')from todatetbl order by id;
drop table todatetbl;

--query
select to_date('20150102','yyyymmdd') from dual;
select to_date('18000202 12:23:59','yyyymmdd hh:mi:ss') from dual;
select to_date('20150102 12:23:59','yyyymmdd hh24:mi:ss') from dual;
select to_date('2015,01,02','yyyy,mm,dd') from dual;
select to_date('01/02/2010','mm/dd/yyyy') from dual;
SELECT TO_DATE('January 15, 1989, 11:00 A.M.','Month dd, YYYY, HH:MI A.M.') FROM DUAL;
SELECT TO_DATE('Jan 15, 2010, 11:00 P.M.','Mon dd, YYYY, HH:MI P.M.') FROM DUAL;
SELECT TO_DATE('15, 10, 11:00 P.M.','ddd, YY, HH:MI P.M.') FROM DUAL;
SELECT TO_date('January 15, 2010, 11:00 P.M.','Mon dd, YYYY, HH:MI P.M.') FROM DUAL;
select to_date('2100-2-29','yyyy-mm-dd') from dual;
select to_date('2015-2-28','') from dual;
select to_date('2015,01,02 14:34:23','yyyy,mm,dd hh24:mi:ss') - to_date('2014,01,01 11:24:21','yyyy,mm,dd hh24:mi:ss') from dual;
select to_date('20100202 10:23:59.190','yyyymmdd hh:mi:ss') from dual;
select to_date('20100202 10:23:59.890','yyyymmdd hh:mi:ss') from dual;
select to_date('20100202 10:23:59.890','yyyymmdd hh:mi:ss.ff3') from dual;
