set grammar to oracle;
set datestyle='ISO,YMD';

--create table
create table t4test(id int, dt date);
insert into t4test values(1,to_date('20150102','yyyymmdd'));
insert into t4test values(2,to_date('18000202 12:23:59','yyyymmdd hh:mi:ss'));
insert into t4test values(3,to_date('2015-10-18'));
select * from t4test order by id;
update t4test set dt=to_date('2012-2-29','yyyy-mm-dd') where id=3;
select * from t4test order by id;
drop table t4test;

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

--test type oracle.date
select pg_typeof(sysdate) from dual;
select pg_typeof(sysdate - 1) from dual;

