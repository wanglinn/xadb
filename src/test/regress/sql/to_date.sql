set grammar to oracle;
set datestyle='ISO,YMD';

--create table
create table aa(id int, dt varchar2(30));
insert into aa values(1,'1-1-1');
insert into aa values(2,'9999-12-1');
insert into aa values(3,'2015-10-18');
select id,to_date(dt,'yyyy-mm-dd')from aa order by id;
drop table aa;

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
select to_date('2020-10-1400:00:00', 'yyyy-mm-ddHH24:mi:ss') from dual;
select to_date('202010 14 00:00:00', 'yyyy-mm-dd HH24:mi:ss') from dual;
select to_date('20201031235959', 'yyyymmddhh24miss') from dual;
select to_date('20200202','yyyy-mm-dd') from dual;
select to_date('2020-02-02 23:59:59','yyyy-mm-dd HH24:Mi:ss') from dual;
select to_date('20201031235959', 'yyyy-mm-dd hh24:mi:ss' ) from dual;
select to_date('2020:11:03','yyyy-mm-dd') from dual;
select to_date('2016-03-15 09:25:59 pm','yyyy-mm-dd hh:mi:ss pm') from dual;
select to_date('20160315092559','yyyymmddhhmiss') from dual;
select to_date('20160315092559pm','yyyymmddhhmisspm') from dual;
select to_date('99-12-1','yy-mm-dd')from dual;
select to_date('20100202 10.23.59','yyyy-mm-dd hh:mi:ss') from dual;
select to_date('20201031235959', 'yyyy-:/-mm-dd hh24:mi:ss' ) from dual;
SELECT to_date('January 15, 1989, 11:00 A.M.','Month dd, YYYY, HH:MI A.M.') from dual;
