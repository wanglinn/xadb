set grammar to oracle;
set datestyle='ISO,YMD';
select to_date('1-3-2016','MM-DD-yyyy') + to_dsinterval('1 1:1:00')  as ndate from dual;
select to_date('1-3-2016','MM-DD-yyyy') + to_dsinterval('1 1:1')  as ndate from dual;
select to_date('1-3-2016 10:30:00','MM-DD-yyyy hh:mi:ss') + to_dsinterval('1 3:50:50')  as ndate from dual;
select to_date('1-3-2016 10:30:00','MM-DD-yyyy hh:mi:ss') - to_dsinterval('1 10:50:50')  as ndate from dual;
select to_date('1-3-2016','MM-DD-yyyy') + to_dsinterval('')  as ndate from dual;
select to_date('1-3-2016','MM-DD-yyyy') + to_dsinterval(null)  as ndate from dual;
select to_char(to_dsinterval('1 10:50:50.123456789'),'dd hh24:mi:ss')  as ndate from dual;
select to_char(to_timestamp('01-03-2016 10:30:00.1111','MM-DD-yyyy hh:mi:ss.ff4') + to_dsinterval('1 10:50:50.12345678'),'dd hh24:mi:ss') as ndate from dual;
select to_char(to_dsinterval('1 10:50:50.12345678') + to_dsinterval('1 10:50:50.12345678'),'dd hh24:mi:ss') as ndate from dual;
select to_char(to_yminterval('2016-02') + to_dsinterval('40 10:50:50.12345678'),'dd hh24:mi:ss') as ndate from dual;
select to_date('01-03-2016 10:30:00','MM-DD-yyyy hh:mi:ss') +  to_dsinterval('-1 01:01:01')  as ndate from dual;


CREATE TABLE t4test (id int,c_time date);
insert into t4test values (1,to_date('1-31-2011','MM-DD-yyyy') + to_dsinterval('1Y1Mon'));
insert into t4test values (2,to_date('3-31-2011','MM-DD-yyyy') + to_dsinterval('12M59S'));
select * from t4test order by id;
update t4test set c_time=c_time-to_dsinterval('20H');
select * from t4test order by id;
drop table t4test;