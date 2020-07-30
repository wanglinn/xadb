set grammar to oracle;
set datestyle='ISO,YMD';
select to_date('1-31-2016','MM-DD-yyyy') + to_yminterval('0-1')  as ndate from dual;
select to_date('1-31-2016','MM-DD-yyyy') - to_yminterval('1-1')  as ndate from dual;
select to_date('1-31-2016','MM-DD-yyyy') - to_yminterval('2016-1')  as ndate from dual;
select to_date('1-31-300','MM-DD-yyyy') - to_yminterval('320-0')  as ndate from dual;
select to_char(to_yminterval('2016-1'),'yyyy-mm')  as ndate from dual;
select to_date('1-31-2016','MM-DD-yyyy') - to_yminterval('')  as ndate from dual;
select to_date('1-31-2016','MM-DD-yyyy') + to_yminterval(null)  as ndate from dual;
select to_date('01-03-2016 10:30:00','MM-DD-yyyy hh:mi:ss') + to_yminterval('-2-02') as ndate from dual;

CREATE TABLE t4test (id int,c_time date);
insert into t4test values (1,to_date('1-31-2011','MM-DD-yyyy') + to_yminterval('1Y1Mon'));
insert into t4test values (2,to_date('3-31-2011','MM-DD-yyyy') + to_yminterval('12M59S'));
select * from t4test order by id;
update t4test set c_time=c_time-to_yminterval('20D');
select * from t4test order by id;
drop table t4test;
