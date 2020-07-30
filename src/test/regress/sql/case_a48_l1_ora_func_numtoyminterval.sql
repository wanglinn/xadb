set grammar to oracle;
set datestyle='ISO,YMD';
select to_date('2015-2-5 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(10,'year')  as ndate from dual;
select to_date('2016-2-29 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(1,'year')  as ndate from dual;
select to_date('2015-1-31 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(1,'month')  as ndate from dual;
select to_date('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(3.567,'year')  as ndate from dual;
select to_date('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(3.123456789,'month')  as ndate from dual;
select to_timestamp('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtoyminterval('3.123456789','year')  as ndate from dual;
select to_char(numtoyminterval(3.123456789,'year'),'yyyy-mm')  as ndate from dual;
select to_char(numtoyminterval(3.29,'year') -numtoyminterval(2.11,'year'),'yyyy-mm') as ndate from dual;
select to_char(numtoyminterval(3.29,'year') -numtoyminterval(2.11,'month'),'yyyy-mm') as ndate from dual;
select to_char(numtoyminterval(3.29,''),'yyyy-mm') as ndate from dual;
select numtoyminterval(null,'year') -numtoyminterval(2.11,'month') as ndate from dual;

create table t4test(id int, dt date);
insert into t4test values(1,to_date('2015-2-5 10:45:55','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(1.5,'year'));
insert into t4test values(2,to_date('2015-2-5 10:45:55','YYYY-MM-DD hh24:mi:ss') + numtoyminterval(1.5,'month'));


select * from t4test order by id;
update t4test set dt=dt+numtoyminterval(2,'year');
select * from t4test order by id;

drop table t4test;
