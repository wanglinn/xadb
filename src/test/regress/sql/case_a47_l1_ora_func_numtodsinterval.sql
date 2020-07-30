set grammar to oracle;
set datestyle='ISO,YMD';
select to_date('2015-2-5 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'DAY')  as ndate from dual;
select to_date('2015-2-25 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'DAY')  as ndate from dual;
select to_date('2015-2-25 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'HOUR')  as ndate from dual;
select to_date('2015-2-25 10:55:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'minute')  as ndate from dual;
select to_date('2015-2-28 23:59:59','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(1,'second')  as ndate from dual;
select to_date('2015-2-28 23:59:59','YYYY-MM-DD hh24:mi:ss') + numtodsinterval('1','second')  as ndate from dual;
select to_date('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(3.567,'second')  as ndate from dual;
select to_date('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(3.123456789,'day')  as ndate from dual;
select to_date('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(3.123456789,'hour')  as ndate from dual;
select to_date('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtodsinterval('3.123456789','hour')  as ndate from dual;
select to_timestamp('2015-2-28 2:1:1','YYYY-MM-DD hh24:mi:ss') + numtodsinterval('3.123456789','hour')  as ndate from dual;
select to_char(numtodsinterval('3.123456789','hour'),'hh24:mi:ss')  as ndate from dual;
select to_char(numtodsinterval('3.1','hour') + numtodsinterval('2.1','minute'),'hh24:mi:ss') as ndate from dual;
select numtodsinterval('3.123456789','')  as ndate from dual;
select numtodsinterval('3.1',null) + numtodsinterval('2.1','minute') as ndate from dual;
select to_char(numtodsinterval('3.1','minute') * 3,'hh24:mi:ss') as ndate from dual;


create table t4test(id int, dt date);
insert into t4test values(1,to_date('2015-2-5 10:45:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'second'));
insert into t4test values(2,to_date('2015-2-5 10:45:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'minute'));
insert into t4test values(3,to_date('2015-2-5 10:45:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'HOUR'));
insert into t4test values(4,to_date('2015-2-5 10:45:55','YYYY-MM-DD hh24:mi:ss') + numtodsinterval(10,'DAY'));

select * from t4test order by id;
update t4test set dt=dt+numtodsinterval(24,'HOUR');
select * from t4test order by id;

drop table t4test;
