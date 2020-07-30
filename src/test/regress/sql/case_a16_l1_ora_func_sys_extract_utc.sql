set grammar to oracle;
set datestyle='ISO,YMD';
set timezone=8;
SELECT SYS_EXTRACT_UTC(TIMESTAMP '2012-04-19 17:30:00 +08:00 ') "CURRENT UTC TIME" FROM   DUAL;
set timezone=0;
SELECT SYS_EXTRACT_UTC(TIMESTAMP WITH TIME ZONE  '2012-04-19 17:30:00 +08:00 ') "CURRENT UTC TIME" FROM  DUAL;

set timezone=8;
CREATE TABLE t4test (id int,c_time date);
insert into t4test values(1,SYS_EXTRACT_UTC(to_date('2016-01-01')));
insert into t4test values(2,SYS_EXTRACT_UTC(to_date('2016-03-01 07:59:00')));
select * from t4test order by id;
update t4test set c_time=SYS_EXTRACT_UTC(c_time);
select * from t4test order by id;
drop table t4test;