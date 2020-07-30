set grammar to oracle;
set datestyle='ISO,YMD';
create table t4test(id int, dt timestamp);
insert into t4test values(1,to_date('2000-3-31','YYYY-MM-DD'));
insert into t4test values(2,to_date('1-1-1','YYYY-MM-DD'));
insert into t4test values(3,to_date('9999-12-30','YYYY-MM-DD'));
insert into t4test values(4,to_date('2001-2-1','YYYY-MM-DD'));
insert into t4test values(5,to_date('2000-2-1','YYYY-MM-DD'));
select id, last_day(dt) from t4test order by id;


insert into t4test values(6,last_day(to_date('2004-2-10','YYYY-MM-DD')));
insert into t4test values(7,last_day(to_date('2014-2-11','YYYY-MM-DD')));

select id, dt from t4test order by id;

drop table t4test;
