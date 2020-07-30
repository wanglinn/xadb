set grammar to oracle;
set datestyle='ISO,YMD';
create table t4test(id int, dt timestamp);
insert into t4test values(1,to_date('2000-3-31','YYYY-MM-DD'));
insert into t4test values(2,to_date('2000-2-29','YYYY-MM-DD'));
select id, next_day(dt,'MONDAY') from t4test order by id;
select id, next_day(dt,'MON') from t4test order by id;
select id, next_day(dt,'Mond') from t4test order by id;
select id, next_day(dt,2) from t4test order by id;
select id, next_day(dt,2.4) from t4test order by id;

insert into t4test values(3,next_day(to_date('2019-6-6','YYYY-MM-DD'),'FRI'));
select * from t4test order by id;
update t4test set dt=next_day(dt,'MON') where id=3;
select * from t4test order by id;;
drop table t4test;
