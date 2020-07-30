set grammar to oracle;
set datestyle='ISO,YMD';
create table t4test(id int, dt timestamp);
insert into t4test values(1,to_date('2000-3-31','YYYY-MM-DD'));
insert into t4test values(2,to_date('2000-2-29','YYYY-MM-DD'));
select add_months(to_date('2000-3-31','YYYY-MM-DD'),1) from dual; 
select id, add_months(dt,1) from t4test where id=1;
select add_months(to_date('2000-3-31','YYYY-MM-DD'),-1) from dual; 
select id, add_months(dt,1) from t4test where id=1;
select add_months(to_date('2000-3-31','YYYY-MM-DD'),0) from dual; 
select id, add_months(dt,0) from t4test where id=1;
select add_months(to_date('2000-2-29','YYYY-MM-DD'),12) from dual; 
select id, add_months(dt,12) from t4test where id=2;
select add_months(to_date('2000-2-29','YYYY-MM-DD'),-12) from dual; 
select id, add_months(dt,-12) from t4test where id=2;
select add_months(to_date('2000-2-29','YYYY-MM-DD'),12.1) from dual; 
select add_months(to_date('2000-2-29','YYYY-MM-DD'),-12.55) from dual; 
select add_months(to_date('1-2-28','YYYY-MM-DD'),-300) from dual;
select add_months(to_date('6000-2-29','YYYY-MM-DD'),30000) from dual;
select add_months(to_date('2000-2-29','YYYY-MM-DD'),'3') from dual;
select add_months(to_date('2000-2-29','YYYY-MM-DD'),'') from dual;
select add_months(to_date('2000-2-29','YYYY-MM-DD'),null) from dual;
select add_months(null,null) from dual;
select add_months('','') from dual;
select add_months('',null) from dual;
select add_months(null,'') from dual;
select add_months('',3) from dual;

-- modify By yuxiuming.
-- 增加insert update 时间格式：'YYYY-MM-DD hh24:mi:ss' 和闰年测试
insert into t4test values(10,add_months(to_date('2010-12-31 12:12:12','YYYY-MM-DD hh24:mi:ss'),2));
select id,dt from t4test where id=10;
update t4test set dt=add_months(to_date('2018-3-31 13:13:13','YYYY-MM-DD hh24:mi:ss'),3) where id=10;
select id,dt from t4test where id=10;

insert into t4test values(11,add_months(to_date('2010-12-31','YYYY-MM-DD'),2));
select id,dt from t4test where id=11;
update t4test set dt=add_months(to_date('2018-3-31','YYYY-MM-DD'),3) where id=11;
select id,dt from t4test where id=11;

insert into t4test values(12,add_months(to_date('2011-12-31','YYYY-MM-DD'),2));
select id,dt from t4test where id=12;
update t4test set dt=add_months(to_date('2016-5-31','YYYY-MM-DD'),-3) where id=12;
select id,dt from t4test where id=12;


drop table t4test;
