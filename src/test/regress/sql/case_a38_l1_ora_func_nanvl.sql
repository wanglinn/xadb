set grammar to oracle;

select nanvl(5,4.7) from dual;
select nanvl(5.8,4.7) from dual;
select nanvl('8',4.7) from dual;
select nanvl('4.8',4.7) from dual;
select nanvl('s',4.7) from dual;
select nanvl(exp(2),4.7) from dual;
select nanvl(2*5.7,4.7) from dual;
select nanvl(exp(2),sin(3)) from dual;
select nanvl('','') from dual;
select nanvl('',null) from dual;
select nanvl(null,4) from dual;
select nanvl(4.5,null) from dual;

create table t4test(id int,num float);
insert into t4test values(1,'nan');
insert into t4test values(2,3.14);
insert into t4test values(3,nanvl(4.14,'nan'));

select * from t4test order by id;

select nanvl(3.33,num) from t4test;

drop table t4test;
