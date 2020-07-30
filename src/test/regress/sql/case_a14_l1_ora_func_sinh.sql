set grammar to oracle;
/*ora*/ select  sinh(exp(2)) from dual;
/*ora*/ select  sinh(-1) from dual;
/*ora*/ select  sinh('0.9') from dual;
/*ora*/ select  sinh(0) from dual;
/*ora*/ select  sinh(to_char(0.5)) from dual;
/*ora*/ select  sinh('0.9') from dual;
/*ora*/ select  sinh(null) from dual;
/*ora*/ select  sinh('') from dual;

--MODIFY BY yuxiuming:add insert update
CREATE TABLE t4test (id int,sinhvalue number,v number default(2.0));
insert into t4test values(1,sinh(12),3.14);
select * from t4test order by id;
insert into t4test values(2,sinh('-2'));
select * from t4test order by id;
update t4test set sinhvalue=sinh('1.20E+1') where id=2;
select * from t4test order by id;
select sinh(v) from t4test where id=1;

drop table t4test;