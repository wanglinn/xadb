set grammar to oracle;
select cosh(exp(2)) from dual;
select cosh(-1) from dual;
select cosh('0.9') from dual;
select cosh(0) from dual;
select cosh(to_char(0.5)) from dual;

select cosh(null) from dual;
select cosh('') from dual;

--MODIFY BY yuxiuming:add insert update
CREATE TABLE t4test (id int,v1 number,v2 number default(2.0));
insert into t4test values(1,cosh(12),3.14);
select * from t4test order by id;
insert into t4test values(2,cosh('-2'));
select * from t4test order by id;
update t4test set v1=cosh('1.20E+1') where id=2;
select * from t4test order by id;
select cosh(v2) from t4test where id=1;

drop table t4test;
