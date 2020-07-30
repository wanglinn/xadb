set grammar to oracle;
select tanh(exp(2)) from dual;
select tanh(-1) from dual;
-- select tanh('0.9') from dual;
select tanh(0) from dual;
select tanh(to_char(0.5)) from dual;
select tanh('0.9') from dual;
select tanh(null) from dual;
select tanh('') from dual;

--MODIFY BY yuxiuming:add insert update
CREATE TABLE t4test (id int,tanhvalue number,v number default(2.0));
insert into t4test values(1,tanh(12),3.14);
select * from t4test order by id;
insert into t4test values(2,tanh('-2'));
select * from t4test order by id;
update t4test set tanhvalue=tanh('1.20E+1') where id=2;
select * from t4test order by id;
select tanh(v) from t4test where id=1;

drop table t4test;
