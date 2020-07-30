set grammar to oracle;

select bitand(1,3) from dual;
select bitand('1',3) from dual;
select bitand(127,3.9456) from dual;
select bitand('1.4999',1.4999) from dual;
select bitand(-1,-1) from dual;
select bitand('-3',2) from dual;
select bitand(127.99,'128.99') from dual;
select bitand('-127','128') from dual;
select bitand(exp(2),exp(2)) from dual;
select bitand(to_char(4),0) from dual;
select bitand(3*3,2*4) from dual;
select bitand('s',0) from dual;
select bitand('',0) from dual;
select bitand('',null) from dual;
select bitand(null,2) from dual;

--MODIFY BY yuxiuming:add insert update
select bitand(2*2,2+(21-2)) from dual;

select bitand(231223123223223,-231223123223223) from dual;

CREATE TABLE t4test (id int,v number default(2.0));
insert into t4test values(1,bitand('1234',1235));
select * from t4test order by id;
insert into t4test values(2,bitand('1234',1235));
select * from t4test order by id;
update t4test set v=bitand('1.20E+1','1.20E+1') where id=2;
select * from t4test order by id;
select bitand(v,12345) from t4test where id=1;

drop table t4test;
