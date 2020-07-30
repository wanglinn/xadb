set grammar to oracle;
select length('abcd!@#$^&*()') from dual;
select length('ab
c') from dual;
select length('你好') as str from dual;
select length('你
好') as str from dual;
select length(to_char(1234)) from dual;
select length(exp(2)) from dual;
select length(12.3456) from dual;
create table t4test (name varchar2(10));
insert into t4test values('1bd2sdfaef');
insert into t4test values('3csadfed');
insert into t4test values('mmsdeq4d');
insert into t4test values('ccd');
select * from t4test where length(name) >6 order by name;
select name,length(name) from t4test order by name;
drop table t4test;
