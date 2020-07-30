set grammar to oracle;
select nullif(1,2) from dual;
select nullif(1,1) from dual;
select nullif(1,1.1) from dual;
select nullif('1','1') from dual;
select nullif(to_char('a'),to_char('b')) from dual;
select nullif(exp(1),exp(2)) from dual;
select nullif('a','') from dual;
select nullif('','') from dual;
select nullif('a',null) from dual;
select nullif(null,'a') from dual;
select nullif(to_char('你好'),'你好') from dual;
select nullif(12,to_number(12)) from dual;


create table t4test(id int,txt varchar);
insert into t4test values(1,null);
insert into t4test values(2,'aaa');
insert into t4test values(3,nullif('aa','12'));

select * from t4test order by id;

select nullif(txt,'aaa') from t4test order by id;


update t4test set txt=nullif(txt,'aa');
select * from t4test order by id;

drop table t4test;
