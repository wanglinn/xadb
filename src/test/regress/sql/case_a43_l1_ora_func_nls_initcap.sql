set grammar to oracle;
select nls_initcap('ab cde') from dual;
select nls_initcap('AAb  CDE') from dual;
select nls_initcap('\?!@#$%^&*()sa') from dual;
select nls_initcap('AA\?!@#$%^&*()AA') from dual;
select nls_initcap('\?!@#ss$%^&*()sC') from dual;
select nls_initcap('test你test好test') as str from dual;
select nls_initcap('1234SOU') from dual;
select nls_initcap(to_char('suv')) from dual;
select nls_initcap(to_number('NaN')) from dual;


create table t4test(id int,txt varchar);
insert into t4test values(1,'asasd dasdas sadasdasd asdasdasdas');
insert into t4test values(2,nls_initcap('asda sd a sd'));

select * from t4test order by id;

update t4test set txt=nls_initcap(txt);

select * from t4test order by id;

drop table t4test;
