set grammar to oracle;
select nls_upper('ab cDe') from dual;
select nls_upper('\?!@#$%^&*()sa') from dual;
select nls_upper('aa\?!@#$%^&*()aa') from dual;
select nls_upper('\?!@#ss$%^&*()sC') from dual;
select nls_upper('Test你tEst好teST') as str from dual;
select nls_upper('1234sou') from dual;
select nls_upper(to_char('suv')) from dual;
select nls_upper(to_number('NaN')) from dual;

create table t4test (id int,txt varchar);
insert into t4test values(1,'ASD dasDSdas SADASD aSDSFasDFas');
insert into t4test values(2,nls_upper('aSDa SD a SSD'));

select * from t4test order by id;

update t4test set txt=nls_upper(txt);

select * from t4test order by id;

drop table t4test;
