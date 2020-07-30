set grammar to oracle;
select nls_lower('AB CDe') from dual;
select nls_lower('\?!@#$%^&*()SA') from dual;
select nls_lower('AA\?!@#$%^&*()AA') from dual;
select nls_lower('\?!@#sS$%^&*()sC') from dual;
select nls_lower('Test你TEst好TeST') as str from dual;
select nls_lower('1234SOU') from dual;
select nls_lower(to_char('SUV')) from dual;
select nls_lower(to_number('NaN')) from dual;

create table t4test (id int,txt varchar);
insert into t4test values(1,'ASD dasDSdas SADASD aSDSFasDFas');
insert into t4test values(2,nls_lower('aSDa SD a SSD'));

select * from t4test order by id;

update t4test set txt=nls_lower(txt);

select * from t4test order by id;

drop table t4test;
