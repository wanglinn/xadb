set grammar to oracle;
set datestyle='ISO,YMD';
select trim('  aa  bb  ') from dual;
select trim('  aa  bb') from dual;
select trim(leading ' ' from '   !@#$    ') from dual;
select trim(both 'x' from 'x!@#$x') from dual;
select trim(leading 'x' from 'x!@#$x') from dual;
select trim(trailing 'x' from 'x!@#$x') from dual;
select trim(both 'x' from ' x!@#$x ') from dual;
select trim(both 'xx' from ' xx!@#$x ') from dual;
select trim(both 'xl' from 'xl!@#$l') from dual;
select trim(both 'xl' from 'xl!@#$xl') from dual;
select trim(both '' from 'xl!@#$l') from dual;
select trim(both 1 from 121) from dual;
select trim(both 1 from 121.1) from dual;
select trim(both 7 from exp(2)) from dual;
select trim(leading 0 from to_date('2016-10','yyyy-mm')) from dual;
select trim(both to_char('a') from 'abc') from dual;
select trim(both null from 'abc') from dual;


create table t4test(id int,txt varchar);
insert into t4test values(1,null);
insert into t4test values(2,'  aaa  ');
insert into t4test values(3,trim('  sdas sdasd sdsad     '));

select * from t4test order by id;

select trim(txt) from t4test order by id;

update t4test set txt=trim(txt);
select * from t4test order by id;
drop table t4test;
