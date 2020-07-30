
set grammar to oracle;
select mod(7,5) from dual;
select mod(11.9,3) from dual;
select mod(-99.8768678,4) from dual;
select mod(-5.6,-2.5) from dual;
select mod(0,2) from dual;
select mod(0,0) from dual;
select mod(3,0) from dual;
select mod(exp(4),3) from dual;
select mod(sin(0.9),tan(1)) from dual;
select mod(1.98*4/(1.9+1.1),1.5*2) from dual;
select mod(to_number('34'),3) from dual;
select mod(to_char(29),3) from dual;
select mod('7',5) from dual;
select mod(11.9,'3') from dual;
select mod('-8.1','4') from dual;
select mod('008','4.000') from dual;
select mod('0',2) from dual;
select mod('NaN',2) from dual;
select mod('NaN','NaN') from dual;
select mod(5,'NaN') from dual;
select mod('',null) from dual;
select mod(null,3) from dual;
select mod('','') from dual;
select mod(11,4) from dual;
select mod(11,-4) from dual;
select mod(-11,4) from dual;
select mod(-11,-4) from dual;

CREATE TABLE t4test (id int,num int);
insert into t4test values(1,mod(1211,12));
insert into t4test values(2,mod(12234,23));
select * from t4test order by id;
update t4test set num=mod(num,4.5);
select * from t4test order by id;
drop table t4test;


create table t_mod (d1 number,d2 number,i1 int ,i2 int ,n1 numeric,n2 numeric);
insert into t_mod values(22.445,-0.3145,-1,5,145.251,-23);
--double precision | integer
select mod(d1,i1) from t_mod;
--double precision | double precision
select mod(d2,d1) from t_mod;
--numeric          | integer
select mod(n1,i2) from t_mod;
--integer          | numeric
select mod(i1,n2) from t_mod;
--unknown          | numeric
select mod('23',n1) from t_mod;
--numeric          | unknown
select mod(n2,'-5.256') from t_mod;
--unknown          | unknown
select mod('4.1','-5.256') from t_mod;
-- -                | -
select mod(0,0) from t_mod;

drop table t_mod;