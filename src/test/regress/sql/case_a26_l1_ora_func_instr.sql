set grammar to oracle;
set datestyle='ISO,YMD';
select instr('abc','bc') from dual;
select instr(to_char('abc'),to_char('bc')) from dual;
select instr('爱存不存','存',1,2) from dual;
select instr(1234,23) from dual;
select instr(to_number(123.56),'.') from dual;
select instr(exp(2),0)  from dual;
select instr(exp(2),exp(2))  from dual;
select instr(to_date('2015-06-06 20:50:30','yyyy-mm-dd hh24:mi:ss'),'15') from dual;
select instr('!@#$%^&*()~','^')  from dual;
select instr('abc
sw','
s') from dual;
select instr('abcswcs','cs',1,2) from dual;
select instr('abcswcs','cs',1000000000) from dual;
select instr('abcswcs','cs',-100) from dual;
select instr('abcswcs','cs',-1) from dual;
select instr('abcswcs','cs','',-1) from dual;
select instr('abcswcs','cs',0) from dual;
select instr('abcswcs','cs',1,10000000000) from dual;
select instr('','')  from dual;
select instr(null,'a') from dual;
select instr('abc','') from dual;


create table t4test (name varchar2(10),idx int);
insert into t4test values('1bd2',0);
insert into t4test values('3cd',0);
insert into t4test values('mmd',0);
insert into t4test values('ccd',instr('sadccdsdas','ccd'));
select * from t4test where instr(name,'d')  >0 order by name;


drop table t4test;

select instr('ababc','ab',2) from dual;
select instr('ababadc','ab',2,2) from dual;
select instr('ababadc','ab',2,1) from dual;
select instr('ababadc','ab',-2,1) from dual;
select instr('ababadc','ab',-1,1) from dual;
select instr('abababc','ab',-1,1) from dual;
select instr('abababc','ab',-1,2) from dual;
