set grammar to oracle;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,2) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,'1') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_number(2)) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,2.1) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,to_char(1)) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1,power(1,1)) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',4) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',5) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',1000000) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@','5') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',5.5) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',to_char(5)) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]','@',power(1,1)) output  from dual;
SELECT regexp_replace(1234, '[[:digit:]]','@',1) output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'i') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]]D','@',1,1,'c') output  from dual;
SELECT regexp_replace('abc1
def2', '[[:digit:]].d','@',1,1,'n') output  from dual;
SELECT regexp_replace('abc1
def2', '[[:digit:]].d','#',1,1,'xic') output  from dual;
SELECT regexp_replace('abc1def2', '[[:digit:]] d','@',1,1,'x') output  from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'m') from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'n') from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'i') from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'x') from dual;
select regexp_replace('abcxxx#%
adfbc','^a','@',1,2,'c') from dual;
SELECT regexp_replace('', '', 1, 1, 0) 
regexp_replace FROM DUAL;
SELECT regexp_replace(null, null, null,1, 1) 
regexp_replace FROM DUAL;


CREATE TABLE t4test (id int,txt varchar,t2 varchar);
insert into t4test values(1,'weqweq23wew454ewqewq', regexp_replace('abc1def22', '[[:digit:]]','@'));
insert into t4test values(2,'qazwsxqazzw42sx234qazwsx', 'ss');
select * from t4test order by id;
update t4test set t2=regexp_replace(txt, '[[:digit:]]','*');
select * from t4test order by id;
drop table t4test;
