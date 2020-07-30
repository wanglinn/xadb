set grammar to oracle;
select regexp_count('abcdfbc','Bc',1,'i') from dual;
select regexp_count('abcdfBc','Bc',1,'c') from dual;
select regexp_count('ab
cdfbc','b.c',1,'n') from dual;
select regexp_count('ab
cdfbc','b.c',1,'i') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'m') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'i') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'n') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'x') from dual;
select regexp_count('abcxxx#%
adfbc','^a',1,'c') from dual;
select regexp_count('abcvvbcvvb c','b c',1,'x') from dual;
select regexp_count('abcvvbcvvb c','b c',1,'n') from dual;
select regexp_count('abcvvbcvvBC','bc',1,'ic') from dual;
select regexp_count('abcvvbcvvBC','bc',1,'ci') from dual;
select regexp_count('abcvvbcvvBC','b c',1,'ix') from dual;
select regexp_count('abcvvb
cvvB
C','b.c',1,'in') from dual;
select regexp_count('abcvvb cvvB C','b c') from dual;
select regexp_count('abacvvb
cvvB C','b.c') from dual;
select regexp_count('abc
abc','bc?') from dual;
select regexp_count('abcvvbcvvbc','bc',2.9,'c') from dual;
select regexp_count('abcvvbcvvbc','bc',exp(2),'c') from dual;
select regexp_count('abcvvbcvvbc','bc','1','c') from dual;
select regexp_count('abcvvbcvvbc','bc',-1,'c') from dual;
select regexp_count('abcvvbcvvbc','bc',1000000,'c') from dual;
select regexp_count('12345',123,1) from dual;
select regexp_count(12345,123,1) from dual;
select regexp_count(12345.8,5.8,1) from dual;
select regexp_count(to_date('2016-01-31','yyyy-mm-dd'),'31',1) from dual;
select regexp_count(to_timestamp('2016-01-31','yyyy-mm-dd'),'31',1) from dual;
select regexp_count(numtoyminterval(10,'year'),'10',1) from dual;
select regexp_count('abcvvbcvvbc','bc','2.1','c') from dual;

select regexp_count(null,'',1,'i') from dual;
select regexp_count('','',1,'i') from dual;



CREATE TABLE t4test (id int,txt varchar,num int);
insert into t4test values(1,'weqweqwewewqewq', regexp_count('weweqweqweqwe','we'));
insert into t4test values(2,'qazwsxqazzwsxqazwsx', 0);
select * from t4test order by id;
update t4test set num=regexp_count(txt,'qaz');
select * from t4test order by id;
drop table t4test;

