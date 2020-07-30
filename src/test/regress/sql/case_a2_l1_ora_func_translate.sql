set grammar to oracle;
set datestyle='ISO,YMD';
SELECT TRANSLATE('abcdefghij','abcdef','123456') FROM dual; 
SELECT TRANSLATE('abcdefghij','a b','1 2') FROM dual;
SELECT TRANSLATE('abcdefghij','a b','1  2') FROM dual;
SELECT TRANSLATE('abcdefghij','a b f j','1 3') FROM dual; 
SELECT TRANSLATE('abcdefghij','a b f j','13') FROM dual; 
SELECT TRANSLATE('abcdefghij','','13') FROM dual;
SELECT TRANSLATE('abcdefghij','a','') FROM dual; 
SELECT TRANSLATE('','','b') FROM dual; 
SELECT TRANSLATE('ab','a',null) FROM dual; 
SELECT TRANSLATE('!!!!@!!#$!!','!','a') FROM dual; 
SELECT TRANSLATE('!!!!@!!#$!!','!!','ab') FROM dual; 
SELECT TRANSLATE('!!!!@!!#$!!','!$@','ace') FROM dual;
SELECT TRANSLATE('!!!!@!!#$!!','$@!','cea') FROM dual; 
SELECT TRANSLATE('abc!!!!@!!#$!!','xabc!@#$','x') FROM dual; 
SELECT TRANSLATE('SQL*Plus User''s Guide', ' *$''', '___') FROM DUAL;
SELECT TRANSLATE(121, 12, 3) FROM DUAL;
SELECT TRANSLATE(exp(2), '.', ' ') FROM DUAL;
SELECT TRANSLATE('999,999.99', '9', 0) FROM DUAL;
SELECT TRANSLATE(121.1, 1., 2) FROM DUAL;
SELECT TRANSLATE(121.1, 1.0, 2.2) FROM DUAL;
SELECT TRANSLATE(to_char(123),'1', '2') FROM DUAL;
SELECT TRANSLATE(to_timestamp('2016-12-01 10:30:00.4534','yyyy-mm-dd hh:mi:ss.ff4'),'DEC', '12') FROM DUAL;
SELECT TRANSLATE('你好','你','我') FROM DUAL;
SELECT TRANSLATE('ab
AB','Aa
Bb','aA*bB') FROM DUAL;


CREATE TABLE t4test (id int,txt varchar);
insert into t4test values(1,TRANSLATE('aassaassffddssaa','as','@@@'));
insert into t4test values(2,TRANSLATE('xxccddeeffrrddss','s','###'));
select * from t4test order by id;
update t4test set txt=TRANSLATE(txt,'ff','$$');
select * from t4test order by id;
drop table t4test;
