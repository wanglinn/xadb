set grammar to oracle;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,1) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,0) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,'0') output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,1.9) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,to_number('1')) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,to_char(1)) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,tan(1)) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,100) output  from dual;
SELECT regexp_instr('abc1def', '[[:digit:]]',1,1,100) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1,2) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1,'1') output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1,to_number(2)) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1,2.1) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1,to_char(1)) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1,power(2,1)) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',4) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',5) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',1000000) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]','5') output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',5.5) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',to_char(5)) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]',power(5,1)) output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]D',1,1,1,'i') output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]]D',1,1,1,'c') output  from dual;
SELECT regexp_instr('abc1
def2', '[[:digit:]].d',1,1,1,'n') output  from dual;
SELECT regexp_instr('abc1
def2', '[[:digit:]].d',1,1,0,'xic') output  from dual;
SELECT regexp_instr('abc1def2', '[[:digit:]] d',1,1,0,'x') output  from dual;
select regexp_instr('abcxxx#%
adfbc','^a',1,2,0,'m') from dual;
select regexp_instr('abcxxx#%
adfbc','^a',1,2,0,'n') from dual;
select regexp_instr('abcxxx#%
adfbc','^a',1,2,0,'i') from dual;
select regexp_instr('abcxxx#%
adfbc','^a',1,2,0,'x') from dual;
select regexp_instr('abcxxx#%
adfbc','^a',1,2,0,'c') from dual;
SELECT REGEXP_INSTR(123456, 123, 1, 1, 0) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR(to_date('2016-01-31','yyyy-mm-dd'), '31', 1, 1, 0) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR(to_timestamp('2016-01-31','yyyy-mm-dd'), '31', 1, 1, 0) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR(numtoyminterval(10,'year'), '10', 1, 1, 0) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 4) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 1) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 0, 'i', 9) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 0) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', 4.5) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', to_char(4)) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', power(2,2)) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890', '(123)(4(56)(78))', 1, 1, 1, 'i', '4') 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('1234567890abcdefg', '(1)(2)(3)(4)(5)(6)(7)(8)(9)(0)(a)(b)', 1, 1, 1, 'i', 10) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR('', '', 1, 1, 0) 
REGEXP_INSTR FROM DUAL;
SELECT REGEXP_INSTR(null, null, 1, 1, 0) 
REGEXP_INSTR FROM DUAL;
