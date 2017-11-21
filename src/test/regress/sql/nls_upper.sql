set grammar to oracle;
select nls_upper('ab cDe') from dual;
select nls_upper('\?!@#$%^&*()sa') from dual;
select nls_upper('aa\?!@#$%^&*()aa') from dual;
select nls_upper('\?!@#ss$%^&*()sC') from dual;
select nls_upper('Test你tEst好teST') from dual;
select nls_upper('1234sou') from dual;
select nls_upper(to_char('suv')) from dual;
select nls_upper(to_number('NaN')) from dual;
