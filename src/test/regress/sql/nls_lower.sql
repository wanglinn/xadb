set grammar to oracle;
select nls_lower('AB CDe') from dual;
select nls_lower('\?!@#$%^&*()SA') from dual;
select nls_lower('AA\?!@#$%^&*()AA') from dual;
select nls_lower('\?!@#sS$%^&*()sC') from dual;
select nls_lower('Test你TEst好TeST') from dual;
select nls_lower('1234SOU') from dual;
select nls_lower(to_char('SUV')) from dual;
select nls_lower(to_number('NaN')) from dual;

