set grammar to oracle;
select length('abcd!@#$^&*()') from dual;
select length('ab
c') from dual;
select length('你好') from dual;
select length('你
好') from dual;
select length(to_char(1234)) from dual;
select length(exp(2)) from dual;
select length(12.3456) from dual;
create table length_tbl (name varchar2(10));
insert into length_tbl values('1bd2sdfaef');
insert into length_tbl values('3csadfed');
insert into length_tbl values('mmsdeq4d');
insert into length_tbl values('ccd');
select * from length_tbl where length(name) >6 order by name;
select name,length(name) from length_tbl order by name;
drop table length_tbl;
