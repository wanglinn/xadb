set grammar to oracle;
select nvl(1.4,4.8) from dual;
select nvl(4,5) from dual;
select nvl(1.4,4) from dual;
select nvl(4,4.8) from dual;
select nvl(exp(2),4.8) from dual;
select nvl(exp(2),exp(3)) from dual;
select nvl('3',exp(4)) from dual;
select nvl('s',exp(4)) from dual;
select nvl(exp(4),'s') from dual;
select nvl(exp(4),'3') from dual;
select nvl('',4) from dual;
select nvl(null,4) from dual;
select nvl('','') from dual;
select nvl(4,'') from dual;
select nvl(null,null) from dual;
select nvl(null,'') from dual;

create table test(id int,num float);
insert into test values(1,null);
insert into test values(2,3.14);
insert into test values(3,nvl(null,4.14));

select * from test order by id;

select nvl(num,3.13) from test order by num;
update test set num=nvl(num,2.22);
select * from test order by id;
drop table test;
