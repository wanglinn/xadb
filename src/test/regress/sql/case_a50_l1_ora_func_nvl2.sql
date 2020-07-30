set grammar to oracle;
select nvl2(3,4,5) from dual;
select nvl2(3,'4',5) from dual;
select nvl2(3,'4.9',5) from dual;
select nvl2(3,'s',5) from dual;
select nvl2(3,exp(3),5) from dual;
select nvl2(3,1.5*2,5) from dual;
select nvl2(4,4.8,5) from dual;
select nvl2(3.9,4,5) from dual;
select nvl2('3.9',4,5) from dual;
select nvl2('s',4,5) from dual;
select nvl2(exp(2),4,5) from dual;
select nvl2(3,4,5.6) from dual;
select nvl2(3,4,'4.8') from dual;
select nvl2(3,4,'0') from dual;
select nvl2(3,4,'aaa') from dual;
select nvl2(3,4,exp(3)) from dual;
select nvl2(tan(2),exp(2),exp(3)) from dual;
select nvl2(4.9,'s','s') from dual;
select nvl2('',4,5) from dual;
select nvl2(3,'',5) from dual;
select nvl2(null,4,5) from dual;
select nvl2(null,4.8,5.9) from dual;
select nvl2(null,4.8,4) from dual;
select nvl2(null,4.8,'s') from dual;
select nvl2(null,4.8,'5.9') from dual;
select nvl2(null,4.8,exp(2)) from dual;
select nvl2(4.9,null,4.9) from dual;
select nvl2(4.9,null,'') from dual;
select nvl2(4.9,null,null) from dual;
select nvl2(null,null,null) from dual;
select nvl2('','','') from dual;


create table t4test(id int,num float);
insert into t4test values(1,null);
insert into t4test values(2,3.14);
insert into t4test values(3,nvl2(null,4.14,3.33));

select * from t4test order by id ;

select nvl2(num,'is not null','is null') from t4test order by num;
update t4test set num=nvl2(num,num,2.22);
select * from t4test order by id;
drop table t4test;
