set grammar to oracle;
create table t4test(name varchar(10), id int);
insert into t4test values(null, 1); 
insert into t4test values('', 2);
insert into t4test values ('null',3);
insert into t4test values('s', 4);
insert into t4test values ('ss',5);
select * from t4test where lnnvl(name is not null) order by id;
select * from t4test where lnnvl(name is null) order by id;
select * from t4test where lnnvl(name like 's%') order by id;
select * from t4test where lnnvl(name ='s') order by id;
select * from t4test where lnnvl(name like 's%') and id <4 order by id;
select * from t4test where lnnvl(id <2 or id >4) order by id;
select * from t4test where lnnvl(id in(1,3)) order by id;
select * from t4test where lnnvl(id between 1 and 3) order by id;
drop table t4test;


create table ct4test(name varchar(10), id int,flag boolean);
insert into ct4test values(null, 1,1=1);
insert into ct4test values('ab', 2,1=1);
insert into ct4test values('abc', 3,1=2);
select lnnvl(name is null) from ct4test where id=1;
select lnnvl(name is not null) from ct4test where id=1;
select lnnvl(name ='ab') from ct4test where id=2;
select lnnvl(name !='ab') from ct4test where id=2;
insert into ct4test values('abc', 4,lnnvl('aca'='ab'));
select * from ct4test where id=4;

drop table ct4test;
