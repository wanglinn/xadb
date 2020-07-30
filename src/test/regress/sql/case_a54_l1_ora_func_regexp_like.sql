set grammar to oracle;
create table t4test(id int,name varchar(20), cont  varchar(20));
insert into t4test values(1,'345','345');
insert into t4test values(2,'345abc','345');
insert into t4test values(3,'','');
insert into t4test values(4,'abc','abcd');
insert into t4test values(5,'mike',null);
insert into t4test values(6,null,null);
select * from t4test where regexp_like(name,'345') order by id;
select * from t4test where regexp_like(name,to_char(345)) order by id;
select * from t4test where regexp_like(name,345) order by id;
select * from t4test where regexp_like(name,to_number('345')) order by id;
select * from t4test where regexp_like(name,exp(1)) order by id;
select * from t4test where regexp_like(name,cont) order by id;
select * from t4test where regexp_like(name,'345abcd') order by id;
select * from t4test where regexp_like(name,'') order by id;
select * from t4test where regexp_like('',name) order by id;
select * from t4test where regexp_like('','') order by id;
select * from t4test where regexp_like(name,null) order by id;
select * from t4test where regexp_like(null,name) order by id;
drop table t4test;


--一般用法
create table t4test(id int,name varchar(10),sal binary_float);
insert into t4test values(1,'Jack',1880.23);
insert into t4test values(2,'Jhon',2300);
insert into t4test values(3,'Ann',5300);
insert into t4test values(4,'jee',5300);
select * from t4test where regexp_like(name,'^J') order by id;
select * from t4test where regexp_like(name,'.*a+.*','i') order by id;
select * from t4test where regexp_like(name,'A|J') order by id;
select * from t4test where regexp_like(name,'[AjJ]') order by id;
select * from t4test where regexp_like(name,'n{2,3}') order by id;
select * from t4test where regexp_like(name,'n{2,3}') order by id;
drop table t4test;
--特殊字符
create table t4test(id int,name varchar(10),sal binary_float);
insert into t4test values(1,'An
ny',4300);
insert into t4test values(2,'a  b',2300);
insert into t4test values(3,'Anny',5300);
insert into t4test values(4,'jee',5300);
insert into t4test values(5,'an\nny',5300.9);
select * from t4test where regexp_like(name,'\n') order by id;
select * from t4test where regexp_like(name,chr(10)) order by id;
select * from t4test where regexp_like(name,'\s') order by id;
select * from t4test where regexp_like(name,'(?i)j') order by id;
select * from t4test where regexp_like(name,'\x0a') order by id;
select * from t4test where regexp_like(name,'.+ny') order by id;
select * from t4test where regexp_like(name,'\') order by id;
select * from t4test where regexp_like(name,'(n)\1') order by id;
drop table t4test;