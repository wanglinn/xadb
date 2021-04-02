set grammar to oracle;
--一般用法
create table regtbl(id int,name varchar(10),sal binary_float);
insert into regtbl values(1,'Jack',1880.23);
insert into regtbl values(2,'Jhon',2300);
insert into regtbl values(3,'Ann',5300);
insert into regtbl values(4,'jee',5300);
select * from regtbl where regexp_like(name,'^J') order by id;
select * from regtbl where regexp_like(name,'.*a+.*','i') order by id;
select * from regtbl where regexp_like(name,'A|J') order by id;
select * from regtbl where regexp_like(name,'[AjJ]') order by id;
select * from regtbl where regexp_like(name,'n{2,3}') order by id;
select * from regtbl where regexp_like(name,'n{2,3}') order by id;
drop table regtbl;
--特殊字符
create table regtbl(id int,name varchar(10),sal binary_float);
insert into regtbl values(1,'An
ny',4300);
insert into regtbl values(2,'a  b',2300);
insert into regtbl values(3,'Anny',5300);
insert into regtbl values(4,'jee',5300);
insert into regtbl values(5,'an\nny',5300.9);
select * from regtbl where regexp_like(name,'\n') order by id;
select * from regtbl where regexp_like(name,chr(10)) order by id;
select * from regtbl where regexp_like(name,'\s') order by id;
select * from regtbl where regexp_like(name,'(?i)j') order by id;
select * from regtbl where regexp_like(name,'\x0a') order by id;
select * from regtbl where regexp_like(name,'.+ny') order by id;
select * from regtbl where regexp_like(name,'\') order by id;
select * from regtbl where regexp_like(name,'(n)\1') order by id;
drop table regtbl;
