set grammar to oracle;
--更新一行
create table tt(id int,name varchar(10));
insert into tt(id,name) values(1, 'mike');
insert into tt(id,name) values(2, 'Jack');
update tt set name ='Jhon' where id = 1;
update tt set id=2,name ='Jhon' where id = 1;
select * from tt;
drop table tt;
--更新多行
create table tt(id int,name varchar(10));
insert into tt(id,name) values(1, 'mike');
insert into tt(id,name) values(2, 'Jack');
update tt set name ='unused';
select * from tt;
drop table tt;
--违反约束更新
create table tt(id int,name varchar(10) not null);
insert into tt(id,name) values(1, 'mike');
insert into tt(id,name) values(2, 'Jack');
update tt set name ='' where id=2;
select * from tt;
drop table tt;
--更新视图
create table tt(tid int,name varchar(5),sal int);
create view vv as select name,sal from tt;
insert into vv(name,sal) values('Mike','5999');
update vv set name='Rose' where sal=5999;
select * from tt;
drop view vv;
drop table tt;
