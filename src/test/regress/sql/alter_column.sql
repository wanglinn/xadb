set grammar to oracle;
--增加列，3种语法
create table altercoltbl(tid int,name varchar(2));
insert into altercoltbl values(1, 'a');
alter table altercoltbl add(dct1 varchar(10));
alter table altercoltbl add dct2 varchar(10);
alter table altercoltbl add column dct3 varchar(10);
drop table altercoltbl;
--删除单列
create table altercoltbl(tid int,name varchar(2));
insert into altercoltbl values(1,'a');
insert into altercoltbl values(2,'b');
alter table altercoltbl drop column name;
drop table altercoltbl;
--删除多列
create table altercoltbl(tid int,name varchar(2),tel char(10));
alter table altercoltbl drop(name,tel);
drop table altercoltbl;
--修改列
create table altercoltbl(tid int,name varchar(2));
alter table altercoltbl modify name char(10);
alter table altercoltbl modify name default('a');
drop table altercoltbl;
create table altercoltbl(tid int,name varchar(2),tel char(11));
alter table altercoltbl modify(name char(10),tel char(12));
drop table altercoltbl;
--重命名列
create table altercoltbl(tid int,name varchar(2));
alter table altercoltbl rename column name to na;
drop table altercoltbl;
