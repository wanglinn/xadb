set grammar to oracle;
--删除视图依赖的基表（oracle中可以删除依赖的table，view失效，重新创建talbe后，view重新有效，pg会不同）
create table tt(tid int,name varchar(2),sal int);
insert into tt values(1,'nn',3000);
create view vv as select name,sal from tt;
drop table tt;
select * from vv;
create table tt(tid int,name varchar(2),sal int);
insert into tt values(1,'nn',3000);
select * from vv;
drop view vv;
drop table tt;
--参数replace
create table tt(id int,name varchar(2));
create view vv as select id from tt;
create view vv as select id from tt;
create or replace view vv as select id from tt;
drop view vv;
drop table tt;
--其他参数测试force、noforce、check option、read only
create force view vv as select id from tt;
create table tt(id int,name varchar(2));
insert into tt values(1,'1');
select * from vv;
drop view vv;
drop table tt;
create noforce view vv as select id from tt;
create table tt(id int,name varchar(2));
create view vv as select id from tt where id>5 with check option constraint ck_id;
insert into vv values(1);
insert into vv values(6);
select * from vv;
select * from tt;
drop view vv;
drop table tt;
create table tt(id int,name varchar(2));
create view vv as select id from tt where id>5 with read only  constraint ck_id ;
insert into vv values(1);
drop view vv;
drop table tt;

--基于多表创建视图
create table tt(tid int primary key,name varchar(2));
create table aa(aid int primary key,name varchar(5), tid int);
create view vv(aid,an,tn) as select aa.aid, aa.name, tt.name from tt,aa where tt.tid=aa.tid;
insert into tt values(1,'ss');
insert into aa values(1,'a',1);
insert into aa values(2,'b',1);
delete from vv where an='b';
insert into vv(aid,an) values(3,'c');
select * from vv order by aid;
select * from aa;
select * from tt;
delete from vv where tn=2;
select * from aa;
drop view vv;
drop table aa;
drop table tt;
--基于视图创建视图
create table tt(tid int,name varchar(2),sal int);
create view vv as select name,sal from tt;
create view v2 as select * from vv where sal<1000;
create view v3 as select * from v2 where sal>100;
drop view v3;
drop view v2;
drop view vv;
drop table tt;
