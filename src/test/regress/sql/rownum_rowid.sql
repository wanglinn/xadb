set grammar to oracle;
--rownum
create table aa(id int,name varchar(20));
insert into aa values(1,'345');
insert into aa values(2,'345abc');
insert into aa values(3,'');
insert into aa values(4,'abc');
insert into aa values(5,'mike');
insert into aa values(6,null);
--多次执行下面语句，10次,观察结果是否相同
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum < 4;
select name,rownum from (select * from aa order by id) where rownum != 4;
select name,rownum from (select * from aa order by id) where rownum != 4;
select name,rownum from (select * from aa order by id) where rownum != 4;
select name,rownum from (select * from aa order by id) where rownum between 1 and 3;
select name,rownum from (select * from aa order by id) where rownum > 0;
select * from(select id,name,rownum page from (select * from aa order by id)where rownum<10) where page between 2 and 3;
drop table aa;
--rowid	rowid的比较运算、聚合函数、order by
create table aa(id int,name varchar(20));
insert into aa values(1,'345');
insert into aa values(2,'345');
insert into aa values(3,'');
insert into aa values(4,'abc');
insert into aa values(5,'abc');
insert into aa values(6,null);
insert into aa values(7,'');
insert into aa values(8,'mike');
insert into aa values(9,'abc');
select name,rowid from aa order by name;
select * from aa where rowid='FbpC+g==AAAAAA==AQA=';
--like：
select * from aa where rowid like 'FbpC+g==AAAAAA==AQA=';
select * from aa where rowid in(select rowid from aa) order by id;
--根据rowid排序不支持：
select name,rowid from aa order by rowid;
--子查询结果集的rowid
select name,rowid from (select * from aa) ;
--查找重复数据,聚合函数rowid类型
select max(rowid) from aa group by name;
select * from aa where rowid not in(select max(rowid) from aa group by name) order by id;
drop table aa;
--设置rowid类型的列
create table tt(id int,name varchar(20),rrid rowid);
insert into tt values(1,'345',null);
insert into tt values(2,'ab',null);
insert into tt values(3,'',null);
update tt set rrid=rowid where id=1;
select * from tt;
drop table tt;
