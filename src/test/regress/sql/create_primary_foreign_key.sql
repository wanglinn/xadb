set grammar to oracle;
--1、integer类型是主键
create table cpfktbl1(id integer primary key,name char(10));
insert into cpfktbl1 values(1,'a');
insert into cpfktbl1 values(1,'b');
drop table cpfktbl1;
--2、varchar2类型是主键
create table cpfktbl1(id integer ,name varchar2(10) primary key);
insert into cpfktbl1 values(1,'a');
insert into cpfktbl1 values(2,'a');
drop table cpfktbl1;
--3、表约束方式创建主键
create table cpfktbl1(id integer,name char(10),constraint pkk primary key(id));
drop table cpfktbl1;
--4、联合主键
create table cpfktbl1(id integer ,name varchar2(10), primary key(id,name));
insert into cpfktbl1 values(1,'b');
insert into cpfktbl1 values(1,'b');
drop table cpfktbl1;
--5、创建外键
create table cpfktbl1(id integer unique,name char(10));
insert into cpfktbl1 values(1,'a');
insert into cpfktbl1 values(2,'b');
create table cpfktbl2(id int, sname char(10),constraint fk FOREIGN KEY(id) REFERENCES cpfktbl1(id));
insert into cpfktbl2 values(1,'s1');
insert into cpfktbl2 values(2,'s2');
insert into cpfktbl2 values(3,'s2');
drop table cpfktbl2;
drop table cpfktbl1;
--6、主键与外键数据类型不同
create table cpfktbl1(id integer unique,name char(10));
create table cpfktbl2(fid number(10),sname char(10), constraint fk FOREIGN KEY(fid) REFERENCES cpfktbl1(id));
drop table cpfktbl2;
drop table cpfktbl1;
create table cpfktbl1(id number unique,name char(10));
create table cpfktbl2(fid integer,sname char(10), constraint fk FOREIGN KEY(fid) REFERENCES cpfktbl1(id));
drop table cpfktbl2;
drop table cpfktbl1;
create table cpfktbl1(id float unique,name char(10));
create table cpfktbl2(fid number,sname char(10), constraint fk FOREIGN KEY(fid) REFERENCES cpfktbl1(id));
drop table cpfktbl2;
drop table cpfktbl1;
--7、联动删除
create table cpfktbl1(id number(4) primary key,name char(10));
create table cpfktbl2(id number(3) primary key,name varchar(20));
ALTER TABLE cpfktbl2 ADD CONSTRAINT FK_ID FOREIGN KEY(id) REFERENCES cpfktbl1(id) ON DELETE CASCADE;  
insert into cpfktbl1 values(1,'Mike');
insert into cpfktbl2 values(1,'Jack');
select * from cpfktbl2;
delete from cpfktbl1 where id=1;
select * from cpfktbl2;
drop table cpfktbl2;
drop table cpfktbl1;

