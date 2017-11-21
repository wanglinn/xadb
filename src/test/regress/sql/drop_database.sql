--1、超时之前删除失败，调用函数后删除成功
create database te;
\q
psql -d te -p 4332
create table tt(id int);
\q
psql -d postgres -p 4332
drop database te;
select pool_close_idle_conn();
drop database te;

--2、调用函数后删除成功
create database te;
\q
psql -d te -p 4332
create table tt(id int);
\q
psql -d postgres -p 4332
select pool_close_idle_conn();
drop database te;

--3、等候一分钟后，删除成功
create database te;
\q
psql -d te -p 4332
create table tt(id int);
\q
psql -d postgres -p 4332
select pg_sleep(60);
drop database te;
