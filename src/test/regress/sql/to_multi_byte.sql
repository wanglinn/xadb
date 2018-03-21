--新创建一个utf-8编码的数据库，退出当前数据库，连接到新数据库上测试
set grammar to default;
CREATE DATABASE shan TEMPLATE = template0 ENCODING = 'UTF8';
\q
psql -d shan -p 5432 -U pom -h 10.0.0.5
set grammar to oracle;
select to_multi_byte('12abc') from dual;
select to_multi_byte('!@#$%^&*(),.;') from dual;
select to_multi_byte('你好') from dual;
\q
psql -d postgres -p 5432 -U pom -h 10.0.0.5
drop database shan;
