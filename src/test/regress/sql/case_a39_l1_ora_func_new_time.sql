set grammar to oracle;
set datestyle='ISO,YMD';
SELECT NEW_TIME(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), 'AST', 'PST') ndt FROM DUAL;
SELECT NEW_TIME(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), 'GMT', 'EST') ndt FROM DUAL;
SELECT NEW_TIME(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), 'GMT', 'EST') ndt FROM DUAL;
SELECT NEW_TIME(TO_timestamp('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), 'HDT', 'YDT') ndt FROM DUAL;
SELECT NEW_TIME('04-MAR-2016 01:23:45', 'HST', 'YDT') ndt FROM DUAL;
SELECT NEW_TIME(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), to_char('MST'), to_char('PDT')) ndt FROM DUAL;
SELECT NEW_TIME(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), '', 'EST') ndt FROM DUAL;
SELECT NEW_TIME(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'), null, 'EST') ndt FROM DUAL;
SELECT NEW_TIME('', 'MST', 'EST') ndt FROM DUAL;

create table t4test (id int,ctime date);
insert into t4test values(1,TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'));
select new_time(ctime,'BST','EST') from t4test;
insert into t4test values (2,new_time(TO_DATE('11-10-09 01:23:45', 'MM-DD-YY HH24:MI:SS'),'BST','YDT'));
select * from t4test order by id;
update t4test set ctime=new_time(TO_DATE('11-11-09 01:23:45', 'MM-DD-YY HH24:MI:SS'),'BST','YDT') where id=1;
select * from t4test order by id;
select * from t4test where ctime=new_time(TO_DATE('11-11-09 01:23:45', 'MM-DD-YY HH24:MI:SS'),'BST','YDT');

drop table t4test;
