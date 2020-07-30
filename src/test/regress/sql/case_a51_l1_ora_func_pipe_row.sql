set grammar to oracle;

--pipe row


------数值类型
-------------------------------------
\set PLSQL_MODE on
CREATE OR REPLACE FUNCTION f1_t4test(x NUMBER) 
RETURN NUMBER PIPELINED 
IS
  BEGIN
    FOR i IN 1..x LOOP
      PIPE ROW(i);
    END LOOP;
    RETURN;
  END f1_t4test;
/

\set PLSQL_MODE off

select * from table(f1_t4test(5));

drop FUNCTION f1_t4test(x NUMBER);

----------字符类型+双行+ROWNUM
-----------------------------------
\set PLSQL_MODE on
CREATE OR REPLACE FUNCTION f2_t4test(x NUMBER) 
RETURN varchar2 PIPELINED 
IS
  BEGIN
    FOR i IN 1..x LOOP
      PIPE ROW('row1:'||to_char(i));
	  PIPE ROW('row2:'||to_char(i));
    END LOOP;
    RETURN;
  END f2_t4test;
/

\set PLSQL_MODE off

select * from table(f2_t4test(5)) where ROWNUM<6;

drop FUNCTION f2_t4test(x NUMBER);

---------------表达式
----------------------------------------------
\set PLSQL_MODE on
CREATE OR REPLACE FUNCTION f3_t4test(x NUMBER) 
RETURN varchar2 PIPELINED 
IS
  BEGIN
    FOR i IN 1..x LOOP
      PIPE ROW(((i+2)*2)/i);
    END LOOP;
    RETURN;
  END f3_t4test;
/

\set PLSQL_MODE off

select * from table(f3_t4test(5));

drop FUNCTION f3_t4test(x NUMBER);
---------------存储过程
----------------------------------------------
\set PLSQL_MODE on

create or replace procedure t4test_pro(id in int,name1 out VARCHAR2, name2 out VARCHAR2)
as  
        begin
          name1:='name1:'||to_char(id);
		  name2:='name2:'||to_char(id);
         end;
/


CREATE OR REPLACE FUNCTION f4_t4test(x NUMBER) 
RETURN varchar2 PIPELINED 
IS
  BEGIN
    FOR i IN 1..x LOOP
      PIPE ROW(select t4test_pro(i));
    END LOOP;
    
  END f4_t4test;
/

\set PLSQL_MODE off

select * from table(f4_t4test(5));

drop FUNCTION f4_t4test(x NUMBER);
drop procedure t4test_pro(id in int,name1 out VARCHAR2, name2 out VARCHAR2);








