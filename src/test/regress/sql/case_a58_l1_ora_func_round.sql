set grammar to oracle;
select round(2.009,2) from dual;
select round(100.987,0) from dual;
select round(100.987,-1) from dual;
select round(100.987,-3) from dual;
select round(exp(2),2) from dual;
select round(100.984,'2') from dual;
-----select round('100.984','2') from dual;

select round(100.984,2.8) from dual;
select round(100.984,-2.8) from dual;
select round(-100.98467,3) from dual;
select round(1000000000000000000,3) from dual;
select round(100,50) from dual;
select round(sinh(5),3) from dual;
select round(sinh(5),null) from dual;
select round(cosh(3),'') from dual;
