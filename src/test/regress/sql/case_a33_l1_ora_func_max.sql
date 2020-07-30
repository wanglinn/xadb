set grammar to oracle;
CREATE TABLE t4test (id int,name varchar,salary int,dep varchar);
insert into t4test values(1,'xu',12000,'a1');
insert into t4test values(2,'zhang',12500,'a2');
insert into t4test values(3,'li',13500,'a1');
insert into t4test values(4,'lin',13500,'a3');
insert into t4test values(5,'xu',23456,'a1');
insert into t4test values(6,'zha0',12500,'a1');
insert into t4test values(7,'xiao',13500,'a2');
insert into t4test values(8,'yang',23500,'a3');

SELECT max(salary) from t4test;

SELECT dep, name, salary,
	MAX(salary) OVER (PARTITION BY dep) AS dep_max
	FROM t4test
	ORDER BY dep, name, salary;
	
	
SELECT dep, name, salary
	FROM (SELECT dep, name, salary, 
			MAX(salary) OVER (PARTITION BY dep) AS max_sal
			FROM t4test)
	WHERE salary = max_sal
	ORDER BY dep, name, salary;	
	

drop table t4test;