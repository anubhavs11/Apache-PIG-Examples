employee = LOAD 'emp.txt' USING PigStorage(',') AS (EID:int,ENAME:chararray,PTITLE:chararray,DID:int,ESAL:float);
describe employee;
DUMP employee_list;

department = LOAD 'dep.txt' USING PigStorage(';') AS (DID:int,DNAME:chararray,ADDRESS:map[]);
describe department;
department_list = FOREACH department GENERATE DID,DNAME;
DUMP department_list;

bonus = LOAD 'bonus.txt' USING PigStorage(',') AS (EID:int,BONUS:float);
describe bonus;
bonus_list = FOREACH bonus GENERATE EID;
DUMP bonus_list;

joins = JOIN employee BY (EID), bonus BY (EID);
grp_by_dep = GROUP joins BY DID;
result = FOREACH grp_by_dep GENERATE group,AVG(joins.BONUS);
dump result;




