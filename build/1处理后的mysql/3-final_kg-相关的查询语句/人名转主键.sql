# code 人名 转 人名 性别年龄学历

CREATE TABLE  tt1 as 
SELECT t.`code`,c_actual_controller,p_intro as c_actual_controller_intro
FROM controller_chairman_person_name as t,Person
where Person.`code`=t.`code` and c_actual_controller=Person.p_name;

CREATE TABLE  tt2 as 
SELECT t.`code`,c_final_controller,p_intro  as c_final_controller_intro
FROM controller_chairman_person_name as t,Person
where Person.`code`=t.`code` and c_final_controller=Person.p_name;

CREATE TABLE  tt3 as 
SELECT t.`code`,c_chairman,p_intro  as c_chairman_intro
FROM controller_chairman_person_name as t,Person
where Person.`code`=t.`code` and c_chairman=Person.p_name;

CREATE TABLE  tt4 as 
SELECT t.`code`,c_chairman_secretary,p_intro  as c_chairman_secretary_intro
FROM controller_chairman_person_name as t,Person
where Person.`code`=t.`code` and c_chairman_secretary=Person.p_name;

CREATE TABLE  tt5 as 
SELECT t.`code`,c_legal_representative,p_intro  as c_legal_representative_intro
FROM controller_chairman_person_name as t,Person
where Person.`code`=t.`code` and c_legal_representative=Person.p_name;

CREATE TABLE  tt6 as 
SELECT t.`code`,c_general_manager,p_intro  as c_general_manager_intro
FROM controller_chairman_person_name as t,Person
where Person.`code`=t.`code` and c_general_manager=Person.p_name;

# 要分两步执行

CREATE TABLE controller_chairman_person_name_intro  as 
SELECT controller_chairman_person_name.c_actual_controller,c_actual_controller_intro,
controller_chairman_person_name.c_final_controller,c_final_controller_intro,
controller_chairman_person_name.c_chairman,c_chairman_intro,
controller_chairman_person_name.c_chairman_secretary,c_chairman_secretary_intro,
controller_chairman_person_name.c_legal_representative,c_legal_representative_intro,
controller_chairman_person_name.c_general_manager,c_general_manager_intro
FROM controller_chairman_person_name LEFT JOIN
tt1 on controller_chairman_person_name.`code` =tt1.`code` LEFT JOIN
tt2 on controller_chairman_person_name.`code` =tt2.`code` LEFT JOIN
tt3 on controller_chairman_person_name.`code` =tt3.`code` LEFT JOIN
tt4 on controller_chairman_person_name.`code` =tt4.`code` LEFT JOIN
tt5 on controller_chairman_person_name.`code` =tt5.`code` LEFT JOIN
tt6 on controller_chairman_person_name.`code` =tt6.`code` 

drop table tt1;
drop table tt2;
drop table tt3;
drop table tt4;
drop table tt5;
drop table tt6;