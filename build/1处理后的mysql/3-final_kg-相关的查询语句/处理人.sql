# 获取不复的所有人
SELECT p_name,p_intro
from Person
GROUP BY p_name,p_intro

SELECT a.*,b.p_mainintro,b.p_mainintro_date
from Person as a,Person as b
where a.p_name=b.p_name and a.p_intro=b.p_intro and a.p_mainintro!=b.p_mainintro


SELECT C_name,C_type
from Holding_detail_now_now
group by C_name,C_type