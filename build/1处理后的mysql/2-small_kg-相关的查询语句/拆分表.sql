/*
#取出所以被控股/参股的企业 包括上市公司
CREATE TABLE  All_holded_C as
select DISTINCT c_consolidated_statement_name,c_net_profit,c_main_business,c_announcement_date 
from C_holding_C
ORDER BY c_announcement_date DESC
*/

#对All_holded_C去重，保留最新的数据 操作失败，改用pandas 处理吧！
/*SELECT t1.*
from All_holded_C as t1,
(SELECT c_consolidated_statement_name,MAX(c_announcement_date) as max_date
from All_holded_C
GROUP BY c_consolidated_statement_name) as t
WHERE t1.c_announcement_date=t.max_date and t1.c_consolidated_statement_name=t.c_consolidated_statement_name
ORDER  BY t1.c_consolidated_statement_name,t1.c_announcement_date

*/
/*
import pandas as pd

df=pd.read_csv('/Users/brobear/Downloads/All_holded_C.csv')
df2=df.groupby(["c_consolidated_statement_name"]).head(1)
df2.to_csv("/Users/brobear/Downloads/All_holded_C-2.csv",index=None)
*/


#处理公司详情表
/*
# 拆分申万行业
CREATE TABLE  first_secondindustry as
SELECT `code`,SUBSTRING_INDEX(c_industry,' — ',1) as first_industry,SUBSTRING_INDEX(c_industry,' — ',-1) as second_industry,c_industry
from Company
*/
#
/*
# 提取 公司详情中与人相关的字段
# 实际控股人和最终控制人 有括号要删
CREATE TABLE controller_chairman_person_name  as
SELECT `code`,SUBSTRING_INDEX(c_actual_controller,'	',1) as c_actual_controller,
							SUBSTRING_INDEX(c_final_controller,'	',1) as c_final_controller,
							replace(c_chairman,'(代)','') as c_chairman,
							replace(c_chairman_secretary,'(代)','') as c_chairman_secretary,
							replace(c_legal_representative,'(代)','') as c_legal_representative,
							replace(c_general_manager,'(代)','') as c_general_manager
from Company
*/

# 根据code和name 找到对应的人（人名，性别，年龄，学历）
# pandas 处理



#处理概念
/*
# 提取所有概念
CREATE TABLE general_other_concept  as
select c_name,c_type
from Concept
GROUP BY c_name,c_type
#手动删除其他概念：幼儿教育 和 移动互联网 
*/



