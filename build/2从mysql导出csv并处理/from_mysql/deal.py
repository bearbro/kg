import pandas as pd
import pickle

## 获取上市公司节点
df=pd.read_csv('Company.csv',dtype={'code': str})
Company_name2code={ df["c_name"][i]:df["code"][i] for i in range(len(df))}

# 添加简称
df2=pd.read_csv('stock_code.csv',dtype={'code': str})
code2jiancheng={ df2["code"][i]:df2["name"][i] for i in range(len(df2))}
jiancheng=[code2jiancheng[df["code"][i]] for i in range(len(df))]
df['简写名称']=jiancheng

en2ch={"code":"股票代码","c_name":"公司名称","c_name_en":"英文名称","c_name_old":"曾用名","c_main_business":"主营业务","c_industry":"所属申万行业","c_website":"公司网址","c_registered_capital":"注册资金","c_employee_count":"员工人数","c_phone":"电话","c_fax":"传真","c_postcode":"邮政编码","c_address":"办公地址","c_introduction":"公司简介","c_territory":"所属地域"
}
col=[ en2ch[i] for i in ["code","c_name","c_name_en","c_name_old","c_industry","c_main_business","c_website","c_registered_capital","c_employee_count","c_phone","c_fax","c_postcode","c_address","c_introduction","c_territory"]]
col.append('简写名称')
df.rename(columns=en2ch, inplace=True) 
df.to_csv("deal/公司.csv",index=None,columns=col)



## 获取一级、二级行业节点 及 
# 关系：上市公司--->一级行业 
#      上市公司--->二级行业
#      二级行业--->一级行业
df=pd.read_csv('first_second_industry.csv',dtype={'code': str})
second2first={df["second_industry"][i]:df["first_industry"][i] for i in range(len(df))}
second_industry=list(second2first.keys())
df2=pd.DataFrame({"申万二级行业":second_industry})
df2.to_csv("deal/申万二级行业.csv",index=None)

df1=pd.DataFrame({"申万一级行业":list(second2first.values())})
df1.to_csv("deal/申万一级行业.csv",index=None)
# 关系
df21=pd.DataFrame({"申万二级行业":second_industry,
                    "申万一级行业":[second2first[i] for i in second_industry]})
df21.to_csv("deal/申万二级行业2申万一级行业.csv",index=None)

dfc21=df[['code','first_industry']].copy()
dfc21.rename(columns={'code':"股票代码",'first_industry':"申万一级行业"}, inplace=True) 
dfc21.to_csv("deal/股票代码2申万一级行业.csv",index=None)

dfc22=df[['code','second_industry']].copy()
dfc22.rename(columns={'code':"股票代码",'second_industry':"申万二级行业"}, inplace=True) 
dfc22.to_csv("deal/股票代码2申万二级行业.csv",index=None)




#获取概念节点 及
#关系：上市公司 ---> 概念
df=pd.read_csv('Concept.csv',dtype={'code': str})
Concept=df[["c_name","c_type"]].copy()
Concept.drop_duplicates(subset=['c_name'], keep='first',inplace=True)
Concept.rename(columns={'c_name':"概念名称",'c_type':"概念类型"}, inplace=True) 
Concept.to_csv("deal/概念.csv",index=None)

code2concept=df[['code','c_name','c_analysis']].copy()
code2concept.rename(columns={'code':"股票代码",'c_name':"概念名称","c_analysis":"概念解析"}, inplace=True) 
code2concept.to_csv("deal/股票代码2概念.csv",index=None)




#获取题材要点节点 及
#关系：上市公司 ---> 题材要点
df=pd.read_csv('Theme_points.csv',dtype={'code': str})
df=df.applymap(lambda x: str(x).strip())#去空格
Point=df[["theme_points_name"]].copy()
Point.drop_duplicates(subset=['theme_points_name'], keep='first',inplace=True)
Point.rename(columns={'theme_points_name':"要点名称"}, inplace=True) 
Point.to_csv("deal/题材要点.csv",index=None)

code2point=df[['code','theme_points_name','theme_points_info']].copy()
code2point.rename(columns={'code':"股票代码",'theme_points_name':"要点名称","theme_points_info":"要点详情"}, inplace=True) 
code2point.to_csv("deal/股票代码-具有-题材要点.csv",index=None)



# 提取主营业务 节点 todo 优化
# 产品类型 节点
# 产品名称 节点
# 经营范围 节点 todo 优化
# 关系 
# 股票代码-》主营业务
# 股票代码-》产品类型
# 股票代码-》产品名称
# 股票代码-》 经营范围
import re
def mysplit(in_str,seps):
    r=re.split(seps, in_str)
    rr=[]
    for x in r:
        x=x.strip()
        if len(x)!=0 and x[0] in ['"',"'"]:
            x=x[1:]
        if len(x)!=0 and x[-1] in ['"',"'"]:
            x=x[:-2]
        if len(x)!=0 :
            rr.append(x)
    return rr
df=pd.read_csv("Main_business.csv",dtype={'code': str})

def split_business(df,en_name,zh_name,seps):
    df2_code=[]
    df2_business=[]
    business_set=set()
    for i in df.index:
        x=mysplit(df[en_name][i],seps) 
        if zh_name in ["主营业务","经营范围"]:# todo 分割不好
            pass
        df2_code+=[df["code"][i]]*len(x)
        df2_business+=x
        business_set|=set(x)

    df1=pd.DataFrame({zh_name:[i for i in business_set if len(i)>0 and i!='-']})
    df1.to_csv("deal/%s.csv"%zh_name,index=None)
    df2=pd.DataFrame({"股票代码":df2_code,zh_name:df2_business})
    df2.to_csv("deal/股票代码2%s.csv"%zh_name,index=None)

split_business(df,"Main_business","主营业务",'[，]+')
split_business(df,"Product_type","产品类型",'[、]+')
split_business(df,"Product_name","产品名称",'[、]+')
split_business(df,"Business_range","经营范围",'[、]+')



# 提取业务名称节点
# 关系
# 股票代码-》业务

df=pd.read_csv("Operate_Composition.csv",dtype={'code': str})
business=list(set(df["B_name"]))
df1=pd.DataFrame({"业务名称":business})
df1.to_csv("deal/业务.csv",index=None)
x=["code","B_name","income","income_rate","op_cost","cost_rate","profit","gross_profit"]
df2=df[x]
en2ch={x[i]:
["股票代码","业务名称","营业收入","收入比例","营业成本","成本比例","利润比例","毛利润"][i] for i in range(len(x))}
df2.rename(columns=en2ch,inplace=True)
df2.to_csv("deal/股票代码--业务.csv",index=None)





#处理参控股公司
# 提取非上市公司节点 来源：被控股公司、十大股东、十大流通股东、持股公司、持股基金  =所有公司/机构-上市公司
# 节点 非上市公司-被控股=被控股公司/机构-上市公司
#关系：
#    被控股公司-->主营业务
#    上市公司--参控--》被控股公司
df=pd.read_csv("deal/公司.csv",dtype={'股票代码': str})
Company_name2code={ df["公司名称"][i]:df["股票代码"][i] for i in range(len(df))}
Company_name2code.update({ df["英文名称"][i]:df["股票代码"][i] for i in range(len(df)) if df["英文名称"][i]!="-" and len(df["英文名称"][i])>0})
Company_name2code.update({ df["简写名称"][i]:df["股票代码"][i] for i in range(len(df)) if df["简写名称"][i]!="-" and len(df["简写名称"][i])>0})
code_company=Company_name2code.keys()
df=pd.read_csv('All_holded_C_2.csv')
df=df.applymap(lambda x: str(x).strip())#去空格
idx=[i for i in df.index if df["c_consolidated_statement_name"][i] not in code_company ]
df=df.loc[idx,:]
df1=df[df["c_consolidated_statement_name"]!='-'][["c_consolidated_statement_name","c_net_profit","c_announcement_date","c_main_business"]].copy()
df1.rename(columns={'c_consolidated_statement_name':"公司名称","c_net_profit":"净利润","c_announcement_date":"公告日期","c_main_business":"主营业务"}, inplace=True) 
df1.to_csv("deal/非上市公司-被控股.csv",index=None)
# # # 拆分 主营业务？  
# df2=df[df["c_main_business"]!="-" and df["c_main_business"]!='nan'][["c_consolidated_statement_name","c_main_business"]].copy()
# df2.rename(columns={'c_consolidated_statement_name':"公司名称","c_main_business":"主营业务"}, inplace=True) 
# df2.to_csv("deal/非上市公司-被控股2主营业务（未拆分）.csv",index=None)

df=pd.read_csv('C_holding_C_now.csv',dtype={'code': str})

idx=[i for i in df.index if df["c_consolidated_statement_name"][i] not in code_company ]
df1=df.loc[idx,:].copy()
df1=df1[["code","c_consolidated_statement_name","c_participation_relationship","c_participation_ratio","c_investment_amount","c_merge_report"]]
df1.rename(columns={'code':"股票代码","c_consolidated_statement_name":"被参控公司",
"c_participation_relationship":"参控关系","c_participation_ratio":"参控比例","c_investment_amount":"投资金额",
"c_merge_report":"是否合并报表","c_announcement_date":"公告日期",}, inplace=True) 
df1.to_csv("deal/股票代码--参控股--非上市公司-被控股.csv",index=None)

idx=[i for i in df.index if df["c_consolidated_statement_name"][i] in code_company ]
df1=df.loc[idx,:].copy()
df1=df1[["code","c_consolidated_statement_name","c_participation_relationship","c_participation_ratio","c_investment_amount","c_merge_report"]]
df1["code2"]=df1["c_consolidated_statement_name"].apply(lambda x:Company_name2code[x] )
df1.rename(columns={'code':"股票代码","code2":"被参控公司股票代码",
"c_participation_relationship":"参控关系","c_participation_ratio":"参控比例","c_investment_amount":"投资金额",
"c_merge_report":"是否合并报表","c_announcement_date":"公告日期",}, inplace=True) 
df1.to_csv("deal/股票代码--参控股--股票代码.csv",index=None,columns=["股票代码","被参控公司股票代码","参控关系","参控比例","投资金额","是否合并报表"])





# 处理人名
# 获得所有高管 节点
df=pd.read_csv('Person.csv',dtype={'code': str})
map_code_name2idx=dict()
person=set()
for i in df.index:
    name=df["p_name"][i]
    info=df["p_intro"][i]
    code=df["code"][i]
    x=(name,info)
    person.add(x)
pid2person=list(person)

map_name_info2idx={pid2person[i]:i for i in range(len(pid2person))}
maininfo_date=[0]*len(pid2person)
for i in df.index:
    name=df["p_name"][i]
    info=df["p_intro"][i]
    code=df["code"][i]
    map_code_name2idx[(code,name)]=map_name_info2idx[(name,info)]
    maininfo_date[map_name_info2idx[(name,info)]]=(df["p_mainintro"][i],df["p_mainintro_date"][i])

df1=pd.DataFrame({
    "pid":list(range(len(pid2person))),
    "姓名":[ i[0] for i in pid2person],
    "信息":[ i[1] for i in pid2person],
    "简介":[ i[0] for i in maininfo_date],
    "简介更新时间":[ i[1] for i in maininfo_date]
})
df1["性别"]=df1["信息"].apply(lambda x: [ i for i in x.split(" ") if len(i)>0] [0] if type(x) is str and len([ i for i in x.split(" ") if len(i)>0])>0 else "-")
df1["年龄"]=df1["信息"].apply(lambda x: [ i for i in x.split(" ") if len(i)>0] [1] if type(x) is str and len([ i for i in x.split(" ") if len(i)>0])>1 else "-")
df1["学历"]=df1["信息"].apply(lambda x: [ i for i in x.split(" ") if len(i)>0] [2] if type(x) is str and len([ i for i in x.split(" ") if len(i)>0])>2 else "-")
df1.to_csv("deal/高管.csv",index=None,columns=["pid","姓名","信息","简介","简介更新时间","性别","年龄","学历"])
pid=[0]*len(df)
for i in range(len(df)):
    name=df["p_name"][i]
    info=df["p_intro"][i]
    pid[i]=map_name_info2idx[(name,info)]
df2=pd.DataFrame({
    "pid":pid,
    "股票代码":df["code"],
    "职务":df["p_job"],
    "薪酬":df["p_salary"],
    "直接持股数":df["p_shares_number"],
    "高管类型":df["p_type"], 
    # "简介":df["p_mainintro"],
    # "简介更新时间":df["p_mainintro_date"]   
})
df2.to_csv("deal/personId2股票代码.csv",index=None)

# 序列化到文件
with open(r"map_code_name2idx.txt", "wb") as f:
    pickle.dump(map_code_name2idx, f)
with open(r"map_name_info2idx.txt", "wb") as f:
    pickle.dump(map_name_info2idx, f)





# 处理关系
# 上市公司---》实际控制人
# 上市公司---》最终控制人
# 上市公司---》董事长
# 上市公司---》董秘
# 上市公司---》法人代表
# 上市公司---》总经理

with open(r"map_code_name2idx.txt", "rb") as f:
    map_code_name2idx=pickle.load(f) 
with open(r"map_name_info2idx.txt", "rb") as f:
    map_name_info2idx=pickle.load(f) 


df=pd.read_csv('controller_chairman_person_name.csv',dtype={'code': str})
add_person=[]

def find_pid(df,t_name,t_name2,x):
    p_name_set=set([ i[1] for i in map_code_name2idx.keys()])
    codes=[  df["code"][i]  for i in range(len(df)) if df[x][i] in p_name_set]
    pids=[]
    for i in range(len(df)):
        if df[x][i] in p_name_set:
            if (df["code"][i],df[x][i]) in map_code_name2idx:
                pids.append(map_code_name2idx[(df["code"][i],df[x][i])]) 
            else : # 出现非高管的控制人
                now_pid=len(map_name_info2idx)
                add_person.append((now_pid,df[x][i],df["code"][i]))
                map_code_name2idx[(df["code"][i],df[x][i])]=now_pid
                map_name_info2idx[(df[x][i],df["code"][i])]=now_pid
                pids.append(map_code_name2idx[(df["code"][i],df[x][i])]) 
    df1=pd.DataFrame({"股票代码":codes,"pid":pids})
    df1.to_csv(t_name,index=None)

    codes=[  df["code"][i]  for i in range(len(df)) if df[x][i] not in p_name_set and df[x][i] not in ["-","--",""] and type(df[x][i]) is str]
    c_names=[ df[x][i] for i in range(len(df)) if df[x][i] not in p_name_set and df[x][i] not in ["-","--",""] and type(df[x][i]) is str]
    df1=pd.DataFrame({"股票代码":codes,"公司名称":c_names})
    df1.to_csv(t_name2,index=None)

find_pid(df,"deal/股票代码---实际控制人-人.csv","deal/股票代码---实际控制人-公司.csv","c_actual_controller")
find_pid(df,"deal/股票代码---最终控制人-人.csv","deal/股票代码---最终控制人-公司.csv","c_final_controller")
find_pid(df,"deal/股票代码---董事长-人.csv","deal/股票代码---董事长-公司.csv","c_chairman")
find_pid(df,"deal/股票代码---董秘-人.csv","deal/股票代码---董秘-公司.csv","c_chairman_secretary")
find_pid(df,"deal/股票代码---法人代表-人.csv","deal/股票代码---法人代表-公司.csv","c_legal_representative")
find_pid(df,"deal/股票代码---总经理-人.csv","deal/股票代码---总经理-公司.csv","c_general_manager")

#更新高管文件
if len(add_person)>0:
    df=pd.read_csv('deal/高管.csv',dtype={'code': str})
    df1=pd.DataFrame({
        "pid":[ i[0] for i in add_person],
        "姓名":[ i[1] for i in add_person],
        "信息":[ i[2] for i in add_person],
        "简介":[ "-"]*len(add_person),
        "简介更新时间":[ "-"]*len(add_person),
        "性别":[ "-"]*len(add_person),
        "年龄":[ "-"]*len(add_person),
        "学历":[ "-"]*len(add_person)
    })
    df2 = pd.concat([df,df1],ignore_index=True)
    df2.to_csv("deal/高管.csv",index=None,columns=["pid","姓名","信息","简介","简介更新时间","性别","年龄","学历"])

    # 序列化到文件
    with open(r"map_code_name2idx.txt", "wb") as f:
        pickle.dump(map_code_name2idx, f)
    with open(r"map_name_info2idx.txt", "wb") as f:
        pickle.dump(map_name_info2idx, f)







## 提取 非上市公司-股东  节点  todo 区分 基金 和 机构/公司
# 关系 
#  股票代码 --十大股东-- 非上市公司-股东
#  股票代码 --十大股东-- 非上市公司-被控股
#  股票代码 --十大股东-- 上市公司
#  股票代码 --十大股东-- 高管
#  股票代码 --十大流通股东-- 非上市公司-股东
#  股票代码 --十大流通股东-- 非上市公司-被控股
#  股票代码 --十大流通股东-- 上市公司
#  股票代码 --十大流通股东-- 高管
#  股票代码 --十大流通股东-- 人（非高管）
#  非上市公司-股东 --持股-- 股票代码
#  非上市公司-被控股--持股-- 股票代码
#  上市公司 --持股-- 股票代码
#  高管 --持股-- 股票代码

df=pd.read_csv("deal/非上市公司-被控股.csv",dtype={'code': str})
no_code_company=set(df["公司名称"])# 被控股的非上市公司
df=pd.read_csv("deal/公司.csv",dtype={'code': str})
code_company=set(df["公司名称"])# 上市公司
df=pd.read_csv("Holder_TOP10_now_now.csv",dtype={'code': str})
no_code_company2=set(df["h_top10_name"])# 十大股东
df=pd.read_csv("Float_Holder_TOP10_now_now.csv",dtype={'code': str})
no_code_company2|=set(df["f_h_top10_name"])# 十大流通股东
df=pd.read_csv("Holding_detail_now_now.csv",dtype={'code': str})
no_code_company2|=set(df["C_name"])# 股东
no_code_company2=no_code_company2-no_code_company-code_company
df=pd.read_csv("deal/高管.csv",dtype={'code': str})
persons=set(df["姓名"])# 高管人名
no_code_company2-=persons
no_code_person=[i for i in no_code_company2 if len(i)<=3]# 非高管
no_code_person=set(no_code_person)
# 把no_code_person加到Person里
persons|=no_code_person
#非上市公司-股东
no_code_company2=set([i for i in no_code_company2 if len(i)>3])
df1=pd.DataFrame({"公司名称":list(no_code_company2)})
df1.to_csv("deal/非上市公司-股东.csv",index=None)


with open(r"map_code_name2idx.txt", "rb") as f:
    map_code_name2idx=pickle.load(f) 
with open(r"map_name_info2idx.txt", "rb") as f:
    map_name_info2idx=pickle.load(f) 

df=pd.read_csv("deal/公司.csv",dtype={'股票代码': str})
Company_name2code={ df["公司名称"][i]:df["股票代码"][i] for i in range(len(df))}
Company_name2code.update({ df["英文名称"][i]:df["股票代码"][i] for i in range(len(df)) if df["英文名称"][i]!="-" and len(df["英文名称"][i])>0})
Company_name2code.update({ df["简写名称"][i]:df["股票代码"][i] for i in range(len(df)) if df["简写名称"][i]!="-" and len(df["简写名称"][i])>0})

add_person=[]

def get_R_Holder_TOP10(nodes,table,df,x,xk,y,z=None):
    idx=[i for i in df.index if df[xk][i] in nodes]
    df1=df.loc[idx,x].copy()
    if z=="上市公司":
        y[1]="股东股票代码2"
        df1[xk]=[ Company_name2code[i] for i in df1[xk]]
    elif z=="pid":
        y[1]="pid"
        n_xk=[]
        for i in df1.index:
            code=df1["code"][i]
            name=df1[xk][i]
            if (code,name) in map_code_name2idx:
                ii=map_code_name2idx[(code,name)]
            else:
                ii=len(map_code_name2idx)
                map_code_name2idx[(code,name)]=ii
                map_name_info2idx[(name,code)]=ii
                add_person.append((ii,code,name))
            n_xk.append(ii)
        df1[xk]=n_xk

    en2ch={x[i]:y[i] for i in range(len(x))}
    df1.rename(columns=en2ch, inplace=True) 
    df1.to_csv(table,index=None)

df=pd.read_csv("Holder_TOP10_now_now.csv",dtype={'code': str})
x=["code","h_top10_name","h_top10_stock_number","h_top10_stock_percent","h_top10_pledge_percent","h_top10_stock_type","h_top10_order","h_top10_date"]
xk="h_top10_name"
y=["股票代码","公司名称","持有数量","占总股比例","质押占其直接持股比例","股份类型","十大股东排名","日期"]
# 股票代码 --十大股东-- 非上市公司-股东
get_R_Holder_TOP10(no_code_company2,"deal/股票代码--十大股东--非上市公司-股东.csv",df,x,xk,y)
# 股票代码 --十大股东--  非上市公司-被控股
get_R_Holder_TOP10(no_code_company,"deal/股票代码--十大股东--非上市公司-被控股.csv",df,x,xk,y)
#  股票代码 --十大股东-- 上市公司
get_R_Holder_TOP10(code_company,"deal/股票代码--十大股东--上市公司.csv",df,x,xk,y,"上市公司")
#  股票代码 --十大股东-- 高管&非高管
get_R_Holder_TOP10(persons,"deal/股票代码--十大股东--高管&非高管.csv",df,x,xk,y,"pid")


df=pd.read_csv("Float_Holder_TOP10_now_now.csv",dtype={'code': str})
x=["code","f_h_top10_name","f_h_top10_stock_number","f_h_top10_stock_percent","f_h_top10_stock_type","f_h_top10_order","f_h_top10_date"]
xk="f_h_top10_name"
y=["股票代码","公司名称","持有数量","占流通股比例","股份类型","十大流通股东排名","日期"]
# 股票代码 --十大流通股东-- 非上市公司-股东
get_R_Holder_TOP10(no_code_company2,"deal/股票代码--十大流通股东--非上市公司-股东.csv",df,x,xk,y)
# 股票代码 --十大流通股东--  非上市公司-被控股
get_R_Holder_TOP10(no_code_company,"deal/股票代码--十大流通股东--非上市公司-被控股.csv",df,x,xk,y)
#  股票代码 --十大流通股东-- 上市公司
get_R_Holder_TOP10(code_company,"deal/股票代码--十大流通股东--上市公司.csv",df,x,xk,y,"上市公司")
#  股票代码 --十大流通股东-- 高管&非高管
get_R_Holder_TOP10(persons,"deal/股票代码--十大流通股东--高管&非高管.csv",df,x,xk,y,"pid")

df=pd.read_csv("Holding_detail_now_now.csv",dtype={'code': str})
map_company2type={df["C_name"][i]:df["C_type"][i] for i in df.index if df["C_name"][i] in no_code_company2|no_code_company}
n=list(map_company2type.keys())
t=[map_company2type[i] for i in n]
df1=pd.DataFrame({"机构名称":n,"类型":t})
df1.to_csv("deal/机构--类型.csv",index=None)


x=["C_name","code","holding_amount","holding_value","rate","D_report_time"]
y=["机构名称","股票代码","持有数量","持有价值","占流通股比例","日期"]
xk="C_name"
def get_R_holding(nodes,table,df,x,xk,y,z=None):
    idx=[i for i in df.index if df[xk][i] in nodes]
    df1=df.loc[idx,x].copy()
    if z=="上市公司":
        y[0]="股东股票代码2"
        df1[xk]=[ Company_name2code[i] for i in df1[xk]]
    elif z=="pid":
        y[0]="pid"
        n_xk=[]
        for i in df1.index:
            code=df1["code"][i]
            name=df1[xk][i]
            if (code,name) in map_code_name2idx:
                ii=map_code_name2idx[(code,name)]
            else:
                ii=len(map_code_name2idx)
                map_code_name2idx[(code,name)]=ii
                map_name_info2idx[(name,code)]=ii
                add_person.append((ii,code,name))
            n_xk.append(ii)
        df1[xk]=n_xk

    en2ch={x[i]:y[i] for i in range(len(x))}
    df1.rename(columns=en2ch, inplace=True) 
    df1.to_csv(table,index=None)

#  非上市公司-股东 --持股-- 股票代码
get_R_holding(no_code_company2,"deal/非上市公司-股东--持股--股票代码.csv",df,x,xk,y)
#  非上市公司-被控股--持股-- 股票代码
get_R_holding(no_code_company,"deal/非上市公司-被控股--持股--股票代码.csv",df,x,xk,y)
#  上市公司 --持股-- 股票代码
get_R_holding(code_company,"deal/股票代码2--持股--股票代码.csv",df,x,xk,y,"上市公司")
#  高管&非高管 --持股-- 股票代码
get_R_holding(persons,"deal/高管&非高管--持股--股票代码.csv",df,x,xk,y,"pid")

# print(add_person)
# print(len(add_person))
#更新高管文件
if len(add_person)>0:
    df=pd.read_csv('deal/高管.csv',dtype={'code': str})
    df1=pd.DataFrame({
        "pid":[ i[0] for i in add_person],
        "姓名":[ i[1] for i in add_person],
        "信息":[ i[2] for i in add_person],
        "简介":[ "-"]*len(add_person),
        "简介更新时间":[ "-"]*len(add_person),
        "性别":[ "-"]*len(add_person),
        "年龄":[ "-"]*len(add_person),
        "学历":[ "-"]*len(add_person)
    })
    df2 = pd.concat([df,df1],ignore_index=True)
    df2.to_csv("deal/高管.csv",index=None,columns=["pid","姓名","信息","简介","简介更新时间","性别","年龄","学历"])

    # 序列化到文件
    with open(r"map_code_name2idx.txt", "wb") as f:
        pickle.dump(map_code_name2idx, f)
    with open(r"map_name_info2idx.txt", "wb") as f:
        pickle.dump(map_name_info2idx, f)