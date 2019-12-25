import baostock as bs
import pandas as pd

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)
# result = pd.DataFrame(data_list, columns=rs.fields)
# #### 结果集输出到csv文件 ####
# result.to_csv("D:/history_k_data.csv", encoding="gbk", index=False)
# print(result)

#### 登出系统 ####
bs.logout()