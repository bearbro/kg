import pandas as pd
import os
for file in os.listdir():
    if file[-4:]!=".csv":
        continue
    print(file)
    df=pd.read_csv(file,dtype={"股票代码":str,"股票代码2":str,"股东股票代码2":str})
    df.fillna("-", inplace = True)
    df=df.applymap(lambda x: x if x not in["","--","---"] else "-")
    df.to_csv(file,index=None)