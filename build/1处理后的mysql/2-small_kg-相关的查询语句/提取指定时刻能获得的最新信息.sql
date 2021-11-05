set @now_time = "2019-12-15";

/*
# 股本结构-总体股本结构
CREATE TABLE EquityStructure_now as 
SELECT EquityStructure.*
FROM EquityStructure, (select Max(report_time) as max_tim ,`code` FROM EquityStructure WHERE report_time <= @now_time GROUP BY `code` ) as t2
WHERE EquityStructure.`code`=t2.`code` and EquityStructure.report_time=t2.max_tim
ORDER BY EquityStructure.`code`
 
*/

/*
# 股东研究-十大流通股股东
CREATE TABLE Float_Holder_TOP10_now as 
SELECT Float_Holder_TOP10.*
FROM Float_Holder_TOP10, (select Max(f_h_top10_date) as max_tim ,`code` FROM Float_Holder_TOP10 WHERE f_h_top10_date <= @now_time GROUP BY `code` ) as t2
WHERE Float_Holder_TOP10.`code`=t2.`code` and Float_Holder_TOP10.f_h_top10_date=t2.max_tim
ORDER BY Float_Holder_TOP10.`code`
*/

/*
# 股东研究-十大股东
CREATE TABLE Holder_TOP10_now as 
SELECT Holder_TOP10.*
FROM Holder_TOP10, (select Max( h_top10_date) as max_tim ,`code` FROM Holder_TOP10 WHERE  h_top10_date <= @now_time GROUP BY `code` ) as t2
WHERE Holder_TOP10.`code`=t2.`code` and Holder_TOP10.h_top10_date=t2.max_tim
ORDER BY Holder_TOP10.`code`
*/

# 主力持仓-机构持仓明细
CREATE TABLE Holding_detail_now as 
SELECT Holding_detail.*
FROM Holding_detail, (select Max( D_report_time) as max_tim ,`code` FROM Holding_detail WHERE  D_report_time <= @now_time GROUP BY `code` ) as t2
WHERE Holding_detail.`code`=t2.`code` and Holding_detail.D_report_time=t2.max_tim
ORDER BY Holding_detail.`code`