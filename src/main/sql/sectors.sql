

CREATE DATABASE stock;

CREATE EXTERNAL TABLE stock.sectors (
                        CompanyName STRING,
                        Industry STRING,
                        Symbol STRING,
                        Series STRING,
                        ISIN STRING
                        )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://172.20.0.100:8020/stock/sectors'
TBLPROPERTIES ("skip.header.line.count"="1")


SELECT * FROM stock.sectors;