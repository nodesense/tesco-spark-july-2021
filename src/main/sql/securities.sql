
CREATE EXTERNAL TABLE stock.securities (
                        Symbol STRING,
                        CompanyName STRING,
                        Series  STRING,
                        ListingDate  STRING,
                        PaidUpValue  DOUBLE,
                        MarketLot  BIGINT,
                        ISIN  STRING,
                        FaceValue DOUBLE
                        )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://172.20.0.100:8020/stock/securities'
TBLPROPERTIES ("skip.header.line.count"="1")


SELECt * FROM TABLE stock.securities;

DROP TABLE stock.securities;

CREATE TABLE stock.securities_internal (
                        Symbol STRING,
                        CompanyName STRING,
                        Series  STRING,
                        ListingDate  DATE,
                        PaidUpValue  DOUBLE,
                        MarketLot  BIGINT,
                        ISIN  STRING,
                        FaceValue DOUBLE
                        )

insert into table stock.securities_internal select Symbol, CompanyName, Series,
from_unixtime(unix_timestamp(ListingDate,'dd-MMM-yyyy'),'yyyy-MM-dd'),
PaidUpValue, MarketLot, ISIN, FaceValue
from stock.securities;

SELECT * FROM stock.securities_internal;