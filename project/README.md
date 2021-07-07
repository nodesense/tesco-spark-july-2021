

CREATE EXTERNAL TABLE iris (sepal_length DECIMAL, sepal_width DECIMAL,
petal_length DECIMAL, petal_width DECIMAL, species STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3a://datasets/iris/'
TBLPROPERTIES ("skip.header.line.count"="1")


CREATE EXTERNAL TABLE orders(
> id int,order_daate string,cust_id int,status string)
> STORED AS PARQUET
> LOCATION ‘/user/abinashparida/parquet’


DROP TABLE popular_movies_parquet;


./scripts/beeline.sh

CREATE EXTERNAL TABLE movies (movieId INT, title STRING, genres STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://namenode:8020/user/movieset/movies'
TBLPROPERTIES ("skip.header.line.count"="1")