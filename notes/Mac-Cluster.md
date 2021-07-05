open terminal 1

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master

Now check, http://localhost:8080/

open terminal 2

<<REPLACE THE COMMAND WITH SPARK MASTER URL>>

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.110:7077

 


open terminal 3


<<REPLACE THE COMMAND WITH SPARK MASTER URL>>

$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.110:7077