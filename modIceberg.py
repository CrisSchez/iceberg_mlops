import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]")\
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type","hadoop") \
    .config("spark.sql.catalog.spark_catalog.type","hive") \
    .getOrCreate()
     
spark.sql("UPDATE spark_catalog.default.telco_iceberg SET tenure = 15 WHERE customerid = '1452-KIOVK'")
#CREATE EXTERNAL TABLE ext_t1 STORED AS ICEBERG TBLPROPERTIES ('iceberg.table_identifier'='default.telco_iceberg');
exec(open("1b_create_iceberg_impala.py").read())

spark.read.format("iceberg").load("spark_catalog.default.telco_iceberg.history").show()
