from pyspark.sql import SparkSession
from dataset import *
from datetime import datetime, date
#import pandas as pd
#import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys

##Creating Spark Session
spark = SparkSession.builder.getOrCreate()

##import dataset from dataset.py
##dataset with schema
##creating dataframe for dataset with schema
df_withschema=spark.createDataFrame(dataset_withschema)
print("printing car dataset with Schema")
df_withschema.show()

##In printschema we could see relevant datatypes
print("printing schema which has relevant datatypes ")
df_withschema.printSchema()


#setting this parameter to enable the casting of string datatype to datetype
spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')

##creating dataframe for dataset without schema
df_withoutschema=spark.createDataFrame(dataset_withoutschema,col_names)
print("printing car dataset without casting to relevant datatypes")
df_withoutschema.show()

## check the datatypes which displays string dataype even for date column(manufactured_date)
print("printing schema without casting to date")
df_withoutschema.printSchema()

##casting string column to date column
df_castingdate=df_withoutschema.withColumn("manufactured_date",to_date(unix_timestamp(col("manufactured_date"), 'yyyy-MM-dd').cast('timestamp')))
print("Printing car dataset after casting to date datatype from string")
df_castingdate.show()

##In printschema Now we could see manufactured_date column data type to date datatype from string.
print("printing schema after casting to date datatype for manufactured_date column")
df_castingdate.printSchema()

