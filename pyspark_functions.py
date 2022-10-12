from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from datetime import datetime, date
#import pandas as pd
#import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys

### create a spark data frame from scratch
df = spark.createDataFrame([
    Row(car='Audi', date=date(2021, 1, 10), color='red', price=int(20000), sold=1000),
    Row(car='Audi', date=date(2021, 1, 20), color='red', price=int(300000), sold=500),
    Row(car='Audi', date=date(2021, 1, 3), color='green', price=int(0), sold=100),
    Row(car='Audi', date=date(2021, 1, 4), color='red', price=int(20000), sold=500),
    Row(car='Audi', date=date(2021, 1, 5), color='green', price=int(35000), sold=200),
    Row(car='Audi', date=date(2021, 1, 6), color='green', price=int(0), sold=600),
    Row(car='Audi', date=date(2021, 1, 7), color='red', price=int(350000), sold=300),
    Row(car='BMW', date=date(2021, 1, 1), color='yellow', price=int(10000), sold=100),
    Row(car='BMW', date=date(2020, 1, 20), color='blue', price=int(20000), sold=150),
    Row(car='BMW', date=date(2020, 1, 3), color='blue', price=int(25000), sold=80),
    Row(car='BMW', date=date(2020, 1, 4), color='yellow', price=int(15000), sold=90),
    Row(car='BMW', date=date(2020, 1, 5), color='yellow', price=int(0), sold=90),
    Row(car='BMW', date=date(2020, 1, 6), color='blue', price=int(25000), sold=80),
    Row(car='BMW', date=date(2020, 1, 7), color='blue', price=int(25000), sold=60),
])

print("printing car details with Row based data")
df.show()
df.printSchema()

data_list=(['Audi', "2021-1-10",'red',20000,1000],
['Audi', "2021-1-20",'red',300000,500],
['Audi', "2021-1-3",'green',0,100],
['Audi', "2021-1-4",'red',20000,500],
['Audi', "2021-1-5",'green',35000,200],
['Audi', "2021-1-6",'green',0,600],
['Audi', "2021-1-7",'red',350000,300],
['BMW', "2021-1-1",'yellow',10000,100],
['BMW', "2020-1-20",'blue',20000,150],
['BMW', "2020-1-3",'blue',25000,80],
['BMW', "2020-1-4",'yellow',15000,90],
['BMW', "2020-1-5",'yellow',0,90],
['BMW', "2020-1-6",'blue',25000,80],
['BMW', "2020-1-7",'blue',25000,6])

schema=('car','manufactured_date','color','price','sold_num')
##setting this parameter to enable the date cast
spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')
dfl=spark.createDataFrame(data_list,schema)
print("printing car details with list data")
dfl.show()

##casting string column to date column
dfdt=dfl.withColumn("manufactured_date",to_date(unix_timestamp(col("manufactured_date"), 'yyyy-MM-dd').cast('timestamp')))
print("After transformed to date")
dfdt.show()

dfdt.printSchema()

