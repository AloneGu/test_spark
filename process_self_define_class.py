# prepare test data and map function
from class_define import wifi_data,determine_type
data_dict=[('a',wifi_data('a',1,2)),('b',wifi_data('b',2,3))]
print 'test func',determine_type(data_dict[1][1])

# prepare spark
import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/src/spark-1.6.0-bin-hadoop2.4"

# Append pyspark  to Python Path
sys.path.append("/usr/local/src/spark-1.6.0-bin-hadoop2.4/python")
sys.path.append("/usr/local/src/spark-1.6.0-bin-hadoop2.4/python/lib/py4j-0.9-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

conf = SparkConf().setAppName("test").setMaster("local[4]")
print 'class path',os.path.join(os.path.abspath(os.path.dirname(__file__)),'class_define.py')
sc = SparkContext(conf=conf,pyFiles=[os.path.join(os.path.abspath(os.path.dirname(__file__)),'class_define.py')])
my_rdd = sc.parallelize(data_dict)
print my_rdd.collect()
result = my_rdd.mapValues(determine_type).collectAsMap()
print 'result',result
import time
time.sleep(10)
sc.stop()
#sys.exit(1)