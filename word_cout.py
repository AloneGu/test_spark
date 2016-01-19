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

from operator import add

sc=SparkContext(appName="myword")
lines=sc.textFile('data_2.txt',1)
tmp=lines.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1))
counts=tmp.reduceByKey(add)

output=counts.collect();
for (word,count) in output:
    print("xxx: %s %i" % (word,count))
sc.stop()