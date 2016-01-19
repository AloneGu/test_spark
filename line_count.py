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

sc = SparkContext('local')
#words = sc.parallelize(["sSDFcala","javSDFSa","hadSDFoop","spSDFark","aSDFSkka"])
#print words.count()

lines = sc.textFile("data.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
sc.stop()

print lineLengths
print totalLength

# output:
# PythonRDD[3] at RDD at PythonRDD.scala:43
# 3972
