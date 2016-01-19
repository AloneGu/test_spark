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

"""
output:
xxx: 16/01/12 6
xxx: BlockManager: 1
xxx: OutputCommitCoordinator$OutputCommitCoordin 1
xxx: hook 1
xxx: SparkContext: 1
xxx: at 1
xxx: shutdown 1
xxx: BlockManager 1
xxx: web 1
xxx: from 1
xxx: MapOutputTrackerMasterEndpoint: 1
xxx: http://10.0.2.15:4040 1
xxx: stopped! 1
xxx: jac@jac-VirtualBox:~/Documents/wei_test_box/spart_test$ 1
xxx: UI 1
xxx: Spark 1
xxx: SparkUI: 1
xxx: MapOutputTrackerMasterEndpoint 1
xxx: MemoryStore 1
xxx: BlockManagerMaster 1
xxx: Invoking 1
xxx: Stopped 1
xxx: cleared 1
xxx: INFO 7
xxx: 18:37:45 6
xxx: BlockManagerMaster: 1
xxx: stopped 2
xxx: MemoryStore: 1
xxx: stop() 1
"""