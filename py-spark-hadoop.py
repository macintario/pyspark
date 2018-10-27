import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName = "Practica")
raw_content = sc.textFile("hdfs:///user/input/file1.txt")
raw_content.count()
raw_content.saveAsTextFile("hdfs:///user/output")
sc.parallelize(raw_content.take(5)).saveAsTextFile("hdfs:///user/output2")
