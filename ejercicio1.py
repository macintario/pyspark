import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName = "Practica")

f = sc.textFile("hdfs:///user/input/file1.txt")
w = f.flatMap(lambda l: l.split(" ")).map(lambda word: (word, 1))
w.reduceByKey(lambda x, y: x+y).saveAsTextFile("hdfs:///user/wc_out")



