import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName = "Ejercicio1")

a = sc.textFile("hdfs:///user/input/file1.txt")
b = a.flatMap(lambda l: l.split(" ")).distinct()
b = b.take(5)
print(b)





