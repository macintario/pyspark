import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName = "Ejercicio2")

a = sc.textFile("hdfs:///user/input/file2.txt")
b = a.flatMap(lambda l: l.split(" "))
c = b.groupBy(lambda l: len(l)).collect()
d = sorted([(x,sorted(y)) for(x,y) in c])
print(d)



