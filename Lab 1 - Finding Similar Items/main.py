from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('MyApp').getOrCreate()
sc = spark.sparkContext

textFile = sc.textFile('./data/bbc/business/*.txt')
words = textFile.flatMap(lambda x: x.split(' '))
ones = words.map(lambda x: (x, 1))
counts = ones.reduceByKey(lambda x, y: x+y)

print(counts.collect())
