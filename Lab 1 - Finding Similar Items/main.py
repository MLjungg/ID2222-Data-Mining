from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('MyApp').getOrCreate()
sc = spark.sparkContext

def create_shingles(data, k):
    characters = data.flatMap(lambda x: "".join(x))
    n_grams = characters.flatMap(lambda x: zip(x, x[1:]))
    return n_grams

def load_data(path):
    textFiles = sc.textFile(path)
    return textFiles

textFiles = load_data('./data/bbc/business/001.txt')
create_shingles(textFiles, 3)

words = textFiles.flatMap(lambda x: x.split(" "))
ones = words.map(lambda x: (x, 1))
counts = ones.reduceByKey(lambda x, y: x+y)

print(counts.collect())
