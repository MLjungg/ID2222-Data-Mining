from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.ml.feature import NGram


def setup_spark():
    spark = SparkSession.builder.master('local[*]').appName('MyApp').getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def create_shingles(data_frames, k):
    new_data_frames = []
    ngram_frame = spark.createDataFrame([], StructType([]))
    ngram = NGram(n=k, inputCol="Chars", outputCol="Ngrams")

    # Add chars and ngram column to dataframes
    for index, data_frame in enumerate(data_frames):
        new_data_frame = data_frame.withColumn("Chars", F.split(data_frame.Text, ""))
        new_data_frame = new_data_frame.withColumn("Chars",
                                                   F.expr("""transform(Chars,x-> regexp_replace(x,"\ ","_"))"""))
        new_data_frame = ngram.transform(new_data_frame)
        new_data_frame = new_data_frame.withColumn("Ngrams",
                                                   F.expr("""transform(Ngrams,x-> regexp_replace(x,"\ ",""))"""))
        new_data_frames.append(new_data_frame)

        ngram_col = F.explode(new_data_frame.Ngrams)
        ngram_frame = ngram_frame.withColumn(document_type[index], ngram_col)

    return new_data_frames

def load_data():
    data_frames = []
    document_type = []

    for _, dirs, _ in os.walk("./data/bbc"):
        for dir in dirs:
            data_frame = sc.wholeTextFiles("./data/bbc/" + dir + "/001.txt").toDF()
            data_frame = data_frame.selectExpr("_1 as Filename", "_2 as Text")
            data_frames.append(data_frame)

            document_type.append(dir)


    return data_frames, document_type

spark, sc = setup_spark()
data_frames, document_type  = load_data()
create_shingles(data_frames, 3)
