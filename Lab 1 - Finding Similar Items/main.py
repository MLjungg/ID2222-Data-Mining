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
    ngram_frames = []
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

        ngram_frame = new_data_frame.select(F.explode(new_data_frame.Ngrams)).distinct()
        ngram_frames.append(ngram_frame)


    # TODO: Check if all_ngrams grows each iteration
    for i in range(1, len(ngram_frames)):
        all_ngrams = ngram_frames[0].join(ngram_frames[i], on=["col"], how="outer")

    all_ngrams = all_ngrams.distinct()
    all_ngrams = all_ngrams.selectExpr("col as shingles")

    for index, ngram_frame in enumerate(ngram_frames):
        all_ngrams = all_ngrams.join(ngram_frame, all_ngrams.shingles == ngram_frame.col, how="left")
        all_ngrams = all_ngrams.withColumnRenamed("col", document_types[index])



    return all_ngrams

def load_data():
    data_frames = []
    document_types = []

    for _, dirs, _ in os.walk("./data/bbc"):
        for dir in dirs:
            data_frame = sc.wholeTextFiles("./data/bbc/" + dir + "/001.txt").toDF()
            data_frame = data_frame.selectExpr("_1 as Filename", "_2 as Text")
            data_frames.append(data_frame)

            document_types.append(dir)


    return data_frames, document_types

spark, sc = setup_spark()
data_frames, document_types = load_data()
create_shingles(data_frames, 3)

#TODO: Lower case?