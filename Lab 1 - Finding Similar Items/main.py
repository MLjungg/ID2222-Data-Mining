from pyspark.sql import SparkSession
import os
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.ml.feature import NGram, CountVectorizer, VectorAssembler


def setup_spark():
    spark = SparkSession.builder.master('local[*]').appName('MyApp').getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def create_shingles(data_frames, k):
    new_data_frames = []
    ngram = NGram(n=k, inputCol="Chars", outputCol="Ngrams")

    # Add chars and ngram column to dataframes
    for data_frame in data_frames:
        new_data_frame = data_frame.withColumn("Chars", F.split(data_frame.Text, ""))
        new_data_frame = new_data_frame.withColumn("Chars",
                                                   F.expr("""transform(Chars,x-> regexp_replace(x,"\ ","_"))"""))
        new_data_frame = ngram.transform(new_data_frame)
        new_data_frame = new_data_frame.withColumn("Ngrams",
                                                   F.expr("""transform(Ngrams,x-> regexp_replace(x,"\ ",""))"""))
        new_data_frames.append(new_data_frame)

    # TODO: Make a new dataFrame consisting of: all_ngrams | buss_ngrams | entertainment_ngrams | ..
    # TODO: First "merge" a column of NGrams, i.e one cell should hold all NGram for a specific document. Then merge all five cells.
    df = new_data_frames[0].select("Ngrams")

    return new_data_frames

def load_data():
    data_frames = []

    for _, dirs, _ in os.walk("./data/bbc"):
        for dir in dirs:
            data_frame = sc.wholeTextFiles("./data/bbc/" + dir + "/*.txt").toDF()
            data_frame = data_frame.selectExpr("_1 as Filename", "_2 as Text")
            data_frames.append(data_frame)



    return data_frames


spark, sc = setup_spark()
data_frames = load_data()
create_shingles(data_frames, 3)
