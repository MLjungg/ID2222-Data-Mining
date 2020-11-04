from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import rand, when, lit
from pyspark.ml.feature import NGram
import itertools
import math


def setup_spark():
    spark = SparkSession.builder.master('local[*]').appName('MyApp').getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def create_characteristic_matrix(characteristic_matrix):
    for document in document_types:
        characteristic_matrix = characteristic_matrix.withColumn(document,
                                                                 when(characteristic_matrix[document].isNull(),
                                                                      0).otherwise(1))

    # Set a random number in range {0, 2^32-1} to each shingle
    characteristic_matrix = characteristic_matrix.withColumn('shingles', rand() * ((2**32) - 1))

    return characteristic_matrix


def create_shingles(data_frames, k):
    new_data_frames = []
    shingle_frames = []
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

        shingle_frame = new_data_frame.select(F.explode(new_data_frame.Ngrams)).distinct()
        shingle_frames.append(shingle_frame)


    # TODO: Check if all_ngrams grows each iteration
    for i in range(1, len(shingle_frames)):
        all_shingles = shingle_frames[0].join(shingle_frames[i], on=["col"], how="outer")

    all_shingles = all_shingles.distinct()
    all_shingles = all_shingles.selectExpr("col as shingles")

    for index, ngram_frame in enumerate(shingle_frames):
        all_shingles = all_shingles.join(ngram_frame, all_shingles.shingles == ngram_frame.col, how="left")
        all_shingles = all_shingles.withColumnRenamed("col", document_types[index])


    return all_shingles


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


def compare_sets(shingles):
    pairwise_document_combinations = list(itertools.combinations(document_types, 2))

    for document1, document2 in pairwise_document_combinations:
        shingles = shingles.withColumn(document1 + "&" + document2 + " union", shingles[document1].bitwiseOR(shingles[document2]))
        shingles = shingles.withColumn(document1 + "&" + document2 + " intersect", shingles[document1].bitwiseAND(shingles[document2]))

        union = shingles.select(F.sum(shingles[document1 + "&" + document2 + " union"])).collect()[0][0]
        intersect = shingles.select(F.sum(shingles[document1 + "&" + document2 + " intersect"])).collect()[0][0]

        print("Jaccard similarity between " + document1 + " and " + document2 + " is " + str(round(intersect/union, 2)))


spark, sc = setup_spark()
data_frames, document_types = load_data()
shingles = create_shingles(data_frames, 5)
characteristic_matrix = create_characteristic_matrix(shingles)
compare_sets(characteristic_matrix)
signature_matrix = min_hashing(characteristic_matrix)

# TODO: Lowercase all words?


