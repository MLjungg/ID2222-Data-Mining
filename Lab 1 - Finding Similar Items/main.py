from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import rand, when, lit
from pyspark.ml.feature import NGram
import itertools
import numpy as np


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
    #characteristic_matrix = characteristic_matrix.withColumn('shingles', rand() * ((2 ** 32) - 1))

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
            data_frame = sc.wholeTextFiles("./data/bbc/" + dir + "/*.txt").toDF()
            data_frame = data_frame.selectExpr("_1 as Filename", "_2 as Text")
            data_frames.append(data_frame)

            document_types.append(dir)

    return data_frames, document_types


def compare_sets(shingles):
    pairwise_document_combinations = list(itertools.combinations(document_types, 2))

    for document1, document2 in pairwise_document_combinations:
        shingles = shingles.withColumn(document1 + "&" + document2 + " union",
                                       shingles[document1].bitwiseOR(shingles[document2]))
        shingles = shingles.withColumn(document1 + "&" + document2 + " intersect",
                                       shingles[document1].bitwiseAND(shingles[document2]))

        union = shingles.select(F.sum(shingles[document1 + "&" + document2 + " union"])).collect()[0][0]
        intersect = shingles.select(F.sum(shingles[document1 + "&" + document2 + " intersect"])).collect()[0][0]

        print(
            "Jaccard similarity between " + document1 + " and " + document2 + " is " + str(round(intersect / union, 2)))


def compare_signatures(signature_matrix):
    pairwise_document_combinations = list(itertools.combinations(range(len(document_types)), 2))

    similar_shingles = 0
    for document1, document2 in pairwise_document_combinations:
        for row in range(signature_matrix.shape[0]):
            if signature_matrix[row, document1] == signature_matrix[row, document2]:
                similar_shingles += 1

        print("Approx jaccard similarity between " + document_types[document1] + " and " + document_types[document2] + " is " + str(round(similar_shingles / signature_matrix.shape[0],2)))
        similar_shingles = 0

def min_hashing(characteristic_matrix, number_of_hash):
    characteristic_matrix = np.array(characteristic_matrix.drop('shingles').collect())
    rows = characteristic_matrix.shape[0]
    primes = get_primes(rows, 2*rows)
    a = np.random.randint(1, max(primes)-1, number_of_hash)
    b = np.random.randint(0, max(primes)-1, number_of_hash)
    p = np.random.choice(primes, number_of_hash)
    signature_matrix = np.ones((number_of_hash, len(document_types)))*np.inf

    for row in range(characteristic_matrix.shape[0]):
        hash_values = [(a[j] * row + b[j]) % p[j] for j in range(number_of_hash)]

        for col in range(characteristic_matrix.shape[1]):
            if characteristic_matrix[row, col] == 1:
                for i in range(number_of_hash):
                    if hash_values[i] < signature_matrix[i, col]:
                        signature_matrix[i, col] = hash_values[i]
    return signature_matrix


# TODO: Copied, add citation
def get_primes(low, high):
    sieve = np.ones(high//3 + (high%6==2), dtype=np.bool)
    for i in range(1,int(high**0.5)//3+1):
        if sieve[i]:
            k=3*i+1|1
            sieve[k*k//3::2*k] = False
            sieve[k*(k-2*(i&1)+4)//3::2*k] = False

    primes = np.r_[2,3,((3*np.nonzero(sieve)[0][1:]+1)|1)]
    primes = primes[primes > low]

    return primes


spark, sc = setup_spark()
data_frames, document_types = load_data()
# A good rule of thumb is to imagine that there are only 20 characters and estimate the number ofk-shingles as 20k.
shingles = create_shingles(data_frames, 3)
characteristic_matrix = create_characteristic_matrix(shingles)
compare_sets(characteristic_matrix)
signature_matrix = min_hashing(characteristic_matrix, 100)
compare_signatures(signature_matrix)

# TODO: Lowercase all words? Remove numbers? Remove special chars.
