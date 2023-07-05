######
#
# BEFORE RUNNING THE SCRIPT,
# INSTALL DE LIBRARY BELOW
# AND FOLLOW THE STEPS
#
######

# DEPENDENCIES
# pip3 install nltk
# python3
#   import nltk
#   nltk.download('stopwords')
#   nltk.download('wordnet')


from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from pyspark import RDD
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
import nltk
import os
import re

os.system("pip3 install nltk")

nltk.download('stopwords')
nltk.download('wordnet')


# CONSTANTS
punctuation_regex = r'[\"\',.:;{}\[\]?!$%&]'
stop_words = stopwords.words("english")
lemmatizer = WordNetLemmatizer()


def create_words_rdd():
    # read from text file, remove symbols, format to lower case and split
    rdd = sc.textFile("Shakespeare_alllines.txt").flatMap(
        lambda line: re.sub(punctuation_regex, "", line).lower().split(" "))
    return rdd


def filter_and_clean_rdd(rdd: RDD):
    # remove stopwords
    filtered_rdd = rdd.filter(lambda x: x not in stop_words)

    # format nouns (remove plural, for example)
    filtered_rdd = filtered_rdd.map(
        lambda word: lemmatizer.lemmatize(word, "n"))

    # change verbs to infinitive
    filtered_rdd = filtered_rdd.map(
        lambda word: lemmatizer.lemmatize(word, "v"))

    # filter any empty space left
    filtered_rdd = filtered_rdd.filter(
        lambda x: x != " " and x != "" and x.split() != "")

    return filtered_rdd


# MAIN PROGRAM
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

limit = int(input("Digite o numero de palavras que serão mostradas: "))

rdd = create_words_rdd()
filtered_rdd = filter_and_clean_rdd(rdd)

print("Top " + str(limit) + " words used by shakespeare")
df = filtered_rdd.map(lambda x: (x, )).toDF(["word"])


df.groupBy('word').count().orderBy('count', ascending=False).show(limit)
