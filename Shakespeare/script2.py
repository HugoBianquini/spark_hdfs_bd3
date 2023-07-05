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

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark import RDD


# CONSTANTS
punctuation_regex = r'[\"\',.:;{}\[\]?!$%&]'


def create_words_rdd():
    # read from text file, remove symbols, format to lower case and split
    rdd = sc.textFile("Shakespeare_alllines.txt").flatMap(
        lambda line: re.sub(punctuation_regex, "", line).lower().split(" "))
    return rdd


def categorize_words(word: str):
    from nltk.stem.wordnet import wn
    word_type_list = wn.synsets(word)
    word_type = 'n'
    if (len(word_type_list) > 0):
        word_type = word_type_list[0].pos()

    if word_type == 'n':
        return 'noun'
    elif word_type == 'v':
        return 'verb'
    elif word_type == 'r':
        return 'adverb'
    elif word_type == 'a':
        return 'adjective'
    else:
        return 'noun'


def filter_and_clean_rdd(rdd: RDD):
    from nltk.corpus import stopwords
    from nltk.stem.wordnet import WordNetLemmatizer
    stop_words = stopwords.words("english")
    lemmatizer = WordNetLemmatizer()

    # remove stopwords
    filtered_rdd = rdd.filter(lambda x: x not in stop_words)

    # format nouns (remove plural, for example)
    filtered_rdd = filtered_rdd.map(
        lambda word: (lemmatizer.lemmatize(word, "n"), categorize_words(word)))

    # change verbs to infinitive
    filtered_rdd = filtered_rdd.map(
        lambda word: (lemmatizer.lemmatize(word[0], "v"), word[1]))

    # filter any empty space left
    filtered_rdd = filtered_rdd.filter(
        lambda x: x[0] != " " and x[0] != "" and x[0].split() != "")

    return filtered_rdd


# MAIN PROGRAM
spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

limit = int(input("Digite o numero de palavras que serão mostradas: "))

rdd = create_words_rdd()
filtered_rdd = filter_and_clean_rdd(rdd)

print(filtered_rdd.collect())

print("Top " + str(limit) + " words used by shakespeare")
df = filtered_rdd.map(lambda x: (x, )).toDF(["word", "category"])
# new_f = udf(categorize_words, StringType())
# df.withColumn("category", new_f("word"))


# print("Top " + limit + " verbs used by shakespeare")

# print("Top " + limit + " adjectives used by shakespeare")

# print("Top " + limit + " nouns used by shakespeare")


df.groupBy('word').count().orderBy('count', ascending=False).show(limit)
