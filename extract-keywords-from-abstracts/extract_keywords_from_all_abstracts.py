#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from google.cloud import storage
import requests
import json
import sys
import re
import os
import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet
import ndjson
import pandas as pd

# Custom packages
import mrc_stopwords
stopwords = mrc_stopwords.stopwords

nltk.download('wordnet')

begin_year = int(sys.argv[1])
end_year = int(sys.argv[2])
input_bucket = 'biotech_lee'
input_path = 'pubmed_json'
output_bucket = 'biotech_lee'
output_path = 'mrc_keywords'

if input_path==output_path:
    raise NameError("Input path and output path are equal!!!")

#Create spark session
spark = SparkSession.builder.appName('Pull pubmed data into GCS bucket').getOrCreate()
sc = spark.sparkContext

sc.broadcast(stopwords)

def parse_and_upload(filename, input_bucket, output_bucket, output_path):
    nltk.download('wordnet', download_dir="/tmp")
    nltk.download('punkt', download_dir="/tmp")
    nltk.download('averaged_perceptron_tagger', download_dir="/tmp")
    nltk.data.path.append('/tmp')
    lemmatizer = WordNetLemmatizer()

    output_file = filename.split(input_path+'/')[1]
    output_file = output_path + '/' + output_file
    client = storage.Client()

    # function to convert nltk tag to wordnet tag
    def nltk_tag_to_wordnet_tag(nltk_tag):
        if nltk_tag.startswith('J'):
            return 'a'
        elif nltk_tag.startswith('V'):
            return 'v'
        elif nltk_tag.startswith('N'):
            return 'n'
        elif nltk_tag.startswith('R'):
            return 'r'
        else:
            return None

    def lemmatize_sentence(sentence):
        #tokenize the sentence and find the POS tag for each token
        nltk_tagged = nltk.pos_tag(nltk.word_tokenize(sentence))
        #tuple of (token, wordnet_tag)
        wordnet_tagged = map(lambda x: (x[0], nltk_tag_to_wordnet_tag(x[1])), nltk_tagged)
        lemmatized_sentence = []
        for word, tag in wordnet_tagged:
            if tag is None:
                #if there is no available tag, append the token as is
                lemmatized_sentence.append(word)
            else:
                #else use the tag to lemmatize the token
                lemmatized_sentence.append(lemmatizer.lemmatize(word, tag))
        words = set([w.lower() for w in lemmatized_sentence if any(c.isalpha() for c in w)])
        words = [w for w in words if w not in stopwords]
        return words

    bucket = client.get_bucket(output_bucket)
    out_blob = bucket.blob(output_file)
    #file_exists = storage.Blob(bucket=output_bucket, name=output_file).exists(client)
    file_exists = out_blob.exists(client)

    if not file_exists:
        in_blob = bucket.blob(filename)
        file = in_blob.download_as_string().decode("utf-8")
        output_file_name = 'output_file.csv'

        year = filename.split('/')[-1][:4]
        df = pd.DataFrame(columns=['pmid', 'keywords'])
        x = ndjson.loads(file)
        for article in x:
            if 'abstract' in article['medent'].keys():
                abstract = article['medent']['abstract']
                words = lemmatize_sentence(abstract)
                df2 = pd.DataFrame(words, columns=['keywords'])
                df2.insert(0, 'pmid', article['pmid'])
                df = df.append(df2)
        df['year'] = year
        df.to_csv(output_file_name, index=False, header=False)
        out_blob.upload_from_filename(output_file_name)

    return out_blob.public_url

# Set pubmed parameters
list_year = range(begin_year, end_year)
gcs_client = storage.Client()
gs_files = []
for year in list_year:
    prefix = input_path + '/' + str(year)
    blobs = gcs_client.list_blobs(input_bucket, prefix=prefix)
    for blob in blobs:
        gs_files.append(blob.name)


dist_files = sc.parallelize(gs_files).repartition(12)  # Otherwise use (sc.defaultParallelism * 3)

print('partitions rdd:')
print(dist_files.getNumPartitions())

print('Partitioning distribution: ' + str(dist_files.glom().map(len).collect()))

# RDD was distributing unevenly and for some reason could only make it work with dataframe:
row = Row("val") # Or some other column name
dist_files_df = dist_files.map(row).toDF()
dist_files_df = dist_files.repartition(12)  # Preferably 3*number of cores

print('Partitioning distribution: ' + str(dist_files_df.glom().map(len).collect()))

dist_files_df.foreach(lambda filename: parse_and_upload(filename, input_bucket, output_bucket, output_path))
