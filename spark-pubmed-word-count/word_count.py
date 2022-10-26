#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage
import requests
import json
import csv
import sys
import re
import os
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
nltk.download('wordnet')
from nltk.stem.wordnet import WordNetLemmatizer
import asn1
from pyspark.sql import Row

begin_year = int(sys.argv[1])
end_year = int(sys.argv[2])
input_bucket = sys.argv[3]
input_path = sys.argv[4]
output_bucket = sys.argv[5]
output_path = sys.argv[6]

if input_path==output_path:
    raise NameError("Input path and output path are equal!!!")

##Creating a list of stop words and adding custom stopwords
stop_words = set(stopwords.words("english"))
##Creating a list of custom stopwords
new_words = ["using", "show", "result", "large", "also", "iv", "one", "two", "new", "previously", "shown"]
stop_words = stop_words.union(new_words)
# Lemmatization
lem = WordNetLemmatizer()

#Create spark session
spark = SparkSession.builder.appName('Pull pubmed data into GCS bucket').getOrCreate()
sc = spark.sparkContext

def _get_keywords_from_abstracts(abstracts):
    """
    abstracts is a dictionary. Keys are pmids, values are abstracts.
    This function returns a list of 2-tuples (word, pmid).
    """
    keyword_list = []

    for pmid, abstract in abstracts.items():
        # Remove punctuations
        text = re.sub('[^a-zA-Z]', ' ', abstract)
        # Convert to lowercase
        text = text.lower()
        # remove tags
        text = re.sub("&lt;/?.*?&gt;", " &lt;&gt; ", text)
        # remove special characters and digits
        text = re.sub("(\\d|\\W)+", " ", text)
        ##Convert to list from string
        text = text.split()
        text = set(word for word in text if not word in stop_words)
        text = set(lem.lemmatize(word) for word in text if not word in stop_words)
        for word in text:
            #keyword_list += word + ", " + pmid + "\n"
            keyword_list.append([word, pmid])

    return keyword_list

def parse_and_upload(filename, input_bucket, output_bucket, output_path):
    nltk.download('wordnet', download_dir="/tmp")
    nltk.data.path.append('/tmp')
    output_file = filename.split('pubmed_data/')[1]
    output_file = output_path + '/' + output_file
    client = storage.Client()

    bucket = client.get_bucket(output_bucket)
    blob = bucket.blob(output_file)
    #file_exists = storage.Blob(bucket=output_bucket, name=output_file).exists(client)
    file_exists = blob.exists(client)

    if not file_exists:
        abstracts = asn1.get_abstracts(filename, from_gc=True, input_bucket=input_bucket)

        keyword_list = _get_keywords_from_abstracts(abstracts)
        n_chunks = 5
        chunk_len = int(len(keyword_list) / n_chunks)

        if len(keyword_list) != 0:
            for chunk in range(n_chunks):
                if chunk==n_chunks-1:
                    current_keyword_list = keyword_list[chunk_len*chunk:]
                else:
                    current_keyword_list = keyword_list[chunk_len*chunk:chunk_len*(chunk+1)]
                current_output_file = output_file + "_" + str(chunk)
                current_blob = bucket.blob(current_output_file)

                temp_file = '/tmp/words_' + str(chunk) + '.csv'
                with open(temp_file, 'w') as myfile:
                    wr = csv.writer(myfile)
                    for word in current_keyword_list:
                        wr.writerow(word)

                current_blob.upload_from_filename(temp_file)

        """
        if len(keyword_list) != 0:
            temp_file = '/tmp/words.csv'
            with open(temp_file, 'w') as myfile:
                wr = csv.writer(myfile)
                for word in keyword_list:
                    wr.writerow(word)

            blob.upload_from_filename(temp_file)
        """

    return blob.public_url

# Set pubmed parameters
list_year = range(begin_year, end_year)
gcs_client = storage.Client()
gs_files = []
for year in list_year:
    prefix = input_path + '/' + str(year)
    blobs = gcs_client.list_blobs(input_bucket, prefix=prefix)
    for blob in blobs:
        gs_files.append(blob.name)

#print('configurations:')
#print(sc._conf.getAll())

dist_files = sc.parallelize(gs_files).repartition(12)  # Otherwise use (sc.defaultParallelism * 3)

#print('dist_urls:')
#print(dist_urls)

print('partitions rdd:')
print(dist_files.getNumPartitions())

print('Partitioning distribution: ' + str(dist_files.glom().map(len).collect()))

# RDD was distributing unevenly and for some reason could only make it work with dataframe:
row = Row("val") # Or some other column name
dist_files_df = dist_files.map(row).toDF()
dist_files_df = dist_files.repartition(12)  # Preferably 3*number of cores

print('Partitioning distribution: ' + str(dist_files_df.glom().map(len).collect()))

dist_files_df.foreach(lambda filename: parse_and_upload(filename, input_bucket, output_bucket, output_path))
