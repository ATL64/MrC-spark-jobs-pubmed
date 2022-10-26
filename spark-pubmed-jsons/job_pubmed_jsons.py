#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage
import requests
import json
import sys
import time as ti
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

#Create spark session
spark = SparkSession.builder.appName('Pull pubmed data into GCS bucket').getOrCreate()
sc = spark.sparkContext

def parse_and_upload(filename, input_bucket, output_bucket, output_path):
    output_file = filename.split('pubmed_data/')[1]
    output_file = output_path + '/' + output_file
    client = storage.Client()

    bucket = client.get_bucket(output_bucket)
    blob = bucket.blob(output_file)
    #file_exists = storage.Blob(bucket=output_bucket, name=output_file).exists(client)
    file_exists = blob.exists(client)

    if not file_exists:
        parsed_input = asn1.to_json(filename, from_gc=True, input_bucket=input_bucket)
        blob.upload_from_string(parsed_input)

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
