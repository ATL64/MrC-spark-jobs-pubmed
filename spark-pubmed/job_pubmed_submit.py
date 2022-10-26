#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession
from google.cloud import storage
import requests
import json
import sys
import time as ti

begin_year = int(sys.argv[1])
end_year = int(sys.argv[2])
input_bucket = sys.argv[3]
input_path = sys.argv[4]

#Create spark session
spark = SparkSession.builder.appName('Pull pubmed data into GCS bucket').getOrCreate()
sc = spark.sparkContext

def upload_to_bucket(blob_name, string, bucket):
    """ Upload data to a bucket"""
  
    blob = bucket.blob(blob_name)
    blob.upload_from_string(string)

    #returns a public url
    return blob.public_url


def run_uploads_year(year_url_total, input_bucket, input_path):
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(input_bucket)
    year = year_url_total[0]
    month = year_url_total[1]
    fetch_url = year_url_total[2]
    total_records = year_url_total[3]
    for i in range(0, total_records, 10000):
        while True:
            file_path = input_path + '/' + str(year) + '_' + str(month) + '_num_' + str(i)            
            file_exists = storage.Blob(bucket=bucket, name=file_path).exists(gcs_client)
            if file_exists:
                break
            this_fetch = fetch_url+"&retstart="+str(i)
            with requests.post(this_fetch) as fetch_r:
                final_string_to_upload = fetch_r.content
            bool_api = 'API rate limit exceeded' in final_string_to_upload
            bool_query = 'Unable to obtain query' in final_string_to_upload
            bool_backend = 'Exception from Backend' in final_string_to_upload
            if bool_api or bool_query or bool_backend:
                ti.sleep(3)
                del final_string_to_upload
            else:
                upload_to_bucket(file_path, final_string_to_upload, bucket)
                del final_string_to_upload
                break





# Set pubmed parameters
list_year = range(begin_year,end_year)
list_month = range(1,13)
year_url_total = []
for year in list_year:
    for month in list_month:
        if month!=12:
            search_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&mindate='+str(year)+'/'+str(month)+'/01&maxdate='+str(year)+'/'+str(month+1)+'/01&usehistory=y&retmode=json'
        else:
            search_url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&mindate='+str(year)+'/'+str(month)+'/01&maxdate='+str(year+1)+'/01/01&usehistory=y&retmode=json'
        search_r = requests.post(search_url)
        search_data = search_r.json()
        webenv = search_data["esearchresult"]['webenv']
        total_records = int(search_data["esearchresult"]['count'])
        fetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmax=9999&query_key=1&webenv="+webenv
        temp_list = [year, month, fetch_url, total_records]
        year_url_total.append(temp_list)
        print(str(year)+'_'+str(month))

#print('configurations:')
#print(sc._conf.getAll())
        
dist_urls = sc.parallelize(year_url_total).repartition(12) # Otherwise use (sc.defaultParallelism * 3) 

#print('dist_urls:')
#print(dist_urls)

print('partitions rdd:')
print(dist_urls.getNumPartitions())

print('Partitioning distribution: '+ str(dist_urls.glom().map(len).collect()))

# RDD was distributing unevenly and for some reason could only make it work with dataframe:
dist_urls_df = dist_urls.toDF(['year','month','url'])
dist_urls_df = dist_urls_df.repartition(12) #Preferrably 3*number of cores

print('Partitioning distribution: '+ str(dist_urls_df.rdd.glom().map(len).collect()))

dist_urls_df.rdd.foreach(lambda year_url_total: run_uploads_year(year_url_total, input_bucket, input_path))

