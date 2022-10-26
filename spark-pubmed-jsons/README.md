## Spark job for parsing Pubmed data
This is a spark job to parse pubmed data and upload it to GCS with parallel downloads and uploads.

We first create the cluster in Google Cloud, run this in google cloud shell, 
or install gcloud and run in your terminal:

```
gcloud beta dataproc clusters create choose_your_cluster_name --optional-components=ANACONDA,JUPYTER 
--image-version=1.3 --enable-component-gateway --bucket your_bucket_name --region europe-west4 --project your_project_id --num-workers 4
```

You need to replace three parameters in that command: 
- your desired cluster name
- your google cloud project_id
- your google cloud storage bucket name

Also, if you want to use more than 4 worker nodes, you need to change that parameter as well, but I would not recommend it since in my experimentation it seems that more than this puts too much load on NCBI backend.


### Submit Job

First you need to create a folder in a bucket of yours and upload the job_pubmed_jsons.py file. 
Also upload the parser modules in the json parser repo.

Then:

Click on your cluster --> Submit Job --> Choose "PySpark", specify the path to your .py file in GCS, and specify 6 arguments,
leave the rest empty and click "Create".

The 6 arguments, in this order, are:
1. First year to load the pubmed articles
1. Last year to load the pubmed articles (this year will NOT be included, i.e. 1990 will load until 1989 only)
1. Your input bucket name (where the pubmed raw files are)
1. Your path in the input bucket, where the files are stored.
1. Your output bucket name
1. Your output path name were JSONs will be stored.

#####Not sure where, but the parser module file path in the bucket should also be specified.

