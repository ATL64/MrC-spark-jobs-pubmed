# Spark job for Pubmed data
This is a spark job to pull pubmed data (abstracts of all published articles) into GCS with 
parallel downloads and uploads.  I describe here how to do this for beginners.

There are two ways to run this job in this tutorial, either submit the .py file in dataproc, or use the notebook in dataproc.

Either way, we first create the cluster in Google Cloud, run this in google cloud shell, 
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


## Notebook

To use the notebook, go to dataproc in your google cloud console in your browser. 

Click on your cluster --> web interfaces --> Jupyter Notebook --> upload your notebook or copy the contents in a new one.

Please note the notebook is not always up to date, so check that the functions are the same as in the .py file.

## Submit Job

First you need to create a folder in a bucket of yours and upload the .py file. Then:

Click on your cluster --> Submit Job --> Choose "PySpark", specify the path to your .py file in GCS, and specify 4 arguments,
leave the rest empty and click "Create".

The 4 arguments, in this order, are:
1. First year to load the pubmed articles
1. Last year to load the pubmed articles (this year will NOT be included, i.e. 1990 will load until 1989 only)
1. Your bucket name
1. Your path in that bucket, where the files will be stored.

## Notes:
Occasionally it seems the cluster runs into memory issues so no new files are being written to GCS.  In this case you can  create a new cluster and re run the job (it will ignore existing files in GCS and continue with the rest).  Will try to merge a fix soon.

## Reference:

https://www.ncbi.nlm.nih.gov/books/NBK25500/
