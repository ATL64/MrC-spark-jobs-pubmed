## Spark job for parsing the Pubmed abstracts and assigning their words to pmids
This is a spark job to parse the abstracs from pubmed data and upload the words of each abstract tegether with their corresponding pmid in csv files.

Let's suppose that some abstract is: "This article is a review of the different publications on breast cancer in men.". Let's suppose that the pmid associated to this abstract is 123456.
Then, the output csv file will have the following rows:

```
article,123456
review,123456
different,123456
publication,123456
breast,123456
cancer,123456
man,123456
```

Note that lemmatization is applied and stop words are ignored.

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

First you need to create a folder in a bucket of yours and upload the word_count.py file. 
Also upload the parser modules in the json parser repo. For this job, we uploaded the asn1 module that can be found in the mrc-utils repository, in order to use the function `get_abstracts`.

Then:

Click on your cluster --> Submit Job --> Choose "PySpark", specify the path to your .py file in GCS, and specify 6 arguments,
leave the rest empty and click "Create".

The 6 arguments, in this order, are:
1. First year to load the pubmed articles
2. Last year to load the pubmed articles (this year will NOT be included, i.e. 1990 will load until 1989 only)
3. Your input bucket name (where the pubmed raw files are)
4. Your path in the input bucket, where the files are stored.
5. Your output bucket name
6. Your output path name were csv files will be stored.
