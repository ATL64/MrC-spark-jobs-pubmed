## Spark job for parsing Pubmed data
This is a spark job to parse the abstracts from pubmed_jsons and save the
keywords of each abstracts in the folder mrc_keywords.

We first create the cluster in Google Cloud, run this in google cloud shell,
or install gcloud and run in your terminal:

```
gcloud dataproc clusters create mrc --region europe-west3 --metadata 'PIP_PACKAGES=pandas ndjson nltk pytest' --initialization-actions gs://goog-
Waiting on operation [projects/xxxxxx/regions/europe-west3/operations/uuid...].
```


### Submit Job

First you need to create a folder in a bucket of yours and upload the files
`extract_keywords_from_all_abstracts.py` and `mrc_stopwords.py`.

Then, click on 'Jobs->Create Job' in Dataproc. Select your cluster, add the
two python files ans main and complementary files, and as arguments add the
initial year and the final year.
