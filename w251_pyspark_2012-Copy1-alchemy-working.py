
# coding: utf-8

# In[1]:
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("spark://spark1:7077").setAppName('sentiment analysis 1')
sc = SparkContext(conf=conf)

# import standard modules
import json
import re
import time
import glob
import sys

# Import third-party libraries
# might not need some of these
import dask
import dask.bag as db
from boto.s3.connection import S3Connection # Python API to AWS; http://docs.pythonboto.org/en/latest/index.html
from boto.s3.key import Key
import bokeh # http://bokeh.pydata.org/en/latest/
#import pyspark # https://spark.apache.org/docs/0.9.0/python-programming-guide.html

# import modules for analysis
import nltk
from nltk.corpus import stopwords
import matplotlib.pyplot as mp

# import alchemyAPI
# sys.path.insert(1,'/root/alchemyapi_python')
# Create the AlchemyAPI Object
from alchemyapi import AlchemyAPI
import json
al = AlchemyAPI()
print al
#get_ipython().magic(u'matplotlib notebook')


# In[ ]:

# Get data from S3 if the data does not exist

AWS_ACCESS = "AKIAIN36PDKBBMUCTV4Q"
AWS_SECRET = "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"

REDDITS3 = "blaze-data" # Continuum Analytics S3 data; reddit in the reddit/json/RC_YYYY-MM.json
REDDIT_MONTH_KEYS = ['reddit/json/2012/RC_2012-04.json', 'reddit/json/2012/RC_2012-05.json' ]

print glob.glob('data/*')
if not glob.glob('data/RC_2012-*') and True:
    S3conn = S3Connection(AWS_ACCESS, AWS_SECRET)
    mybucket = S3conn.get_bucket(REDDITS3)
    for key in mybucket.list():
        # print key.name.encode('utf-8')
        if key.key in REDDIT_MONTH_KEYS:  # get data for months specified in REDDIT_MONTH_KEYS
            key.get_contents_to_filename(''.join(['data/', key.name.encode('utf-8').split('/')[-1]]))
            print key.key, key.name, key.name.encode('utf-8').split('/')[-1]
            print "downloaded json"


# In[2]:

# load RDD from local disk/s3
data = sc.textFile('data/RC_2012*.json').map(json.loads)


# In[3]:

# data.take(1)


# In[4]:

#donald_trump = data.filter(lambda x: x['subreddit'].lower() == 'the_donald')
donald_trump = data.filter(lambda x: (('donald' in x['body'].lower())                           and ('trump' in x['body'].lower())))
t = time.time()
#print donald_trump.count()
print time.time() - t


# In[5]:

# let's analyze the sentiments about donald trump. Instead of targeted sentiments, we will just look for overall
# sentiment of the comment
# we will also use timestamp of the comment
def sentiment(comment):
    al = AlchemyAPI()
    senti = al.sentiment('text', comment['body'].lower())
    score = senti.get('docSentiment', {}).get('score', 0)
    return (comment.get('created_utc', '0'), (comment['body'], score))
t = time.time()
trump = donald_trump.map(sentiment)                    .sortByKey(ascending=True)
print time.time() - t

trump.saveAsTextFile('trump_sentiment')
# In[ ]:

scores_time = trump.map(lambda x: (x[0], x[1][1])) #.collect()
scores_time.saveAsTextFile('scores_vs_time')

