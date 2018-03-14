# Prerequiste 
# praw (pip3 install praw) and set config.json file

import praw
import logging
import pprint
import json
from datetime import datetime
from random import randint

logging.basicConfig(filename='redditFetcher.log',level=logging.ERROR, format='%(asctime)s %(message)s')

##### Load from Configuration Files
from config_json import config_json
obj = config_json()
config = obj.read()
#print(config)

config_client_id = config['config_client_id']
config_client_secret = config['config_client_secret']
config_user_agent = config['config_user_agent']

config_subreddit =  config['config_subreddits'][randint(0,len(config['config_subreddits'])-1)]
config_limit = config['config_limit']
#print(config_subreddit)
##### Load from Configuration Files DONE
from elasticsearch import Elasticsearch
es = Elasticsearch()

#### Check if exact value exists
def is_es_exists(es,field,value):
	try:
		res = es.search(body={"query": {"match": {field: value}}})
		if (res['hits']['total'] > 0):
			for doc in res['hits']['hits']:
				if(doc['_index']!='.kibana' and doc['_score']>=1.0):
					if(doc['_source'][field]==value):
						return(True)
		return(False)
	except Exception as e:
		return(False)

#### get raw boiler html etc.

g_submission_permalink = None
########## Now connect with Reddit
try:
	reddit = praw.Reddit(client_id=config_client_id,client_secret=config_client_secret,user_agent=config_user_agent)
	print('Logged In .....')

	subreddit = reddit.subreddit(config_subreddit)
	submissions_python = subreddit.new(limit=config_limit)
	
	for submission in submissions_python:
		reddit_doc={}
		if not submission.stickied and not is_es_exists(es,'reddit_permalink',submission.permalink):
			reddit_doc['reddit_title'] = submission.title
			reddit_doc['reddit_score'] = (submission.score)
			reddit_doc['reddit_id'] = (submission.id)
			reddit_doc['reddit_url'] = submission.url  
			reddit_doc['reddit_permalink'] = (submission.permalink)
			reddit_doc['reddit_subreddit_id'] = (submission.subreddit_id)
			reddit_doc['reddit_author_name'] = (submission.author.name)
			reddit_doc['reddit_subreddit_name_prefixed'] = (submission.subreddit_name_prefixed)
			reddit_doc['reddit_created_utc'] = (submission.created_utc)
			reddit_doc['reddit_selftext_html'] = (submission.selftext_html)
			reddit_doc['reddit_domain'] = (submission.domain)
			reddit_doc['reddit_is_self'] = (submission.is_self)
			reddit_doc['reddit_ups'] = (submission.ups)
			reddit_doc['reddit_downs'] = (submission.downs)

			g_submission_permalink = (submission.permalink)
			py_is_html_fetched = submission.is_self
			py_dt_created = (str(datetime.now())[0:19])
			py_dt_updated = py_dt_created

			reddit_comments_list = []
			submission.comments.replace_more(limit=None)
			for comment in submission.comments.list():
			    reddit_comments_list.append(comment.body)

			temp_dict = None
			try:
				reddit_doc['reddit_comments_list'] = reddit_comments_list
			except Exception as e:
				reddit_doc['reddit_comments_list'] = None
				pass


			reddit_doc['py_created_utc'] = datetime.utcfromtimestamp(submission.created_utc)
			reddit_doc['py_dt_created'] = py_dt_created
			reddit_doc['py_dt_updated'] = py_dt_updated
	
			print('========================-------------')
			res = es.index(index="reddit_index", doc_type='reddit_doc', body=reddit_doc)
			logging.error('INSERTED DOC :: ' + (submission.permalink))

			if(len(reddit_comments_list)>25):
				time.sleep(15)


except Exception as e:
	logging.error('FAILED DOC :: ' + str(g_submission_permalink) + ' :: ' + str(e))
	

