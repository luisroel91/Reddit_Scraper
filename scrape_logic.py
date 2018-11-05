"""
This script
"""

from pymongo import MongoClient
from bson import objectid
import pandas as pd
import datetime as dt
import praw
import gridfs

mongo_client = MongoClient().reddit_scraped_items
collection_name = input("Please enter sub to scrape: ")
db_collection_name = mongo_client[collection_name]
mongo_filesystem = gridfs.GridFS(mongo_client)

reddit_client = praw.Reddit(
    client_id='Insert your client ID here',
    client_secret='Insert your secret here',
    username='Insert your username here',
    password='Insert your password here',
    user_agent='reddit_scraper v.01'
)

subreddit = reddit_client.subreddit(str(collection_name))
top_posts = subreddit.top(limit=100)
hot_posts = subreddit.hot(limit=100)

data_dict = {
    'title': [],
    'score': [],
    'id': [],
    'url': [],
    'comms_num': [],
    'created': [],
    'body': []
}

for post in hot_posts:
    data_dict['title'].append(post.title)
    data_dict['score'].append(post.score)
    data_dict['id'].append(post.id)
    data_dict['url'].append(post.url)
    data_dict['comms_num'].append(post.num_comments)
    data_dict['created'].append(post.created)
    data_dict['body'].append(post.selftext)

data = pd.DataFrame(data_dict)


def get_date(created):
    return dt.datetime.fromtimestamp(created)


_timestamp = data['created'].apply(get_date)
data = data.assign(timestamp=_timestamp)

insert_to_db = data.to_dict(orient='records')
db_collection_name.insert_many(insert_to_db)
