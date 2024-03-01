import pandas as pd 
import json
from datetime import datetime
import s3fs
import re


def run _twitter_etl():

    # STEP 1 : read data from the csv file, filename.csv
    tweets = pd.read_csv('/TwitterDataPipeline_Airflow/data/rawdata.csv')

    # STEP 2 : Clean and Transform the data


    #create a list and structure the JSON data into the list

    tweet_list = []
    for index,tweet in tweets.iterrows():
        #text = tweet._json["full_text"]
        #username = extract_username_tweet(tweet['Tweets'])

        refined_tweet = {"user": 'elonmusk',
                        "text": tweet.Cleaned_Tweets,
                        "favourite_count": tweet.Likes,
                        "retweet_count": tweet.Retweets,
                        "created_at": tweet.Date
                        }
        tweet_list.append(refined_tweet)

    # converting the list to a pandas dataframe
    df = pd.DataFrame(tweet_list)

    # cleaning the data by removing rows that have null values for username and text

    df = df.dropna(subset=['user', 'text'], how='all')

    df = df[(df['user'].notna() & df['user'] != '') | (df['text'].notna() & df['text'] != '')]

    # Update the index so that it starts from 1
    df.index = range(1, len(df) + 1)

    # saving locally as a csv file
    df.to_csv('/TwitterDataPipeline_Airflow/data/cleandata.csv')