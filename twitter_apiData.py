import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs

def run _twitter_etl():
    
    consumer_key = "<REPLACE>"
    consumer_secret = "<REPLACE>"
    access_key = "<REPLACE>"
    access_secret = "<REPLACE>"

    # STEP 1 : create connection between this python code and twitter API
    # Tweeter authentication
    auth = tweepy.OAuth1UserHandler(consumer_key,consumer_secret, access_key, access_secret)

    # Creating an API object
    api = tweepy.API(auth)

    tweets = api.home_timeline(screen_name='@elonmusk',
                                count = 200, # max allowed count
                                include_rts =False,
                                tweet_mode = 'extended' 
                                )

    # STEP 2 : Clean and Transform the data
    #create a list and structure the JSON data into the list

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        "text": text,
                        "favourite_count": tweet.favourite_count,
                        "retweet_count": tweet.retweet_count,
                        "created_at": tweet.created_at
                        }
        tweet_list.append(refined_tweet)

    # converting the list to a pandas dataframe and saving locally as a csv file

    df = pd.DataFrame(tweet_list)
    df.to_csv("/cleaned_twitter_data.csv")


