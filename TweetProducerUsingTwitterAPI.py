import tweepy
from kafka import KafkaProducer
import logging

consumerKey = "<put the consumer key here>"
consumerSecret = "<put the consumer key secret here>"
accessToken = "<put the token here>"
accessTokenSecret = "<Put the token secret here>"

producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'Bitcoin'
topic_name = 'twitter'

def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api

class TweetListener(tweepy.Stream):
    
    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        print("Hello World")
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False
        if status_code == 404:
            logging.info(raw_data)
            return False

    def start_streaming_tweets(self, search_term):
        print("Hello World")
        print("Hello World")
        try:
            self.filter(track=search_term, stall_warnings=True, languages=["en"])
        except:
            print("Something went wrong")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("tweepy")
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename="tweepy.log")
    logger.addHandler(handler)
    print("Hello World")
    twitter_stream = TweetListener(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    print("Hello World")
    print(consumerKey)
    print(consumerSecret)
    print(accessToken)
    print(accessTokenSecret)
    print(search_term)
    try:
        twitter_stream.start_streaming_tweets(search_term)
    except:
        print("Hello World")
