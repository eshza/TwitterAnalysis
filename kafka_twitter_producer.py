import config
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import pykafka

#Creating authentication object, setting up access token and secret, Creating the API object using authentication info
auth = OAuthHandler(config.consumer_api_key, config.consumer_api_secret)
auth.set_access_token(config.access_token, config.access_token_secret)
api = tweepy.API(auth)

class KafkaTwitterProducer(StreamListener):

    def __init__(self):
        ''' creates a pykafka client using 
        the bootstrap servers, the locations of which may 
        be found in config.py and a Kafka producer that associates 
        the producer to the twitter_kafka_topic_name Kafka topic 
        also defined in config.py'''
        self.client = pykafka.KafkaClient(config.bootstrap_servers)
        self.producer = self.client.topics[bytes(config.twitter_kafka_topic_name, config.data_encoding)].get_producer()

    def on_data(self, data):
        '''publishes the data to the Kafka topic.'''
        self.producer.produce(bytes(data, config.data_encoding))
        return True

    def on_error(self, status):
        '''prints the error to the console and goes
         on to process the next message'''
        print(status)
        return True

if __name__ == '__main__':

    # instantiate a Twitter stream using the Stream module of the tweepy library
    print("Instantiating a Twitter Stream and publishing to the '%s' Kafka Topic..." % config.twitter_kafka_topic_name)
    twitter_stream = Stream(auth, KafkaTwitterProducer())

    print("Filtering the Twitter Stream based on the query '%s'..." % config.twitter_stream_filter)
    twitter_stream.filter(track=[config.twitter_stream_filter], languages=["en"])