from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pykafka import KafkaClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import config
from unidecode import unidecode
import re
import sqlite3

def create_db():
    conn = sqlite3.connect('tweets.db')
    c = conn.cursor()
    return conn, c

def create_table(conn, c):
    c.execute("CREATE TABLE IF NOT EXISTS tesla(username TEXT, followers REAL, tweet TEXT, tweet_clean TEXT, sentiment TEXT)")
    conn.commit()


def clean_tweet_text(text):
        '''removing links, hashtags, and retweet handles'''
        text = re.sub(r"http\S+", "", text) 
        text = re.sub(r"#\S+", "", text) 
        text = re.sub(r"RT \S+", "", text) 
        text = re.sub(r"@", "", text)   
        return text     

def assign_sentiment(analyzer, text):
    vs = analyzer.polarity_scores(text)
    sentiment = vs['compound']
    if(sentiment > 0.0): return 'positive'
    if(sentiment == 0.0): return 'neutral'
    if(sentiment < 0.0): return 'negative'
if __name__ == "__main__":

	#Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")

	#Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

    #initialize vader sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Establish sqlite db connection, and populate table 
    conn, c = create_db()
    create_table(conn, c)
    
    client = KafkaClient()
    topic = client.topics[bytes(config.twitter_kafka_topic_name, config.data_encoding)]
    consumer = topic.get_simple_consumer()
    for message in consumer:
         if message is not None:
             data = json.loads(message.value)
             tweet_text = unidecode(data['text'])
             tweet_text_clean = clean_tweet_text(tweet_text)
             user = data['user']['screen_name']
             sent = assign_sentiment(analyzer, tweet_text)
             tweet_follow = data['user']['followers_count']
             c.execute("INSERT INTO tesla (username,followers, tweet, tweet_clean, sentiment) VALUES (?, ?, ?, ?, ?)",
                  (user, tweet_follow,tweet_text, tweet_text_clean, sent))
             conn.commit()
            #  print ('username: {} \n tweet: {} \n followers: {} \n sentiment: {}'.format(user, tweet_text, tweet_follow, sent))
                        
                
    ssc.start()
    ssc.awaitTermination()