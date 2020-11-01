

""" config.py: Environmental and Application Settings """

""" ENVIRONMENT SETTINGS """

# Apache Kafka
bootstrap_servers = 'localhost:9092'
data_encoding = 'utf-8'

""" TWITTER APP SETTINGS """

consumer_api_key = 'Qx133zwJ9m6OVmQi8u8bcK64U'
consumer_api_secret = 'NBf92Chtv8E9gHiUMNIZzNITGEFzDFqkQyb6w4Wtt9wyYcsegp'
access_token = '1117174296873455616-kgYkerVrkMQI3yehdsBYcbT4iuwnMW'
access_token_secret = 'HLlPcpxCAhCnGqd9yund0ozojYK3f5r0f1Bs01cEcGYgW'

""" SENTIMENT ANALYSIS MODEL SETTINGS """

# Name of an existing Kafka Topic to publish tweets to
twitter_kafka_topic_name = 'twitter2'

# Keywords, Twitter Handle or Hashtag used to filter the Twitter Stream
twitter_stream_filter = '#Tesla'

