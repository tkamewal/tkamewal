# Kafka producer for ad impressions
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to publish ad impression data
def publish_ad_impression(ad_impression):
    producer.send('ad_impressions_topic', json.dumps(ad_impression).encode('utf-8'))

# Usage: publish_ad_impression(ad_impression_data)
