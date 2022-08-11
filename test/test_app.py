#!/usr/bin/env python

import sys
from random import choice
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING
import pytest

if __name__ == '__main__':
    
    config_file = """
    [default]
    bootstrap.servers=localhost:9092
    
    [consumer]
    group.id=python_example_group_1
    
    # 'auto.offset.reset=earliest' to start reading from the beginning of
    # the topic if no committed offsets exist.
    auto.offset.reset=earliest
    """
    
    
    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_string(config_file)
    config = dict(config_parser['default'])
    
    # Create Producer instance
    producer = Producer(config)
    
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    
    # Produce data by selecting random values from these lists.
    topic = "purchases"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']
    
    count = 0
    for _ in range(10):
    
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1
    
    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
    print("messages sent")
    
    
    # Parse the command line.
    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_string(config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    
    # Create Consumer instance
    consumer = Consumer(config)
    
    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        pass
    
    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic], on_assign=reset_offset)
    
    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
    
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

