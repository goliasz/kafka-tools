#!/usr/bin/env python

# Copyright KOLIBERO under one or more contributor license agreements.  
# KOLIBERO licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import uuid
import argparse
from datetime import datetime
from kafka import KafkaConsumer, SimpleConsumer
from kafka import KafkaProducer
#

def millis_interval(start, end):
    """start and end are datetime instances"""
    diff = end - start
    millis = diff.days * 24 * 60 * 60 * 1000
    millis += diff.seconds * 1000
    millis += diff.microseconds / 1000
    return millis

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Kafka 2 Couchbase")
  parser.add_argument('--kafka_bootstrap_srvs', default="localhost:9092")
  parser.add_argument('--kafka_group_id', default="topic2topic")
  parser.add_argument('--kafka_source_topic', default="snowplow-enriched-good-json")
  parser.add_argument('--kafka_target_topic', default="snowplow-metrics")
  parser.add_argument('--timestamp_col', default="collector_tstamp")

  args = parser.parse_args()
  print "Kafka boostrap servers",args.kafka_bootstrap_srvs
  print "Kafka group id",args.kafka_group_id
  print "Kafka source topic",args.kafka_source_topic
  print "Kafka target topic",args.kafka_target_topic
  print "Kafka timestamp column",args.timestamp_col

  consumer = KafkaConsumer(bootstrap_servers=args.kafka_bootstrap_srvs, group_id=args.kafka_group_id)
  consumer.subscribe([args.kafka_source_topic])

  producer = KafkaProducer(bootstrap_servers=args.kafka_bootstrap_srvs,value_serializer=lambda v: json.dumps(v).encode('utf-8'))


  for msg in consumer:
    #print (msg)
    #
    msgj = json.loads(msg.value)
    #print msgj

    collector_tstamp = msgj.get(args.timestamp_col)
    collector_tstamp_m = None
    if collector_tstamp:
      utc_dt = datetime.strptime(collector_tstamp, '%Y-%m-%dT%H:%M:%S.%fZ')
      collector_tstamp_m = millis_interval(datetime(1970, 1, 1), utc_dt)
      msgj[args.timestamp_col+"_m"] = collector_tstamp_m
    print "Send to ",args.kafka_target_topic
    producer.send(args.kafka_target_topic,msgj)

