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
from kafka import KafkaConsumer
from pubnub import Pubnub
import argparse

def error(message):
    print("ERROR : " + str(message))

def connect(message):
    print("CONNECTED")

def reconnect(message):
    print("RECONNECTED")


def disconnect(message):
    print("DISCONNECTED")

def callback(message, channel):
    print(message)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Kafka 2 Couchbase")
  parser.add_argument('--pubnub_subkey', default="demo")
  parser.add_argument('--pubnub_pubkey', default="demo")
  parser.add_argument('--pubnub_channel', default="my_channel")
  parser.add_argument('--kafka_bootstrap_srvs', default="localhost:9092")
  parser.add_argument('--kafka_group_id', default="snowplow_k2c")
  parser.add_argument('--kafka_source_topic', default="snowplow-enriched-good-json")


  args = parser.parse_args()
  print "PUBNUB Sub Key:",args.pubnub_subkey
  print "PUBNUB Pub Key:",args.pubnub_pubkey
  print "PUBNUB Channel:",args.pubnub_channel
  print "Kafka boostrap servers",args.kafka_bootstrap_srvs
  print "Kafka group id",args.kafka_group_id
  print "Kafka source topic",args.kafka_source_topic

  #
  consumer = KafkaConsumer(bootstrap_servers=args.kafka_bootstrap_srvs, group_id=args.kafka_group_id)
  consumer.subscribe([args.kafka_source_topic])

  pubnub = Pubnub(publish_key=args.pubnub_pubkey, subscribe_key=args.pubnub_subkey)

  for msg in consumer:
    key = str(uuid.uuid4())
    msgj = json.loads(msg.value)
    #print msgj
    print "Write to pubnub"
    pubnub.publish(args.pubnub_channel, msgj, error=callback)
