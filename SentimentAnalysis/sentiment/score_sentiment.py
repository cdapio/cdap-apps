#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2013-2014 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import sys
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(__file__))

import re
import csv
import pprint
import nltk.classify
import pickle
import time
import json

stopWords = []
featuresDict = {}

logFile = open('/tmp/score_sentiment.log', 'w')
def log(toLog):
  # logFile.write(str(time.time()) + ":  " + toLog)
  logFile.write(str(toLog) + "\n")
  logFile.flush()

#---------------------------------------------------------------------------------
# Reads the tweets from standard input and trains the model.
#---------------------------------------------------------------------------------
f = open(script_dir + '/data/naive-bayes.model.pickle')
classifier = pickle.load(f)
f.close()

#---------------------------------------------------------------------------------
# Pre-Processes a tweet
#---------------------------------------------------------------------------------
def process(tweet):
  # lower case the tweet.
  tweet = tweet.lower()

  # remove url. 
  tweet = re.sub('((http://[^\s]+)|(www\.[\s]+)|(https?://[^\s]+))','URL',tweet)

  # Convert user name to USER
  tweet = re.sub('@[^\s]+', 'USER', tweet)

  # Remove any additional space
  tweet = re.sub('[\s]+', ' ', tweet)

  # Replace #word with word
  tweet = re.sub(r'#([^\s]+)', r'\1', tweet)

  # trim
  tweet = tweet.strip('\'"')

  return tweet


#---------------------------------------------------------------------------------
# Loads a list of lines from a file.
#---------------------------------------------------------------------------------
def load(filename):
  words = []
  fp = open(script_dir + '/' +  filename, 'r')
  line = fp.readline()
  while line:
    word = line.strip()
    words.append(word)
    line = fp.readline()
  fp.close()
  return words

#---------------------------------------------------------------------------------
# Split the tweet into vector of words.
#---------------------------------------------------------------------------------
def vector(tweet, stopWords):
  pattern = re.compile(r"(.)\1{1,}", re.DOTALL)
  vector = []
  words = tweet.split()
  for w in words:
    w = pattern.sub(r"\1\1", w)
    w = w.strip('\'"?,.')
    val = re.search(r"^[a-zA-Z][a-zA-Z0-9]*[a-zA-Z]+[a-zA-Z0-9]*$", w)
    if not val or not w in stopWords:
      vector.append(w)
  return vector

#---------------------------------------------------------------------------------
# Convert the tweets into the fixed dimensions.
#---------------------------------------------------------------------------------
def dimension(tweet):
  tweet_words = set(tweet)
  features = dict(featuresDict)
  for word in tweet_words:
    features['contains(%s)' % word] = True
  return features

#---------------------------------------------------------------------------------
# Generates a sentiment for a tweet based on the trained model.
#---------------------------------------------------------------------------------
def sentiment(tweet):

  # process tweet.
  processedTweet = process(tweet)

  # Get vectors from processed tweet.
  v = vector(processedTweet, stopWords)

  # Add to training data
  sentiment = classifier.classify(dimension(v))

  return sentiment


def main():
  """ 
  Load stop words and feature dictionary. Translate the dictionary 
  into set.
  """
  stopWords = load('data/stopwords.txt');
  stopWords.append('USER')
  stopWords.append('URL')
  featureDict = load('data/features.txt')

  for word in featureDict:
    featuresDict['contains(%s)' % word] = False

  while True:
    try:
      tweets = sys.stdin.readline()
    except KeyboardInterrupt:
      break
    
    try:
      tweet = json.loads(tweets)
      tweet["sentiment"] = sentiment(tweet["text"])
      print json.dumps(tweet)
      sys.stdout.flush()

    except:
      log("failed to analyze tweet: " + tweets)
      print "Failed to process tweet"
      sys.stdout.flush()
if __name__ in "__main__":
  main()
