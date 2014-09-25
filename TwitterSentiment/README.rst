TwitterSentiment
=================
TwitterSentiment Analysis application for CDAP.

Overview
--------
The TwitterSentiment application analyzes the sentiments of Twitter tweets and categorizes them as
either positive, negative or neutral.  Key features include:

- Real-time collection of data from Twitter (requires `Twitter Configuration` to be setup)
- Real-time statistics on analyzed tweets with breakdown by sentiment

Implementation Details
----------------------
The TwitterSentiment application is primarily composed of:
- A stream for ingesting data into the system
- ``SentimentAnalysisFlow`` - collects and emits tweets based upon a sample stream from
  Twitter, analyzes the sentiment of the text, and stores the results. 
- Datasets - ``Table`` and ``TimeseriesTable`` provide persistence for analytics algorithms and
  store results
- ``SentimentQueryProcedure`` - to query the datasets and serve this information to the front end
- A simple single-page web UI

The main part of the application is the ``SentimentAnalysisFlow`` that ingests and collects
tweet data, analyzes the tweet text, and stores the results. 

|(SentimentFlow)|

Statements can optionally be ingested into the stream, which feeds into the ``Normalization``
flowlet. This flowlet deserializes the simple statements into a ``Tweet`` object and then passes the
``Tweet`` object to the analyzer flowlet.

By retrieving a sample stream of tweets from Twitter, the ``TweetCollector`` flowlet also produces
``Tweet`` objects and passes them to the ``Normalization`` flowlet and then to the analyze flowlets for
processing.

As the tweets arrive to the analyzer flowlet, the ``ExternalProgramFlowlet`` is responsible for
passing them to a python script which uses NLTK to yield a sentiment for each tweet.

After the tweet is analyzed, it proceeds to the ``Update`` flowlet which persists the data to a
timeseries table based upon the tweet’s timestamp. It also updates a table which keeps track of
the running total of each sentiment.

The stored data can be queried by sending requests to the sentiment-query procedure. This
procedure exposes 3 endpoints: 

- ``aggregates`` - yields a running total of each sentiment.
- ``sentiments`` - yields a list of tweets for a given sentiment, from the past 300 seconds (this
  time is configurable, as an argument in the http request). 
- ``counts`` - yields the count of tweets for a given sentiment, from the past 300 seconds (this
  time argument is also configurable). 


Installation & Usage
====================

Build the Application jar::

  mvn clean package

Deploy the Application to a CDAP instance defined by its host (defaults to localhost)::

  bin/app-manager.sh --host [host] --action deploy

Start Application Flows and Procedures::

  bin/app-manager.sh --host [host] --action start

Make sure they are running::

  bin/app-manager.sh --host [host] --action status

Ingest sample statements::

  bin/ingest-statements.sh --host [host]

Run Web UI::

  mvn -Pweb jetty:run [-Dcdap.host=hostname] [-Dcdap.port=port]

(optionally use ``-Dcdap.host=hostname`` and ``-Dcdap.port=port`` to point to CDAP,
``localhost:10000`` is used by default)

Once the Web UI is running, it can be viewed at http://localhost:8080/TwitterSentiment/

Configuration
=============

Twitter Configuration
---------------------
In order to utilize the TweetCollector flowlet, which pulls a small sample stream via the Twitter
API, the Twitter API key and Access token to use must be configured. Follow the steps provided by
Twitter to obtain `OAuth access tokens`_.

.. _OAuth access tokens: https://dev.twitter.com/oauth/overview/application-owner-access-tokens

These configurations must be provided as runtime arguments to the flow prior to starting it, in
order to use the ``TweetCollector`` flowlet. To avoid this, configure the ``disable.public``
argument as described below.

Flow Runtime Arguments
----------------------
When starting the ``TwitterSentimentAnalysis`` flow from the UI, the following runtime arguments
can be specified:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``disable.public``
     - Specify any value for this key in order to disable the source flowlet ``TweetCollector``.
   * - ``oauth.consumerKey``
     - Use the value shown under "Application Settings" -> "API key" from the `Twitter
       Configuration`_ setup.
   * - ``oauth.consumerSecret``
     - Use the value shown under "Application Settings" -> "API secret" from the `Twitter
       Configuration`_ setup.
   * - ``oauth.accessToken``
     - Use the value shown under "Your access token" -> "Access token" from the `Twitter
       Configuration`_ setup.
   * - ``oauth.accessTokenSecret``
     - Use the value shown under "Your access token" -> "Access token secret" from the `Twitter
       Configuration`_ setup.


License
=======

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License
is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing permissions and limitations under
the License. 


.. |(SentimentFlow)| image:: docs/img/sentiment-flow.png
