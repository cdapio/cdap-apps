========================
Web Analytics using CDAP
========================

Overview
============
Performing analytics on a web application using access logs is a common use case when managing a web site.
A system capable of that needs to ingest logs and implement realtime or batch processing computations
to process the data. The information has to be stored somewhere in the system, and
the system should expose ways to retrieve it. Even in this case, where the system performs very simple analytics
such as counting the number of visits made to a website in a day, the components needed to make it possible demand
a lot of work.

Wise, which stands for *Web Insights Engine Application*, is such a system built on the Cask Data Application Platform (CDAP_)
that is easy, concise, and powerful. Wise extracts information from web server access logs, counts visits made
by different IP addresses seen in the logs in realtime, and computes the bounce ratio of
each web page encountered using batch processing.

.. _CDAP: http://cdap.io

The Wise application uses these CDAP constructs to analyze web server logs:

- **Stream:** Ingests log data in realtime
- **Flow:** Computes web page visits counts per IP address based on the log data in realtime
- **Datasets:** Store web page visits counts and bounce ratio based on custom data access patterns
- **MapReduce:** Computes the bounce ratio of each web page present in the log data
- **Service:** Exposes HTTP APIs to query the page visit counts per IP address

On top of those, the **Explore** feature of CDAP can be used to run SQL queries on the data stored
by Wise.


Overview of Wise
================
Let's first have a look at a diagram showing an overview of the Wise application's architecture:

.. image:: docs/img/wise_architecture_diagram.png

- The Wise application has one Stream, ``logEventStream``, which receives web server access logs. It sends the events
  it receives to two CDAP components: the ``WiseFlow`` Flow and the ``WiseWorkflow`` Workflow.

- ``WiseFlow`` has two Flowlets. The first, ``parser``, extracts information from the logs received from the
  Stream. It then sends the information to the second Flowlet, ``pageViewCount``, whose role is to store
  the information in a custom-defined Dataset, ``pageViewStore``.

- ``WiseWorkflow`` executes a MapReduce every ten minutes. The input of this job are events from the Stream
  which have not yet been processed by the Workflow. For each web page recorded in the
  access logs, the MapReduce program counts the number of times people have "bounced" from it.
  A "bounce" is counted whenever a user's activity stops for a specified amount of time.
  The last page they visited is counted as a bounce. This information is stored in the
  Dataset ``bounceCountStore``.

- The Wise application contains the ``WiseService``, a Service which exposes RESTful endpoints to query the ``pageViewStore``
  Dataset.

- Finally, both the ``pageViewStore`` and ``bounceCountStore`` Datasets expose a SQL interface.
  They can be queried using SQL queries through our ``Explore`` module in the CDAP Console.


Here is how ``WiseFlow`` looks in the CDAP Console:

.. image:: docs/img/wise_flow.png
   :width: 6in


.. highlight:: console

Installation & Usage
====================
Building and running Wise is straightforward. We'll assume that you have already downloaded
and installed CDAP.

From the project root, build ``Wise`` with `Apache Maven <http://maven.apache.org/>`_ ::

  $ MAVEN_OPTS="-Xmx512m" mvn clean package

Note that the remaining commands assume that the ``cdap-cli.sh`` script is available on your PATH.
If this is not the case, please add it::

  $ export PATH=$PATH:<cdap-home>/bin

If you haven't already started a standalone CDAP installation, start it with the command::

  $ cdap.sh start

On Windows, substitute ``cdap.bat`` for ``cdap.sh``.

Deploy the Application to a CDAP instance defined by its host (defaults to ``localhost``)::
  
  $ cdap-cli.sh deploy app target/Wise-<version>.jar

On Windows, substitute ``cdap-cli.bat`` for ``cdap-cli.sh``.

To start the application::

  $ cdap-cli.sh start flow Wise.WiseFlow
  $ cdap-cli.sh start service Wise.WiseService

On Windows, substitute ``cdap-cli.bat`` for ``cdap-cli.sh``.

You can ingest sample data::

  $ bin/inject-data.sh

On Windows, substitute ``inject-data.bat`` for ``inject-data.sh``.


Realtime Log Analytics with WiseFlow
=====================================
The goal of ``WiseFlow`` is to perform realtime analytics on the web server access logs
received by ``logEventStream``. For each IP address in the logs, ``WiseFlow`` counts the
number of visits they made to different web pages.


Accessing Wise Data through WiseService
=======================================
``WiseService`` is a Wise component that exposes specific HTTP endpoints to retrieve the content of the ``pageViewStore``
Dataset. For example, ``WiseService`` defines this endpoint::

  GET http://localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/methods/ip/<ip-address>/count

You can use a ``curl`` command to make calls to the service URL. For example, to query the total pageview count
from IP ``255.255.255.207``::

  $ curl http://localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/methods/ip/255.255.255.207/count

The ``PageViewCountHandler`` has another endpoint for retrieving the pageview count of a particular page from
a specific IP address. For example, to query the pageview count of page ``/index.html`` from IP ``255.255.255.154``::

  $ curl -d /index.html http://localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/methods/ip/255.255.255.207/count


Exploring Wise Datasets through SQL
===================================
With Wise, you can explore the Datasets using SQL queries. The SQL interface on CDAP, called *Explore*,
can be accessed through the CDAP Console:

#. After deploying Wise in your Standalone CDAP instance, go to the **Store** page,
   which is one of the five pages you can access from the left pane of CDAP Console:

   .. image:: docs/img/wise_store_page.png


#. Click on the **Explore** button in the top-right corner of the page. You will land on this page:

   .. image:: docs/img/wise_explore_page.png

This is the *Explore* page, where you can run ad-hoc SQL queries and see information about the Datasets that expose
a SQL interface.

You will notice that the Datasets have unusual names, such as *cdap_user_bouncecounts*. Those are the SQL table names
of the Datasets which have a SQL interface.

Here are some of the SQL queries that you can run:

- Retrieve the web pages from where IP addresses have bounced more than 10% of the time::

    SELECT uri FROM dataset_bouncecountstore WHERE bounces > 0.1 * totalvisits

- Retrieve all the IP addresses which visited the page '/contact.html'::

    SELECT key FROM dataset_pageviewstore WHERE array_contains(map_keys(value), '/contact.html')=TRUE

As the SQL engine that CDAP runs internally is Hive, the SQL language used to submit queries is HiveQL.
A description of it is in the `Hive language manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries>`__.


Tutorial
========

An extensive tutorial, based on the Wise application, is available through  
`CDAP Examples, Guides and Tutorials <http://docs.cask.co/cdap/current/en/examples-manual/tutorials/wise.html>`__.


License
=======

Copyright © 2014-2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License
