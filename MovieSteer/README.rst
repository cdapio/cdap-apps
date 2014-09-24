.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform MovieSteer Application
     :Copyright: Copyright © 2014 Cask Data, Inc.

====================================
MovieSteer Application Example
====================================

**A Cask Data Application Platform (CDAP) Example Demonstrating Spark**

Overview
========
This example demonstrates the usage of Spark MLlib library, computing the movie prediction based on the movie rating
from an input stream.

Data from a sample file is sent to CDAP by the external script *inject-data* to the *ratingsStream*. This data is
processed by the ``RatingsFlow``, which stores ratings in their entirety in *ratings*, an ObjectStore Dataset.

As these entries are created, they are taken up by the *MovieSteerProgram*, which
goes through the entries, calculates predictions and tabulates results in another ObjectStore Dataset, *predictions*.

Once the application completes, you can query the *predictions* Dataset by using the ``getPrediction`` method of
the *PredictionProcedure*. It will send back a JSON-formatted result with the prediction based
on the ``userId`` and ``movieId`` parameters.

Let's look at some of these elements, and then run the Application and see the results.

The MovieSteer Application
--------------------------------
As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``MovieSteerApp``::

  public class MovieSteerApp extends AbstractApplication {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public void configure() {
      setName("MovieSteer");
      setDescription("Movie Steer Prediction App");

      // Ingest data into the Application via a Stream
      addStream(new Stream("ratingsStream"));

      // Process ratings data in real-time using a Flow
      addFlow(new RatingsFlow());

      // Run a Spark program on the acquired data
      addSpark(new MovieSteerSpecification());

      // Query the processed data using a Procedure
      addProcedure(new PredictionProcedure());

      // Store input and processed data in ObjectStore Datasets
      try {
        ObjectStores.createObjectStore(getConfigurer(), "ratings", String.class);
        ObjectStores.createObjectStore(getConfigurer(), "predictions", String.class);
      } catch (UnsupportedTypeException e) {
        // This exception is thrown by ObjectStore if its parameter type cannot be
        // (de)serialized (for example, if it is an interface and not a class, then there is
        // no auto-magic way deserialize an object.) In this case that will not happen
        // because String is an actual class.
        throw new RuntimeException(e);
      }
    }
  }

Data Storage ObjectStores ``ratings`` and ``predictions``
---------------------------------------------------------
The raw ratings data is stored in an ObjectStore Dataset, *ratings*.
The calculated predictions data is stored in a second ObjectStore Dataset, *predictions*.

Procedure ``PredictionProcedure``
---------------------------------
This procedure has a ``getPrediction`` method to obtain the prediction for a given ``userId`` and ``movieId``.

Building and Running the Application and Example
================================================

.. highlight:: console

In the remainder of this document, we refer to the CDAP runtime as "CDAP", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you need to build the app from source and then deploy the compiled JAR file.
You start the CDAP, deploy the app, start the Flow and then run the example by
injecting ratings data into the Stream.

When finished, stop the Application as described below.

Building the MovieSteer Application
-----------------------------------------
From the project root, build ``MovieSteer`` with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package

Deploying and Starting the Application
--------------------------------------
Make sure an instance of the CDAP is running and available.
From within the SDK root directory, this command will start CDAP in standalone mode::

	$ ./bin/cdap.sh start

  On Windows::

	~SDK> bin\cdap.bat start

From within the CDAP Console (`http://localhost:9999/ <http://localhost:9999/>`__ in standalone mode):

#. Drag and drop the Application .JAR file (``target/MovieSteer-<version>.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the CDAP Console.
#. Once loaded, select the ``MovieSteer`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.

To deploy and start the Application from the command-line:

#. To deploy the App JAR file, run ``$ ./bin/app-manager.sh --action deploy``
#. To start the App, run ``$ ./bin/app-manager.sh --action start``

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy``
#. To start the App, run ``~SDK> bin\app-manager.bat start``

Running the Example
-------------------

Injecting Ratings Data
......................

Run this script to inject ratings data
to the Stream named *ratingsStream* in the ``MovieSteer`` application::

	$ ./bin/inject-data.sh [--host <hostname>]

:Note: ``[--host ]`` is not available for a *Standalone CDAP*.

On Windows::

	~SDK> bin\inject-data.bat

Running the Spark Program
.........................

There are three ways to start the Spark program:

1. Click on the ``MovieSteerProgram`` in the Application page of the CDAP Console to get to the
   Spark dialogue, then click the *Start* button.

2. Send a query via an HTTP request using the ``curl`` command::

     curl -v -d '{args="20 1.5 10"}' \
    	  -X POST 'http://localhost:10000/v2/apps/MovieSteerProgram/spark/MovieSteerProgram/start'

   On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

3. Use the command::

    $ ./bin/app-manager.sh --action run

  On Windows::

	~SDK> bin\app-manager.bat run

Querying the Results
....................
If the Procedure has not already been started, you start it either through the
CDAP Console or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/MovieSteerProgram/procedures/PredictionProcedure/start'

There are two ways to query the *predictions* ObjectStore through the ``PredictionProcedure`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

	 curl -v -d '{"userId":"1","movieId":"2"}' \
	  -X POST 'http://localhost:10000/v2/apps/MovieSteerProgram/procedures/PredictionProcedure/methods/getPrediction'

   On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	  libexec\curl...

2. Type a Procedure method name, in this case PredictionProcedure, in the Query page of the CDAP Console:

   In the CDAP Console:

   #. Click the *Query* button.
   #. Click on the *PredictionProcedure* Procedure.
   #. Type ``getPrediction`` in the *Method* text box.
   #. Type the parameters required for this method, a JSON string with the user id and movie id in the parameters
   field, such as::

        { "userId" : "1", "movieId" : "2" }

   #. Click the *Execute* button.
   #. The prediction will be displayed in the Console in JSON format. For example::

	   "1.945542873"

Stopping the Application
---------------------------
Either:

- On the Application detail page of the CDAP Console, click the *Stop* button on **both** the *Process* and *Query* lists;

or:

- Run ``$ ./bin/app-manager.sh --action stop [--host <hostname>]``

  :Note: ``[--host ]`` is not available for a *Standalone CDAP*.

  On Windows, run ``~SDK> bin\app-manager.bat stop``

.. highlight:: java

Downloading the Example
=======================
This example (and more!) is included with our `software development kit <http://cask.co/download>`__.