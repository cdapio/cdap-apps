SentimentAnalysis
=================

Sentiment analytis application.

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

Installation
============

Build the Application jar:
```
mvn clean package
```

Deploy the Application to a CDAP instance defined by its host (defaults to localhost):
```
bin/app-manager.sh --host [host] --action deploy
```

Start Application Flows and Procedures:
```
bin/app-manager.sh --host [host] --action start
```

Make sure they are running:
```
bin/app-manager.sh --host [host] --action status
```

Ingest sample statements:
```
bin/ingest-statements.sh --host [host]
```

Run Web UI: (TODO)
```
mvn -Pweb jetty:run
```

External Documentation
======================

TBD: link to web-site page with app overview
