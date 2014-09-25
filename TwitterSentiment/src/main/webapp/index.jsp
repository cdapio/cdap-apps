<!DOCTYPE html>
<!--
  ~ Copyright © 2014 Cask Data, Inc.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License"); you may not
  ~ use this file except in compliance with the License. You may obtain a copy of
  ~ the License at
  ~
  ~ http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  ~ WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  ~ License for the specific language governing permissions and limitations under
  ~ the License.
  -->
	    <script src="js/jquery.js"></script>
      <script src="third_party/bootstrap.js"></script>
      <script src="js/main.js"></script>
      <!--[if lte IE 8]><script src="js/excanvas.min.js"></script><![endif]-->
      <script src="js/jquery.flot.js"></script></head>
      <script src="js/jquery.flot.time.js"></script></head>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">
    <title>Sentiment Analysis Dashboard</title>
    <!-- Bootstrap core CSS -->
    <link rel="stylesheet" href="./css/bootstrap.css" type="text/css" >
    <!-- Add custom CSS here -->
    <link rel="stylesheet" href="./css/style.css" type="text/css">

  </head>
  <body>
    <h1 style="padding-left:20px;">  <small>Twitter Sentiment Analysis Overview</small></h1>
  	<div id="header">
  	</div>
    <div id="wrapper">
      <div id="page-wrapper">
        <div class="col" style="min-width:10%; float:left;">
          <div class="row-lg-3">
            <div class="panel panel-gray">
              <div class="panel-heading processed">
                <div class="row">
                  <div class="col-xs-6 text-right">
                    <p class="announcement-heading" id="all-sentences-processed">0</p>
                    <p class="announcement-text">Processed</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="row-lg-3">
            <div class="panel panel-success">
              <div class="panel-heading neutral">
                <div class="row">
                  <div class="col-xs-6 text-right">
                    <p class="announcement-heading" id="positive-sentences-processed">0</p>
                      <p class="announcement-text">Positive</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="row-lg-3">
            <div class="panel panel-danger">
              <div class="panel-heading">
                <div class="row">
                  <div class="col-xs-6 text-right">
                    <p class="announcement-heading" id="negative-sentences-processed">0</p>
                      <p class="announcement-text">Negative</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="row-lg-3">
              <div class="panel panel-info">
                  <div class="panel-heading panel" id="neutral-sentences-panel">
                      <div class="row">
                          <div class="col-xs-6 text-right">
                              <p class="announcement-heading" id="neutral-sentences-processed">0</p>
                              <p class="announcement-text">Neutral</p>
                          </div>
                      </div>
                  </div>
              </div>
          </div>
        </div><!-- /.col -->


  	<div id="content" style="width:85%;padding-left:20px;float:left;">
  		<div class="graph-container">
  		<td><p style="color:grey;">Tweets per second</p></td>
  			<div id="placeholder" class="graph-placeholder"></div>
  		</div>
  	</div>

    <div style="clear: both"></div>

        <div class="row">
          <div class="" style="width:33%; padding:5px; float:left;">
            <div class="panel panel-primary">
              <div class="panel-heading" id="panel-positive">
                <h3 class="panel-title">Most Recent Positive Sentences</h3>
              </div>
              <div class="panel-body" style="min-height:800px;">
                <div class="table-responsive">
                  <a id="positive-sentences-table-link"></a>
                  <table class="table table-bordered table-hover table-striped" id="positive-sentences-table">
                    <thead>
                      <tr>
                        <th>Phrase</th>
                      </tr>
                    </thead>
                    <tbody>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
          <div class="" style="width:33%; padding:5px; float:left;">
              <div class="panel panel-primary">
                  <div class="panel-heading" id="panel-negative">
                      <h3 class="panel-title">Most Recent Negative Sentences</h3>
                  </div>
              <div class="panel-body" style="min-height:800px;">
                      <div class="table-responsive">
                          <a id="negative-sentences-table-link"></a>
                          <table class="table table-bordered table-hover table-striped" id="negative-sentences-table">
                              <thead>
                              <tr>
                                  <th>Phrase</th>
                              </tr>
                              </thead>
                              <tbody>
                              </tbody>
                          </table>
                      </div>
                  </div>
              </div>
          </div>
          <div class="" style="width:33%; padding:5px;  float:left;">
              <div class="panel panel-primary">
                  <div class="panel-heading" id="panel-neutral">
                      <h3 class="panel-title">Most Recent Neutral Sentences</h3>
                  </div>
                    <div class="panel-body" style="min-height:800px;">
                      <div class="table-responsive">
                          <a id="neutral-sentences-table-link"></a>
                          <table class="table table-bordered table-hover table-striped" id="neutral-sentences-table">
                              <thead>
                              <tr>
                                  <th>Phrase</th>
                              </tr>
                              </thead>
                              <tbody>
                              </tbody>
                          </table>
                      </div>
                  </div>
              </div>
          </div>
        </div><!-- /.row -->

      </div><!-- /#page-wrapper -->

    </div><!-- /#wrapper -->
  </body>
</html>
