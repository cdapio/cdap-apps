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
      <!-- Bootstrap core JavaScript -->
      <script src="third_party/jquery-1.9.1.js"></script>
      <script src="third_party/bootstrap.js"></script>
      <script src="js/main.js"></script>
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
    <link rel="stylesheet" href="./font-awesome/css/font-awesome.min.css">



	<!--[if lte IE 8]><script language="javascript" type="text/javascript" src="js/excanvas.min.js"></script><![endif]-->
	    <script language="javascript" type="text/javascript" src="js/jquery.js"></script>
      <script language="javascript" type="text/javascript" src="js/jquery.flot.js"></script></head>
      <script language="javascript" type="text/javascript" src="js/jquery.flot.time.js"></script></head>

    <script type="text/javascript">
    	$(function() {
    		var data = { positive: [], negative: [], neutral: [] };
    		var sentiments = ['positive', 'negative', 'neutral'];
        totalPoints = 60*5;
        sentiments.forEach(function(sentiment){
          var arr = data[sentiment];
          while (arr.length < totalPoints) {
		  		  arr.push(0);
          }
        });
    		function zipData(sentiment) {
          data[sentiment] = data[sentiment];
    		  var dataArr = data[sentiment];

          // Zip the generated y values with the x values
          var res = [];
          for (var i = 0; i < dataArr.length; ++i) {
            res.push([Date.now()-(dataArr.length - i)*1000, dataArr[i]])
          }
    			return res;
    		}
    		// Set up the control widget
    		var updateInterval = 1000;
    		// the chart will interpolate the results over the last few updates, as defined by:
    		var interpolateOver = 20;


    		function update() {
    			$.ajax({
            url: 'proxy/v2/apps/TwitterSentiment/procedures/sentiment-query/methods/counts?sentiments=[negative,positive,neutral]&seconds=' + interpolateOver,
            type: 'GET',
            contentType: "application/json",
            dataType: 'json',
            cache: false,
            success: function(response) {
              sentiments.forEach(function(sentiment){
                data[sentiment].push(response[sentiment] / interpolateOver);
              });
            }
          });

          var posData = {data:zipData('positive'), label:"Positive", color: "#468847" };
          var negData = {data:zipData('negative'), label:"Negative", color: "#b94a48" };
          var neutData = {data:zipData('neutral'), label:"Neutral", color: "#428bca" };
          var plot = $.plot("#placeholder", [posData, negData, neutData], {
            series: { shadowSize: 0 },
            yaxis: { min: 0 },
            xaxis: { mode:"time" },
          });
    			plot.draw();
    			setTimeout(update, updateInterval);
    		}
    		update();
    	});
    	</script>
  </head>


  <body>
            <h1 style="padding-left:20px;">  Dashboard <small>Twitter Sentiment Analysis Overview</small></h1>
  	<div id="header">
  	</div>


    <div id="wrapper">
      <div id="page-wrapper">

        <div class="row">
          <div class="col-lg-12">
            <!--
            <p>Say something!</p>
              <form class="bs-example" id="text-inject-form">
                  <input type="text" class="form-control" placeholder="Type a phrase and click enter..." id="stream-inject-textarea">
                  <br />
                  <button style="float: right" class="btn btn-primary" type="submit" id="analyze-button">Analyze</button>
              </form>
            -->

          </div>
        </div><!-- /.row -->

        <div class="col" style="width:20%; float:left;">
          <div class="row-lg-3">
            <div class="panel panel-info">
              <div class="panel-heading positive">
                <div class="row">
                  <div class="col-xs-6">
                    <i class="icon-inbox icon-5x"></i>
                  </div>
                  <div class="col-xs-6 text-right">
                    <p class="announcement-heading" id="all-sentences-processed">0</p>
                    <p class="announcement-text">Processed!</p>
                  </div>
                </div>
              </div>
              <a href="#">
              </a>
            </div>
          </div>
          <div class="row-lg-3">
            <div class="panel panel-success">
              <div class="panel-heading neutral">
                <div class="row">
                  <div class="col-xs-6">
                    <i class="icon-thumbs-up icon-5x"></i>
                  </div>
                  <div class="col-xs-6 text-right">
                    <p class="announcement-heading" id="positive-sentences-processed">0</p>
                      <p class="announcement-text">Positive</p>
                  </div>
                </div>
              </div>
              <a href="#">
              </a>
            </div>
          </div>
          <div class="row-lg-3">
            <div class="panel panel-danger">
              <div class="panel-heading">
                <div class="row">
                  <div class="col-xs-6">
                    <i class="icon-thumbs-down icon-5x"></i>
                  </div>
                  <div class="col-xs-6 text-right">
                    <p class="announcement-heading" id="negative-sentences-processed">0</p>
                      <p class="announcement-text">Negative</p>
                  </div>
                </div>
              </div>
              <a href="#">
              </a>
            </div>
          </div>
          <div class="row-lg-3">
              <div class="panel panel-gray">
                  <div class="panel-heading panel negative" id="neutral-sentences-panel">
                      <div class="row">
                          <div class="col-xs-6">
                              <i class="icon-ok icon-5x"></i>
                          </div>
                          <div class="col-xs-6 text-right">
                              <p class="announcement-heading" id="neutral-sentences-processed">0</p>
                              <p class="announcement-text">Neutral</p>
                          </div>
                      </div>
                  </div>
                  <a href="#">
                  </a>
              </div>
          </div>
        </div><!-- /.col -->


  	<div id="content" style="width:70%;float:left;">
  		<div class="graph-container">
  			<div id="placeholder" class="graph-placeholder"></div>
  		</div>
  	</div>


        <div class="row">
          <div class="col-lg-4">
            <div class="panel panel-primary">
              <div class="panel-heading" id="panel-positive">
                <h3 class="panel-title">Most Recent Positive Sentences</h3>
              </div>
              <div class="panel-body">
                <div class="table-responsive">
                  <a id="positive-sentences-table-link"></a>
                  <table class="table table-bordered table-hover table-striped tablesorter" id="positive-sentences-table">
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
          <div class="col-lg-4">
              <div class="panel panel-primary">
                  <div class="panel-heading" id="panel-negative">
                      <h3 class="panel-title">Most Recent Negative Sentences</h3>
                  </div>
                  <div class="panel-body">
                      <div class="table-responsive">
                          <a id="negative-sentences-table-link"></a>
                          <table class="table table-bordered table-hover table-striped tablesorter" id="negative-sentences-table">
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
          <div class="col-lg-4">
              <div class="panel panel-primary">
                  <div class="panel-heading" id="panel-neutral">
                      <h3 class="panel-title">Most Recent Neutral Sentences</h3>
                  </div>
                  <div class="panel-body">
                      <div class="table-responsive">
                          <a id="neutral-sentences-table-link"></a>
                          <table class="table table-bordered table-hover table-striped tablesorter" id="neutral-sentences-table">
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
