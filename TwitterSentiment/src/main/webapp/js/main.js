/**
 * Js for main page.
 */

var Homepage = function () {
  this.init(arguments);
};

Homepage.prototype.init  = function () {
  var self = this;
  this.initGraph();
  this.enableIntervals();

  $("#text-inject-form").submit(function(e) {
    e.preventDefault();
    self.injectIntoStream();
  });



};

Homepage.prototype.injectIntoStream = function() {
  var injectText = $("#stream-inject-textarea").val();
  $.ajax({
    url: 'proxy/v2/streams/sentence',
    type: 'POST',
    dataType: 'json',
    contentType: 'application/json',
    data: JSON.stringify(injectText)
  });
  $("#stream-inject-textarea").val('');
};

Homepage.prototype.initGraph = function () {
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
    data[sentiment] = data[sentiment].slice(1);
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
  var interpolateOver = 10;


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
      ylab:"tps",
      yaxis: { min: 0 },
      xaxis: { mode:"time" },
    });
    plot.draw();
    setTimeout(update, updateInterval);
  }
  update();
}
appendK = function (str) {
  val = parseInt(str);
  if ( val < 1000 ) {
    return val
  }
  val = ~~(val / 1000)
  val += "K";
  return val;
}

Homepage.prototype.enableIntervals = function () {
  var self = this;
  var updateFunc = function() {
   $.ajax({
     url: 'proxy/v2/apps/TwitterSentiment/procedures/sentiment-query/methods/aggregates',
     type: 'GET',
     contentType: "application/json",
     cache: false,
     dataType: 'json',
     success: function(data) {
       if (!data.positive) {
           data.positive = 0;
       }
       if (!data.negative) {
           data.negative= 0;
       }
       if (!data.neutral) {
           data.neutral= 0;
       }
       $("#positive-sentences-processed").text(appendK(data.positive));
       $("#neutral-sentences-processed").text(appendK(data.neutral));
       $("#negative-sentences-processed").text(appendK(data.negative));
       $("#all-sentences-processed").text(appendK(parseInt(data.negative) + parseInt(data.positive) + parseInt(data.neutral)));
     }
   });

   $.ajax({
     url: 'proxy/v2/apps/TwitterSentiment/procedures/sentiment-query/methods/sentiments?sentiment=positive',
     type: 'GET',
     contentType: "application/json",
     dataType: 'json',
     cache: false,
     success: function(data) {
       var list = [];
       for (item in data) {
         if(data.hasOwnProperty(item)) {
           list.push('<tr><td>' + item + '</td></tr>');
         }
       }
       $('#positive-sentences-table tbody').html(list.join(''));
     }
   });

   $.ajax({
     url: 'proxy/v2/apps/TwitterSentiment/procedures/sentiment-query/methods/sentiments?sentiment=neutral',
     type: 'GET',
     contentType: "application/json",
     dataType: 'json',
     cache: false,
     success: function(data) {
       var list = [];
       for (item in data) {
         if(data.hasOwnProperty(item)) {
           list.push('<tr><td>' + item + '</td></tr>');
         }
       }
       $('#neutral-sentences-table tbody').html(list.join(''));
     }
   });

   $.ajax({
     url: 'proxy/v2/apps/TwitterSentiment/procedures/sentiment-query/methods/sentiments?sentiment=negative',
     type: 'GET',
     contentType: "application/json",
     dataType: 'json',
     cache: false,
     success: function(data) {
       var list = [];
       for (item in data) {
         if(data.hasOwnProperty(item)) {
           list.push('<tr><td>' + item + '</td></tr>');
         }
       }
       $('#negative-sentences-table tbody').html(list.join(''));
     }
   });

  }
  updateFunc();
  this.interval = setInterval(updateFunc, 5000);
};


$(document).ready(function() {
  new Homepage();
});

