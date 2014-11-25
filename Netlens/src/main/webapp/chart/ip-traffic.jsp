<!--
Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

<div class="table_container">
    <div class="table_title">
        Outbound Traffic
    </div>
    <div id="traffic" style="width: 100%; height: 75%">&nbsp;</div>
</div>

<script type="text/javascript">
    $(function () {
        reloadTrafficChart();
    });

    function reloadTrafficChart() {
        drawTrafficChart();
        setTimeout(function () {
            reloadTrafficChart();
        }, 5000);
    }

    function drawTrafficChart() {
        var fact = JSON.parse(decodeURIComponent('<%= request.getParameter("fact") %>'));
        var src = fact.dimensions.src;
        // todo: hack: we should not know here format of the key
        var key = '3src' + src.length + src;
        var startTs = Date.now() - 5000 * 120;
        var endTs = Date.now();
        $.ajax({
            url: "proxy/v2/apps/Netlens/services/CountersService/methods/counts/"
                    + startTs + "/" + endTs + "?key=" + key,
            type: 'GET',
            contentType: "application/json",
            dataType: 'json',
            cache: false,
            success: function (data) {
                renderTrafficChart(data);
            },
            error: function (xhr, textStatus, errorThrown) {
                $('#traffic').html("<div class='server_error''>Failed to get data from server<div>");
            }
        });
        $.post(url)
                .done(function (data) {
                    renderTrafficChart(data);
                })
                .fail(function (xhr, textStatus, errorThrown) {
                    $('#traffic').html("<div class='server_error''>Failed to get data from server<div>");
                })
    }

    function renderTrafficChart(points) {
        var data = [];
        points.forEach(function (point) {
            data.push([point.ts, point.value]);
        });

        var plot = $.plot("#traffic", [data], {
            series: { shadowSize: 0 },
            yaxis: { min: 0 },
            xaxis: { mode: 'time'},
            grid: {borderWidth: 0}
        });

        plot.draw();
    }

</script>