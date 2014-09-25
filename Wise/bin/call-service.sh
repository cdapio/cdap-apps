#!/usr/bin/env bash

#
# Copyright © 2014 Cask Data, Inc.
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

dir=`dirname "${BASH_SOURCE-$0}"`
dir=`cd "$dir"; pwd`
script=`basename $0`
user=$USER
epoch=`date +%s`

app="Wise"
service="WiseService"

auth_token=
auth_file="$HOME/.cdap.accesstoken"

function get_auth_token() {
  if [ -f $auth_file ]; then
    auth_token=`cat $auth_file`
  fi
}

function usage() {
  echo "Tool to call the endpoints defined by the WiseService."
  echo "Usage: $script --ip <ip_address> [--uri <uri>] [--host <hostname>]"
  echo ""
  echo "  Options"
  echo "    --ip        Specifies the IP address to get the page counts of."
  echo "    --uri       Specifies a Web page URI visited by the IP address. (Default: get counts of all Web pages visited)"
  echo "    --host      Specifies the host that CDAP is running on. (Default: localhost)"
  echo "    --help      This help message"
  echo ""
}

function count_ip() {
  local ip=$1; shift;
  local host=$1; shift;

  status=$(curl -w "APP_MANAGER_HTTP_CODE%{http_code}" -s $http -H "Authorization: Bearer $auth_token" http://$host:10000/v2/apps/$app/services/$service/methods/ip/$ip/count 2>/dev/null)

  # extract status and code
  code=`echo $status | grep -o '[^APP_MANAGER_HTTP_CODE]*$'`
  body=`echo $status | sed "s/APP_MANAGER_HTTP_CODE[^APP_MANAGER_HTTP_CODE]*$//"`

  if [ $code == 401 ]; then
    if [ "x$auth_token" == "x" ]; then
      echo "No access token provided"
    else
      echo "Invalid access token"
    fi
    exit 1;
  fi

  echo "Count: $body"
}

function count_ip_uri() {
  local ip=$1; shift;
  local uri=$1; shift;
  local host=$1; shift;
  # encode the uri twice
  local encoded_url="$(perl -MURI::Escape -e 'print uri_escape($ARGV[0]);' "$uri")"
  local encoded_url_2="$(perl -MURI::Escape -e 'print uri_escape($ARGV[0]);' "$encoded_url")"

  status=$(curl -w "APP_MANAGER_HTTP_CODE%{http_code}" -s -H "Authorization: Bearer $auth_token" http://$host:10000/v2/apps/$app/services/$service/methods/ip/$ip/uri/$encoded_url_2/count 2>/dev/null)

  # extract status and code
  code=`echo $status | grep -o '[^APP_MANAGER_HTTP_CODE]*$'`
  body=`echo $status | sed "s/APP_MANAGER_HTTP_CODE[^APP_MANAGER_HTTP_CODE]*$//"`

  if [ $code == 401 ]; then
    if [ "x$auth_token" == "x" ]; then
      echo "No access token provided"
    else
      echo "Invalid access token"
    fi
    exit 1;
  fi

  echo "Count: $body"
}


if [ $# -lt 1 ]; then
  usage
  exit 1
fi

host="localhost"
ip=
uri=
while [ $# -gt 0 ]
do
  case "$1" in
    --ip) shift; ip="$1"; shift;;
    --uri) shift; uri="$1"; shift;;
    --host) shift; host="$1"; shift;;
    *)  usage; exit 1
  esac
done


if [ "x$ip" == "x" ]; then
  usage
  echo
  echo "IP address not specified."
fi

# read the access token
get_auth_token

if [ "x$uri" == "x" ]; then
  count_ip $ip $host
else
  count_ip_uri $ip $uri $host
fi