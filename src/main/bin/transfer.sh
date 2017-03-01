#!/bin/bash
dir=`dirname "$0"`
dir=`cd "$dir/../"; pwd`

java -Xms512m -Xmx512m -classpath $dir/lib/*:$dir com.icecat.elasticsearch.tools.transfer.TransferTool $*