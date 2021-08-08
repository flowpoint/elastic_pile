#!/bin/bash

pilekg_piledir=$(pwd)/pile
pilekg_datadir=$(pwd)/data
pilekg_configdir=$(pwd)/config
pilekg_logdir=$(pwd)/logs

echo using $pilekg_piledir as piledir
echo using $pilekg_datadir as datadir
echo using $pilekg_configdir as configdir
mkdir -vp $pilekg_piledir $pilekg_datadir $pilekg_configdir 

docker run -p 9200:9200 -p 9300:9300 --name es02-test --net elastic -e ES_JAVA_OPTS="-Xms16g -Xmx16g" -e "discovery.type=single-node" -v $pilekg_datadir:/usr/share/elasticsearch/data:Z -v $pilekg_configdir/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml:Z docker.elastic.co/elasticsearch/elasticsearch:7.13.4 

