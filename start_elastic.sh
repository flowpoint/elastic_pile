#!/bin/bash

pilekg_piledir=$(pwd)/pile
pilekg_datadir=$(pwd)/data
pilekg_configdir=$(pwd)/config
pilekg_logdir=$(pwd)/logs

echo using $pilekg_piledir as piledir
echo using $pilekg_datadir as datadir
echo using $pilekg_configdir as configdir
mkdir -vp $pilekg_piledir $pilekg_datadir $pilekg_configdir 

docker run \
    --memory=28g --memory-swap=28g \
    -p 9200:9200 \
    -p 9300:9300 \
    --name es00-pile \
    --net elastic \
    -e ELASTIC_PASSWORD=$ELASTIC_PASSWORD \
    -e ES_JAVA_OPTS="-Xms10g -Xmx10g" \
    -e "discovery.type=single-node" \
    -v $pilekg_datadir:/usr/share/elasticsearch/data:z \
    -v $pilekg_configdir/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml:z \
    docker.elastic.co/elasticsearch/elasticsearch:7.13.4 

