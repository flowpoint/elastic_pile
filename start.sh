#!/bin/bash

pilekg_datadir=$(pwd)/data
pilekg_configdir=$(pwd)/config

echo using $pilekg_datadir as datadir
echo using $pilekg_configdir as configdir
mkdir -vp $pilekg_datadir $pilekg_configdir

echo -n "copying "
cp -v elasticsearch.yml config/elasticsearch.yml

#docker run --rm -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -v $pilekg_datadir:/usr/share/elasticsearch/data:Z -v $pilekg_configdir:/usr/share/elasticsearch/config/:Z docker.elastic.co/elasticsearch/elasticsearch:7.13.4 

