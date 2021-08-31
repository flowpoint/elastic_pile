#!/bin/bash

#install neosemantics plugin
# 
#wget "https://github.com/neo4j-labs/neosemantics/releases/download/4.3.0.0/neosemantics-4.3.0.0.jar"


pilekg_graphdir=$(pwd)/graphdir
neo4j_data=$(pwd)/neo4j/data
neo4j_logs=$(pwd)/neo4j/logs
neo4j_import=$(pwd)/neo4j/import

mkdir -vp graphdir $neo4j_data $neo4j_logs $neo4j_import

docker run -p7474:7474 -p7687:7687 -v $pilekg_graphdir:/home:Z -v $neo4j_import:/var/lib/neo4j/import:Z -v $neo4j_data:/data:Z -v $neo4j_logs:/logs:Z -e NEO4J_AUTH=neo4j/s3cr3t -e NEO4JLABS_PLUGINS=\[\"apoc\"\,\"n10s\"] \
    -e NEO4J_apoc_export_file_enabled=true \
    -e NEO4J_apoc_import_file_enabled=true \
    -e NEO4J_apoc_import_file_use__neo4j__config=true \
    neo4j
