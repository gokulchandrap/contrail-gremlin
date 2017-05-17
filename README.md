Run gremlin server
==================

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.4/apache-tinkerpop-gremlin-server-3.2.4-bin.zip
    unzip apache-tinkerpop-gremlin-server-3.2.4-bin.zip
    cd apache-tinkerpop-gremlin-server-3.2.4
    bin/gremlin-server.sh conf/gremlin-server-modern.yaml

Run loader
==========

From this repo root

    cd gremlin-loader
    go build

Setup tunnel to Contrail DB

    ssh d-cascld-1000.adm.lab2.aub.cloudwatt.net -L 9042:10.35.2.26:9042

Load Contrail DB in gremlin server

    ./gremlin-loader 
