Tool to load and sync Contrail DB in gremlin server.

Run gremlin server
==================

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.4/apache-tinkerpop-gremlin-server-3.2.4-bin.zip
    unzip apache-tinkerpop-gremlin-server-3.2.4-bin.zip
    cp <REPO_ROOT>/conf/* apache-tinkerpop-gremlin-server-3.2.4/conf
    cd apache-tinkerpop-gremlin-server-3.2.4
    # for large DBs tune Xmx and Xms
    export JAVA_OPTIONS="-Xmx2048m -Xms512m"
    bin/gremlin-server.sh conf/gremlin-server-contrail.yaml

Run gremlin-sync
================

Build (you need golang 1.7)

    cd gremlin-sync
    go get ./...
    go build

Load and sync Contrail DB in gremlin server

    ./gremlin-sync --rabbit <server> --rabbit-vhost <vhost> --rabbit-user <user> --rabbit-password <pass> --cassandra <server> --gremlin localhost:8182

Connect to server with Gremlin console
======================================

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.4/apache-tinkerpop-gremlin-console-3.2.4-bin.zip
    unzip apache-tinkerpop-gremlin-console-3.2.4-bin.zip
    cd apache-tinkerpop-gremlin-console-3.2.4
    bin/gremlin.sh

In the console do

    :remote connect tinkerpop.server conf/remote.yaml

Then you can query the remote graph with ':>'

    :> g.V()
    :> g.V().hasLabel('virtual_network')

Run gremlin-fsck
================

gremlin-fsck is a contrail-api-cli command. It will run different consistency checks on the gremlin server and clean/fix resources if needed.

    cd gremlin-fsck
    pip install -r requirements.txt
    python setup.py install

    contrail-api-cli fsck [--clean] [--loop] ...

See --help for all options.
