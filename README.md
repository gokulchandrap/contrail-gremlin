contrail-gremlin
================

This project contains a set of tools for using the Gremlin server in the context of Contrail:

gremlin-sync: a go program that load and sync the Contrail DB in the Gremlin server

gremlin-fsck: a contrail-api-cli command that runs consistency checks and apply fixes where possible in Contrail

Run gremlin server
------------------

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.4/apache-tinkerpop-gremlin-server-3.2.5-bin.zip
    unzip apache-tinkerpop-gremlin-server-3.2.5-bin.zip
    # copy the configuration from this repo
    cp <REPO_ROOT>/conf/* apache-tinkerpop-gremlin-server-3.2.5/conf
    cd apache-tinkerpop-gremlin-server-3.2.5
    # for large DBs tune Xmx and Xms
    export JAVA_OPTIONS="-Xmx2048m -Xms512m"
    bin/gremlin-server.sh conf/gremlin-server-contrail.yaml

Run gremlin-sync
----------------

Build (you need golang 1.7)

    cd gremlin-sync
    go get ./...
    go build

Load and sync Contrail DB in gremlin server

    ./gremlin-sync --rabbit <server> --rabbit-vhost <vhost> --rabbit-user <user> --rabbit-password <pass> --cassandra <server> --gremlin localhost:8182
    
The DB is reloaded completely every 30min

Connect to server with Gremlin console
--------------------------------------

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.5/apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    unzip apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    cd apache-tinkerpop-gremlin-console-3.2.5
    bin/gremlin.sh

In the console do

    :remote connect tinkerpop.server conf/remote.yaml

Then you can query the remote graph with ':>'

    :> g.V()
    :> g.V().hasLabel('virtual_network')

Run gremlin-fsck
----------------

gremlin-fsck is a contrail-api-cli command. It will run different consistency checks on the gremlin server and clean/fix resources if needed.

    cd gremlin-fsck
    pip install -r requirements.txt
    python setup.py install

    contrail-api-cli fsck [--clean] [--loop] ...

See --help for all options.
