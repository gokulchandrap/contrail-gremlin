# contrail-gremlin

This project contains a set of tools for using the Gremlin server in the context of Contrail:

 * gremlin-sync: a go program that load and sync the Contrail DB in the Gremlin server
 * gremlin-dump: a go program that dumps the Contrail DB in a GraphSON file that can be loaded by Gremlin server/console
 * gremlin-fsck: a contrail-api-cli command that runs consistency checks and apply fixes where possible in Contrail

Binairies are available at https://github.com/eonpatapon/contrail-gremlin-binaries

# Run gremlin server

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.4/apache-tinkerpop-gremlin-server-3.2.5-bin.zip
    unzip apache-tinkerpop-gremlin-server-3.2.5-bin.zip
    # copy the configuration from this repo
    cp <REPO_ROOT>/conf/* apache-tinkerpop-gremlin-server-3.2.5/conf
    cd apache-tinkerpop-gremlin-server-3.2.5
    # for large DBs tune Xmx and Xms
    export JAVA_OPTIONS="-Xmx2048m -Xms512m"
    bin/gremlin-server.sh conf/gremlin-server-contrail.yaml

## Connect to server with Gremlin console

    wget http://www-eu.apache.org/dist/tinkerpop/3.2.5/apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    unzip apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    cd apache-tinkerpop-gremlin-console-3.2.5
    bin/gremlin.sh

In the console do

    gremlin> :remote connect tinkerpop.server conf/remote.yaml
    gremlin> :remote console
    gremlin> g.V()
    gremlin> g.V().hasLabel('virtual_network')

# Using gremlin-sync

Load and sync Contrail DB in gremlin server

    ./gremlin-sync --rabbit <server> --rabbit-vhost <vhost> --rabbit-user <user> --rabbit-password <pass> --cassandra <server> --gremlin localhost:8182

The DB is reloaded completely every 30min

# Using gremlin-dump

    ./gremlin-dump --cassandra localhost dump.json

Then you can load the dump in the gremlin console:

    bin/gremlin.sh
             \,,,/
             (o o)
    -----oOOo-(3)-oOOo-----
    plugin activated: tinkerpop.server
    plugin activated: tinkerpop.utilities
    plugin activated: tinkerpop.tinkergraph
    gremlin> graph = TinkerGraph.open()
    ==>tinkergraph[vertices:0 edges:0]
    gremlin> graph.io(IoCore.graphson()).readGraph("/path/to/dump.json");
    ==>null
    gremlin> graph
    ==>tinkergraph[vertices:1735 edges:1349]
    gremlin> g = graph.traversal()
    ==>graphtraversalsource[tinkergraph[vertices:1735 edges:1349], standard]
    gremlin> g.V().hasLabel('virtual_network').count()
    ==>109

The dump contains all contrail resources including incomplete or missing ones. Incomplete are resources that have no type or fq_name or id_perms properties. Missing are resources that are not in the DB but still referenced by other resources. Incomplete resources have an `_incomplete` property, missings ones have a `_missing` property.

## Use cases

### Broken references

For example to get resources that reference missing references we can do:

    gremlin> g.V().hasNot('_missing').both().has('_missing').path()
    ==>[v[58b78816-8df1-4c4f-9854-d618732b984d],v[d110bfaa-6a8d-49e0-b527-cf166eef2329]]
    ==>[v[3285ef82-5e74-433a-9d7b-7a39cc5bce81],v[b3b48af9-ce7b-43bd-94b5-7ad4e393e66b]]
    [...]

This builds a list of paths where each path is 2 resources. The first one exists in the Contrail DB and it is linked to the second one that is not in the Contrail DB because we match on the `_missing` property.

From the path we can get the type of resources by augmenting the query to:

    gremlin> g.V().hasNot('_missing').both().has('_missing').path().map(unfold().union(label(), id()).fold())
    ==>[routing_instance,58b78816-8df1-4c4f-9854-d618732b984d,virtual_network,d110bfaa-6a8d-49e0-b527-cf166eef2329]
    ==>[routing_instance,3285ef82-5e74-433a-9d7b-7a39cc5bce81,virtual_network,b3b48af9-ce7b-43bd-94b5-7ad4e393e66b]
    [...]

In this case the parent link between some RI and VN is broken because the VN is not in the DB anymore but the RI is still referencing it. Let's check the cassandra DB to confirm this:

    cqlsh
    cqlsh> use config_db_uuid ;
    cqlsh:config_db_uuid> SELECT blobAsText(key), blobAsText(column1) FROM obj_uuid_table WHERE key = textAsBlob('58b78816-8df1-4c4f-9854-d618732b984d');

     blobAsText(key)                      | blobAsText(column1)
    --------------------------------------+-------------------------------------------------------------
     58b78816-8df1-4c4f-9854-d618732b984d |                                                     fq_name
     58b78816-8df1-4c4f-9854-d618732b984d | parent:virtual_network:d110bfaa-6a8d-49e0-b527-cf166eef2329
     58b78816-8df1-4c4f-9854-d618732b984d |                                                 parent_type
     58b78816-8df1-4c4f-9854-d618732b984d |                                           prop:display_name
     58b78816-8df1-4c4f-9854-d618732b984d |                                               prop:id_perms
     58b78816-8df1-4c4f-9854-d618732b984d |                            prop:routing_instance_is_default
     58b78816-8df1-4c4f-9854-d618732b984d |       ref:route_target:79cf45c3-457a-4269-8fc4-bf41d5525110
     58b78816-8df1-4c4f-9854-d618732b984d |                                                        type

    (8 rows)

    cqlsh:config_db_uuid> SELECT blobAsText(key), blobAsText(column1) FROM obj_uuid_table WHERE key = textAsBlob('d110bfaa-6a8d-49e0-b527-cf166eef2329');

    (0 rows)

    cqlsh:config_db_uuid>


## Run gremlin-fsck

gremlin-fsck is a contrail-api-cli command. It will run different consistency checks on the gremlin server and clean/fix resources if needed with some contrail-api-cli-extra commands.

    cd gremlin-fsck
    pip install -r requirements.txt
    python setup.py install

    contrail-api-cli fsck [--clean] [--loop] ...

See --help for all options.
