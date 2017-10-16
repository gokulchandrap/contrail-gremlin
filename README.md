# contrail-gremlin

This project contains a set of tools for using gremlin in the context of contrail:

 * gremlin-dump: a go program that dumps the contrail DB in a GraphSON file that can be loaded by gremlin server/console
 * gremlin-sync: a go program that sync the contrail DB in the gremlin server
 * gremlin-fsck: a `contrail-api-cli` command that runs consistency checks and apply fixes where possible in contrail

Binairies are available at https://github.com/eonpatapon/contrail-gremlin-binaries

# Use cases for contrail-gremlin

Checks for inconsistencies are the main use cases. With the speed and expressivity of the gremlin language we can easily detect them almost in realtime.

More checks can be found in the `gremlin-fsck` package. Let's take a look at some examples:

### A note on links

In the contrail DB we have 2 kinds of links : `parent` links, and `ref` links. The inverse links are `children` links and `backref` links. In gremlin a link has a label, an in vertex and an out vertex. The mapping of contrail links is done as follow.

 * there is only 2 types of links : `parent` and `ref` links.
 * a `parent` link is from the child to the parent (eg: `virtual_network` to `project`)
 * a `ref` link is from a resource refering to another resource (eg: `virtual_machine_interface` to `virtual_machine`)

For example to find the parent of a VN, which is a `project` in the contrail schema:

    gremlin> g.V('5f8923ec-38d5-47db-8b73-d56a6f5deb29').out('parent')
    ==>v[b80d035e-39cf-4b13-ac12-53fe62accd50]

Inversely, to find a project VNs:

    gremlin> g.V('b80d035e-39cf-4b13-ac12-53fe62accd50').in('parent').hasLabel('virtual_network')
    ==>v[236674b4-8659-4b5b-a476-caf9e54eb810]
    ==>v[5f8923ec-38d5-47db-8b73-d56a6f5deb29]
    ==>v[68d34fd6-4a6c-4021-b9a2-084c771f9561]

## Finding instance-ips without ip address

    gremlin> g.V().hasLabel("instance_ip").not(has("instance_ip_address"))

This will simply return all `instance_ip` resources that don't have any `instance_ip_address` property.

## Finding duplicate ips

For each VN, we get all instance ips:

    gremlin> g.V().hasLabel("virtual_network").as('vn').map(
        union(
            select('vn'),
            __.in().hasLabel("instance_ip").has("instance_ip_address")
        ).fold()
    )
    ==>[v[ec92b9e1-6d07-4a8d-901c-ebd992c31af3], v[149873a1-27ad-4c2c-86d9-5a4185bc9ba5], v[1d565dab-847e-48e4-81e4-9b3215bec725], v[66328246-0b81-446d-8306-2b1eb9606f55], v[98a2aefe-0b91-4d21-9cb4-7ba1141e4388], v[c4a47a42-9498-445d-9b47-72216e1900ca], v[df8e58cf-5d3d-4b47-8224-41a60ce7d645], v[e3fcb51d-a655-4c9a-aa0f-1f1ddb8fef9b], v[e4f8267a-62b0-4e42-8691-476d0f094908], v[eba384de-143a-4138-8f67-cb2ba4126456], v[f4d19d9b-5ff7-4e40-835e-9790422f739d], v[f8157776-3b81-4a96-a239-4113f8f1f28d]]
    ==>[v[3dd0d0f8-2d4c-4575-b7d6-563557f5b236], v[b00608ca-7fe2-468e-b872-41098846bb4a]]
    ==>[v[b13962d4-62d1-4a18-90df-87d8f768aa3a]]
    [...]

VMI has a backref to an IIP, so `__.in().hasLabel("instance_ip")` gets the IIPs of the current VN (`__`). The first element is the VN, next ones are instance-ips.

We can group instance ips by ip address:

    gremlin> g.V().hasLabel("virtual_network").as('vn').map(
        union(
            select('vn'),
            __.in().hasLabel("instance_ip").has("instance_ip_address").group().by("instance_ip_address").unfold()
        ).fold()
    )
    ==>[v[ec92b9e1-6d07-4a8d-901c-ebd992c31af3], 22.56.62.99=[v[c4a47a42-9498-445d-9b47-72216e1900ca]], 22.56.63.150=[v[f4d19d9b-5ff7-4e40-835e-9790422f739d]], 22.56.63.153=[v[149873a1-27ad-4c2c-86d9-5a4185bc9ba5], v[e4f8267a-62b0-4e42-8691-476d0f094908]], 22.56.62.123=[v[98a2aefe-0b91-4d21-9cb4-7ba1141e4388]], 22.56.62.96=[v[1d565dab-847e-48e4-81e4-9b3215bec725]], 22.56.63.155=[v[eba384de-143a-4138-8f67-cb2ba4126456]], 22.56.62.97=[v[f8157776-3b81-4a96-a239-4113f8f1f28d]], 22.56.63.145=[v[66328246-0b81-446d-8306-2b1eb9606f55]], 22.56.63.156=[v[df8e58cf-5d3d-4b47-8224-41a60ce7d645]], 22.56.62.98=[v[e3fcb51d-a655-4c9a-aa0f-1f1ddb8fef9b]]]
    ==>[v[3dd0d0f8-2d4c-4575-b7d6-563557f5b236], 100.64.0.4=[v[b00608ca-7fe2-468e-b872-41098846bb4a]]]
    ==>[v[b13962d4-62d1-4a18-90df-87d8f768aa3a]]
    [...]

Filter out ip addresses that have only 1 instance ip:

    gremlin> g.V().hasLabel("virtual_network").as('vn').map(
        union(
            select('vn'),
            __.in().hasLabel("instance_ip").has("instance_ip_address").group().by("instance_ip_address").unfold()
              .filter{it.get().value.size > 1}
        ).fold()
    )
    ==>[v[ec92b9e1-6d07-4a8d-901c-ebd992c31af3], 22.56.63.153=[v[149873a1-27ad-4c2c-86d9-5a4185bc9ba5], v[e4f8267a-62b0-4e42-8691-476d0f094908]]]
    ==>[v[3dd0d0f8-2d4c-4575-b7d6-563557f5b236]]
    ==>[v[b13962d4-62d1-4a18-90df-87d8f768aa3a]]
    [...]

Finally, filter out VNs that have no duplicates ip adresses:

    gremlin> g.V().hasLabel("virtual_network").as('vn').map(
        union(
            select('vn'),
            __.in().hasLabel("instance_ip").has("instance_ip_address").group().by("instance_ip_address").unfold()
                   .filter{it.get().value.size > 1}
        ).fold()
    ).filter{it.get().size > 1}
    ==>[v[ec92b9e1-6d07-4a8d-901c-ebd992c31af3], 22.56.63.153=[v[149873a1-27ad-4c2c-86d9-5a4185bc9ba5], v[e4f8267a-62b0-4e42-8691-476d0f094908]]]

In this case, the VN ec92b9e1-6d07-4a8d-901c-ebd992c31af3 has two instance-ips that use the 22.56.63.153 ip address.

## Finding broken references

For example to get resources that reference missing resources we can do:

    gremlin> g.V().hasNot('_missing').both().has('_missing').path()
    ==>[v[58b78816-8df1-4c4f-9854-d618732b984d],v[d110bfaa-6a8d-49e0-b527-cf166eef2329]]
    ==>[v[3285ef82-5e74-433a-9d7b-7a39cc5bce81],v[b3b48af9-ce7b-43bd-94b5-7ad4e393e66b]]
    [...]

This builds a list of paths where each path is 2 resources. The first one exists in the Contrail DB and it is linked to the second one (by a `ref` or `parent` link) that is not in the Contrail DB because we match on the `_missing` property.

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

# Using gremlin-dump

First step is to use `gremlin-dump` that will read the whole contrail config DB and transform it to a graph in the GraphSON format. This file then can be loaded in a gremlin server or console.

    $ ./gremlin-dump --cassandra localhost dump.json
    11:35:15.043 setupCassandra ▶ NOTI 001 Connecting to Cassandra...
    11:35:19.577 setupCassandra ▶ NOTI 002 Connected.
    Processing nodes [read:1717 correct:1715 incomplete:0 missing:30 dup:2]

The dump contains all contrail resources including incomplete or missing ones. Incomplete are resources that have no `type` or `fq_name` or `id_perms` properties. Missing are resources that are not in the DB but still referenced by other resources. Incomplete resources have an `_incomplete` property, missings ones have a `_missing` property so that we can easily find them.

## Loading the dump in the gremlin console

    $ wget https://archive.apache.org/dist/tinkerpop/3.2.5/apache-tinkerpop-apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    $ unzip apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    $ cd apache-tinkerpop-gremlin-console-3.2.5
    $ bin/gremlin.sh
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

## Loading the dump in the gremlin server

    $ wget https://archive.apache.org/dist/tinkerpop/3.2.5/apache-tinkerpop-gremlin-server-3.2.5-bin.zip
    $ unzip apache-tinkerpop-gremlin-server-3.2.5-bin.zip
    $ cd apache-tinkerpop-gremlin-server-3.2.5

Create a `conf/contrail.properties` file with the following:

    gremlin.graph=org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
    gremlin.tinkergraph.vertexIdManager=UUID
    gremlin.tinkergraph.graphLocation=/path/to/dump.json
    gremlin.tinkergraph.graphFormat=graphson

Copy `conf/gremlin-server-modern.yaml` to `conf/contrail.yaml`. And replace `graph: conf/tinkergraph-empty.properties` by `graph: conf/contrail.properties` and `scripts: [scripts/generate-modern.groovy]` by `scripts: [scripts/empty-sample.groovy]`. Then start the server:

    $ bin/gremlin-server.sh conf/contrail.yaml

If the graph is large, you will need to increase the memory allocated to the JVM (512M by default):

    $ JAVA_OPTIONS="-Xmx2048m -Xms512m" bin/gremlin-server.sh conf/contrail.yaml

### Connecting to the server with the gremlin console

    $ wget https://archive.apache.org/dist/tinkerpop/3.2.5/apache-tinkerpop-apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    $ unzip apache-tinkerpop-gremlin-console-3.2.5-bin.zip
    $ cd apache-tinkerpop-gremlin-console-3.2.5
    $ bin/gremlin.sh
             \,,,/
             (o o)
    -----oOOo-(3)-oOOo-----
    plugin activated: tinkerpop.server
    plugin activated: tinkerpop.utilities
    plugin activated: tinkerpop.tinkergraph
    gremlin> :remote connect tinkerpop.server conf/remote.yaml
    ==>Configured localhost/127.0.0.1:8182
    gremlin> :remote console
    ==>All scripts will now be sent to Gremlin Server - [localhost/127.0.0.1:8182] - type ':remote console' to return to local mode
    gremlin> g
    ==>graphtraversalsource[tinkergraph[vertices:9 edges:4], standard]

# Using gremlin-sync

If you are using the gremlin server you can keep it sync with the contrail DB by using `gremlin-sync`. `gremlin-sync` will listen to the contrail rabbitmq exchange and apply changes in the gremlin server.

## Gremlin server additionnal configuration

In the `conf/contrail.yaml` file created before, you have to add:

    processors:
      - { className: org.apache.tinkerpop.gremlin.server.op.standard.StandardOpProcessor, config: { maxParameters: 64 }}

## Usage

With `gremlin-sync` it is possible to keep deleted resources in the gremlin server. Each vertex has `deleted`, `created` and `updated` properties set to the respective timestamp. To keep deleted resources, pass the `--historize` option to `gremlin-sync`.

    $ ./gremlin-sync --rabbit <server> --rabbit-vhost <vhost> --rabbit-user <user> --rabbit-password <pass> --cassandra <server> --gremlin localhost:8182
    12:06:06.631 setupGremlin ▶ NOTI 001 Connected to Gremlin server.
    12:06:06.631 setupCassandra ▶ NOTI 002 Connecting to Cassandra...
    12:06:10.948 setupCassandra ▶ NOTI 003 Connected.
    12:06:10.948 setupRabbit ▶ NOTI 004 Connecting to RabbitMQ...
    12:06:11.099 setupRabbit ▶ NOTI 005 Connected.
    12:06:11.099 setup ▶ NOTI 006 Listening for updates.
    12:06:11.099 setup ▶ NOTI 007 To exit press CTRL+C

## Using gremlin-fsck

`gremlin-fsck` is a contrail-api-cli command. It will run different consistency checks on the gremlin server and clean/fix resources if needed with some `contrail-api-cli-extra` commands.

    cd gremlin-fsck
    pip install -r requirements.txt
    python setup.py install

    contrail-api-cli fsck [--clean] [--loop] ...

See --help for all options.
