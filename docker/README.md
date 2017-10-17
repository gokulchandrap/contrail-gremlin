# Running gremlin checks with docker

The provided docker image includes `gremlin-dump`, `gremlin-checks` and the `gremlin-console`.

Once run, the image will make a dump of the contrail DB, load it in the `gremlin-console` and run different consistency checks.

To try it, make sure you have access to your cassandra cluster then run:

    $ docker run -it --net host eonpatapon/contrail-fsck:3.2.5-latest <CASSANDRA_HOST>
    10:35:20.010 setupCassandra ▶ NOTI 001 Connecting to Cassandra...
    10:35:24.119 setupCassandra ▶ NOTI 002 Connected.
    Processing nodes [read:1720 correct:1718 incomplete:0 missing:31 dup:2] D
    Oct 17, 2017 10:35:29 AM java.util.prefs.FileSystemPreferences$1 run
    INFO: Created user preferences directory.

             \,,,/
             (o o)
    -----oOOo-(3)-oOOo-----
    plugin activated: tinkerpop.server
    plugin activated: tinkerpop.utilities
    plugin activated: tinkerpop.tinkergraph
    Loading the graphson file '/srv/apache-tinkerpop-gremlin-console/dump.json'...
    broken references
      routing_instance/58b78816-8df1-4c4f-9854-d618732b984d -> virtual_network/d110bfaa-6a8d-49e0-b527-cf166eef2329
      routing_instance/3285ef82-5e74-433a-9d7b-7a39cc5bce81 -> virtual_network/b3b48af9-ce7b-43bd-94b5-7ad4e393e66b
    snat without any logical-router
    [...]

By default, `CASSANDRA_HOST` is `localhost`.
