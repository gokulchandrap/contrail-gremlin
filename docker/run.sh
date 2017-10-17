#!/bin/bash

CASSANDRA_HOST=${1:-localhost}

gremlin-dump --cassandra ${CASSANDRA_HOST} dump.json && bin/gremlin.sh -i checks.groovy dump.json
