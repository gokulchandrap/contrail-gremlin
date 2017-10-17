FROM openjdk:8-jre

ARG application_version
ARG branch
ARG repo

RUN wget https://archive.apache.org/dist/tinkerpop/${application_version}/apache-tinkerpop-gremlin-console-${application_version}-bin.zip && wget https://archive.apache.org/dist/tinkerpop/${application_version}/apache-tinkerpop-gremlin-console-${application_version}-bin.zip.sha1 && echo "`cat apache-tinkerpop-gremlin-console-${application_version}-bin.zip.sha1` apache-tinkerpop-gremlin-console-${application_version}-bin.zip" | sha1sum -c - && unzip -d /srv/ apache-tinkerpop-gremlin-console-${application_version}-bin.zip && rm -f apache-tinkerpop-gremlin-console-${application_version}-bin.zip && rm -f apache-tinkerpop-gremlin-console-${application_version}-bin.zip.sha1 && mv /srv/apache-tinkerpop-gremlin-console-${application_version} /srv/apache-tinkerpop-gremlin-console

RUN wget ${repo}/raw/${branch}/gremlin-dump -O /usr/local/bin/gremlin-dump && chmod 755 /usr/local/bin/gremlin-dump

ADD checks.groovy /srv/apache-tinkerpop-gremlin-console
ADD run.sh /usr/local/bin/run.sh

WORKDIR /srv/apache-tinkerpop-gremlin-console

ENTRYPOINT ["bash", "/usr/local/bin/run.sh"]
