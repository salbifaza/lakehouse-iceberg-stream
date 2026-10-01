# apache/fluss's base image only bundles fluss-lake-iceberg (the connector
# glue). The Iceberg REST catalog client needs, on top of that:
#   - iceberg-aws: actually contains org.apache.iceberg.aws.s3.S3FileIO
#     (iceberg-aws-bundle, despite the name, is just the *shaded AWS SDK*
#     dependencies S3FileIO needs at runtime -- it does NOT contain the
#     S3FileIO class itself. Confirmed by inspecting the jar: zero
#     "S3FileIO" hits anywhere in iceberg-aws-bundle-1.10.1.jar).
#   - iceberg-aws-bundle: the shaded AWS SDK S3FileIO depends on.
#   - hadoop-apache: Hadoop's Configurable class, which
#     org.apache.iceberg.CatalogUtil.loadFileIO's DynConstructors path
#     touches even when S3FileIO (not HadoopFileIO) is the impl actually
#     used -- without it, class loading fails before S3FileIO is even
#     reached, with a misleading "No FileSystem for scheme s3" error.
#   - failsafe: a S3FileIO runtime dependency not pulled in transitively.
# All four placed in FLUSS_HOME/plugins/iceberg/, since the
# CoordinatorServer/TabletServer load the Iceberg catalog directly (not
# just the separate Flink tiering job, which needs the same jars under
# FLINK_HOME/lib -- see flink/Dockerfile).
FROM apache/fluss:0.9.1-incubating

ARG ICEBERG_VERSION=1.10.1

USER root

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar \
    /opt/fluss/plugins/iceberg/iceberg-aws-${ICEBERG_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/fluss/plugins/iceberg/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
ADD https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar \
    /opt/fluss/plugins/iceberg/failsafe-3.3.2.jar
ADD https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar \
    /opt/fluss/plugins/iceberg/hadoop-apache-3.3.5-2.jar

RUN chown -R fluss:fluss /opt/fluss/plugins/iceberg

# Wraps the base image's own entrypoint to source /polaris/creds.env at
# container *start* time (not build/create time). This is necessary, not
# just tidy: Docker Compose creates a container (baking in `env_file`
# contents) as soon as its `depends_on` conditions are satisfied, but
# `env_file` is captured at container *creation*, before polaris-bootstrap
# has necessarily finished writing the real FLUSS_CLIENT_ID/SECRET into
# that file. Confirmed by testing: on a clean `docker compose up`,
# fluss-coordinator's container was created ~60s before polaris-bootstrap
# exited, baking in empty credential values from a still-empty creds.env --
# and since `restart: unless-stopped` restarts the same container rather
# than recreating it, the coordinator kept retrying with permanently empty
# credentials until manually recreated. Sourcing the file ourselves at
# `docker-entrypoint.sh` runtime (every start, every restart) avoids that
# entirely.
COPY docker-entrypoint-wrapper.sh /docker-entrypoint-wrapper.sh
USER root
RUN chmod +x /docker-entrypoint-wrapper.sh
USER fluss
ENTRYPOINT ["/docker-entrypoint-wrapper.sh"]
