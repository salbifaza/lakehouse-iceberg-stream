# Flink 1.20 + Flink CDC (postgres source, fluss sink) + Fluss Flink connector
# + Fluss Iceberg tiering connector + S3 filesystem support, all baked into
# one image so jobmanager/taskmanager/sql-client/tiering-job containers share
# identical dependencies. Mirrors the "prebuilt image, zero manual jar
# wrangling" approach Fluss's own upstream issue #2683 argues for, instead of
# the fragile "download 8 jars separately, hope curl didn't truncate one"
# quickstart pattern.
FROM flink:1.20-scala_2.12-java11

ARG FLUSS_VERSION=0.9.1-incubating
ARG ICEBERG_VERSION=1.10.1
ARG FLINK_CDC_VERSION=3.6.0-1.20

USER root

# ── Fluss <-> Flink table/SQL connector (lets Flink SQL / the tiering job
# talk to the Fluss cluster as a catalog and read/write Fluss tables) ───────
ADD https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/${FLUSS_VERSION}/fluss-flink-1.20-${FLUSS_VERSION}.jar \
    /opt/flink/lib/fluss-flink-1.20-${FLUSS_VERSION}.jar

# ── Fluss datalake tiering: Iceberg lake connector + tiering service jar ──
ADD https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/${FLUSS_VERSION}/fluss-lake-iceberg-${FLUSS_VERSION}.jar \
    /opt/flink/lib/fluss-lake-iceberg-${FLUSS_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar \
    /opt/flink/opt/fluss-flink-tiering-${FLUSS_VERSION}.jar

# ── Fluss S3 filesystem plugin (Fluss client side needs this too, since the
# Flink SQL client / tiering job read Fluss's remote (tiered) log segments
# directly from S3-compatible storage) ─────────────────────────────────────
ADD https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-s3/${FLUSS_VERSION}/fluss-fs-s3-${FLUSS_VERSION}.jar \
    /opt/flink/lib/fluss-fs-s3-${FLUSS_VERSION}.jar

# ── Iceberg AWS FileIO (S3-compatible storage) + Hadoop, required by the
# tiering service to commit Iceberg snapshots/metadata to SeaweedFS.
# iceberg-aws-bundle alone is NOT enough -- despite the name it's just the
# shaded AWS SDK dependencies; org.apache.iceberg.aws.s3.S3FileIO itself
# lives in the separate iceberg-aws jar (confirmed: iceberg-aws-bundle has
# zero S3FileIO class entries). See fluss/Dockerfile for the same finding
# and the Hadoop-jar rationale, which applies identically here. ───────────
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar \
    /opt/flink/lib/iceberg-aws-${ICEBERG_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar
ADD https://repo1.maven.org/maven2/dev/failsafe/failsafe/3.3.2/failsafe-3.3.2.jar \
    /opt/flink/lib/failsafe-3.3.2.jar
ADD https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar \
    /opt/flink/lib/hadoop-apache-3.3.5-2.jar

# ── Flink CDC: standalone CLI (flink-cdc.sh) + Postgres source connector +
# Fluss sink connector, used to run the Postgres -> Fluss pipeline job ────
RUN mkdir -p /opt/flink-cdc
ADD https://dist.apache.org/repos/dist/release/flink/flink-cdc-3.6.0/flink-cdc-3.6.0-1.20-bin.tar.gz \
    /tmp/flink-cdc.tar.gz
RUN tar -xzf /tmp/flink-cdc.tar.gz -C /opt/flink-cdc --strip-components=1 && \
    rm /tmp/flink-cdc.tar.gz
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/${FLINK_CDC_VERSION}/flink-cdc-pipeline-connector-postgres-${FLINK_CDC_VERSION}.jar \
    /opt/flink-cdc/lib/flink-cdc-pipeline-connector-postgres-${FLINK_CDC_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-fluss/${FLINK_CDC_VERSION}/flink-cdc-pipeline-connector-fluss-${FLINK_CDC_VERSION}.jar \
    /opt/flink-cdc/lib/flink-cdc-pipeline-connector-fluss-${FLINK_CDC_VERSION}.jar

ENV FLINK_CDC_HOME=/opt/flink-cdc
ENV PATH="/opt/flink-cdc/bin:${PATH}"

RUN chown -R flink:flink /opt/flink/lib /opt/flink/opt /opt/flink-cdc

# ── Pipeline definition + submit helpers ───────────────────────────────────
RUN mkdir -p /opt/flink-cdc-pipelines
COPY postgres-to-fluss.yaml /opt/flink-cdc-pipelines/postgres-to-fluss.yaml
COPY run-cdc-pipeline.sh /opt/flink-cdc-pipelines/run-cdc-pipeline.sh
COPY run-tiering-service.sh /opt/flink-cdc-pipelines/run-tiering-service.sh
COPY run-medallion.sh /opt/flink-cdc-pipelines/run-medallion.sh
COPY medallion/ /opt/flink-cdc-pipelines/medallion/
RUN chmod +x /opt/flink-cdc-pipelines/run-cdc-pipeline.sh \
             /opt/flink-cdc-pipelines/run-tiering-service.sh \
             /opt/flink-cdc-pipelines/run-medallion.sh && \
    chown -R flink:flink /opt/flink-cdc-pipelines

USER flink
