# Kafka Connect with the JDBC connector installed.
#
# Since Confluent Platform 6.0 the cp-kafka-connect image ships with no
# connectors at all, so the JDBC one has to be added explicitly. The Confluent
# Hub package already bundles the PostgreSQL driver (postgresql-42.7.12.jar),
# so there is no need to download it separately.
FROM confluentinc/cp-kafka-connect:8.3.2

USER root
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.9.9
USER appuser
