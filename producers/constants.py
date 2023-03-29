"""The models constants_"""

###Connector###
KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
CONNECTION_URL = "jdbc postgresql://localhost:5432/cta"
CONNECTION_USER = "cta_admin"
CONNECTION_PASSWORD = "chicago"
TABLE_WHITELIST = "stations"
MODE = "incrementing"
INCREMENTING_COLUMN_NAME = "stop_id"
TOPIC_PREFIX = "org.chicago.cta."
POLL_INTERVAL_MS = 5000