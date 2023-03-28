"""The consumer constants."""

###Faust Stream constants###
FAUST_APP_NAME = "stations-stream"
FAUST_BROKER_URL = "kafka://localhost:9092"
FAUST_STORE = "memory://"
FAUST_STREAM_TOPIC = "org.chicago.cta.stations"
FAUST_OUTPUT_TOPIC = "org.chicago.cta.stations.table.v1"
FAUST_OUTPUT_TOPIC_PARTITION = 1
FAUST_TABLE_PARTITION = 4

###KSQL constants###
KSQL_URL = "http://localhost:8088"
KSQL_TURNSTILE_TOPIC_NAME = "org.chicago.cta.stations.turnstile.v1"

###Consumer constants###
CONSUMER_GROUP_ID = "0"
CONSUMER_AUTO_OFFSET_RESET = "earliest"
CONSUMER_BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092"
ARVO_BROKER_URL = "http://localhost:8081"