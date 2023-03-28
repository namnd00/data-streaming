"""The producer models constants_"""

###Producer constants###
# the broker properties
SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092"
GROUP_ID = "0"
SESSION_TIMEOUT_MS = 30000

# the topic properties
AUTO_OFFSET_RESET = "earliest"
AUTO_CREATE_TOPICS_ENABLE = "true"
CLEANUP_POLICY = "delete"
COMPRESSION_TYPE = "lz4"
DELETE_RETENTION_MS = 100
FILE_DELETE_DELAY_MS = 100

###Station constants###
# station properties
STATION_NUM_PARTIONS = 1
STATION_NUM_REPLICAS = 1
STATION_TOPIC_NAME = "org.chicago.cta.stations.arrivals"

###Turnstile constants###
# turnstile properties
TURNSTILE_NUM_PARTIONS = 1
TURNSTILE_NUM_REPLICAS = 1
TURNSTILE_TOPIC_NAME = "org.chicago.cta.stations.turnstile.v1"

###Weather constants###
# weather properties
REST_PROXY_URL = "http://localhost:8082"
WEATHER_TOPIC_NAME = "org.chicago.cta.weather"