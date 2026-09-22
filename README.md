# Step 3: event generator needs just Kafka
docker compose up -d kafka kafka-ui
# Kafka UI at http://localhost:8080

# Step 4: Flink speed layer
docker compose up -d jobmanager taskmanager
# Flink UI at http://localhost:8081

# Step 5-6: schemas + batch layer (needs object storage + metastore + Spark)
docker compose up -d minio hive-metastore-postgres hive-metastore spark-master spark-worker
# MinIO console at http://localhost:9001 (minioadmin / minioadmin)
# Spark master UI at http://localhost:8082

# Step 7: serving + federated queries
docker compose up -d clickhouse trino
# ClickHouse HTTP at http://localhost:8123
# Trino UI at http://localhost:8083


#checking things are alive:

# Kafka: list topics
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# ClickHouse: quick query
curl http://localhost:8123 --data-binary "SELECT 1"

# Trino: connect with the CLI (or use the web UI)
docker exec -it trino trino --catalog hive