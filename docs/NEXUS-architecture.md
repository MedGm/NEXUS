# NEXUS — Architecture Reference

**Version:** Phase 06 (2026-05-29)
**Stack:** Kafka · Postgres · MinIO · MLflow · Airflow · Prometheus · Grafana · Next.js

---

## System Diagram

```
External Sources
  ├── EventSimulator (synthetic e-commerce events)       → nexus.raw.events
  └── ApiPoller (CoinGecko prices, 60s)                  → nexus.raw.events

nexus.raw.events (Kafka, 3 partitions, 7d retention)
  ├── Validator   → trust.validation_results + trust.dataset_trust_scores (nexus_metrics)
  │               → nexus.validated (Kafka)
  └── LakeWriter  → MinIO raw-data/ (JSONL, year/month/day/hour partitioned)

nexus.raw.events → AnomalyDetector
  ├── HalfSpaceTrees per event type (River online ML)
  ├── Checkpoints + score_distribution.json → MLflow
  └── → nexus.anomalies (JSON: event_type, anomaly_score, is_anomaly, tier)

nexus.anomalies → IncidentEngine
  ├── CausalGraphBuilder (NetworkX DiGraph)
  ├── 10 causal rules → edges (caused_by / preceded_by / correlated_with)
  ├── Idle timeout (10min) → persist → nexus_incidents.incidents.incidents
  ├── OpenAI gpt-4o → llm_summary
  └── → nexus.incidents (Kafka summary event)

Pollers (within IncidentEngine, 60s each):
  ├── TrustDrop      → trust.dataset_trust_scores delta
  ├── PredictionDrift → models.model_drift_flags
  └── KafkaLag       → Kafka Admin API consumer group offset

Airflow DAGs (@weekly / manual):
  ├── aging_monitor_dag  → KS-test vs MLflow score distribution → model_drift_flags
  ├── retraining_dag     → MinIO fetch → GE validate → retrain HST → shadow deploy
  └── promote_model_dag  → deployment_status=production

InferenceAPI (:8001):
  ├── POST /predict → trust gate → tier routing (production model or IQR fallback)
  ├── shadow scoring → models.shadow_log
  └── → models.inference_log

APIs:
  ├── trust-api       :8000 → GET /trust/{dataset}/latest, GET /metrics
  ├── inference-api   :8001 → POST /predict, GET /metrics
  └── incident-engine :8002 → GET/POST /incidents, GET /health, POST /events

Dashboard (:3001):
  └── Next.js 14 App Router → SWR polls /api/* → FastAPI backends + Prometheus

Monitoring:
  ├── kafka-exporter :9308 → Prometheus :9090 → Grafana :3000
  └── trust-api, inference-api, incident-engine → Prometheus scrape
```

---

## Component Table

| Service | Port | Image | Responsibility |
|---|---|---|---|
| Zookeeper | — | cp-zookeeper:7.6.1 | Kafka coordination |
| Kafka | 9092 | cp-kafka:7.6.1 | Event bus — 4 nexus topics |
| Schema Registry | 8081 | cp-schema-registry:7.6.1 | Avro schema storage (5 subjects) |
| Postgres | 5433 | postgres:16 | 5 DBs: airflow, mlflow, nexus_meta, nexus_metrics, nexus_incidents |
| MinIO | 9000/9001 | minio | Object store — 5 buckets |
| MLflow | 5000 | mlflow:v2.13.2 | Model registry + artifact store |
| Airflow (×2) | 8080 | airflow:2.9.2 | DAG scheduler + webserver |
| simulator | 8003 | nexus-simulator | Synthetic event producer + live control API |
| api-poller | — | nexus-api-poller | CoinGecko price polling |
| lake-writer | — | nexus-lake-writer | Kafka → MinIO JSONL (60s windows) |
| validator | — | nexus-validator | GE expectations + PSI drift + trust scoring |
| anomaly-detector | — | nexus-anomaly-detector | HalfSpaceTrees online anomaly detection |
| inference-api | 8001 | nexus-inference-api | Two-tier prediction + shadow scoring |
| trust-api | 8000 | nexus-trust-api | Trust score read API + Prometheus exposition |
| incident-engine | 8002 | nexus-incident-engine | Causal graph + LLM narratives + incident API |
| kafka-exporter | 9308 | kafka-exporter | Kafka metrics → Prometheus |
| Prometheus | 9090 | prometheus:v2.52.0 | Metrics scraping + alerting |
| Grafana | 3000 | grafana:10.4.3 | Dashboards (5 provisioned) |
| nexus-dashboard | 3001 | nexus-dashboard | Next.js 14 operator UI (4 screens) |

---

## Data Flow

```
1. Raw events arrive at nexus.raw.events (Avro, TopicRecordNameStrategy)
2. Validator consumes → runs GE expectations per batch → computes null_rate + PSI
   → writes validation_results + dataset_trust_scores (trust.*, nexus_metrics)
3. LakeWriter consumes → buffers 60s → flushes to MinIO raw-data/year=.../...
4. AnomalyDetector consumes → scores each event via HalfSpaceTrees
   → publishes to nexus.anomalies (JSON, not Avro)
   → checkpoints model pickle + score_distribution.json to MLflow every 1000 events
5. IncidentEngine consumes nexus.anomalies → builds causal graph
   → also polls Postgres (TrustDrop, PredictionDrift) and Kafka Admin API (KafkaLag)
   → on idle timeout: persists incident → calls OpenAI → publishes to nexus.incidents
6. InferenceAPI serves POST /predict: checks trust gate → loads River model from MLflow
   → scores event → logs to models.inference_log → returns anomaly_score + tier
7. Airflow weekly: aging_monitor_dag runs KS-test → flags drifted models
   → operator triggers retraining_dag → shadow deployment → promote_model_dag
8. Dashboard polls all APIs every 5s → renders 4 screens
```

---

## Design Decisions

### Why LocalExecutor not CeleryExecutor?
Single-host deployment. CeleryExecutor requires Redis/RabbitMQ + worker containers, adding 2+ services for no benefit at this scale. LocalExecutor runs tasks in subprocesses on the scheduler host — correct for the 3-DAG lifecycle use case here.

### Why single-thread IncidentEngine not distributed?
Incidents are rare (< 10/day in production), the graph is small (< 50 nodes), and thread-safe state is simpler than distributed consensus. A multi-process design would require Redis pub/sub for shared `_builder` state — overengineering for the data volume.

### Why PSI over KS for drift detection in the Validator?
PSI (Population Stability Index) is binned and bounded [0, 1], making it interpretable as a percentage shift between reference and observation. KS is used in the Aging Monitor DAG because it works on raw score distributions (continuous) without binning. Both serve different layers.

### Why River (online ML) not scikit-learn?
Events arrive continuously at 10–200/s. Batch retraining on a scikit-learn model requires stopping the world, retraining on a snapshot, and reloading. River's HalfSpaceTrees update in O(1) per event, never stop, and checkpoint to MLflow. Scikit-learn is used only in the retraining DAG where a full retrain from historical data is intentional.

### Why Avro for raw.events but JSON for anomalies?
Schema Registry adds governance to the data lake — schema evolution, compatibility guarantees, cross-team contract. Anomaly events are internal signals between the anomaly-detector and the incident engine; they change frequently during development, have no external consumers, and don't need schema validation. JSON here is deliberate pragmatism.

### Why Next.js proxy routes instead of direct browser→FastAPI?
Direct calls from the browser would require CORS headers on all FastAPI services and expose internal port layout to clients. The proxy layer decouples the frontend URL contract from the backend topology — renaming a service or changing a port is invisible to the browser.

---

## Postgres Schema Summary

| DB | Schema | Key Tables |
|---|---|---|
| nexus_metrics | trust | dataset_trust_scores, validation_results |
| nexus_meta | models | inference_log, model_drift_flags, shadow_log |
| nexus_incidents | incidents | incidents |
| nexus_incidents | audit | (reserved) |
| mlflow | (mlflow internal) | experiments, runs, model_versions |
| airflow | (airflow internal) | dag_run, task_instance |

---

## Phase 07 Roadmap — Fault Injection Replay

> **Fault-Injection Replay Service** (`nexus-fault-injector`)
> - Reads raw JSONL from MinIO for a selected time window
> - Re-publishes to `nexus.replay` (new Kafka topic) with configurable fault modes:
>   - Message drops (0–100% drop rate per topic)
>   - Field corruption (null injection, out-of-range values)
>   - Delay spikes (added sleep per message)
> - AnomalyDetector adds a shadow consumer on `nexus.replay`
> - Detection latency measured from replay timestamp vs anomaly publish timestamp
> - Results stored in `nexus_meta.models.replay_results`
> - Failure Lab UI gains a "Replay Mode" tab calling `POST /replay/start`
>
> Groundwork already in place: `replay-snapshots` MinIO bucket, MinIO JSONL partitioning by hour, incident-engine replay engine (rule re-analysis).
