# NEXUS Foundation — Completed Tasks

---

## 1. Infrastructure — `docker-compose.yml`

| Service | Image | Port(s) |
|---|---|---|
| Zookeeper | `confluentinc/cp-zookeeper:7.6.1` | — |
| Kafka | `confluentinc/cp-kafka:7.6.1` | 9092 |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.1` | 8081 |
| PostgreSQL | `postgres:16` | 5432 |
| MinIO | `minio/minio` | 9000, 9001 |
| MLflow | `ghcr.io/mlflow/mlflow:v2.13.2` | 5000 |
| Airflow Webserver | `apache/airflow:2.9.2` | 8080 |
| Airflow Scheduler | `apache/airflow:2.9.2` | — |
| Prometheus | `prom/prometheus:v2.52.0` | 9090 |
| Grafana | `grafana/grafana:10.4.3` | 3000 |

---

## 2. Kafka Topics

| Topic | Partitions | Retention |
|---|---|---|
| `nexus.raw.events` | 3 | 7d |
| `nexus.validated` | 3 | 7d |
| `nexus.anomalies` | 3 | 7d |
| `nexus.incidents` | 3 | 7d |

---

## 3. Postgres Schemas

| DB | Schemas |
|---|---|
| `nexus_meta` | `pipelines`, `models` |
| `nexus_metrics` | `trust`, `drift` |
| `nexus_incidents` | `incidents`, `audit` |

(Plus `airflow` and `mlflow` DBs for internal service use.)

---

## 4. MinIO Buckets

| Bucket | Purpose |
|---|---|
| `raw-data` | Ingested events pre-validation |
| `processed` | Validated / transformed data |
| `model-artifacts` | MLflow model binaries, weights |
| `replay-snapshots` | Point-in-time snapshots for replay |
| `mlflow` | MLflow artifact store (internal) |

---

## 5. MLflow Tracking Server

| Check | Status |
|---|---|
| Backend store → Postgres `mlflow` DB | ✓ |
| `MLFLOW_S3_ENDPOINT_URL` → MinIO `:9000` | ✓ |
| `AWS_*` creds → `minioadmin` | ✓ |
| `--artifacts-destination s3://mlflow` | ✓ |
| `--serve-artifacts` (proxy mode) | ✓ |
| Waits for `minio-init` to complete | ✓ |
| Healthcheck on `/health` at `:5000` | ✓ |

---

## 6. Airflow Bootstrap

| Check | Status |
|---|---|
| `LocalExecutor` set | ✓ |
| Postgres metadata DB (`airflow`) | ✓ |
| `airflow-init` runs `db migrate` + creates admin user | ✓ |
| Webserver on `:8080` | ✓ |
| Scheduler with healthcheck | ✓ |
| `./dags` mounted → `/opt/airflow/dags` | ✓ |
| `dags/nexus/` package folder | ✓ |

---

## 8. Phase 02 — Real Data Ingestion

### Common Package (`nexus_common`)

| File | Responsibility |
|---|---|
| `services/common/setup.py` | Package manifest, deps: `confluent-kafka`, `fastavro`, `certifi`, `httpx`, `authlib`, `cachetools`, `attrs`, `sniffio` |
| `services/common/nexus_common/kafka.py` | `make_producer()`, `make_consumer()`, `make_schema_registry_client()` |
| `services/common/nexus_common/avro.py` | `get_serializer()`, `get_deserializer()`, `serialize()`, `deserialize()` — `TopicRecordNameStrategy` defined once |

### Simulator Service

| Check | Status |
|---|---|
| `events.py` — 4 dataclasses with Faker: `OrderPlaced`, `PaymentProcessed`, `SessionStarted`, `RecommendationServed` | ✓ |
| `temporal.py` — `rate_multiplier()`: business hours ×3, weekends ×0.3, micro-bursts ×8 (Poisson λ=0.05/s) | ✓ |
| `simulator.py` — `EventSimulator.next()` weighted dispatch (Session 40%, Order 30%, Payment 20%, Rec 10%) | ✓ |
| `main.py` — Avro serialization, SIGTERM/SIGINT flush, `SIMULATOR_BASE_RATE` env var | ✓ |
| 4 Avro schemas in `src/schemas/` — `TopicRecordNameStrategy` subjects | ✓ |
| 14 tests passing | ✓ |
| Docker image built and running | ✓ |

### API Poller Service

| Check | Status |
|---|---|
| `poller.py` — `fetch_prices()` polls CoinGecko `/simple/price` for bitcoin + ethereum | ✓ |
| `main.py` — sync `httpx.Client`, 60s interval, logs warning on HTTP error, does not crash | ✓ |
| `PriceSnapshot.avsc` — `symbol`, `price_usd`, `market_cap_usd`, `volume_24h_usd`, `pct_change_24h`, `sampled_at` | ✓ |
| 3 tests passing | ✓ |
| Docker image built and running | ✓ |

### Lake-Writer Service

| Check | Status |
|---|---|
| `writer.py` — `LakeWriter` class: time-based 60s windowing, `minio_path()` uses event-time `timestamp` field | ✓ |
| `main.py` — consumer group `nexus-lake-writer`, `consumer.commit()` after each flush, SIGTERM/SIGINT final flush | ✓ |
| MinIO path: `year=YYYY/month=MM/day=DD/hour=HH/<epoch_ms>.jsonl` | ✓ |
| 7 tests passing | ✓ |
| Docker image built and running | ✓ |

### Compose Integration

| Service | Image | Depends On |
|---|---|---|
| `simulator` | built from `services/simulator/Dockerfile` | `kafka-init` completed, `schema-registry` healthy |
| `api-poller` | built from `services/api-poller/Dockerfile` | `kafka-init` completed, `schema-registry` healthy |
| `lake-writer` | built from `services/lake-writer/Dockerfile` | `kafka-init` completed, `minio-init` completed, `schema-registry` healthy |

### End-to-End Verification

| Check | Result |
|---|---|
| Schema Registry subjects | 5 registered: `nexus.raw.events-com.nexus.events.{OrderPlaced,PaymentProcessed,RecommendationServed,SessionStarted,PriceSnapshot}` |
| MinIO files | `raw-data/year=2026/month=05/day=27/hour=02/*.jsonl` — 1.4 MiB per flush window |
| JSONL content | Valid JSON per line with `event_id`, `timestamp`, and event-specific fields (Faker data) |
| Kafka consumer lag | Trending to 0 — lake-writer commits after each 60s flush |
| Fixes applied | Postgres port remapped `5432→5433` (host conflict); Zookeeper 4LW whitelist added; Compose `$$` escaping for kafka-init vars; `certifi`, `httpx`, `authlib`, `cachetools`, `attrs`, `sniffio` added to common deps |

---

## 9. Phase 03 — Data Trust Scoring

### Postgres Migration

| Check | Status |
|---|---|
| `scripts/migrate-phase03.sql` — idempotent `IF NOT EXISTS` | ✓ |
| `trust.validation_results` — 8 columns, `chk_null_rate` CHECK, index on `(dataset_id, batch_timestamp DESC)` | ✓ |
| `trust.dataset_trust_scores` — `drift_psi` nullable, `CHECK lineage_depth >= 1`, 3 score CHECKs, index on `(dataset_id, timestamp DESC)` | ✓ |
| `nexus-migrate` init container — `restart: "no"`, runs before validator | ✓ |

### Validator Service

| Check | Status |
|---|---|
| `psi.py` — `compute_psi()`: bins, epsilon guard, degenerate distribution, `min_reference_samples` | ✓ |
| `trust.py` — `compute_freshness()`, `score_from_psi()`, `TrustScoreCalculator`, `WEIGHTS` (with/without PSI), `PSI_FLAG_THRESHOLD=0.20`, `PSI_ZERO_THRESHOLD=0.25` | ✓ |
| `validator.py` — `GEValidator`: loads YAML suites, runs GE expectations, computes `null_rate`, extracts `psi_values` into `results_json` | ✓ |
| `db.py` — `write_validation_and_trust()` single Postgres transaction; `get_reference_window()` via `jsonb_array_elements_text` | ✓ |
| `main.py` — consumer group `nexus-validator`, per-type 60s buffers, schema ID → record name via Confluent wire format, Kafka offset commit always last | ✓ |
| 5 expectation YAMLs in `expectations/` — one per event type, with `lineage_depth`, `psi_columns`, `min_reference_batches` | ✓ |
| 29 unit tests passing (6 PSI + 16 trust + 7 validator) | ✓ |
| Docker image built and running | ✓ |

### trust-api Service

| Check | Status |
|---|---|
| `GET /trust/{dataset_id}/latest` — 200 with score JSON, 404 if missing, 503 on DB error | ✓ |
| `GET /metrics` — Prometheus exposition: `nexus_trust_composite`, `nexus_trust_freshness`, `nexus_trust_completeness`, `nexus_psi_flags_total` | ✓ |
| `GET /health` — liveness check | ✓ |
| Background thread polls Postgres every 30s, updates in-memory Gauges | ✓ |
| Docker image built and running on `:8000` | ✓ |

### Compose Integration

| Service | Depends On |
|---|---|
| `nexus-migrate` | `postgres` healthy |
| `validator` | `kafka-init` completed, `schema-registry` healthy, `postgres` healthy, `nexus-migrate` completed |
| `trust-api` | `postgres` healthy, `nexus-migrate` completed |

### End-to-End Verification

| Check | Result |
|---|---|
| Datasets scoring | All 5: `OrderPlaced`, `PaymentProcessed`, `SessionStarted`, `RecommendationServed`, `PriceSnapshot` |
| Composite scores | 0.95–1.0 (healthy synthetic data) |
| Validation rows | 39+ rows in `trust.validation_results`, 8+ batches per dataset |
| `drift_psi` | Non-null from batch 1 — reference window populated during catchup flush |
| `nexus_trust_composite` in Prometheus | 5 series, all scraping correctly |
| `NexusTrustLow` alert rule | Loaded (`nexus_trust_composite < 0.6 for 5m`) |
| Bugs fixed | `nexus_psi_flags_total` was counting inverted — fixed threshold: `drift_psi_score < (1 - FLAG/ZERO)` |
| Known debt | `drift_psi` column stores score (0–1), not raw PSI value — rename when Phase 04 dashboards built |

### Monitoring Changes

| Change | Detail |
|---|---|
| `prometheus.yml` `nexus-services` target | Updated from `[]` to `['trust-api:8000']` |
| `rule_files:` section added | Points to `/etc/prometheus/alerts/*.yml` |
| `monitoring/alerts/trust.yml` | `NexusTrustLow` alert: composite < 0.6 for 5m, severity: warning |
| Prometheus container | Added `./monitoring/alerts:/etc/prometheus/alerts:ro` volume mount |

---

## 10. Phase 04a — Anomaly Detection + Inference Service

### Postgres Migration

| Check | Status |
|---|---|
| `scripts/migrate-phase04-metrics.sql` — `ALTER TABLE trust.dataset_trust_scores RENAME COLUMN drift_psi TO drift_psi_score` | ✓ |
| `scripts/migrate-phase04-meta.sql` — `CREATE TABLE models.inference_log` with index | ✓ |
| `nexus-migrate-04` init container — runs both migrations against `nexus_metrics` + `nexus_meta` | ✓ |
| Phase 03 debt resolved: `drift_psi` → `drift_psi_score` in validator db.py, trust-api db.py, and all SQL | ✓ |

### anomaly-detector Service

| Check | Status |
|---|---|
| `features.py` — `FeatureExtractor`: 5 per-type feature vectors, `inter_arrival_ms` (in-memory per-type tracking), `consumer_lag_ms` (Kafka processing lag), `is_failure` (0/1), `is_new_user` (LRU cache 10k) | ✓ |
| `detector.py` — `AnomalyDetector`: HalfSpaceTrees (n_trees=25, height=8, window_size=250) for 4 e-commerce types; ADWIN for PriceSnapshot; score-before-learn order; per-type threshold override | ✓ |
| `model_store.py` — `ModelRegistry`: loads from MLflow on startup, independent checkpoint every 1000 events per type, temp file cleanup in finally | ✓ |
| `main.py` — consumer group `nexus-anomaly-detector`, `enable.auto.commit: True` (fire-and-forget), Confluent wire format schema ID parsing, publishes JSON to `nexus.anomalies` | ✓ |
| 23 unit tests passing (14 features + 9 detector) | ✓ |
| Docker image built and running | ✓ |

### inference-api Service

| Check | Status |
|---|---|
| `fallback.py` — `StatisticalFallback`: IQR bounds per feature per dataset, warm-up 500 events, memory freed after warmup, resets on restart | ✓ |
| `router.py` — `TwoTierRouter`: trust gate (< 0.6 → 422), lag threshold (≥ 5000ms → fallback), None trust → fallback | ✓ |
| `db.py` — `get_trust_score()` from `nexus_metrics`, `write_inference_log()` to `nexus_meta.models.inference_log` | ✓ |
| `model_store.py` — `InferenceModelStore`: loads 4 HST models from MLflow (PriceSnapshot always fallback — ADWIN read-only unsupported), background refresh every 5min | ✓ |
| `POST /predict` — trust gate → tier routing → River score or IQR fallback → inference_log → response | ✓ |
| `GET /health`, `GET /metrics` — liveness + Prometheus exposition | ✓ |
| Trust score TTL cache 30s via `cachetools.TTLCache` | ✓ |
| Inference log write best-effort (DB error does not fail request) | ✓ |
| 13 unit tests passing (5 fallback + 8 router) | ✓ |
| Docker image built and running on `:8001` | ✓ |

### Compose Integration

| Service | Depends On |
|---|---|
| `nexus-migrate-04` | `postgres` healthy |
| `anomaly-detector` | `kafka-init` completed, `schema-registry` healthy, `nexus-migrate-04` completed |
| `inference-api` | `postgres` healthy, `nexus-migrate-04` completed |

### End-to-End Verification

| Check | Result |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `POST /predict` (OrderPlaced) | `tier:"fallback"`, `trust_score_at_inference: 0.986`, `anomaly_score: 0.0` |
| `nexus.anomalies` topic | Events flowing: PriceSnapshot + SessionStarted with `event_type`, `anomaly_score`, `threshold` |
| Prometheus targets | `up` for `trust-api:8000` and `inference-api:8001` |
| Prometheus metrics | `nexus_inference_total`, `nexus_inference_anomalies_total`, `nexus_anomaly_score` |
| MLflow fix | `ghcr.io/mlflow/mlflow:v2.13.2` missing `psycopg2` and `boto3` — fixed via `pip install` in entrypoint |

### Notes for Phase 04b / Future Tests

| Note | Detail |
|---|---|
| Tier 1 activation | `POST /predict` returns `tier:"fallback"` until anomaly-detector checkpoints 1000 events per type to MLflow (~100s at 10 events/sec base rate). After checkpoint + inference-api 5min refresh, tier 1 activates automatically |
| PriceSnapshot always fallback | ADWIN requires stateful `update()` — incompatible with read-only inference-api. anomaly-detector still trains ADWIN and publishes to `nexus.anomalies` |
| `drift_psi_score` rename | Phase 03 debt resolved — all code and SQL updated. `GET /trust/{id}/latest` now returns `drift_psi_score` key (breaking change for any existing API consumers) |
| `models.inference_log` | Written on every `/predict` call — feeds Phase 04b `ModelAgingMonitor` KS-test and shadow deployment comparisons |
| `MLFLOW_HTTP_REQUEST_MAX_RETRIES=1` | Added to inference-api compose env to prevent 16-minute cold-start timeout when MLflow is temporarily unavailable |

### Monitoring Changes

| Change | Detail |
|---|---|
| `prometheus.yml` `nexus-services` targets | Updated from `['trust-api:8000']` to `['trust-api:8000', 'inference-api:8001']` |

---

## 11. Phase 04b — Model Lifecycle Management

### Postgres Migration

| Check | Status |
|---|---|
| `scripts/migrate-phase04b.sql` — `models.model_drift_flags` + `models.shadow_log` in `nexus_meta` | ✓ |
| `model_drift_flags` — `status` CHECK (`flagged`/`shadow`/`promoted`/`dismissed`), index on `(event_type, status, detected_at DESC)` | ✓ |
| `shadow_log` — FK to `model_drift_flags(id)`, index on `(drift_flag_id, timestamp DESC)` | ✓ |
| `nexus-migrate-04b` init container — `restart: "no"`, runs against `nexus_meta` | ✓ |

### anomaly-detector Changes

| Check | Status |
|---|---|
| `SCORE_BUFFER_SIZE = 250` — rolling buffer per event type accumulates last 250 anomaly scores | ✓ |
| `_get_score_distribution()` — returns `{scores, n_samples, event_type, captured_at_ms}` | ✓ |
| `_checkpoint()` — writes `score_distribution.json` alongside pickled model as second MLflow artifact | ✓ |
| 28 unit tests passing (23 existing + 5 score buffer) | ✓ |

### inference-api Changes

| Check | Status |
|---|---|
| `model_store.py` — `_shadow_models`/`_shadow_versions` dicts; `_load_one()` uses `search_model_versions` + `deployment_status` tag to find production vs shadow; `get_shadow()` accessor | ✓ |
| `db.py` — `get_active_shadow_flag_id()` + `write_shadow_log()` added | ✓ |
| `main.py` — shadow scoring in `predict()` after production result, before return; `_log_shadow()` best-effort (try/except); `_shadow_flag_ids` cache | ✓ |
| PriceSnapshot always fallback in inference-api (ADWIN read-only incompatible) | ✓ |
| 19 unit tests passing (13 existing + 6 shadow model store) | ✓ |

### Airflow DAGs

| File | Schedule | Purpose |
|---|---|---|
| `dags/nexus/aging_monitor_dag.py` | `@weekly` | KS-test per event type vs training reference; writes `model_drift_flags`, sets MLflow tag `drift_flag=true` |
| `dags/nexus/retraining_dag.py` | manual trigger | 6-task pipeline: `get_flag_id` → `fetch_data` (MinIO) → `validate_data` (GE) → `retrain_model` → `register_shadow` → `await_shadow_completion` |
| `dags/nexus/promote_model_dag.py` | manual trigger | Sets `deployment_status=production`, clears drift flag to `promoted` |
| `dags/nexus/dag_helpers.py` | — | `compute_ks()`, `detect_event_type()`, `extract_features_for_type()` — testable without Airflow |
| `dags/nexus/expectations/` | — | 5 GE YAML copies from validator service (for retraining validation step) |
| `dags/tests/test_dag_helpers.py` | — | 11 unit tests for KS math, event type detection, feature extraction |

### Airflow Setup

| Change | Detail |
|---|---|
| `_PIP_ADDITIONAL_REQUIREMENTS` in `x-airflow-common` | `scipy river>=0.21.0 mlflow>=2.13.0 boto3 psycopg2-binary great-expectations>=0.18.0,<0.19 pyyaml` |
| Env vars added | `POSTGRES_META_DSN`, `MLFLOW_TRACKING_URI`, `MINIO_*`, `KS_THRESHOLD=0.15`, `KS_MIN_SAMPLES=50`, `SHADOW_COMPLETION_N=1000` |
| `.airflowignore` | Excludes `dags/venv/` and `dags/tests/` from DAG scanning |
| Airflow `start_date` fix | All 3 DAGs require `start_date` in Airflow 2.x — added `pendulum.datetime(2025,1,1)` |
| Import fallback | DAGs use `try/except ImportError` for relative vs absolute import (Airflow DagBag scans files individually) |

### Lifecycle Flow

| Step | Trigger | Result |
|---|---|---|
| anomaly-detector checkpoint | Every 1000 events | `score_distribution.json` stored in MLflow alongside model pickle |
| `aging_monitor_dag` | @weekly | KS-stat per type; if > 0.15 → `model_drift_flags` row + MLflow tag |
| `retraining_dag` | Operator manual | Fetch raw events from MinIO → GE validate → retrain HST → shadow deploy to MLflow |
| inference-api refresh | Every 5min | Loads shadow model via `deployment_status=shadow` tag; shadow scoring runs in `predict()` |
| `promote_model_dag` | Operator manual (after reviewing shadow metrics) | `deployment_status=production`; drift flag resolved |

### Test Counts

| Service | Tests |
|---|---|
| `services/anomaly-detector/` | 28 passing |
| `services/inference-api/` | 19 passing |
| `dags/` | 11 passing |

---

## 12. Phase 05 — Incident Intelligence

### Postgres Migration

| Check | Status |
|---|---|
| `scripts/migrate-phase05.sql` — `incidents.incidents` table in `nexus_incidents` | ✓ |
| Columns: `id` (UUID PK), `started_at`, `resolved_at`, `root_cause_node_id`, `causal_graph_json` (JSONB), `llm_summary`, `severity`, `node_count`, `status` (default `'open'`), `created_at` | ✓ |
| `chk_inc_severity` CHECK (`LOW`/`MEDIUM`/`HIGH`), `chk_inc_status` CHECK (`open`/`resolved`) | ✓ |
| Index on `(started_at DESC)`, index on `(status, started_at DESC)` | ✓ |
| `nexus-migrate-05` init container — `restart: "no"`, runs against `nexus_incidents` | ✓ |

### Node Taxonomy (`taxonomy.py`)

| Node Type | Source | Severity Logic |
|---|---|---|
| `ErrorRateSpike` | `nexus.anomalies` Kafka — `is_anomaly=True` | HIGH >0.85, MEDIUM >0.75, LOW else |
| `TrustDrop` | `trust.dataset_trust_scores` Postgres — composite_score drop delta | HIGH >0.30, MEDIUM >0.15, LOW else |
| `PredictionDrift` | `models.model_drift_flags` Postgres — new `status='flagged'` row | Always MEDIUM |
| `KafkaLag` | Kafka Admin API — `nexus-anomaly-detector` group lag | HIGH >10000, MEDIUM >5000, LOW else |
| `SchemaChange` | `POST /events` manual injection | Default MEDIUM, override via payload |
| `Deploy` | `POST /events` manual injection | Default MEDIUM, override via payload |

### Causal Rules (`rules.py`)

| ID | Source | Target | Edge | Window | Same dataset? |
|---|---|---|---|---|---|
| R01 | `SchemaChange` | `KafkaLag` | `caused_by` | 5 min | no |
| R02 | `SchemaChange` | `ErrorRateSpike` | `caused_by` | 10 min | yes |
| R03 | `KafkaLag` | `TrustDrop` | `caused_by` | 10 min | no |
| R04 | `ErrorRateSpike` | `TrustDrop` | `caused_by` | 15 min | yes |
| R05 | `TrustDrop` | `PredictionDrift` | `caused_by` | 30 min | yes |
| R06 | `Deploy` | `SchemaChange` | `preceded_by` | 30 min | no |
| R07 | `Deploy` | `ErrorRateSpike` | `preceded_by` | 60 min | no |
| R08 | `SchemaChange` | `PredictionDrift` | `correlated_with` | 60 min | no |
| R09 | `KafkaLag` | `ErrorRateSpike` | `correlated_with` | 5 min | no |
| R10 | `ErrorRateSpike` | `ErrorRateSpike` | `correlated_with` | 5 min | no (different dataset required) |

### incident-engine Service

| Check | Status |
|---|---|
| `taxonomy.py` — 8 functions: `severity_for_*()`, `anomaly_event_to_node()`, `node_from_event_payload()`, `make_*_node()` | ✓ |
| `rules.py` — `CausalRule` frozen dataclass, `CAUSAL_RULES` (10 rules), `apply_rules(graph, new_node)` | ✓ |
| `graph.py` — `CausalGraphBuilder`: thread-safe `add_node()`, `to_json()`, `max_severity()`, `root_cause_node_id()`; `find_root_cause_node()` prefers in_degree==0, highest severity, earliest timestamp | ✓ |
| `db.py` — `persist_incident()`, `get_incident()`, `list_incidents()`, `resolve_incident()`, `update_llm_summary()`, `get_last_trust_scores()`, `get_new_drift_flags()` | ✓ |
| `narrator.py` — `generate_summary()`: OpenAI gpt-4o, 3-sentence prompt, lazy client init, returns None on failure | ✓ |
| `replay.py` — `replay_incident()`: fetches MinIO `raw-data/` for incident time window, re-runs `apply_rules()`, returns `{original_graph, replay_graph, added_edges, removed_edges, root_cause_changed}` — no side effects | ✓ |
| `consumer.py` — 5 daemon threads: Kafka consumer (`nexus.anomalies`), incident closer (30s check, 10min idle timeout), TrustDrop poller (60s), PredictionDrift poller (60s), KafkaLag poller (60s) | ✓ |
| Single global `CausalGraphBuilder` + `threading.Lock()`; incident opens when `apply_rules()` produces ≥1 edge; closes and persists on idle timeout | ✓ |
| `main.py` — FastAPI on `:8002`: 8 endpoints + `on_event("startup")` starts background threads | ✓ |
| Docker image built and running | ✓ |

### API Endpoints

| Endpoint | Behavior |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `GET /metrics` | Prometheus exposition: `nexus_open_incidents`, `nexus_closed_incidents_total` |
| `GET /incidents?status=&limit=` | List incidents with id/severity/status/node_count/root_cause_node_id |
| `GET /incidents/{id}` | Full row including `causal_graph_json` + `llm_summary` |
| `GET /incidents/{id}/graph` | `{nodes: [...], edges: [...]}` JSON |
| `GET /incidents/{id}/summary` | LLM narrative (lazy-generates from OpenAI if NULL) |
| `POST /incidents/{id}/replay` | Re-analyze with current rules, return diff |
| `POST /incidents/{id}/resolve` | UPDATE `status='resolved'`, set `resolved_at` |
| `POST /events` | Inject `Deploy` or `SchemaChange` node manually |

### Compose Integration

| Service | Port | Depends On |
|---|---|---|
| `nexus-migrate-05` | — | `postgres` healthy |
| `incident-engine` | `8002` | `kafka-init` completed, `schema-registry` healthy, `postgres` healthy, `nexus-migrate-05` completed |

### End-to-End Verification

| Check | Result |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `POST /events` (Deploy) | `{"node_id":"...","type":"Deploy","injected":true}` |
| `POST /events` (SchemaChange) | R06 fires → incident opened in logs |
| `GET /incidents` | Array with persisted incidents |
| `GET /incidents/{id}/graph` | `{nodes:[...], edges:[{rule_id:"R06",...}]}` confirmed |
| `GET /incidents/{id}/summary` | `{"llm_summary": null}` (expected — `OPENAI_API_KEY` not set in dev) |
| `POST /incidents/{id}/replay` | Returns `{original_graph, replay_graph, added_edges, removed_edges, root_cause_changed}` |
| `nexus.incidents` Kafka topic | Events flowing: `incident_id`, `severity`, `node_count`, `closed_at` |
| Prometheus target | `up` for `incident-engine:8002` after prometheus restart |
| Unit tests | 36 passing (17 taxonomy + 11 rules + 8 graph) |

### Monitoring Changes

| Change | Detail |
|---|---|
| `prometheus.yml` `nexus-services` targets | Updated to `['trust-api:8000', 'inference-api:8001', 'incident-engine:8002']` |
| `.env.example` | Added `OPENAI_API_KEY=` |

### Notes

| Note | Detail |
|---|---|
| OPENAI_API_KEY | Not set in dev — `llm_summary` stores NULL; lazy-generates on `GET /incidents/{id}/summary` once key is added to `.env` |
| Ambient anomaly absorption | On first start, incident absorbs all queued `nexus.anomalies` messages from prior phases — expected behavior |
| `INCIDENT_IDLE_TIMEOUT_SECONDS` | Hardcoded `"600"` in compose; env override via `docker compose up` does not work — use `docker run` or edit compose for testing |

---

## 13. Phase 06 — Dashboard & Visibility

### Simulator Control Endpoint

| Check | Status |
|---|---|
| `services/simulator/src/main.py` — `_config` dict (`base_rate`, `burst_probability`, `burst_size`) + `_config_lock` at module level | ✓ |
| `control_app = FastAPI()` — `GET /control` returns config, `POST /control` updates fields present in body | ✓ |
| `ControlBody` Pydantic model — all 3 fields optional for partial updates | ✓ |
| `__main__` block — producer loop in daemon thread + uvicorn on `SIMULATOR_CONTROL_PORT` (default 8003) | ✓ |
| Simulator ports `8003:8003` + `SIMULATOR_CONTROL_PORT: "8003"` in compose | ✓ |
| 6 unit tests passing (`test_control.py`) | ✓ |
| Live on `http://localhost:8003/control` | ✓ |

### Dashboard Service (`nexus-dashboard`)

| File | Responsibility |
|---|---|
| `services/dashboard/Dockerfile` | `FROM node:20-alpine`, `npm ci`, `CMD ["npm", "run", "dev"]` |
| `services/dashboard/package.json` | next@14.2.3, swr, recharts, react-force-graph-2d, @radix-ui/react-slider, pg |
| `services/dashboard/next.config.js` | `serverRuntimeConfig` — TRUST_API_URL, INCIDENT_API_URL, PROMETHEUS_URL, POSTGRES_META_DSN, SIMULATOR_URL |
| `app/api/trust/route.ts` | Fetches trust-api:8000/metrics + Prometheus lag query → `{datasets, kafka_lag}` |
| `app/api/incidents/route.ts` | Proxies incident-engine:8002/incidents |
| `app/api/incidents/[id]/route.ts` | GET detail + POST resolve |
| `app/api/models/route.ts` | Queries Postgres nexus_meta (inference_log + model_drift_flags) directly via `pg.Client` |
| `app/api/control/route.ts` | GET/POST → simulator:8003/control |
| `app/layout.tsx` | Dark sidebar nav: Overview / Incidents / Failure Lab / Models |
| `app/page.tsx` | Redirect → /overview |

### Screens

| Screen | Route | Key Features |
|---|---|---|
| Overview | `/overview` | 5 trust score cards with rolling sparklines (20 readings, 5s), Kafka lag gauge, active incidents count |
| Incidents | `/incidents` | Sortable table: severity badge (HIGH/MEDIUM/LOW), status badge, node count, root cause |
| Incident Detail | `/incidents/[id]` | `react-force-graph-2d` causal graph (6 node types, colored), LLM summary panel, "Mark Resolved" button |
| Failure Lab | `/failure-lab` | 3 Radix UI sliders → `POST /api/control`, live anomaly pressure chart (1 − avg trust, 60 readings) |
| Model Aging | `/models` | Per-model score sparklines, drift status badge (6 states), "Promote in Airflow →" link |

### Compose Integration

| Service | Port | Depends On |
|---|---|---|
| `nexus-dashboard` | `3001` | `trust-api` started, `incident-engine` started, `postgres` healthy |

### Grafana Dashboards

| Dashboard | UID | Key Panels |
|---|---|---|
| NEXUS Trust Score Trends | `nexus-trust` | composite trust per dataset (timeseries), PSI flag count (stat), freshness (timeseries) |
| NEXUS Inference & Anomaly Scores | `nexus-inference` | `nexus_anomaly_score` per dataset, `nexus_inference_total` by tier, anomaly detections |
| NEXUS Kafka Health | `nexus-kafka` | `kafka_consumergroup_lag{consumergroup="nexus-anomaly-detector"}`, total lag stat, offset rate |
| NEXUS Pipeline Throughput | `nexus-pipeline` | events/s on nexus.raw.events + nexus.anomalies, all nexus topics offset rate |

Datasource provisioning updated: `uid: prometheus` added to `monitoring/grafana/provisioning/datasources/prometheus.yml` so dashboard JSON can reference it by stable UID.

### Documentation

| File | Content |
|---|---|
| `docs/NEXUS-architecture.md` | 158 lines: ASCII system diagram, 19-row component table, 8-step data flow, 6 design decisions (LocalExecutor, online ML, PSI vs KS, Avro vs JSON, Next.js proxy, IncidentEngine threading), Postgres schema summary, Phase 07 fault-injection replay roadmap |

### End-to-End Verification

| Check | Result |
|---|---|
| `http://localhost:3001/overview` | Trust cards with live scores, Kafka lag, incident count |
| `http://localhost:3001/incidents` | Incident table showing 1 MEDIUM incident |
| `http://localhost:3001/failure-lab` | Sliders functional, `POST /api/control` → simulator updates in real time |
| `http://localhost:3001/models` | 5 model rows with Healthy status, Airflow link |
| `http://localhost:3000` | 5 Grafana dashboards (4 new + Kafka Exporter Overview) |
| Simulator control | `curl http://localhost:8003/control` returns `{base_rate:10.0,...}` |
| Bugs fixed | Failure Lab chart: `useRef` → `useState` for anomaly history (useRef doesn't trigger re-renders) |
| MLflow healthcheck | Fixed: `curl` → `python urllib.request` (curl not in mlflow image) |

---

## 7. Prometheus + Grafana Monitoring

| Change | Detail |
|---|---|
| `kafka-exporter` service | `danielqsj/kafka-exporter`, port `9308`, connects to `kafka:29092` |
| `prometheus.yml` fixed | `kafka:9999` (broken raw JMX) → `kafka-exporter:9308` |
| MinIO metrics | `minio:9000/minio/v2/metrics/cluster` |
| `nexus-services` scrape block | Placeholder — add app containers as built |
| Grafana datasource provisioner | Prometheus auto-wired on startup |
| Grafana dashboard provisioner | Auto-loads JSONs from provisioning folder |
| `kafka-overview.json` | Grafana dashboard 7589 "Kafka Exporter Overview" — 5 panels |
