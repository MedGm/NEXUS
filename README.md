# NEXUS — Next-generation EXecution & Understanding System

Real-time ML platform: streaming anomaly detection, causal incident intelligence, model lifecycle management, and observability dashboard. Built on Kafka, Postgres, MinIO, MLflow, Airflow, and Next.js.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Docker | ≥ 24.0 |
| Docker Compose | ≥ 2.20 |
| RAM | ≥ 8 GB |
| Disk | ≥ 20 GB |

---

## Quick Start

```bash
cp .env.example .env          # add OPENAI_API_KEY for LLM incident summaries
docker compose pull
docker compose up -d
docker compose logs -f airflow-init kafka-init minio-init   # wait ~2min
```

---

## Service Directory

| Service | URL | Credentials | Purpose |
|---|---|---|---|
| **Dashboard** | http://localhost:3001 | — | Operator UI: trust scores, incidents, failure lab, model aging |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | 5 pre-provisioned dashboards (trust, inference, Kafka, pipeline, exporter) |
| **Airflow** | http://localhost:8080 | `admin` / `admin` | DAG management: aging monitor, retraining, promotion |
| **MLflow** | http://localhost:5000 | — | Model registry + artifact store |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` | Object storage browser |
| **Prometheus** | http://localhost:9090 | — | Metrics + alerting |
| **Schema Registry** | http://localhost:8081 | — | Avro schema catalog |

### Internal APIs

| Service | Port | Key Endpoints |
|---|---|---|
| trust-api | 8000 | `GET /trust/{dataset}/latest` · `GET /metrics` |
| inference-api | 8001 | `POST /predict` · `GET /metrics` |
| incident-engine | 8002 | `GET /incidents` · `POST /events` · `POST /incidents/{id}/resolve` |
| simulator control | 8003 | `GET /control` · `POST /control` |
| Postgres | 5433 | DBs: `nexus_meta` `nexus_metrics` `nexus_incidents` `mlflow` `airflow` |
| MinIO API | 9000 | Buckets: `raw-data` `processed` `model-artifacts` `replay-snapshots` `mlflow` |

---

## Kafka Topics

| Topic | Partitions | Retention | Format |
|---|---|---|---|
| `nexus.raw.events` | 3 | 7d | Avro (TopicRecordNameStrategy) |
| `nexus.validated` | 3 | 7d | Avro |
| `nexus.anomalies` | 3 | 7d | JSON |
| `nexus.incidents` | 3 | 7d | JSON |

---

## Data Flow

```
EventSimulator + ApiPoller
  → nexus.raw.events
      ├── Validator      → nexus_metrics.trust.*
      ├── LakeWriter     → MinIO raw-data/ (JSONL, hourly partitioned)
      └── AnomalyDetector → nexus.anomalies
              → IncidentEngine → nexus_incidents.incidents.incidents
                                → nexus.incidents
InferenceAPI (POST /predict) → nexus_meta.models.inference_log
Airflow aging_monitor_dag    → nexus_meta.models.model_drift_flags
Dashboard (SWR 5s)           → /api/* → all backends
```

---

## Environment Variables

| Variable | Default | Notes |
|---|---|---|
| `POSTGRES_PASSWORD` | `postgres` | Postgres superuser |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO admin |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | Change before network exposure |
| `AIRFLOW_FERNET_KEY` | (set in .env.example) | Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow UI |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana UI |
| `OPENAI_API_KEY` | — | Required for LLM incident summaries (GPT-4o) |
| `SIMULATOR_BASE_RATE` | `10` | Events per second baseline |

---

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `aging_monitor_dag` | `@weekly` | KS-test per model vs training distribution → flags drift |
| `retraining_dag` | manual | Fetch MinIO data → GE validate → retrain HST → shadow deploy |
| `promote_model_dag` | manual | Promote shadow model to production |

---

## Design Decisions

**LocalExecutor over CeleryExecutor** — single-host deployment; no Redis/RabbitMQ overhead needed for 3 DAGs.

**River (online ML) over scikit-learn** — HalfSpaceTrees update in O(1) per event without stopping the producer. Scikit-learn is used only in `retraining_dag` for deliberate full retrains.

**Avro for `nexus.raw.events`, JSON for `nexus.anomalies`** — Avro adds schema governance and compatibility guarantees for the data lake. Anomaly events are internal signals with no external consumers; JSON avoids Schema Registry coupling.

**Next.js proxy routes over direct browser→FastAPI** — eliminates CORS configuration on all backends; decouples frontend URL contract from internal service topology.

**Single-thread IncidentEngine** — incidents are rare (<10/day), graphs are small (<50 nodes); thread-safe in-memory state is simpler than distributed consensus via Redis pub/sub.

**PSI for validator drift, KS for aging monitor** — PSI is binned and bounded [0,1], interpretable as a percentage shift. KS works on continuous raw score distributions from MLflow checkpoints without binning.

---

## Stop / Teardown

```bash
docker compose down          # stop, keep volumes
docker compose down -v       # stop + delete all data
```

---

## Architecture Reference

See [`docs/NEXUS-architecture.md`](docs/NEXUS-architecture.md) for the full system diagram, component table, and Phase 07 roadmap.
