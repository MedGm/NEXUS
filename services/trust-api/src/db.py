import os
import psycopg2
import psycopg2.extras


def get_conn():
    return psycopg2.connect(os.environ["POSTGRES_DSN"])


def get_latest_trust_score(conn, dataset_id: str) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT dataset_id, timestamp, freshness, completeness,
                   drift_psi_score, lineage_depth, composite_score
            FROM trust.dataset_trust_scores
            WHERE dataset_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (dataset_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_all_latest_trust_scores(conn) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (dataset_id)
                dataset_id, timestamp, freshness, completeness,
                drift_psi_score, lineage_depth, composite_score
            FROM trust.dataset_trust_scores
            ORDER BY dataset_id, timestamp DESC
            """
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_psi_flag_count(conn, dataset_id: str, psi_flag_threshold: float = 0.20, psi_zero_threshold: float = 0.25) -> int:
    # drift_psi_score in DB is a score (0-1, high=good): score = 1 - raw_psi / psi_zero_threshold
    # raw_psi > psi_flag_threshold  ↔  score < 1 - psi_flag_threshold / psi_zero_threshold
    score_threshold = 1.0 - psi_flag_threshold / psi_zero_threshold
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM trust.dataset_trust_scores
            WHERE dataset_id = %s AND drift_psi_score IS NOT NULL AND drift_psi_score < %s
            """,
            (dataset_id, score_threshold),
        )
        row = cur.fetchone()
    return row[0] if row else 0
