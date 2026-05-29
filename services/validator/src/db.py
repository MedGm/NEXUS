import logging
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def write_validation_and_trust(
    conn,
    dataset_id: str,
    batch_timestamp: datetime,
    batch_size: int,
    success: bool,
    null_rate: float,
    results_json: dict,
    freshness: float,
    completeness: float,
    drift_psi_score: float | None,
    lineage_depth: int,
    composite_score: float,
) -> None:
    """
    Writes validation_results + dataset_trust_scores in a single transaction.
    Commits the transaction. Caller must NOT commit before calling this.
    Kafka offset commit must happen AFTER this function returns successfully.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trust.validation_results
                (dataset_id, batch_timestamp, batch_size, success, null_rate, results_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                dataset_id,
                batch_timestamp,
                batch_size,
                success,
                null_rate,
                psycopg2.extras.Json(results_json),
            ),
        )
        cur.execute(
            """
            INSERT INTO trust.dataset_trust_scores
                (dataset_id, timestamp, freshness, completeness, drift_psi_score, lineage_depth, composite_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                dataset_id,
                batch_timestamp,
                freshness,
                completeness,
                drift_psi_score,
                lineage_depth,
                composite_score,
            ),
        )
    conn.commit()


def get_reference_window(
    conn,
    dataset_id: str,
    column: str,
    days: int = 7,
) -> list[float]:
    """
    Returns flat list of observed float values for `column` from validation_results
    in the last `days` days. Used to build the PSI reference distribution.

    Extracts values from results_json->'psi_values'->column (a JSON array of floats).
    Returns empty list if no data or column not present.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT jsonb_array_elements_text(
                results_json -> 'psi_values' -> %s
            )::float
            FROM trust.validation_results
            WHERE dataset_id = %s
              AND batch_timestamp > %s
              AND results_json -> 'psi_values' ? %s
            """,
            (column, dataset_id, cutoff, column),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows]
