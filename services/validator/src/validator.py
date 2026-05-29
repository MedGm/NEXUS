import os
import glob
import yaml
import pandas as pd
import great_expectations as gx
import logging

log = logging.getLogger(__name__)


class GEValidator:
    def __init__(self, expectations_dir: str):
        self._suites: dict[str, dict] = {}
        for path in glob.glob(os.path.join(expectations_dir, "*.yml")):
            with open(path) as f:
                suite = yaml.safe_load(f)
            self._suites[suite["dataset_id"]] = suite

    def validate(self, dataset_id: str, records: list[dict]) -> dict:
        suite = self._suites.get(dataset_id)
        if suite is None:
            raise ValueError(f"No expectation suite for dataset_id={dataset_id!r}")

        df = pd.DataFrame(records)
        ge_df = gx.from_pandas(df)

        exp_results = []
        overall_success = True
        for exp in suite.get("expectations", []):
            try:
                method = getattr(ge_df, exp["type"])
                result = method(**exp.get("kwargs", {}))
                exp_results.append({
                    "type": exp["type"],
                    "kwargs": exp.get("kwargs", {}),
                    "success": result.success,
                })
                if not result.success:
                    overall_success = False
            except Exception as e:
                log.warning("Expectation %s failed with error: %s", exp["type"], e)
                exp_results.append({"type": exp["type"], "kwargs": exp.get("kwargs", {}), "success": False})
                overall_success = False

        # null_rate = total_null_fields / (total_fields * batch_size)
        batch_size = len(records)
        total_cells = len(df.columns) * batch_size if batch_size > 0 else 1
        null_count = int(df.isnull().sum().sum())
        null_rate = null_count / total_cells

        # Extract raw values for psi_columns — needed by PSI reference window queries
        psi_values: dict[str, list[float]] = {}
        for col in suite.get("psi_columns", []):
            if col in df.columns:
                psi_values[col] = [float(v) for v in df[col].dropna().tolist()]

        return {
            "dataset_id": dataset_id,
            "batch_size": batch_size,
            "success": overall_success,
            "null_rate": float(null_rate),
            "results_json": {
                "expectations": exp_results,
                "psi_values": psi_values,
            },
        }

    def get_suite_meta(self, dataset_id: str) -> dict:
        suite = self._suites.get(dataset_id, {})
        return {
            "lineage_depth": suite.get("lineage_depth", 1),
            "psi_columns": suite.get("psi_columns", []),
            "min_reference_batches": suite.get("min_reference_batches", 10),
        }
