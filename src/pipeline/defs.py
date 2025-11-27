from dagster import Definitions, job, op
from pathlib import Path
import subprocess

# Hitta projektroten (Dataflow/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"
PIPELINE_DIR = PROJECT_ROOT / "src" / "pipeline"


@op
def run_dlt(context):
    """Kör dlt-pipelinen som fyller på DuckDB."""
    script_path = PIPELINE_DIR / "load_job_ads_dlt.py"
    context.log.info(f"Kör dlt-script: {script_path}")
    subprocess.run(["python", str(script_path)], check=True)


@op
def run_dbt(context, _after_dlt):
    """Kör dbt-modellerna mot DuckDB (efter dlt)."""
    context.log.info(f"Kör dbt run i katalog: {DBT_DIR}")
    subprocess.run(
        [
            "dbt",
            "run",
            "--profiles-dir",
            str(Path.home() / ".dbt"),
        ],
        check=True,
        cwd=str(DBT_DIR),
    )


@job
def job_ads_pipeline():
    """Dagster-jobb som först kör dlt, sedan dbt."""
    dlt_result = run_dlt()
    run_dbt(dlt_result)


defs = Definitions(jobs=[job_ads_pipeline])
