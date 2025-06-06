import datetime
import statistics
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import bigquery
import great_expectations as ge

# ---------------------------------------------------------------------------
# Configuration placeholders
# ---------------------------------------------------------------------------
@dataclass
class TableConfig:
    name: str
    date_column: str
    primary_keys: List[str]
    numeric_ranges: Dict[str, Tuple[Optional[float], Optional[float]]]

@dataclass
class DatabaseConfig:
    type: str  # 'postgres' or 'bigquery'
    connection: str  # SQLAlchemy connection string or BigQuery project
    tables: List[TableConfig] = field(default_factory=list)
    # For BigQuery, this can also store credentials path if needed

# Example configuration; replace with real values
DATABASES = {
    "local_postgres": DatabaseConfig(
        type="postgres",
        connection="postgresql+psycopg2://user:pass@localhost/dbname",
        tables=[
            TableConfig(
                name="public.example_table",
                date_column="created_at",
                primary_keys=["id"],
                numeric_ranges={"value": (0, None)},
            )
        ],
    ),
    "bigquery": DatabaseConfig(
        type="bigquery",
        connection="your-gcp-project",
        tables=[
            TableConfig(
                name="dataset.example_table",
                date_column="created_at",
                primary_keys=["id"],
                numeric_ranges={"metric": (0, 100)},
            )
        ],
    ),
    "carbon_postgres": DatabaseConfig(
        type="postgres",
        connection="postgresql+psycopg2://user:pass@remote_host/carbon",
        tables=[
            TableConfig(
                name="public.carbon_table",
                date_column="created_at",
                primary_keys=["id"],
                numeric_ranges={"measurement": (0, None)},
            )
        ],
    ),
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_pg_engine(conn_string: str):
    """Create SQLAlchemy engine for PostgreSQL."""
    return create_engine(conn_string)


def get_bq_client(project: str):
    """Create BigQuery client."""
    return bigquery.Client(project=project)


def fetch_counts_pg(engine, table: str, date_col: str, start: datetime.date, end: datetime.date) -> pd.DataFrame:
    query = text(
        f"""
        SELECT {date_col}::date AS dt, COUNT(*) AS ct
        FROM {table}
        WHERE {date_col} >= :start AND {date_col} < :end
        GROUP BY dt
        ORDER BY dt
        """
    )
    with engine.begin() as conn:
        df = pd.DataFrame(conn.execute(query, {"start": start, "end": end}).fetchall(), columns=["date", "count"])
    return df


def fetch_counts_bq(client, table: str, date_col: str, start: datetime.date, end: datetime.date) -> pd.DataFrame:
    query = f"""
        SELECT {date_col} AS dt, COUNT(1) AS ct
        FROM `{table}`
        WHERE {date_col} >= @start AND {date_col} < @end
        GROUP BY dt
        ORDER BY dt
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start.isoformat()),
            bigquery.ScalarQueryParameter("end", "DATE", end.isoformat()),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe()
    df.columns = ["date", "count"]
    return df


def fetch_daily_data_pg(engine, table: str, date_col: str, day: datetime.date) -> pd.DataFrame:
    query = text(
        f"""
        SELECT * FROM {table}
        WHERE {date_col} >= :day AND {date_col} < :next_day
        """
    )
    params = {"day": day, "next_day": day + datetime.timedelta(days=1)}
    with engine.begin() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    return df


def fetch_daily_data_bq(client, table: str, date_col: str, day: datetime.date) -> pd.DataFrame:
    query = f"""
        SELECT * FROM `{table}`
        WHERE {date_col} >= @day AND {date_col} < @next_day
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("day", "DATE", day.isoformat()),
            bigquery.ScalarQueryParameter("next_day", "DATE", (day + datetime.timedelta(days=1)).isoformat()),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    return query_job.to_dataframe()


def run_expectations(df: pd.DataFrame, pk_cols: List[str], numeric_ranges: Dict[str, Tuple[Optional[float], Optional[float]]]):
    ge_df = ge.from_pandas(df)
    results = []

    for col in pk_cols:
        res = ge_df.expect_column_values_to_not_be_null(col)
        results.append((f"no nulls in {col}", res.success))

    for col, (min_val, max_val) in numeric_ranges.items():
        res = ge_df.expect_column_values_to_be_between(col, min_value=min_val, max_value=max_val)
        results.append((f"{col} in range", res.success))

    return results


def status_color(within_5: bool, within_10: bool) -> str:
    if within_5:
        return "green"
    if within_10:
        return "yellow"
    return "red"


def main():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    start_period = yesterday - datetime.timedelta(days=30)

    lines = ["# Table Quality Report", f"Generated: {today.isoformat()}", ""]

    for db_name, db_conf in DATABASES.items():
        lines.append(f"## {db_name}")
        if db_conf.type == "postgres":
            engine = get_pg_engine(db_conf.connection)
        else:
            engine = None
        if db_conf.type == "bigquery":
            client = get_bq_client(db_conf.connection)
        else:
            client = None

        for tbl in db_conf.tables:
            if db_conf.type == "postgres":
                counts = fetch_counts_pg(engine, tbl.name, tbl.date_column, start_period, today)
            else:
                counts = fetch_counts_bq(client, tbl.name, tbl.date_column, start_period, today)
            median_count = statistics.median(counts[counts["date"] < yesterday]["count"])
            yesterday_count = counts.loc[counts["date"] == yesterday, "count"].iloc[0] if not counts.loc[counts["date"] == yesterday].empty else 0

            within_5 = abs(yesterday_count - median_count) <= 0.05 * median_count
            within_10 = abs(yesterday_count - median_count) <= 0.10 * median_count
            color = status_color(within_5, within_10)

            if db_conf.type == "postgres":
                df = fetch_daily_data_pg(engine, tbl.name, tbl.date_column, yesterday)
            else:
                df = fetch_daily_data_bq(client, tbl.name, tbl.date_column, yesterday)

            tests = run_expectations(df, tbl.primary_keys, tbl.numeric_ranges)
            test_summary = all(res for _, res in tests)

            lines.append(f"### {tbl.name}")
            lines.append(f"Row count yesterday: {yesterday_count}")
            lines.append(f"30-day median: {int(median_count)}")
            lines.append(f"Status: **{color.upper()}**")
            lines.append("")
            lines.append("| Test | Result |")
            lines.append("|------|--------|")
            for test_name, success in tests:
                emoji = ":white_check_mark:" if success else ":x:"
                lines.append(f"| {test_name} | {emoji} |")
            lines.append("")

        lines.append("")

    with open("report.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
