"""
Migrate Historical Data: PostgreSQL → BigQuery
================================================
Reads all curated_metrics from the 1x-Dashboard PostgreSQL database
and loads them into BigQuery's daily_metrics_staging table.

Run once to backfill historical data.

Usage:
    python3 migrate_postgres_to_bq.py
"""

import os
import datetime as dt

import psycopg2
from google.cloud import bigquery

# ─── Config ───
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:CsQiBZmLZUfPfcYOfFJTGERNDiTlXTYF@switchback.proxy.rlwy.net:33972/railway"
)
GCP_PROJECT = os.environ.get("GCP_PROJECT", "autonex-488609")
BQ_DATASET  = os.environ.get("BQ_DATASET", "encord_metrics")
STAGING_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.daily_metrics_staging"


def main():
    print(f"[{dt.datetime.now().isoformat()}] Starting PostgreSQL → BigQuery migration")

    # ── Connect to PostgreSQL ──
    print("  Connecting to PostgreSQL...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ── Read all curated metrics ──
    cur.execute("""
        SELECT
            project_hash,
            date,
            active_annotators,
            tasks,
            tpt_seconds,
            ann_time_seconds,
            vid_duration_seconds,
            ratio,
            notes,
            created_at
        FROM curated_metrics
        ORDER BY date ASC
    """)

    rows = cur.fetchall()
    print(f"  Found {len(rows)} historical records in PostgreSQL")

    if not rows:
        print("  No data to migrate. Exiting.")
        cur.close()
        conn.close()
        return

    # ── Transform to BigQuery format ──
    bq_rows = []
    for row in rows:
        project_hash, date_str, active_ann, tasks, tpt_sec, ann_time_sec, vid_dur_sec, ratio_val, notes, created_at = row

        # Calculate ratio if not stored
        if ratio_val is None and vid_dur_sec and vid_dur_sec > 0:
            ratio_val = (ann_time_sec or 0) / vid_dur_sec

        bq_rows.append({
            "date":                    str(date_str),
            "project_hash":            project_hash,
            "project_title":           "",  # not in postgres, will be filled by lookup
            "active_annotators":       active_ann or 0,
            "tasks_annotated":         tasks or 0,
            "tpt_seconds":             round(float(tpt_sec or 0), 2),
            "annotation_time_seconds": round(float(ann_time_sec or 0), 2),
            "video_duration_seconds":  round(float(vid_dur_sec or 0), 2),
            "ratio":                   round(float(ratio_val or 0), 2),
            "total_label_rows":        0,
            "snapshot_time":           (created_at or dt.datetime.now()).isoformat(),
            "is_approved":             True,   # historical data is already curated
            "approved_by":             "migrated-from-postgres",
            "approved_at":             (created_at or dt.datetime.now()).isoformat(),
            "admin_notes":             notes or None,
        })

    cur.close()
    conn.close()
    print(f"  Transformed {len(bq_rows)} rows")

    # ── Show preview ──
    print(f"\n  Preview (first 5 rows):")
    print(f"  {'Date':<12} {'Tasks':>7} {'TPT(s)':>8} {'AnnTime(s)':>11} {'VidDur(s)':>10} {'Ratio':>7} {'Anns':>5}")
    print(f"  {'-'*12} {'-'*7} {'-'*8} {'-'*11} {'-'*10} {'-'*7} {'-'*5}")
    for r in bq_rows[:5]:
        print(f"  {r['date']:<12} {r['tasks_annotated']:>7} {r['tpt_seconds']:>8} {r['annotation_time_seconds']:>11} {r['video_duration_seconds']:>10} {r['ratio']:>7} {r['active_annotators']:>5}")
    if len(bq_rows) > 5:
        print(f"  ... and {len(bq_rows) - 5} more rows")

    # ── Load to BigQuery ──
    print(f"\n  Loading {len(bq_rows)} rows → {STAGING_TABLE}...")
    bq = bigquery.Client(project=GCP_PROJECT)
    bq.create_dataset(BQ_DATASET, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("date",                    "DATE"),
            bigquery.SchemaField("project_hash",            "STRING"),
            bigquery.SchemaField("project_title",           "STRING"),
            bigquery.SchemaField("active_annotators",       "INTEGER"),
            bigquery.SchemaField("tasks_annotated",         "INTEGER"),
            bigquery.SchemaField("tpt_seconds",             "FLOAT"),
            bigquery.SchemaField("annotation_time_seconds", "FLOAT"),
            bigquery.SchemaField("video_duration_seconds",  "FLOAT"),
            bigquery.SchemaField("ratio",                   "FLOAT"),
            bigquery.SchemaField("total_label_rows",        "INTEGER"),
            bigquery.SchemaField("snapshot_time",           "TIMESTAMP"),
            bigquery.SchemaField("is_approved",             "BOOLEAN"),
            bigquery.SchemaField("approved_by",             "STRING",  mode="NULLABLE"),
            bigquery.SchemaField("approved_at",             "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("admin_notes",             "STRING",  mode="NULLABLE"),
        ],
    )
    job = bq.load_table_from_json(bq_rows, STAGING_TABLE, job_config=job_config)
    job.result()

    print(f"  ✓ Loaded {len(bq_rows)} historical rows → {STAGING_TABLE}")
    print(f"\n  Date range: {bq_rows[0]['date']} → {bq_rows[-1]['date']}")
    print(f"\n[{dt.datetime.now().isoformat()}] Migration complete! 🎉")


if __name__ == "__main__":
    main()
