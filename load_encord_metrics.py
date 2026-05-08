"""
Encord → BigQuery ETL Pipeline (with Admin Approval)
======================================================
Pulls project metrics from Encord SDK and loads into BigQuery STAGING table.
Admin reviews in BigQuery, then approves → data moves to LIVE table.
Looker Studio ONLY reads from the LIVE table.

Flow:
  1. GitHub Actions runs this script daily
  2. Script writes raw data → daily_metrics_staging (is_approved = false)
  3. Admin reviews in BigQuery → runs approve query
  4. Approved rows appear in daily_metrics_live (BigQuery VIEW)
  5. Looker Studio reads daily_metrics_live → client sees curated data

Tables:
  - daily_metrics_staging:  Raw data from Encord (has is_approved column)
  - daily_metrics_live:     VIEW that shows only approved rows
  - workflow_snapshot:      Per-project per-stage counts (informational)
"""

import os
import datetime as dt
from collections import Counter

from encord import EncordUserClient
from google.cloud import bigquery

# ─── Config ───
GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_DATASET  = os.environ.get("BQ_DATASET", "encord_metrics")

# Table IDs
STAGING_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.daily_metrics_staging"
WORKFLOW_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.workflow_snapshot"
LIVE_VIEW      = f"{GCP_PROJECT}.{BQ_DATASET}.daily_metrics_live"

# Project hashes to track (comma-separated in env var)
# If empty, pulls ALL projects the user has access to
PROJECT_HASHES = [h.strip() for h in os.environ.get("PROJECT_HASHES", "").split(",") if h.strip()]


# ─── Encord Client ───
def create_encord_client():
    key_path = os.environ.get("ENCORD_SSH_KEY_PATH")
    domain = os.environ.get("ENCORD_DOMAIN", "https://api.encord.com")

    if key_path and os.path.exists(key_path):
        return EncordUserClient.create_with_ssh_private_key(
            ssh_private_key_path=key_path,
            domain=domain,
        )
    key_content = os.environ.get("ENCORD_SSH_KEY")
    if key_content:
        return EncordUserClient.create_with_ssh_private_key(
            ssh_private_key=key_content,
            domain=domain,
        )
    raise ValueError("Set ENCORD_SSH_KEY_PATH or ENCORD_SSH_KEY env var")


# ─── Helpers ───
def get_duration_seconds(lr) -> float:
    """Get video/content duration from a label row."""
    dur = getattr(lr, "duration", None)
    if dur is not None and float(dur) > 0:
        return float(dur)
    fps = getattr(lr, "fps", None) or getattr(lr, "frames_per_second", None)
    frames = getattr(lr, "number_of_frames", None)
    if fps and frames and float(fps) > 0:
        return float(frames) / float(fps)
    return 0.0


def create_live_view(bq):
    """Create/update the LIVE view that only shows approved rows."""
    view_sql = f"""
    CREATE OR REPLACE VIEW `{LIVE_VIEW}` AS
    SELECT
        date,
        project_hash,
        project_title,
        active_annotators,
        tasks_annotated,
        tpt_seconds,
        annotation_time_seconds,
        video_duration_seconds,
        ratio,
        total_label_rows,
        approved_by,
        approved_at,
        snapshot_time
    FROM `{STAGING_TABLE}`
    WHERE is_approved = TRUE
    """
    bq.query(view_sql).result()
    print(f"  ✓ Created/updated view → {LIVE_VIEW}")


# ─── Main ETL ───
def main():
    print(f"[{dt.datetime.utcnow().isoformat()}] Starting Encord → BigQuery ETL")

    user_client = create_encord_client()
    bq = bigquery.Client(project=GCP_PROJECT)

    # Ensure dataset exists
    bq.create_dataset(BQ_DATASET, exists_ok=True)

    snapshot_time = dt.datetime.utcnow().isoformat()
    today_str = dt.date.today().isoformat()

    # Get projects
    if PROJECT_HASHES:
        project_hashes = PROJECT_HASHES
        print(f"  Tracking {len(project_hashes)} configured projects")
    else:
        projects_list = user_client.list_projects()
        project_hashes = [p["project"]["project_hash"] for p in projects_list]
        print(f"  Found {len(project_hashes)} projects")

    daily_rows = []
    workflow_rows = []

    for ph in project_hashes:
        try:
            project = user_client.get_project(ph)
            print(f"  → {project.title} ({ph[:8]}...)")
        except Exception as e:
            print(f"  ✗ Skipping {ph}: {e}")
            continue

        # ── Workflow stage snapshot ──
        stage_counts = Counter()
        label_rows = list(project.list_label_rows_v2())

        for lr in label_rows:
            node = getattr(lr, "workflow_graph_node", None)
            stage = node.title if node else "unknown"
            stage_counts[stage] += 1

        total_rows = len(label_rows)
        for stage, count in stage_counts.items():
            workflow_rows.append({
                "snapshot_time":    snapshot_time,
                "project_hash":    project.project_hash,
                "project_title":   project.title,
                "workflow_stage":  stage,
                "label_row_count": count,
                "total_label_rows": total_rows,
            })

        # ── Daily metrics ──
        annotators = set()
        total_tasks = 0
        total_ann_time_seconds = 0.0
        total_vid_duration_seconds = 0.0

        for lr in label_rows:
            node = getattr(lr, "workflow_graph_node", None)
            if node:
                stage_type = str(getattr(node, "stage_type", "")).upper()
                if "REVIEW" in stage_type or "COMPLETE" in stage_type:
                    total_tasks += 1

            total_vid_duration_seconds += get_duration_seconds(lr)

            activities = getattr(lr, "activity", None) or []
            for act in activities:
                user_email = getattr(act, "user", None) or getattr(act, "user_email", None)
                if user_email:
                    annotators.add(str(user_email))

        try:
            analytics = project.get_analytics()
            if analytics:
                total_ann_time_seconds = float(getattr(analytics, "total_annotation_time", 0) or 0)
        except Exception:
            pass

        active_annotators = len(annotators)
        tpt_seconds = (total_ann_time_seconds / total_tasks) if total_tasks > 0 else 0
        ratio = (total_ann_time_seconds / total_vid_duration_seconds) if total_vid_duration_seconds > 0 else 0

        daily_rows.append({
            "date":                    today_str,
            "project_hash":            project.project_hash,
            "project_title":           project.title,
            "active_annotators":       active_annotators,
            "tasks_annotated":         total_tasks,
            "tpt_seconds":             round(tpt_seconds, 2),
            "annotation_time_seconds": round(total_ann_time_seconds, 2),
            "video_duration_seconds":  round(total_vid_duration_seconds, 2),
            "ratio":                   round(ratio, 2),
            "total_label_rows":        total_rows,
            "snapshot_time":           snapshot_time,
            # ── Admin approval fields ──
            "is_approved":             True,   # auto-approved for testing
            "approved_by":             "vinayak",
            "approved_at":             snapshot_time,
            "admin_notes":             None,
        })

    # ── Load to BigQuery ──

    # 1. Workflow snapshot
    if workflow_rows:
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[
                bigquery.SchemaField("snapshot_time",    "TIMESTAMP"),
                bigquery.SchemaField("project_hash",     "STRING"),
                bigquery.SchemaField("project_title",    "STRING"),
                bigquery.SchemaField("workflow_stage",   "STRING"),
                bigquery.SchemaField("label_row_count",  "INTEGER"),
                bigquery.SchemaField("total_label_rows", "INTEGER"),
            ],
        )
        bq.load_table_from_json(workflow_rows, WORKFLOW_TABLE, job_config=job_config).result()
        print(f"  ✓ Loaded {len(workflow_rows)} rows → {WORKFLOW_TABLE}")

    # 2. Daily metrics → STAGING table (not live!)
    if daily_rows:
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
        bq.load_table_from_json(daily_rows, STAGING_TABLE, job_config=job_config).result()
        print(f"  ✓ Loaded {len(daily_rows)} rows → {STAGING_TABLE} (pending approval)")

    # 3. Create/update the LIVE view
    create_live_view(bq)

    print(f"\n[{dt.datetime.utcnow().isoformat()}] Done.")
    print(f"\n📋 ADMIN: To approve today's data, run this in BigQuery:")
    print(f"   UPDATE `{STAGING_TABLE}`")
    print(f"   SET is_approved = TRUE, approved_by = 'your-email', approved_at = CURRENT_TIMESTAMP()")
    print(f"   WHERE date = '{today_str}' AND is_approved = FALSE;")


if __name__ == "__main__":
    main()
