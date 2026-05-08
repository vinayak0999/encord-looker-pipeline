# Encord → BigQuery → Looker Studio Pipeline

Automated daily ETL that pulls project metrics from Encord and loads them into BigQuery.  
Looker Studio (free) connects to BigQuery for the client-facing dashboard.

## Architecture

```
Encord SDK  →  GitHub Actions (cron)  →  BigQuery  →  Looker Studio
              runs daily at 06:00 UTC
```

## Setup (one-time, ~10 min)

### 1. Google Cloud
1. Create a GCP project (or use existing). Note the **project ID**.
2. Go to **IAM & Admin → Service Accounts → Create**.
3. Give it roles: `BigQuery Data Editor` + `BigQuery Job User`.
4. Create a **JSON key** and download it.

### 2. Encord
1. Generate SSH key: `ssh-keygen -t ed25519 -f encord_key`
2. Upload `encord_key.pub` in Encord → Settings → Public Keys.

### 3. GitHub
1. Create a **private** repo with these files.
2. Go to **Settings → Secrets and variables → Actions**, add:
   - `GCP_SA_KEY` — paste entire JSON key file contents
   - `ENCORD_SSH_KEY` — paste entire private key (`encord_key`) contents
3. Go to **Settings → Variables → Actions**, add:
   - `GCP_PROJECT` — your GCP project ID

### 4. Looker Studio
1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Create Report → Add Data → BigQuery
3. Select `encord_metrics.daily_metrics` table
4. Build charts using the fields:
   - `date`, `project_title`, `tasks_annotated`, `tpt_seconds`,
   - `annotation_time_seconds`, `video_duration_seconds`, `ratio`,
   - `active_annotators`

## BigQuery Tables

### `daily_metrics`
| Field | Type | Description |
|-------|------|-------------|
| date | DATE | Snapshot date |
| project_hash | STRING | Encord project ID |
| project_title | STRING | Project display name |
| active_annotators | INTEGER | Annotators who worked that day |
| tasks_annotated | INTEGER | Tasks completed that day |
| tpt_seconds | FLOAT | Avg time per task (seconds) |
| annotation_time_seconds | FLOAT | Total annotation time (seconds) |
| video_duration_seconds | FLOAT | Total video duration (seconds) |
| ratio | FLOAT | Annotation time ÷ video duration |

### `workflow_snapshot`
| Field | Type | Description |
|-------|------|-------------|
| snapshot_time | TIMESTAMP | When snapshot was taken |
| project_hash | STRING | Encord project ID |
| project_title | STRING | Project display name |
| workflow_stage | STRING | Workflow stage name |
| label_row_count | INTEGER | Rows in this stage |
| total_label_rows | INTEGER | Total rows in project |

## Manual Run
```bash
# Trigger manually from GitHub Actions tab, or locally:
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-key.json
export ENCORD_SSH_KEY_PATH=/path/to/encord_key
export GCP_PROJECT=your-project-id
export BQ_DATASET=encord_metrics
python load_encord_metrics.py
```
