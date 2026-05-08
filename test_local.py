"""
Local Test Script — Encord Data Pull Only
==========================================
Pulls data from Encord SDK and saves to a local JSON file.
No BigQuery/GCP needed. Use this to verify the data before going live.

Usage:
    ENCORD_SSH_KEY_PATH=/path/to/key python3 test_local.py
"""

import os
import json
import datetime as dt
from collections import Counter

from encord import EncordUserClient

# ─── Config ───
# Use the 1X project for testing, or set PROJECT_HASHES env var
PROJECT_HASHES = [h.strip() for h in os.environ.get(
    "PROJECT_HASHES", "2a813775-341b-4408-954d-83f7dab3e840"
).split(",") if h.strip()]


def create_encord_client():
    key_path = os.environ.get("ENCORD_SSH_KEY_PATH")
    domain = os.environ.get("ENCORD_DOMAIN", "https://api.encord.com")
    if key_path and os.path.exists(key_path):
        return EncordUserClient.create_with_ssh_private_key(
            ssh_private_key_path=key_path, domain=domain,
        )
    raise ValueError("Set ENCORD_SSH_KEY_PATH env var")


def get_duration_seconds(lr) -> float:
    dur = getattr(lr, "duration", None)
    if dur is not None and float(dur) > 0:
        return float(dur)
    fps = getattr(lr, "fps", None) or getattr(lr, "frames_per_second", None)
    frames = getattr(lr, "number_of_frames", None)
    if fps and frames and float(fps) > 0:
        return float(frames) / float(fps)
    return 0.0


def main():
    print(f"[{dt.datetime.now().isoformat()}] Starting LOCAL test (no BigQuery)")
    print(f"  Projects to check: {PROJECT_HASHES}\n")

    user_client = create_encord_client()
    print("  ✓ Connected to Encord\n")

    today_str = dt.date.today().isoformat()
    all_daily = []
    all_workflow = []

    for ph in PROJECT_HASHES:
        try:
            project = user_client.get_project(ph)
            print(f"  ─── {project.title} ({ph[:12]}...) ───")
        except Exception as e:
            print(f"  ✗ Skipping {ph}: {e}")
            continue

        # Get label rows
        print(f"    Fetching label rows...", end=" ", flush=True)
        label_rows = list(project.list_label_rows_v2())
        print(f"found {len(label_rows)}")

        # Workflow stages
        stage_counts = Counter()
        for lr in label_rows:
            node = getattr(lr, "workflow_graph_node", None)
            stage = node.title if node else "unknown"
            stage_counts[stage] += 1

        print(f"    Workflow stages:")
        for stage, count in sorted(stage_counts.items()):
            all_workflow.append({
                "project_title": project.title,
                "workflow_stage": stage,
                "label_row_count": count,
            })
            print(f"      {stage}: {count}")

        # Metrics
        annotators = set()
        total_tasks = 0
        total_vid_duration = 0.0

        for lr in label_rows:
            node = getattr(lr, "workflow_graph_node", None)
            if node:
                stage_type = str(getattr(node, "stage_type", "")).upper()
                if "REVIEW" in stage_type or "COMPLETE" in stage_type:
                    total_tasks += 1
            total_vid_duration += get_duration_seconds(lr)

            activities = getattr(lr, "activity", None) or []
            for act in activities:
                user_email = getattr(act, "user", None) or getattr(act, "user_email", None)
                if user_email:
                    annotators.add(str(user_email))

        # Try project analytics
        total_ann_time = 0.0
        try:
            analytics = project.get_analytics()
            if analytics:
                total_ann_time = float(getattr(analytics, "total_annotation_time", 0) or 0)
        except Exception as e:
            print(f"    ⚠ Could not get analytics: {e}")

        active_annotators = len(annotators)
        tpt = (total_ann_time / total_tasks) if total_tasks > 0 else 0
        ratio = (total_ann_time / total_vid_duration) if total_vid_duration > 0 else 0

        daily_row = {
            "date": today_str,
            "project_title": project.title,
            "active_annotators": active_annotators,
            "tasks_annotated": total_tasks,
            "tpt_seconds": round(tpt, 2),
            "tpt_minutes": round(tpt / 60, 2),
            "annotation_time_seconds": round(total_ann_time, 2),
            "annotation_time_minutes": round(total_ann_time / 60, 2),
            "video_duration_seconds": round(total_vid_duration, 2),
            "video_duration_minutes": round(total_vid_duration / 60, 2),
            "ratio": round(ratio, 2),
            "total_label_rows": len(label_rows),
        }
        all_daily.append(daily_row)

        print(f"\n    📊 METRICS:")
        print(f"    ┌─────────────────────────┬──────────────┐")
        print(f"    │ Active Annotators       │ {active_annotators:<12} │")
        print(f"    │ Tasks Annotated         │ {total_tasks:<12} │")
        print(f"    │ TPT                     │ {round(tpt/60, 2):<12} min │")
        print(f"    │ Annotation Time         │ {round(total_ann_time/60, 2):<12} min │")
        print(f"    │ Video Duration          │ {round(total_vid_duration/60, 2):<12} min │")
        print(f"    │ Ratio                   │ {round(ratio, 2):<12}×  │")
        print(f"    │ Total Label Rows        │ {len(label_rows):<12} │")
        print(f"    └─────────────────────────┴──────────────┘\n")

    # Save to JSON
    output = {
        "generated_at": dt.datetime.now().isoformat(),
        "daily_metrics": all_daily,
        "workflow_stages": all_workflow,
    }
    out_file = "test_output.json"
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"  ✓ Saved results to {out_file}")
    print(f"\n  Compare these numbers with the daily metrics the client sends.")
    print(f"  If they match → pipeline is good to go!")
    print(f"  If they don't → we need to adjust the script.\n")


if __name__ == "__main__":
    main()
