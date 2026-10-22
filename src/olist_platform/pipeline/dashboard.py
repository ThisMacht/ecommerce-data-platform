"""Render the health snapshot as a standalone, script-free operational report."""

from html import escape


def render(result: dict) -> str:
    def value(item, rate=False):
        if item is None:
            return "Unavailable"
        if rate:
            return f"{item:.3%}"
        if isinstance(item, float):
            return f"{item:,.3f}"
        return escape(str(item))

    def table(caption, headings, rows):
        return (
            '<div class="scroll"><table><caption>'
            + escape(caption)
            + "</caption>"
            + "<thead><tr>"
            + "".join(f'<th scope="col">{escape(h)}</th>' for h in headings)
            + "</tr></thead><tbody>"
            + "".join(
                "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
                for row in rows
            )
            + "</tbody></table></div>"
        )

    columns = (
        ("freshness_hours", "Freshness (h)", False),
        ("processing_input_rows", "Processing inputs", False),
        ("reject_rate", "Reject rate", True),
        ("warning_rate", "Warning rate", True),
        ("pending_rows", "Pending", False),
        ("expired_rows", "Expired", False),
        ("oldest_pending_age_hours", "Oldest pending (h)", False),
        ("due_pending_rows", "Resolution due", False),
    )
    metrics = table(
        "Dataset metrics",
        ["Dataset"] + [label for _, label, _ in columns],
        [
            [value(name)] + [value(data[key], rate) for key, _, rate in columns]
            for name, data in result["datasets"].items()
        ],
    )
    thresholds = table(
        "Configured limits (actual values above must not exceed these)",
        ["Dataset", "Freshness (h)", "Reject rate", "Warning rate", "Pending age (h)"],
        [
            [
                value(name),
                value(policy["max_freshness_hours"]),
                value(policy["max_reject_rate"], True),
                value(policy["max_warning_rate"], True),
                value(policy["max_pending_age_hours"]),
            ]
            for name, policy in result["thresholds"]["datasets"].items()
        ],
    )
    run = result["latest_run"] or {}
    accounting = table(
        "Input accounting (last successful run)",
        [
            "Dataset",
            "Source input",
            "Dimension snapshot",
            "Bootstrap",
            "Backfill",
            "After cursor",
            "Lookback",
            "Retry inputs",
            "Full FK source scans",
        ],
        [
            [value(name)]
            + [
                value(data.get(key))
                for key in (
                    "source_rows_extracted",
                    "dimension_snapshot_rows",
                    "source_bootstrap_rows",
                    "source_backfill_rows",
                    "source_after_cursor_rows",
                    "source_lookback_rows",
                    "deferred_retry_input_rows",
                    "full_parent_source_rows",
                )
            ]
            for name, data in result["datasets"].items()
        ],
    )
    run_table = table(
        "Latest execution (active run takes priority)",
        ["Run ID", "Status", "Attempt", "Failed task", "Elapsed (s)", "Limit (s)"],
        [
            [
                value(run.get(key))
                for key in (
                    "run_id",
                    "status",
                    "attempt",
                    "failed_task",
                    "elapsed_seconds",
                )
            ]
            + [value(result["thresholds"]["max_run_seconds"])]
        ],
    )
    issues = (
        "<ul>" + "".join(f"<li>{value(i)}</li>" for i in result["issues"]) + "</ul>"
        if result["issues"]
        else "<p>No threshold violations detected.</p>"
    )
    return """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline Operations</title>
<style>
body{font:16px/1.5 system-ui,sans-serif;color:#172634;background:#f4f7fa;margin:0}
main{max-width:1400px;margin:auto;padding:24px}h1{margin-bottom:0}
.scroll{overflow-x:auto;margin:24px 0;background:white;border:1px solid #c7d1db}
table{border-collapse:collapse;width:100%}caption{text-align:left;font-weight:700;padding:12px}
th,td{text-align:left;padding:10px;border-top:1px solid #d5dde5;overflow-wrap:anywhere}
th{background:#e9eff5}li{overflow-wrap:anywhere}
</style></head><body><main>
<h1>Pipeline Operations</h1>""" + (
        f"<p><strong>{value(result['status'])}</strong> · {value(result['pipeline_name'])}</p>"
        f"<p>Snapshot checked at: {value(result['checked_at'])}. "
        "Static report; rerun the health command to refresh.</p>"
        f"<p>Last successful run: {value(result['last_success_run_id'])}</p>"
        "<h2>Issues</h2>"
        + issues
        + run_table
        + metrics
        + thresholds
        + accounting
        + "<h2>Metric interpretation</h2><p>Freshness measures time since the last "
        "successful pipeline completion, not source change lag. Quality rates use "
        "processing occurrences from that success, including retries. Backlog uses "
        "the current queue; occurrences are not distinct business keys. Runtime "
        "includes time waiting for resume. Resolution due uses persisted retry "
        "limits. Unavailable values are not zero.</p>"
        "<p>After-cursor rows count source versions beyond the previous ordered cursor, "
        "not business inserts or net source growth. Bootstrap, backfill, after-cursor and "
        "lookback partition the source window. Customers/orders full FK scans are separate auxiliary inputs. Products/sellers "
        "use full dimension snapshots, not delta growth.</p>"
        "<p>This read-only snapshot does not verify live source or object storage "
        "and does not publish analytics or change watermarks.</p></main></body></html>"
    )
