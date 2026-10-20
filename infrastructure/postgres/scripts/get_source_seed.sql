SELECT manifest_sha256 || '|' || status
FROM audit.source_seed_runs
WHERE snapshot_id = :'snapshot_id';
