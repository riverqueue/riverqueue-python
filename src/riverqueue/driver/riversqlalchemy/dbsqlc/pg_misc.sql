-- name: PGAdvisoryXactLock :exec
SELECT pg_advisory_xact_lock(@key);