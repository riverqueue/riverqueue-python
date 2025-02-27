CREATE TYPE river_job_state AS ENUM(
    'available',
    'cancelled',
    'completed',
    'discarded',
    'pending',
    'retryable',
    'running',
    'scheduled'
);

CREATE TABLE river_job(
    id bigserial PRIMARY KEY,
    args jsonb NOT NULL DEFAULT '{}',
    attempt smallint NOT NULL DEFAULT 0,
    attempted_at timestamptz,
    attempted_by text[],
    created_at timestamptz NOT NULL DEFAULT NOW(),
    errors jsonb[],
    finalized_at timestamptz,
    kind text NOT NULL,
    max_attempts smallint NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    priority smallint NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default',
    state river_job_state NOT NULL DEFAULT 'available',
    scheduled_at timestamptz NOT NULL DEFAULT NOW(),
    tags varchar(255)[] NOT NULL DEFAULT '{}',
    unique_key bytea,
    unique_states bit(8),
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded'))
    ),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

-- name: JobGetAll :many
SELECT *
FROM river_job;

-- name: JobGetByID :one
SELECT *
FROM river_job
WHERE id = @id;

-- name: JobInsertFastMany :many
INSERT INTO river_job(
    args,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key,
    unique_states
) SELECT
    unnest(@args::jsonb[]),
    unnest(@kind::text[]),
    unnest(@max_attempts::smallint[]),
    unnest(@metadata::jsonb[]),
    unnest(@priority::smallint[]),
    unnest(@queue::text[]),
    unnest(@scheduled_at::timestamptz[]),
    unnest(@state::river_job_state[]),
    -- Unnest on a multi-dimensional array will fully flatten the array, so we
    -- encode the tag list as a comma-separated string and split it in the
    -- query.
    string_to_array(unnest(@tags::text[]), ','),

    nullif(unnest(@unique_key::bytea[]), ''),
    -- Strings of bits are used for the input type here to make sqlalchemy play nicely with bit(8):
    nullif(unnest(@unique_states::text[]), '')::bit(8)

ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state)
    -- Something needs to be updated for a row to be returned on a conflict.
    DO UPDATE SET kind = EXCLUDED.kind
RETURNING river_job.*, (xmax != 0) AS unique_skipped_as_duplicate;

-- name: JobInsertFull :one
INSERT INTO river_job(
    args,
    attempt,
    attempted_at,
    created_at,
    errors,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags,
    unique_key
) VALUES (
    @args::jsonb,
    coalesce(@attempt::smallint, 0),
    @attempted_at,
    coalesce(sqlc.narg('created_at')::timestamptz, now()),
    @errors::jsonb[],
    @finalized_at,
    @kind::text,
    @max_attempts::smallint,
    coalesce(@metadata::jsonb, '{}'),
    @priority::smallint,
    @queue::text,
    coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
    @state::river_job_state,
    coalesce(@tags::varchar(255)[], '{}'),
    @unique_key
) RETURNING *;
