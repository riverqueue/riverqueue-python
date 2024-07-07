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
    args jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempt smallint NOT NULL DEFAULT 0,
    attempted_at timestamptz,
    attempted_by text[],
    created_at timestamptz NOT NULL DEFAULT NOW(),
    errors jsonb[],
    finalized_at timestamptz,
    kind text NOT NULL,
    max_attempts smallint NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}' ::jsonb,
    priority smallint NOT NULL DEFAULT 1,
    queue text NOT NULL DEFAULT 'default' ::text,
    state river_job_state NOT NULL DEFAULT 'available' ::river_job_state,
    scheduled_at timestamptz NOT NULL DEFAULT NOW(),
    tags varchar(255)[] NOT NULL DEFAULT '{}' ::varchar(255)[],
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

-- name: JobGetByKindAndUniqueProperties :one
SELECT *
FROM river_job
WHERE kind = @kind
    AND CASE WHEN @by_args::boolean THEN args = @args ELSE true END
    AND CASE WHEN @by_created_at::boolean THEN tstzrange(@created_at_begin::timestamptz, @created_at_end::timestamptz, '[)') @> created_at ELSE true END
    AND CASE WHEN @by_queue::boolean THEN queue = @queue ELSE true END
    AND CASE WHEN @by_state::boolean THEN state::text = any(@state::text[]) ELSE true END;

-- name: JobInsertFast :one
INSERT INTO river_job(
    args,
    created_at,
    finalized_at,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) VALUES (
    @args::jsonb,
    coalesce(sqlc.narg('created_at')::timestamptz, now()),
    @finalized_at,
    @kind::text,
    @max_attempts::smallint,
    coalesce(@metadata::jsonb, '{}'),
    @priority::smallint,
    @queue::text,
    coalesce(sqlc.narg('scheduled_at')::timestamptz, now()),
    @state::river_job_state,
    coalesce(@tags::varchar(255)[], '{}')
) RETURNING *;

-- name: JobInsertFastMany :execrows
INSERT INTO river_job(
    args,
    kind,
    max_attempts,
    metadata,
    priority,
    queue,
    scheduled_at,
    state,
    tags
) SELECT
    unnest(@args::jsonb[]),
    unnest(@kind::text[]),
    unnest(@max_attempts::smallint[]),
    unnest(@metadata::jsonb[]),
    unnest(@priority::smallint[]),
    unnest(@queue::text[]),
    unnest(@scheduled_at::timestamptz[]),
    unnest(@state::river_job_state[]),

    -- Had trouble getting multi-dimensional arrays to play nicely with sqlc,
    -- but it might be possible. For now, join tags into a single string.
    string_to_array(unnest(@tags::text[]), ',');

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
    tags
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
    coalesce(@tags::varchar(255)[], '{}')
) RETURNING *;