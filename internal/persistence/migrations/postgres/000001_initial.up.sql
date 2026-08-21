CREATE TABLE workflow_definitions (
    workflow_id text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT workflow_definitions_id_not_empty CHECK (workflow_id <> '')
);

CREATE TABLE task_definitions (
    workflow_id text NOT NULL,
    task_id text NOT NULL,
    position integer NOT NULL,
    task_name text NOT NULL,
    handler_name text NOT NULL,
    task_input text NOT NULL DEFAULT '',
    max_attempts integer NOT NULL,
    initial_backoff_ns bigint NOT NULL,
    max_backoff_ns bigint NOT NULL,
    PRIMARY KEY (workflow_id, task_id),
    CONSTRAINT task_definitions_position_unique UNIQUE (workflow_id, position),
    CONSTRAINT task_definitions_workflow_fk
        FOREIGN KEY (workflow_id) REFERENCES workflow_definitions (workflow_id) ON DELETE CASCADE,
    CONSTRAINT task_definitions_id_not_empty CHECK (task_id <> ''),
    CONSTRAINT task_definitions_name_not_empty CHECK (btrim(task_name) <> ''),
    CONSTRAINT task_definitions_position_nonnegative CHECK (position >= 0),
    CONSTRAINT task_definitions_attempts_nonnegative CHECK (max_attempts >= 0),
    CONSTRAINT task_definitions_initial_backoff_nonnegative CHECK (initial_backoff_ns >= 0),
    CONSTRAINT task_definitions_max_backoff_valid CHECK (
        max_backoff_ns >= 0
        AND (max_backoff_ns = 0 OR max_backoff_ns >= initial_backoff_ns)
    )
);

CREATE TABLE task_dependencies (
    workflow_id text NOT NULL,
    task_id text NOT NULL,
    dependency_task_id text NOT NULL,
    position integer NOT NULL,
    PRIMARY KEY (workflow_id, task_id, dependency_task_id),
    CONSTRAINT task_dependencies_position_unique UNIQUE (workflow_id, task_id, position),
    CONSTRAINT task_dependencies_task_fk
        FOREIGN KEY (workflow_id, task_id)
        REFERENCES task_definitions (workflow_id, task_id) ON DELETE CASCADE,
    CONSTRAINT task_dependencies_dependency_fk
        FOREIGN KEY (workflow_id, dependency_task_id)
        REFERENCES task_definitions (workflow_id, task_id) ON DELETE CASCADE,
    CONSTRAINT task_dependencies_no_self_reference CHECK (task_id <> dependency_task_id),
    CONSTRAINT task_dependencies_position_nonnegative CHECK (position >= 0)
);

CREATE TABLE workflow_runs (
    run_id text PRIMARY KEY,
    workflow_id text NOT NULL,
    version bigint NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT workflow_runs_identity_unique UNIQUE (run_id, workflow_id),
    CONSTRAINT workflow_runs_workflow_fk
        FOREIGN KEY (workflow_id) REFERENCES workflow_definitions (workflow_id),
    CONSTRAINT workflow_runs_id_not_empty CHECK (run_id <> ''),
    CONSTRAINT workflow_runs_version_positive CHECK (version >= 1),
    CONSTRAINT workflow_runs_status_valid CHECK (
        status IN ('pending', 'running', 'succeeded', 'failed', 'canceled')
    ),
    CONSTRAINT workflow_runs_timestamps_ordered CHECK (updated_at >= created_at)
);

CREATE TABLE task_runs (
    run_id text NOT NULL,
    workflow_id text NOT NULL,
    task_id text NOT NULL,
    task_run_id text NOT NULL,
    status text NOT NULL,
    output text NOT NULL DEFAULT '',
    error_message text NOT NULL DEFAULT '',
    attempt_count integer NOT NULL,
    current_attempt_id text NOT NULL DEFAULT '',
    next_attempt_at timestamptz,
    updated_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (run_id, task_id),
    CONSTRAINT task_runs_task_run_id_unique UNIQUE (run_id, task_run_id),
    CONSTRAINT task_runs_run_fk
        FOREIGN KEY (run_id, workflow_id)
        REFERENCES workflow_runs (run_id, workflow_id) ON DELETE CASCADE,
    CONSTRAINT task_runs_task_definition_fk
        FOREIGN KEY (workflow_id, task_id)
        REFERENCES task_definitions (workflow_id, task_id),
    CONSTRAINT task_runs_id_not_empty CHECK (task_run_id <> ''),
    CONSTRAINT task_runs_status_valid CHECK (
        status IN ('pending', 'ready', 'running', 'retry_wait', 'succeeded', 'failed', 'canceled')
    ),
    CONSTRAINT task_runs_attempt_count_nonnegative CHECK (attempt_count >= 0),
    CONSTRAINT task_runs_running_has_attempt CHECK (
        status <> 'running' OR (attempt_count > 0 AND current_attempt_id <> '' AND started_at IS NOT NULL)
    ),
    CONSTRAINT task_runs_retry_has_deadline CHECK (
        status <> 'retry_wait' OR (attempt_count > 0 AND current_attempt_id <> '' AND next_attempt_at IS NOT NULL)
    ),
    CONSTRAINT task_runs_terminal_has_finish CHECK (
        status NOT IN ('succeeded', 'failed', 'canceled') OR finished_at IS NOT NULL
    )
);

CREATE TABLE workers (
    run_id text NOT NULL,
    worker_id text NOT NULL,
    last_heartbeat_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, worker_id),
    CONSTRAINT workers_run_fk
        FOREIGN KEY (run_id) REFERENCES workflow_runs (run_id) ON DELETE CASCADE,
    CONSTRAINT workers_id_not_empty CHECK (worker_id <> '')
);

CREATE TABLE task_leases (
    run_id text NOT NULL,
    task_id text NOT NULL,
    task_run_id text NOT NULL,
    worker_id text NOT NULL,
    attempt_id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, task_id),
    CONSTRAINT task_leases_attempt_unique UNIQUE (run_id, attempt_id),
    CONSTRAINT task_leases_worker_unique UNIQUE (run_id, worker_id),
    CONSTRAINT task_leases_task_fk
        FOREIGN KEY (run_id, task_id) REFERENCES task_runs (run_id, task_id) ON DELETE CASCADE,
    CONSTRAINT task_leases_task_run_fk
        FOREIGN KEY (run_id, task_run_id)
        REFERENCES task_runs (run_id, task_run_id) ON DELETE CASCADE,
    CONSTRAINT task_leases_worker_fk
        FOREIGN KEY (run_id, worker_id) REFERENCES workers (run_id, worker_id) ON DELETE CASCADE,
    CONSTRAINT task_leases_attempt_not_empty CHECK (attempt_id <> '')
);
