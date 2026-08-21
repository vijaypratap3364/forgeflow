CREATE TABLE users (
    user_id text PRIMARY KEY,
    display_name text NOT NULL,
    disabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    CONSTRAINT users_id_not_empty CHECK (user_id <> ''),
    CONSTRAINT users_display_name_not_empty CHECK (btrim(display_name) <> '')
);

CREATE TABLE projects (
    project_id text PRIMARY KEY,
    project_name text NOT NULL,
    created_by text NOT NULL REFERENCES users (user_id),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT projects_id_not_empty CHECK (project_id <> ''),
    CONSTRAINT projects_name_not_empty CHECK (btrim(project_name) <> ''),
    CONSTRAINT projects_timestamps_ordered CHECK (updated_at >= created_at)
);

CREATE TABLE project_memberships (
    project_id text NOT NULL REFERENCES projects (project_id) ON DELETE CASCADE,
    user_id text NOT NULL REFERENCES users (user_id),
    role text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (project_id, user_id),
    CONSTRAINT project_memberships_role_valid CHECK (role IN ('member', 'operator', 'admin')),
    CONSTRAINT project_memberships_timestamps_ordered CHECK (updated_at >= created_at)
);

CREATE INDEX project_memberships_user_project_idx
    ON project_memberships (user_id, project_id);

CREATE TABLE workflow_ownership (
    workflow_id text PRIMARY KEY REFERENCES workflow_definitions (workflow_id) ON DELETE CASCADE,
    project_id text NOT NULL,
    owner_user_id text NOT NULL,
    created_at timestamptz NOT NULL,
    CONSTRAINT workflow_ownership_member_fk
        FOREIGN KEY (project_id, owner_user_id)
        REFERENCES project_memberships (project_id, user_id)
);

CREATE INDEX workflow_ownership_project_workflow_idx
    ON workflow_ownership (project_id, workflow_id);

CREATE TABLE audit_events (
    event_id text PRIMARY KEY,
    project_id text NOT NULL REFERENCES projects (project_id) ON DELETE CASCADE,
    actor_user_id text NOT NULL REFERENCES users (user_id),
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    occurred_at timestamptz NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT audit_events_id_not_empty CHECK (event_id <> ''),
    CONSTRAINT audit_events_action_not_empty CHECK (action <> ''),
    CONSTRAINT audit_events_resource_type_not_empty CHECK (resource_type <> ''),
    CONSTRAINT audit_events_resource_id_not_empty CHECK (resource_id <> '')
);

CREATE INDEX audit_events_project_occurred_idx
    ON audit_events (project_id, occurred_at DESC, event_id DESC);
