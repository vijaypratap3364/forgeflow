# ForgeFlow Security Model

## Scope

ForgeFlow now has a real application security boundary, not a complete identity platform. It authenticates externally issued bearer tokens, persists project membership and resource ownership, authorizes each resource access, limits authenticated API request volume, and records important project-administration changes. The API never accepts a project role from a JWT as authority.

## Authentication

All `/api/v1` routes require `Authorization: Bearer <token>`. `/healthz`, `/readyz`, and `/metrics` are intentionally public and contain no tenant identifiers. Metrics expose aggregate process activity, so a production deployment should restrict all three operational endpoints to trusted infrastructure at the network edge.

The configured verifier accepts only Ed25519-signed JWTs using `alg=EdDSA`. It validates the signature and requires matching `iss` and `aud` values, a nonempty stable `sub`, and an unexpired `exp`; standard `nbf` validation applies when present. A small configurable leeway handles clock skew. Failures return the same `401 unauthenticated` envelope so parsing details do not become an oracle. Signing keys are never generated, stored, or logged by ForgeFlow. The PEM public verification key and trust settings come from environment-backed process configuration.

ForgeFlow deliberately does not issue tokens, store passwords, implement OAuth/OIDC redirects, or provide a login UI. A production deployment must use an identity provider or trusted internal issuer and deliver its Ed25519 public key securely. The current static-key configuration does not implement JWKS discovery, key rotation, token revocation, or per-device sessions.

## Projects, ownership, and RBAC

A project is the tenant authorization boundary. An authenticated subject can create a project and becomes its first admin. Workflows have immutable persisted ownership containing the project and creator. Runs inherit their project from the persisted workflow definition; handlers do not trust a caller-supplied project ID when reading a workflow or run.

Roles are project-scoped:

| Capability | Member | Operator | Admin |
| --- | ---: | ---: | ---: |
| Read project/workflow and aggregate run state | yes | yes | yes |
| Create workflows and start runs | yes | yes | yes |
| Inspect task-level state, failures, leases, and outputs | no | yes | yes |
| Cancel a run or create a fresh retry run | no | yes | yes |
| Rename projects and manage memberships | no | no | yes |
| Read administrative audit events | no | no | yes |

Authentication and authorization are deliberately separate. A missing, malformed, badly signed, expired, wrong-issuer, or wrong-audience token returns `401`. An authenticated project member whose role lacks a capability receives `403`. A user without membership receives a tenant-concealing `404`, including when the underlying workflow or run belongs to another project. A persisted disabled user receives `403` even if its token remains cryptographically valid.

Project creation, project metadata changes, and membership role changes write immutable audit events atomically with the administrative mutation in both storage backends. The embedded journal checksum detects accidental corruption, but its audit log is not a tamper-proof security ledger against a host administrator. Production deployments should export audit events to access-controlled, append-only infrastructure.

## Rate limiting

The API applies a fixed-window limit keyed by authenticated JWT subject before routing any `/api/v1` request. Responses expose `RateLimit-Limit`, `RateLimit-Remaining`, and `RateLimit-Reset`; rejected requests return `429 rate_limited` and `Retry-After`. Limits and windows are configurable.

The limiter is process-local. It bounds accidental or abusive traffic to one ForgeFlow process but is not a global quota when multiple API replicas are deployed, does not survive restart, and does not replace upstream connection, body-size, or denial-of-service protection. A horizontally scaled deployment needs a shared limiter or gateway policy.

## Protected and unprotected threats

ForgeFlow currently protects against:

- forged or altered bearer tokens under the configured Ed25519 trust key;
- acceptance of unexpected JWT algorithms, issuers, audiences, expired tokens, or missing subjects;
- privilege escalation through caller-supplied role claims;
- normal cross-project reads and mutations through resource-derived authorization;
- members invoking operator/admin APIs;
- silent administrative project or membership changes within the Store transaction;
- one authenticated subject exceeding the configured per-process request rate.

ForgeFlow does not currently protect against:

- theft of a valid bearer token, issuer compromise, missing issuer-side revocation, or stale static verification keys;
- plaintext transport if deployed without TLS termination;
- malicious task-handler implementations registered by the host process;
- host/database administrators altering state or reading task inputs and outputs;
- a compromised process bypassing its own authorization code;
- distributed denial of service or globally coordinated rate-limit evasion;
- exhaustive tenant isolation through PostgreSQL row-level security;
- audit-log deletion by a privileged storage administrator;
- duplicate external side effects from at-least-once task execution.

Tokens and database/broker credentials must stay out of source control and logs. Task inputs and outputs may contain sensitive application data, so production retention, encryption, redaction, backup, and access policies remain deployment responsibilities. Exactly-once execution is not a security guarantee: handlers should use the stable task-run or attempt ID as an idempotency key when calling an external system.
