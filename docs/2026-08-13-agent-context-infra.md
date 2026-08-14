# Agent Context Homelab Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy and operate the Agent Context platform privately in a personal homelab with reproducible versions, independent backups, observable failure modes and tested recovery.

**Architecture:** Docker Compose runs PostgreSQL, Garage, Neo4j Community, platform replicas and the observability stack on an encrypted host volume. Nginx exposes only the stateless `/mcp` and health surface over the NetBird interface. Infisical injects secrets at process launch. PostgreSQL plus verified sanitized S3 objects are canonical; Neo4j is rebuilt after recovery.

**Tech Stack:** Docker Compose v2, Ansible, optional OpenTofu for Proxmox allocation, PostgreSQL, pgBackRest, Garage v2.3, Neo4j Community, Nginx, NetBird, Infisical, OpenTelemetry Collector, Prometheus, Grafana, Alertmanager, rclone and Bats/ShellCheck.

## Global constraints

- Repository name is exactly `agent-context-infra`.
- Application/data services have no public host bindings. Nginx binds only to the resolved NetBird address.
- The internal Compose data network is not attachable from unrelated stacks.
- Secrets never exist in Git, rendered Compose output, `.env`, Ansible facts/cache, command arguments or CI artifacts.
- Infisical references are separated into `/agent-context/prod/postgres`, `/agent-context/prod/neo4j`, `/agent-context/prod/garage`, `/agent-context/prod/ingestion`, `/agent-context/prod/mcp` and `/agent-context/prod/otel`.
- Persistent root is `/srv/agent-context`; Ansible refuses to deploy if it is a symlink, unencrypted mount or owned by an unexpected account.
- Independent backup root is `/srv/infra-backups/agent-context`; it must not resolve to the same filesystem or Garage data volume.
- New installations use the generic S3 API backed by Garage. Existing MinIO endpoints remain an adapter compatibility target, not the recommended deployed service.
- Garage starts single-node with replication factor 1 for the personal MVP, so it is not itself a backup. Off-volume object copies and metadata snapshots are mandatory.
- Garage uses SQLite metadata, `metadata_fsync = true`, `data_fsync = true` and automatic metadata snapshots. These trade throughput for power-loss resilience appropriate to this workload.
- PostgreSQL WAL is archived continuously; target RPO is 15 minutes and PostgreSQL/MCP RTO is four hours.
- Neo4j data is disposable. Recovery rebuild target is 24 hours and no runbook treats a Neo4j dump as canonical.
- Container images are pinned by immutable digest in `versions.lock`; floating `latest` tags are forbidden.
- Destructive maintenance requires an exact service/resource identifier, a preflight listing and a recent successful backup check.
- Root Compose, lockfiles, inventory and shared Nginx/Alertmanager routers have one integration owner.

---

## File map

```text
agent-context-infra/
├── compose/
│   ├── datastores.yml
│   ├── platform.yml
│   ├── ingress.yml
│   └── observability.yml
├── config/
│   ├── garage/garage.toml
│   ├── nginx/agent-context.conf
│   ├── otel/collector.yaml
│   ├── prometheus/prometheus.yml
│   ├── prometheus/rules.yml
│   ├── alertmanager/alertmanager.yml
│   └── grafana/provisioning/
├── ansible/
│   ├── ansible.cfg
│   ├── inventories/homelab/hosts.yml
│   ├── playbooks/deploy.yml
│   ├── playbooks/rollback.yml
│   ├── playbooks/restore-drill.yml
│   └── roles/agent_context/
├── tofu/
│   └── proxmox/
├── scripts/
│   ├── compose-with-secrets.sh
│   ├── bootstrap.sh
│   ├── health.sh
│   ├── smoke.sh
│   ├── backup-postgres.sh
│   ├── backup-garage.sh
│   ├── restore-drill.sh
│   ├── scan-canaries.sh
│   └── lock-versions.sh
├── tests/
│   ├── bats/
│   ├── compose/
│   ├── nginx/
│   └── restore/
├── runbooks/
├── docker-compose.yml
├── versions.lock
└── README.md
```

## Task INFRA-001: Repository and validation scaffold

**Files:** `README.md`, `.gitignore`, `.editorconfig`, `docker-compose.yml`, `ansible/ansible.cfg`, `tests/bats/smoke.bats`, `.github/workflows/quality.yml`.

**Interfaces:** Produces `make validate`/documented validation command and empty Compose/Ansible entry points.

- [ ] Write a failing test that requires the four Compose fragments, six mandatory runbook names and a schema-valid `versions.lock`.
- [ ] Add CI for `docker compose config --quiet`, ansible-lint, yamllint, ShellCheck, Bats and secret scanning.
- [ ] Ignore only runtime/secret/rendered paths; do not ignore committed lock/config templates.
- [ ] Document repository boundaries, host directory contract and worktree rules.
- [ ] Run all scaffold validators and commit `chore: scaffold homelab infrastructure`.

## Task INFRA-010: PostgreSQL, Neo4j and Garage fragments

**Files:** `compose/datastores.yml`, `config/garage/garage.toml`, `tests/compose/test_datastores.bats`, `tests/compose/test_image_pins.bats`.

**Interfaces:** Produces named services `postgres`, `neo4j`, `garage` and private volumes rooted below `/srv/agent-context/data`.

- [ ] Write Compose-model tests asserting no datastore publishes a host port, every image uses a digest, every persistent path is explicit and every service has a bounded health check.
- [ ] Configure PostgreSQL with checksums at initialization, SCRAM authentication, `wal_level=replica`, archive command, statement/lock/idle-transaction timeouts and separate API/projector roles created later by migrations.
- [ ] Configure Neo4j Community with Bolt/HTTP only on the private network, APOC excluded unless a tested projector requires its core allowlist, page cache/heap limits and a read-only retrieval credential.
- [ ] Pin `dxflrs/garage:v2.3.0` by registry digest. Configure SQLite metadata, `metadata_fsync = true`, `data_fsync = true`, `metadata_auto_snapshot_interval = "6h"`, private S3/admin binds and bucket `agent-context-content`.
- [ ] Keep Garage S3 endpoint/path-style/region settings generic in platform environment so a compatibility profile can point at an existing MinIO service without Compose changes.
- [ ] Add CPU/memory/PID limits and log rotation appropriate to the homelab host; validate OOM restart behavior.
- [ ] Commit `feat: define canonical datastores`.

## Task INFRA-011: Networks, host volumes and Infisical contract

**Files:** `compose/datastores.yml`, `ansible/roles/agent_context/tasks/preflight.yml`, `ansible/roles/agent_context/tasks/directories.yml`, `ansible/roles/agent_context/templates/infisical-contract.yml.j2`, `tests/bats/preflight.bats`, `runbooks/secrets.md`.

**Interfaces:** Produces internal networks `agent_context_data`, `agent_context_telemetry`, edge network `agent_context_edge`, host directory tree and required secret-name manifest.

- [ ] Test that only Nginx joins edge and platform joins edge/data/telemetry; PostgreSQL/Garage/Neo4j join data only.
- [ ] Preflight `/srv/agent-context` with `findmnt`/`lsblk`, exact owner/group and mode `0750`. Stop rather than formatting, mounting or changing an unexpected filesystem.
- [ ] Create explicit subdirectories for PostgreSQL, Garage metadata/data/snapshots, Neo4j, platform cache and observability with least-privilege UIDs/modes.
- [ ] Define secret names for database roles, Neo4j, Garage RPC/admin/access keys, separate ingestion/MCP token verifier material, signing checkpoint key and Grafana admin. Store only Infisical paths/key names in Git.
- [ ] Make `no_log: true` mandatory for secret-bearing Ansible tasks and disable persistent fact caching.
- [ ] Write `scripts/compose-with-secrets.sh` to exec the Infisical CLI directly into Docker Compose without temp files or argument-expanded values.
- [ ] Commit `feat: isolate networks volumes and secret references`.

## Task INFRA-012: Idempotent datastore bootstrap and health

**Files:** `scripts/bootstrap.sh`, `scripts/health.sh`, `tests/bats/bootstrap.bats`, `tests/bats/health.bats`, `runbooks/bootstrap.md`.

**Interfaces:** Produces retry-safe bucket/key initialization and aggregate component readiness.

- [ ] Write tests that invoke bootstrap twice and compare PostgreSQL extensions/roles, Garage bucket/key permissions and Neo4j status.
- [ ] Wait on bounded health conditions; distinguish dependency unavailable from authentication/configuration failure.
- [ ] Create/allow the Garage application key only for `agent-context-content`; do not grant admin API access to platform credentials.
- [ ] Validate S3 put/head/get/delete against a namespaced bootstrap object and remove only that exact object.
- [ ] Validate PostgreSQL read/write transaction and Neo4j parameterized read through their least-privilege accounts.
- [ ] Emit a content-free JSON status summary and commit `feat: bootstrap homelab datastores safely`.

## Task INFRA-040: Platform image, migration job and projector workers

**Files:** `compose/platform.yml`, `scripts/migrate.sh`, `tests/compose/test_platform.bats`, `tests/bats/migrate.bats`, `runbooks/deploy.md`, `runbooks/rollback.md`.

**Interfaces:** Produces `platform-migrate`, `platform-api`, `platform-projector`, `platform-indexer` services from one immutable platform image.

- [ ] Test that API replicas cannot start healthy until the one-shot migration job succeeds; projector/indexer never run migrations.
- [ ] Pin platform image digest from the G4 release and record its Git SHA, SDK version and schema head in labels/`versions.lock`.
- [ ] Run migrations with a dedicated migration credential, advisory lock and exact head assertion. Refuse a downgrade in normal deploy.
- [ ] Give API, projector and indexer distinct least-privilege credentials and commands. Mount repositories read-only into the isolated indexer only when a scan job is active.
- [ ] Configure rootless user, read-only root filesystem, dropped capabilities, `no-new-privileges`, tmpfs scratch and bounded resources.
- [ ] Implement rollback as image reversal only when schema compatibility metadata allows it; otherwise route to restore runbook.
- [ ] Commit `feat: deploy pinned context platform roles`.

## Task INFRA-041: Private NetBird HTTPS ingress and token injection

**Files:** `compose/ingress.yml`, `config/nginx/agent-context.conf`, `ansible/roles/agent_context/tasks/netbird.yml`, `tests/nginx/test_ingress.bats`, `tests/bats/private_endpoint.bats`, `runbooks/networking.md`.

**Interfaces:** Exposes `https://context.home.arpa/mcp`, `/v1/ingestion/batches`, `/health/live` and `/health/ready` only on the host NetBird address.

- [ ] Resolve the active NetBird IPv4 during Ansible preflight and render an explicit listen address; fail if the interface is absent or the address is public/unspecified.
- [ ] Configure TLS 1.2/1.3, trusted private certificate chain, strict Host allowlist, request/body/header/time limits, no buffering of unbounded bodies and sanitized access logs.
- [ ] Proxy `/mcp` round-robin to platform API replicas with no cookie insertion, source-IP affinity or session cache.
- [ ] Permit POST only for `/mcp` and `/v1/ingestion/batches`, apply a stricter ingestion body/rate limit, and block every other path by default; keep datastore/admin/metrics endpoints internal.
- [ ] Inject platform-side ingestion/MCP verifier material and the two Codex client bearers through separate Infisical identities. Never expose either client bearer to Nginx configuration or reuse it across planes.
- [ ] Test public LAN address refusal, wrong Host/Origin, missing/invalid bearer and successful NetBird request.
- [ ] Commit `feat: expose private stateless MCP ingress`.

## Task INFRA-042: Two-replica stateless scale and restart test

**Files:** `compose/platform.yml`, `config/nginx/agent-context.conf`, `scripts/smoke-replicas.sh`, `tests/bats/replicas.bats`, `runbooks/scaling.md`.

**Interfaces:** Produces two independently restartable `platform-api` replicas and response header `X-Agent-Context-Replica` for private diagnostics.

- [ ] Send sequential direct MCP calls until both replica IDs are observed; do not retain cookies between requests.
- [ ] Kill the replica serving the prior request and assert the next request succeeds against the other replica without discovery/initialization replay.
- [ ] Assert neither response nor proxy log contains `Mcp-Session-Id` and Nginx has no sticky directive.
- [ ] Load-test the personal target plus 3× headroom; record CPU/memory and warm p95 at or below two seconds.
- [ ] Commit `test: prove stateless MCP replica failover`.

## Task INFRA-050: OpenTelemetry, Prometheus and Grafana

**Files:** `compose/observability.yml`, `config/otel/collector.yaml`, `config/prometheus/prometheus.yml`, `config/grafana/provisioning/`, `tests/compose/test_observability.bats`, `tests/bats/telemetry_canary.bats`.

**Interfaces:** Collects platform/Codex metadata telemetry and exports bounded metrics/traces without feeding platform-origin data back into ingestion.

- [ ] Configure OTel Collector memory limiter, batch processor, attribute allowlist/redaction processor and separate receivers for local NetBird Codex clients and Compose services.
- [ ] Reject log bodies, prompt content, tool arguments/results, authorization, cookies and S3/database DSNs at the collector boundary.
- [ ] Provision dashboards for ingestion rate/error, spool age, outbox/DLQ, projection lag, MCP latency/errors, Postgres WAL/archive, Garage health, Neo4j rebuild and host capacity.
- [ ] Label platform-origin telemetry and drop it from any Codex-event normalization input to prevent loops.
- [ ] Inject secret canaries through synthetic telemetry and assert zero exporter/storage occurrences.
- [ ] Commit `feat: observe the platform without content leakage`.

## Task INFRA-051: Actionable alerts

**Files:** `config/prometheus/rules.yml`, `config/alertmanager/alertmanager.yml`, `tests/bats/alerts.bats`, `runbooks/alerts.md`.

**Interfaces:** Produces alerts linked to exact runbook sections; default local receiver is explicit and content-free.

- [ ] Add alerts for oldest spool item over 30 minutes, DLQ nonzero, projection lag over 60 seconds, ledger integrity failure, WAL archive failure, Garage unhealthy/lost block, disk over 80%, MCP 5xx/error ratio and backup/restore drill age.
- [ ] Use `for` windows to avoid flapping and labels `severity`, `component`, `runbook`; avoid IDs and repository names as high-cardinality labels.
- [ ] Unit-test PromQL with promtool fixtures and fire each alert through synthetic metrics.
- [ ] Keep notification routing local until the user explicitly connects an external recipient.
- [ ] Commit `feat: alert on recoverable context failures`.

## Task INFRA-052: Canonical backup pipeline

**Files:** `config/pgbackrest/pgbackrest.conf`, `scripts/backup-postgres.sh`, `scripts/backup-garage.sh`, `scripts/verify-backups.sh`, `tests/bats/backup.bats`, `runbooks/backup.md`.

**Interfaces:** Produces continuous PostgreSQL WAL archive, daily full/differential backups, Garage metadata snapshots and logical S3 object copy to the independent backup root.

- [ ] Configure pgBackRest repository below `/srv/infra-backups/agent-context/postgres`, encryption from Infisical, retention and `archive-async`; alert if the newest archived WAL exceeds 15 minutes.
- [ ] Schedule weekly full and daily differential PostgreSQL backup, plus post-backup `check` and manifest recording.
- [ ] Trigger `garage meta snapshot --all`; copy the clean snapshot, `cluster_layout`, `data_layout`, `node_key` and public node key into a dated independent backup set with mode `0600`.
- [ ] Copy the `agent-context-content` bucket through the S3 API to `/srv/infra-backups/agent-context/objects/current`, then create a read-only filesystem snapshot/versioned backup before any sync can propagate deletions.
- [ ] Verify a deterministic sample and all objects referenced since the previous backup against PostgreSQL SHA-256 values.
- [ ] Sign the backup manifest and ledger stream-head checkpoint with the independent checkpoint key.
- [ ] Never include Neo4j as a prerequisite for canonical recovery; optionally retain short diagnostic dumps only.
- [ ] Test disk-full, stale WAL, interrupted object copy, corrupt sample and inaccessible Infisical cases.
- [ ] Commit `feat: back up canonical context data independently`.

## Task INFRA-053: Automated isolated restore and graph rebuild drill

**Files:** `scripts/restore-drill.sh`, `ansible/playbooks/restore-drill.yml`, `tests/restore/test_drill.bats`, `runbooks/restore.md`, `runbooks/replay-projections.md`, `runbooks/purge-content.md`.

**Interfaces:** Restores into exact isolated root `/srv/agent-context-restore-drill` and distinct Compose project `agent_context_restore_drill`.

- [ ] Preflight that the isolated root is neither the live root nor its parent and that no live service/container/volume name is targeted.
- [ ] Restore PostgreSQL to a selected timestamp, restore Garage metadata/object copy, start API/MCP, then rebuild Neo4j from an empty database through projector replay.
- [ ] Verify PostgreSQL constraints, stream hash chains, signed checkpoint, every content reference, object digests, projection checkpoint and deterministic graph digest.
- [ ] Run 30 retrieval fixtures and the canary scanner against both restored canonical data and rebuilt graph.
- [ ] Record measured RPO, PostgreSQL/MCP RTO and graph rebuild RTO as a signed drill report; fail outside 15 minutes/four hours/24 hours.
- [ ] Clean up only after preserving the report and only by exact Compose project/root checks. A failed drill stays available for diagnosis.
- [ ] Schedule quarterly through the existing homelab scheduler and commit `feat: prove isolated disaster recovery`.

## Task INFRA-060: Immutable homelab release

**Files:** `versions.lock`, `scripts/lock-versions.sh`, `ansible/playbooks/deploy.yml`, `ansible/playbooks/rollback.yml`, `CHANGELOG.md`, `runbooks/release.md`, `README.md`.

**Interfaces:** Produces one auditable release manifest for all four repositories and runtime images.

- [ ] Generate `versions.lock` from verified registry manifests and Git tags; include repository commit SHAs, SDK wheel digest, platform/Codex versions, database/observability image digests, schema head and MCP conformance version.
- [ ] Verify signatures/SBOMs where upstream provides them and record license/source URL for every image.
- [ ] Run deployment from a clean clone using only the lock plus Infisical/Ansible inventory; reject any image whose resolved digest differs.
- [ ] Execute smoke, two-replica, canary, backup verification and restore drill before tagging.
- [ ] Document the Garage replacement decision: maintained default for new installs, generic S3 contract, existing MinIO compatibility, and no product-specific API dependency.
- [ ] Tag `v0.1.0` only after G6 and commit `chore: lock homelab v0.1.0 release`.

## OpenTofu boundary for Proxmox

OpenTofu is optional because the MVP can deploy onto an existing encrypted Linux VM. If used, `tofu/proxmox` may create exactly one VM named `agent-context-01` with a dedicated encrypted data disk and NetBird bootstrap metadata. It must not manage database schemas, Compose resources, DNS secrets or application configuration. State uses the existing protected homelab backend; local state is forbidden. An independent worktree owns this directory and merges before INFRA-011 preflight integration.

## Completion gate

Run on the target homelab host from a clean clone:

```bash
docker compose config --quiet
ansible-lint ansible
yamllint .
shellcheck scripts/*.sh
bats tests/bats
scripts/compose-with-secrets.sh up -d --wait
scripts/smoke.sh
scripts/smoke-replicas.sh
scripts/scan-canaries.sh
scripts/verify-backups.sh
scripts/restore-drill.sh
```

Acceptance requires valid private TLS, no public datastore/API listeners, two stateless MCP replicas, zero secret canaries, current WAL/object backups, successful isolated restore, verified event chains/content digests, deterministic Neo4j rebuild and measured RPO/RTO within targets.

## Primary references

- [Garage quick start and v2.3 single-node configuration](https://garagehq.deuxfleurs.fr/documentation/quick-start/)
- [Garage configuration and metadata snapshots](https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/)
- [Garage durability and repairs](https://garagehq.deuxfleurs.fr/documentation/operations/durability-repairs/)
- [Garage recovery](https://garagehq.deuxfleurs.fr/documentation/operations/recovering/)
- [Garage project](https://github.com/deuxfleurs-org/garage)
- [Archived MinIO Community repository](https://github.com/minio/minio)
- [PostgreSQL continuous archiving](https://www.postgresql.org/docs/current/continuous-archiving.html)
- [Neo4j Community operations manual](https://neo4j.com/docs/operations-manual/current/introduction/)
