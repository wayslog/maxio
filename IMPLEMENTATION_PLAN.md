# Maxio Implementation Plan: Phases 21-24

## Implementation Status

| Phase | Feature | Status | Files Changed |
|-------|---------|--------|---------------|
| 21 | SSE for Erasure Mode | COMPLETED | erasure/objects.rs, xl/storage.rs |
| 22 | S3 Select | COMPLETED | New crate: maxio-s3select, handlers/select.rs |
| 23 | Site Replication | COMPLETED | New module: site_replication/, handlers/site_replication.rs |
| 24.1 | KMS Integration | COMPLETED | New crate: maxio-kms |
| 24.2 | LDAP Identity Provider | COMPLETED | maxio-iam/src/ldap.rs |
| 24.3 | OpenID Connect | COMPLETED | maxio-iam/src/openid.rs |
| 24.4 | Tiering to S3 | COMPLETED | maxio-storage/src/tiering/ |
| 24.5 | Extended STS Operations | COMPLETED | handlers/sts.rs (GetCallerIdentity, GetSessionToken) |

## Gap Analysis Summary

### Current State (Phases 0-20 Complete)
- 11 crates, ~18,743 lines of Rust
- S3 API: 83 operations implemented (~98% coverage)
- Storage: Single-disk + Erasure coding (Reed-Solomon 4+2)
- Distributed: Grid RPC, dsync, bucket replication, healing
- Auth: Signature V2/V4, presigned URLs, basic IAM
- Admin: Metrics, batch jobs, health endpoints

### Feature Gaps vs MinIO Go

| Feature | MinIO Status | Maxio Status | Priority |
|---------|--------------|--------------|----------|
| S3 Select | Full (CSV/JSON/Parquet/SQL) | MISSING | HIGH |
| SSE Erasure Mode | Full | NOT_IMPLEMENTED (3 locations) | HIGH |
| Site Replication | Full | MISSING | HIGH |
| KMS Integration | KES backend | MISSING | MEDIUM |
| LDAP/OpenID | Full | MISSING | MEDIUM |
| Tiering/Warm Backends | S3/Azure/GCS | MISSING | MEDIUM |
| FTP/SFTP | Full | MISSING | LOW |
| Metrics v3 | 15+ subsystems | Basic only | LOW |
| Extended STS | Full | AssumeRole only | LOW |

---

## Phase 21: SSE for Erasure Mode (HIGH PRIORITY)

### Objective
Enable server-side encryption for erasure-coded storage mode.

### Current Blockers
- `crates/maxio-storage/src/erasure/objects.rs:380` - SSE encryption not implemented
- `crates/maxio-storage/src/erasure/objects.rs:551` - SSE decryption not implemented
- `crates/maxio-storage/src/erasure/objects.rs:720` - SSE multipart not implemented

### Tasks

#### 21.1 SSE-S3 for Erasure Objects
- **Effort**: M
- **Files**: `maxio-storage/src/erasure/objects.rs`, `maxio-crypto/src/`
- **Description**: Implement SSE-S3 encryption/decryption for erasure-coded objects
- **Success Criteria**: Objects encrypted with SSE-S3 can be stored and retrieved in erasure mode

#### 21.2 SSE-C for Erasure Objects
- **Effort**: M
- **Files**: `maxio-storage/src/erasure/objects.rs`
- **Description**: Implement SSE-C (customer-provided keys) for erasure mode
- **Success Criteria**: Objects encrypted with customer keys work in erasure mode

#### 21.3 SSE for Multipart Uploads (Erasure)
- **Effort**: M
- **Files**: `maxio-storage/src/erasure/objects.rs`, `maxio-s3-api/src/handlers/multipart.rs`
- **Description**: Enable SSE for multipart uploads in erasure mode
- **Success Criteria**: Large encrypted objects can be uploaded via multipart API

### Dependencies
```
21.1 ─┬─> 21.3
21.2 ─┘
```

---

## Phase 22: S3 Select (HIGH PRIORITY)

### Objective
Implement S3 Select API for querying object contents with SQL.

### Reference
- MinIO: `internal/s3select/` (csv, json, parquet, sql packages)

### Tasks

#### 22.1 Create maxio-s3select Crate
- **Effort**: S
- **Files**: New crate `crates/maxio-s3select/`
- **Description**: Initialize crate structure with SQL parser foundation
- **Success Criteria**: Crate compiles, basic SQL AST types defined

#### 22.2 CSV Input Format
- **Effort**: M
- **Files**: `maxio-s3select/src/csv.rs`
- **Description**: Implement CSV parsing with configurable delimiters, headers, comments
- **Success Criteria**: CSV files can be parsed and queried

#### 22.3 JSON Input Format
- **Effort**: M
- **Files**: `maxio-s3select/src/json.rs`
- **Description**: Implement JSON/JSON Lines parsing
- **Success Criteria**: JSON objects can be queried with SQL

#### 22.4 SQL Query Engine
- **Effort**: L
- **Files**: `maxio-s3select/src/sql/`
- **Description**: Implement SQL parser and evaluator (SELECT, WHERE, LIMIT, aggregations)
- **Success Criteria**: Basic SQL queries execute correctly

#### 22.5 S3 Select API Handler
- **Effort**: M
- **Files**: `maxio-s3-api/src/handlers/select.rs`, `maxio-s3-api/src/router.rs`
- **Description**: Add POST object select handler with XML request/response
- **Success Criteria**: AWS CLI `s3api select-object-content` works

#### 22.6 Parquet Support (Optional)
- **Effort**: L
- **Files**: `maxio-s3select/src/parquet.rs`
- **Description**: Add Parquet file format support using arrow-rs
- **Success Criteria**: Parquet files can be queried

### Dependencies
```
22.1 ──> 22.2 ──┐
         22.3 ──┼──> 22.4 ──> 22.5
         22.6 ──┘
```

---

## Phase 23: Site Replication (HIGH PRIORITY)

### Objective
Implement multi-cluster federation for disaster recovery.

### Reference
- MinIO: `cmd/site-replication.go`, `cmd/site-replication-metrics.go`

### Tasks

#### 23.1 Site Replication Configuration
- **Effort**: M
- **Files**: `maxio-distributed/src/site_replication/config.rs`
- **Description**: Define site replication configuration (peer sites, credentials, sync settings)
- **Success Criteria**: Site replication can be configured via admin API

#### 23.2 IAM Sync
- **Effort**: M
- **Files**: `maxio-distributed/src/site_replication/iam_sync.rs`
- **Description**: Synchronize users, groups, policies across sites
- **Success Criteria**: IAM changes propagate to peer sites

#### 23.3 Bucket Metadata Sync
- **Effort**: M
- **Files**: `maxio-distributed/src/site_replication/bucket_sync.rs`
- **Description**: Synchronize bucket configurations (versioning, lifecycle, etc.)
- **Success Criteria**: Bucket settings replicate across sites

#### 23.4 Object Replication (Site-level)
- **Effort**: L
- **Files**: `maxio-distributed/src/site_replication/object_sync.rs`
- **Description**: Extend existing replication for site-level object sync
- **Success Criteria**: Objects replicate bidirectionally between sites

#### 23.5 Site Replication Admin API
- **Effort**: M
- **Files**: `maxio-admin/src/handlers/site_replication.rs`
- **Description**: Admin endpoints for site-add, site-remove, site-status
- **Success Criteria**: mc admin replicate commands work

### Dependencies
```
23.1 ──> 23.2 ──┐
         23.3 ──┼──> 23.4 ──> 23.5
```

---

## Phase 24: Enterprise Features (MEDIUM PRIORITY)

### Objective
Add enterprise-grade features for production deployments.

### Tasks

#### 24.1 KMS Integration
- **Effort**: L
- **Files**: New `maxio-kms/` crate
- **Description**: External key management with KES backend support
- **Success Criteria**: SSE-KMS works with external key server

#### 24.2 LDAP Identity Provider
- **Effort**: L
- **Files**: `maxio-iam/src/ldap.rs`
- **Description**: LDAP authentication and group mapping
- **Success Criteria**: Users can authenticate via LDAP

#### 24.3 OpenID Connect
- **Effort**: M
- **Files**: `maxio-iam/src/oidc.rs`
- **Description**: OpenID Connect authentication flow
- **Success Criteria**: SSO via OpenID providers works

#### 24.4 Tiering to S3
- **Effort**: L
- **Files**: `maxio-storage/src/tier/s3.rs`
- **Description**: Transition objects to remote S3 storage
- **Success Criteria**: Lifecycle transitions to S3 tier work

#### 24.5 Extended STS Operations
- **Effort**: S
- **Files**: `maxio-s3-api/src/handlers/sts.rs`
- **Description**: Add GetCallerIdentity, GetSessionToken
- **Success Criteria**: Full STS API compatibility

### Dependencies
```
24.1 (independent)
24.2 ──> 24.3 (can parallel with 24.1)
24.4 (independent)
24.5 (independent)
```

---

## Execution Order

### Wave 1 (Parallel)
- Phase 21: SSE Erasure Mode (blocks distributed encryption)
- Phase 22.1-22.3: S3 Select foundation

### Wave 2 (Parallel)
- Phase 22.4-22.5: S3 Select completion
- Phase 23.1-23.3: Site Replication foundation

### Wave 3 (Parallel)
- Phase 23.4-23.5: Site Replication completion
- Phase 24.1: KMS Integration
- Phase 24.5: Extended STS

### Wave 4 (Parallel)
- Phase 24.2-24.3: Identity Providers
- Phase 24.4: Tiering
- Phase 22.6: Parquet (optional)

---

## Effort Estimates

| Size | Hours | Description |
|------|-------|-------------|
| S | 2-4 | Single file, straightforward |
| M | 4-8 | Multiple files, moderate complexity |
| L | 8-16 | New subsystem, significant complexity |
| XL | 16+ | Major feature, cross-cutting |

### Total Estimated Effort
- Phase 21: ~20 hours (3x M)
- Phase 22: ~40 hours (1S + 3M + 2L)
- Phase 23: ~36 hours (4M + 1L)
- Phase 24: ~44 hours (3L + 1M + 1S)

**Total: ~140 hours for 100% MinIO binary compatibility**

---

## Success Criteria for 100% Compatibility

1. All S3 API operations pass AWS SDK compatibility tests
2. SSE-S3/SSE-C work in both single-disk and erasure modes
3. S3 Select queries work for CSV and JSON formats
4. Site replication syncs IAM, buckets, and objects
5. mc (MinIO Client) commands work without modification
6. Prometheus metrics match MinIO's metric names
