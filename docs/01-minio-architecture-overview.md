# MinIO Architecture Overview

This document provides a comprehensive analysis of the MinIO Go codebase structure for porting to maxio (Rust).

## Project Structure

MinIO is organized into two main areas:
- `/cmd/` - Main application handlers, routers, and business logic (~254,000 lines across 453 Go files)
- `/internal/` - Reusable internal packages (~34 subdirectories)

## Internal Modules Summary

| Module | Files | Purpose |
|--------|-------|---------|
| bucket | 47 | Lifecycle, replication, versioning, encryption, bandwidth, object lock |
| config | 91 | Configuration system with submodules (api, dns, etcd, identity, lambda, notify) |
| crypto | 15 | SSE-C, SSE-S3, SSE-KMS encryption |
| disk | 24 | Platform-specific disk I/O operations |
| dsync | 12 | Distributed sync/locking |
| event | 34 | Event notification system |
| grid | 23 | Two-way grid communication for distributed operations |
| hash | 8 | Checksum/hash operations (MD5, SHA256, CRC32, etc.) |
| http | 17 | HTTP utilities and server configuration |
| kms | 11 | KMS integration (Vault, AWS KMS, etc.) |
| lock | 6 | File locking primitives |
| logger | 19 | Logging system with multiple targets |
| s3select | 54 | S3 Select SQL query processing |
| store | 6 | Queue store for notifications |

## Handler Categories

### S3 API Handlers (Core Operations)

| Category | Files | Handlers | Key Operations |
|----------|-------|----------|----------------|
| Object | 4 | 22 | GetObject, PutObject, DeleteObject, CopyObject, Multipart |
| Bucket | 8 | 42 | CreateBucket, DeleteBucket, ListBuckets, Lifecycle, Policy |
| List | 1 | 7 | ListObjectsV1, ListObjectsV2, ListObjectVersions |

### Admin API Handlers

| File | Handlers | Purpose |
|------|----------|---------|
| admin-handlers.go | 50+ | Server info, profiling, trace, heal, update |
| admin-handlers-users.go | 30+ | IAM user/group/policy management |
| admin-handlers-config-kv.go | 10+ | Configuration key-value management |
| admin-handlers-idp-ldap.go | 8 | LDAP identity provider |
| admin-handlers-idp-openid.go | 5 | OpenID Connect provider |
| admin-handlers-site-replication.go | 15 | Site replication management |
| admin-handlers-pools.go | 5 | Storage pool management |
| admin-heal-ops.go | 10 | Healing operations |

### Specialized Handlers

| Category | Files | Purpose |
|----------|-------|---------|
| Batch | 3 | Batch job operations (replicate, expire, keyrotate) |
| STS | 1 | Security Token Service (AssumeRole, etc.) |
| Tier | 1 | Tiering to remote storage (S3, Azure, GCS) |
| Metrics | 2 | Prometheus metrics endpoints |
| Health | 1 | Health check endpoints |

## Key Data Structures

### Object Layer Interface
```go
type ObjectLayer interface {
    GetObjectNInfo(ctx, bucket, object, rs, h, opts) (*GetObjectReader, error)
    PutObject(ctx, bucket, object, data, opts) (ObjectInfo, error)
    DeleteObject(ctx, bucket, object, opts) (ObjectInfo, error)
    CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts) (ObjectInfo, error)
    // ... 50+ methods
}
```

### Storage Format (XL)
- xl-storage-format-v1.go - Legacy format
- xl-storage-format-v2.go - Current format with erasure coding metadata

### Erasure Coding
- erasure-object.go - Object-level erasure operations
- erasure-sets.go - Erasure set management
- erasure-healing.go - Data healing operations

## Feature Matrix

| Feature | MinIO Status | Description |
|---------|--------------|-------------|
| S3 API | Complete | Full S3 API compatibility |
| Erasure Coding | Complete | Reed-Solomon with configurable data/parity |
| Encryption | Complete | SSE-S3, SSE-KMS, SSE-C |
| Object Locking | Complete | WORM compliance |
| Versioning | Complete | Full version support |
| Replication | Complete | Active-active, site replication |
| Lifecycle | Complete | Expiration, transition, NoncurrentVersion |
| Notifications | Complete | Kafka, AMQP, Redis, NATS, Webhook, etc. |
| IAM | Complete | Users, groups, policies, LDAP, OIDC |
| Tiering | Complete | S3, Azure, GCS backends |
| Batch Jobs | Complete | Replicate, expire, keyrotate |
| S3 Select | Complete | SQL queries on CSV, JSON, Parquet |
| FTP/SFTP | Complete | FTP and SFTP protocol support |
| Metrics | Complete | Prometheus metrics |
| Audit | Complete | Audit logging to multiple targets |
