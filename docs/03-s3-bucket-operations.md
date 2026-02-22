# S3 Bucket Operations Analysis

This document details all S3 bucket operations implemented in MinIO.

## Core Bucket Operations (bucket-handlers.go)

| Operation | Handler | Description |
|-----------|---------|-------------|
| CreateBucket | PutBucketHandler | Create a new bucket |
| DeleteBucket | DeleteBucketHandler | Delete an empty bucket |
| HeadBucket | HeadBucketHandler | Check bucket existence |
| GetBucketLocation | GetBucketLocationHandler | Get bucket region |
| ListBuckets | ListBucketsHandler | List all buckets |
| DeleteMultipleObjects | DeleteMultipleObjectsHandler | Batch delete objects |
| PostPolicyBucket | PostPolicyBucketHandler | POST form upload |
| GetBucketPolicyStatus | GetBucketPolicyStatusHandler | Check if bucket is public |

## Bucket Tagging Operations

| Operation | Handler |
|-----------|---------|
| PutBucketTagging | PutBucketTaggingHandler |
| GetBucketTagging | GetBucketTaggingHandler |
| DeleteBucketTagging | DeleteBucketTaggingHandler |

## Object Lock Operations

| Operation | Handler |
|-----------|---------|
| PutBucketObjectLockConfig | PutBucketObjectLockConfigHandler |
| GetBucketObjectLockConfig | GetBucketObjectLockConfigHandler |

## List Operations (bucket-listobjects-handlers.go)

| Operation | Handler | Description |
|-----------|---------|-------------|
| ListObjectsV1 | ListObjectsV1Handler | Legacy list (marker-based) |
| ListObjectsV2 | ListObjectsV2Handler | Modern list (continuation token) |
| ListObjectVersions | ListObjectVersionsHandler | List with versions |

## Lifecycle Operations (bucket-lifecycle-handlers.go)

| Operation | Handler |
|-----------|---------|
| PutBucketLifecycle | PutBucketLifecycleHandler |
| GetBucketLifecycle | GetBucketLifecycleHandler |
| DeleteBucketLifecycle | DeleteBucketLifecycleHandler |

### Lifecycle Rule Types
- Expiration: Delete objects after N days
- NoncurrentVersionExpiration: Delete old versions
- Transition: Move to different storage class
- AbortIncompleteMultipartUpload: Clean up incomplete uploads

## Notification Operations (bucket-notification-handlers.go)

| Operation | Handler |
|-----------|---------|
| PutBucketNotification | PutBucketNotificationHandler |
| GetBucketNotification | GetBucketNotificationHandler |

### Notification Targets
- Kafka
- AMQP (RabbitMQ)
- Redis
- NATS
- Elasticsearch
- Webhook
- PostgreSQL
- MySQL

## Policy Operations (bucket-policy-handlers.go)

| Operation | Handler |
|-----------|---------|
| PutBucketPolicy | PutBucketPolicyHandler |
| GetBucketPolicy | GetBucketPolicyHandler |
| DeleteBucketPolicy | DeleteBucketPolicyHandler |

## Versioning Operations (bucket-versioning-handler.go)

| Operation | Handler |
|-----------|---------|
| PutBucketVersioning | PutBucketVersioningHandler |
| GetBucketVersioning | GetBucketVersioningHandler |

### Versioning States
- Enabled: All objects get version IDs
- Suspended: New objects get null version ID
- Unversioned: No versioning (default)

## Encryption Operations (bucket-encryption-handlers.go)

| Operation | Handler |
|-----------|---------|
| PutBucketEncryption | PutBucketEncryptionHandler |
| GetBucketEncryption | GetBucketEncryptionHandler |
| DeleteBucketEncryption | DeleteBucketEncryptionHandler |

### Encryption Types
- SSE-S3: AES-256 with S3-managed keys
- SSE-KMS: Customer-managed keys via KMS

## Replication Operations (bucket-replication-handlers.go)

| Operation | Handler |
|-----------|---------|
| PutBucketReplicationConfig | PutBucketReplicationConfigHandler |
| GetBucketReplicationConfig | GetBucketReplicationConfigHandler |
| DeleteBucketReplicationConfig | DeleteBucketReplicationConfigHandler |
| GetBucketReplicationMetrics | GetBucketReplicationMetricsHandler |
| ResetBucketReplicationStart | ResetBucketReplicationStartHandler |
| ResetBucketReplicationStatus | ResetBucketReplicationStatusHandler |
| ValidateBucketReplicationCreds | ValidateBucketReplicationCredsHandler |

### Replication Features
- Active-active replication
- Replica modification sync
- Delete marker replication
- Existing object replication
- Replication metrics and status

## ACL Operations (acl-handlers.go)

| Operation | Handler |
|-----------|---------|
| PutBucketACL | PutBucketACLHandler |
| GetBucketACL | GetBucketACLHandler |
| PutObjectACL | PutObjectACLHandler |
| GetObjectACL | GetObjectACLHandler |

Note: MinIO implements ACLs for compatibility but recommends using bucket policies.

## CORS Operations

| Operation | Handler |
|-----------|---------|
| PutBucketCORS | PutBucketCORSHandler |
| GetBucketCORS | GetBucketCORSHandler |
| DeleteBucketCORS | DeleteBucketCORSHandler |
