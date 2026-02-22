# S3 Object Operations Analysis

This document details all S3 object operations implemented in MinIO.

## Standard Object Operations

### GetObject
- Handler: `GetObjectHandler` (object-handlers.go:717)
- Features:
  - Range requests (HTTP Range header)
  - Conditional gets (If-Modified-Since, If-Unmodified-Since, If-Match, If-None-Match)
  - SSE-C decryption
  - Checksums (x-amz-checksum-*)
  - Part number selection
  - Archive extraction

### HeadObject
- Handler: `HeadObjectHandler` (object-handlers.go:1012)
- Features:
  - Range requests
  - Conditional gets
  - SSE-C validation
  - Checksum headers
  - Part number support

### GetObjectAttributes
- Handler: `GetObjectAttributesHandler` (object-handlers.go:988)
- Features:
  - Selective attribute retrieval (ETag, Checksum, ObjectSize, StorageClass, ObjectParts)

### PutObject
- Handler: `PutObjectHandler` (object-handlers.go:1793)
- Features:
  - SSE-S3, SSE-KMS, SSE-C encryption
  - Checksums (Content-MD5, x-amz-checksum-*)
  - Object tagging
  - Retention/legal hold
  - Storage class
  - Streaming signatures (v4 chunked)

### CopyObject
- Handler: `CopyObjectHandler` (object-handlers.go:1157)
- Features:
  - Range requests (x-amz-copy-source-range)
  - Conditional copy (x-amz-copy-source-if-*)
  - SSE-C source/destination encryption
  - Metadata directives (COPY/REPLACE)
  - Tag directives

### DeleteObject
- Handler: `DeleteObjectHandler` (object-handlers.go:2563)
- Features:
  - Version deletion
  - Retention bypass
  - Delete markers
  - Object locking

### SelectObjectContent (S3 Select)
- Handler: `SelectObjectContentHandler` (object-handlers.go:105)
- Features:
  - SQL queries on objects
  - SSE-C decryption
  - CSV, JSON, Parquet formats

## Object Metadata Operations

| Operation | Handler | Line |
|-----------|---------|------|
| PutObjectLegalHold | PutObjectLegalHoldHandler | 2752 |
| GetObjectLegalHold | GetObjectLegalHoldHandler | 2847 |
| PutObjectRetention | PutObjectRetentionHandler | 2909 |
| GetObjectRetention | GetObjectRetentionHandler | 3016 |
| PutObjectTagging | PutObjectTaggingHandler | 3179 |
| GetObjectTagging | GetObjectTaggingHandler | 3087 |
| DeleteObjectTagging | DeleteObjectTaggingHandler | 3292 |
| RestoreObject | PostRestoreObjectHandler | 3398 |

## Multipart Upload Operations

### InitiateMultipartUpload
- Handler: `NewMultipartUploadHandler` (object-multipart-handlers.go:64)
- Features:
  - SSE-S3, SSE-KMS, SSE-C encryption
  - Checksums (x-amz-checksum-algorithm)
  - Storage class
  - Object tagging
  - Retention/legal hold

### UploadPart
- Handler: `PutObjectPartHandler` (object-multipart-handlers.go:590)
- Features:
  - SSE-C encryption
  - Checksums (Content-MD5, x-amz-checksum-*)
  - Part size validation (5MB-5GB)

### UploadPartCopy
- Handler: `CopyObjectPartHandler` (object-multipart-handlers.go:252)
- Features:
  - Range requests (x-amz-copy-source-range)
  - Conditional copy
  - SSE-C source/destination encryption

### CompleteMultipartUpload
- Handler: `CompleteMultipartUploadHandler` (object-multipart-handlers.go:914)
- Features:
  - ETag computation (multipart ETag)
  - Checksum finalization
  - Part ordering validation

### AbortMultipartUpload
- Handler: `AbortMultipartUploadHandler` (object-multipart-handlers.go:1107)
- Features:
  - Cleanup of incomplete uploads

### ListParts
- Handler: `ListObjectPartsHandler` (object-multipart-handlers.go:1152)
- Features:
  - Pagination
  - Part listing with ETags and sizes

## Key Features

### Encryption Support
- SSE-S3: Server-Side Encryption with S3-managed keys
- SSE-KMS: Server-Side Encryption with KMS
- SSE-C: Server-Side Encryption with Customer-provided keys

### Range Request Support
- GetObject: Standard HTTP Range header (bytes=start-end)
- CopyObject: x-amz-copy-source-range header
- CopyObjectPart: x-amz-copy-source-range with part-specific validation

### Conditional Operations
- If-Modified-Since / x-amz-copy-source-if-modified-since
- If-Unmodified-Since / x-amz-copy-source-if-unmodified-since
- If-Match / x-amz-copy-source-if-match (ETag)
- If-None-Match / x-amz-copy-source-if-none-match (ETag)

### Checksum Support
- Content-MD5 (legacy)
- x-amz-checksum-algorithm (CRC32, CRC32C, SHA1, SHA256)
- Trailing checksums in streaming uploads

### Object Locking
- Legal hold (ON/OFF)
- Retention (GOVERNANCE/COMPLIANCE modes)
- Bypass enforcement for authorized users

### Multipart Constraints
- Part size: 5MB - 5GB per part
- Maximum parts: 10,000
- Maximum object size: 5TB
