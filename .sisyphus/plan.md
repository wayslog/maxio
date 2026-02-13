# Maxio -- Rust S3 兼容对象存储服务实现计划

> 参考实现: MinIO (Go) -- /Users/xuesong.zhao/repo/go/minio
> 目标项目: maxio (Rust) -- /Users/xuesong.zhao/repo/rust/maxio
> 创建日期: 2026-02-13

---

## 一、项目概述

Maxio 是一个用 Rust 实现的、完全兼容 AWS S3 协议的高性能分布式对象存储服务。
其架构完全参照 MinIO，包括纠删码存储引擎、分布式集群管理、IAM 安全模型、
事件通知、生命周期管理等全部子系统。

### 1.1 核心设计目标

- 完全兼容 AWS S3 API (包括 Signature V4/V2、Presigned URL、Streaming Signature)
- 纠删码 (Reed-Solomon) 存储引擎，支持可配置的数据块/校验块比例
- 多池多集群分布式架构 (erasureServerPools -> erasureSets -> erasureObjects)
- IAM 用户/组/策略/服务账户管理
- 服务端加密 (SSE-S3, SSE-C, SSE-KMS)
- 事件通知 (Kafka, NATS, Webhook 等)
- 对象生命周期管理与版本控制
- 分布式锁 (dsync) 与数据修复 (healing)

### 1.2 技术栈选型

| 组件 | 推荐 Crate | 说明 |
|------|-----------|------|
| S3 协议层 | s3s | S3 Service trait 适配器，处理路由/XML/类型 |
| HTTP 服务器 | hyper + axum | s3s 基于 hyper，axum 用于 Admin API |
| 异步运行时 | tokio | 标准异步运行时 |
| 纠删码 | reed-solomon-simd | SIMD 加速的 Reed-Solomon 编解码 |
| XML 解析 | quick-xml + serde | S3 API XML 请求/响应序列化 |
| 元数据序列化 | rmp-serde (msgpack) | 兼容 MinIO XLv2 格式 |
| 嵌入式数据库 | redb | 纯 Rust 嵌入式 KV 存储，用于元数据索引 |
| 分布式共识 | openraft | Raft 共识协议 (可选，用于强一致性场景) |
| TLS/加密 | rustls + ring | TLS 终止与加密原语 |
| 日志/追踪 | tracing + tracing-subscriber | 结构化日志与分布式追踪 |
| 配置管理 | config + serde | 多源配置加载 |
| 命令行 | clap | CLI 参数解析 |
| Prometheus | prometheus-client | 指标暴露 |

---

## 二、项目结构 (Rust Workspace)

```
maxio/
  Cargo.toml                    # workspace 根配置
  crates/
    maxio-server/               # 主服务入口 (对应 MinIO cmd/server-main.go)
      src/
        main.rs                 # 入口点
        config.rs               # 服务配置
        globals.rs              # 全局状态 (对应 MinIO cmd/globals.go)
        server.rs               # HTTP 服务启动
    maxio-s3-api/               # S3 API 层 (对应 MinIO cmd/api-router.go + handlers)
      src/
        lib.rs
        router.rs               # S3 路由注册
        handlers/
          bucket.rs             # Bucket 操作处理器
          object.rs             # Object 操作处理器
          multipart.rs          # 分片上传处理器
          tagging.rs            # 标签操作
          versioning.rs         # 版本控制
          retention.rs          # 对象保留
          acl.rs                # ACL 处理
          notification.rs       # 通知配置处理
          lifecycle.rs          # 生命周期配置处理
        middleware/
          auth.rs               # 认证中间件
          cors.rs               # CORS 处理
          tracing.rs            # 请求追踪
          rate_limit.rs         # 限流
        signature/
          v4.rs                 # AWS Signature V4 验证
          v2.rs                 # AWS Signature V2 验证
          presigned.rs          # 预签名 URL
          streaming.rs          # 流式签名 (aws-chunked)
          post_policy.rs        # POST 策略验证
        error.rs                # S3 错误码定义
        response.rs             # XML 响应构建
    maxio-storage/              # 存储引擎 (对应 MinIO cmd/erasure-*.go + xl-storage*.go)
      src/
        lib.rs
        traits.rs               # ObjectLayer + StorageAPI trait 定义
        erasure/
          mod.rs
          coding.rs             # Reed-Solomon 编解码
          encode.rs             # 数据编码
          decode.rs             # 数据解码
          metadata.rs           # 纠删码元数据
          object.rs             # erasureObjects 实现
          sets.rs               # erasureSets 实现
          server_pool.rs        # erasureServerPools 实现
          healing.rs            # 数据修复
          common.rs             # 公共工具
        xl/
          mod.rs
          storage.rs            # XL 磁盘存储实现 (对应 xl-storage.go)
          format_v2.rs          # XLv2 元数据格式 (msgpack)
          format_v1.rs          # XLv1 兼容 (JSON, 可选)
        format.rs               # 磁盘格式管理 (format.json)
        datatypes.rs            # FileInfo, ErasureInfo, ObjectPartInfo
        multipart.rs            # 分片上传状态管理
        disk.rs                 # 磁盘管理与健康检查
    maxio-iam/                  # IAM 子系统 (对应 MinIO cmd/iam*.go)
      src/
        lib.rs
        system.rs               # IAMSys 核心
        store.rs                # IAM 存储抽象 (IAMStorageAPI trait)
        object_store.rs         # 对象存储后端
        etcd_store.rs           # etcd 后端 (可选)
        cache.rs                # iamCache 内存缓存
        policy.rs               # 策略评估引擎
        credentials.rs          # 凭证管理
        user.rs                 # 用户管理
        group.rs                # 组管理
        service_account.rs      # 服务账户
    maxio-auth/                 # 认证与签名 (对应 MinIO cmd/signature-v4.go + auth-handler.go)
      src/
        lib.rs
        signature_v4.rs         # Signature V4 计算与验证
        signature_v2.rs         # Signature V2 兼容
        streaming.rs            # 流式签名验证
        jwt.rs                  # JWT 令牌处理
        sts.rs                  # Security Token Service
    maxio-crypto/               # 加密子系统 (对应 MinIO internal/crypto/)
      src/
        lib.rs
        sse_s3.rs               # SSE-S3 实现
        sse_c.rs                # SSE-C 实现
        sse_kms.rs              # SSE-KMS 实现
        key.rs                  # 密钥管理
        metadata.rs             # 加密元数据
    maxio-distributed/          # 分布式子系统 (对应 MinIO cmd/peer-rest-*.go + internal/dsync/)
      src/
        lib.rs
        endpoint.rs             # 端点解析与节点发现
        grid/
          mod.rs
          manager.rs            # Grid 连接管理器
          connection.rs         # 持久化 WebSocket 连接
          stream.rs             # 流式通信
          handler.rs            # RPC 处理器注册
        dsync/
          mod.rs
          drwmutex.rs           # 分布式读写锁
          locker.rs             # NetLocker trait
        peer/
          mod.rs
          server.rs             # Peer RPC 服务端
          client.rs             # Peer RPC 客户端
        healing/
          mod.rs
          tracker.rs            # 修复进度追踪
          sequence.rs           # 修复序列管理
          global.rs             # 全局修复协调
          mrf.rs                # MRF 队列
        replication/
          mod.rs
          bucket.rs             # Bucket 级复制
          site.rs               # 站点级复制
    maxio-notification/         # 事件通知 (对应 MinIO internal/event/)
      src/
        lib.rs
        system.rs               # EventNotifier 核心
        config.rs               # 通知配置
        target/
          mod.rs
          webhook.rs            # Webhook 目标
          kafka.rs              # Kafka 目标
          nats.rs               # NATS 目标
          amqp.rs               # AMQP 目标
          redis.rs              # Redis 目标
          postgresql.rs         # PostgreSQL 目标
          mysql.rs              # MySQL 目标
          elasticsearch.rs      # Elasticsearch 目标
    maxio-lifecycle/            # 生命周期管理 (对应 MinIO internal/bucket/lifecycle/)
      src/
        lib.rs
        rules.rs                # 生命周期规则定义
        evaluator.rs            # 规则评估器
        scanner.rs              # 后台扫描器 (对应 data-scanner.go)
    maxio-admin/                # Admin API (对应 MinIO cmd/admin-*.go)
      src/
        lib.rs
        router.rs               # Admin 路由
        handlers/
          info.rs               # 服务器信息
          config.rs             # 配置管理
          user.rs               # 用户管理
          policy.rs             # 策略管理
          heal.rs               # 修复操作
          pool.rs               # 池管理
          metrics.rs            # 指标查询
    maxio-common/               # 公共类型与工具
      src/
        lib.rs
        types.rs                # 公共数据类型
        error.rs                # 统一错误类型
        hash.rs                 # 哈希工具 (SipHash, CRC32)
        time.rs                 # 时间工具
        xml.rs                  # XML 序列化辅助
```


---

## 三、MinIO 架构到 Rust 模块映射

| MinIO (Go) | Maxio (Rust) | 说明 |
|-------------|-------------|------|
| cmd/server-main.go | maxio-server | 服务入口、启动流程 |
| cmd/api-router.go | maxio-s3-api/router.rs | S3 API 路由注册 |
| cmd/object-handlers.go | maxio-s3-api/handlers/object.rs | 对象操作处理器 |
| cmd/bucket-handlers.go | maxio-s3-api/handlers/bucket.rs | Bucket 操作处理器 |
| cmd/object-multipart-handlers.go | maxio-s3-api/handlers/multipart.rs | 分片上传处理器 |
| cmd/signature-v4.go | maxio-auth/signature_v4.rs | Signature V4 验证 |
| cmd/auth-handler.go | maxio-s3-api/middleware/auth.rs | 认证中间件 |
| cmd/object-api-interface.go | maxio-storage/traits.rs (ObjectLayer) | 存储抽象层 trait |
| cmd/storage-interface.go | maxio-storage/traits.rs (StorageApi) | 磁盘存储 trait |
| cmd/erasure-coding.go | maxio-storage/erasure/coding.rs | 纠删码核心 |
| cmd/erasure-object.go | maxio-storage/erasure/object.rs | 单集合对象操作 |
| cmd/erasure-sets.go | maxio-storage/erasure/sets.rs | 多集合管理 |
| cmd/erasure-server-pool.go | maxio-storage/erasure/server_pool.rs | 多池管理 |
| cmd/erasure-multipart.go | maxio-storage/multipart.rs | 分片上传存储 |
| cmd/erasure-healing.go | maxio-distributed/healing/ | 数据修复 |
| cmd/xl-storage.go | maxio-storage/xl/storage.rs | XL 磁盘操作 |
| cmd/xl-storage-format-v2.go | maxio-storage/xl/format_v2.rs | XLv2 元数据格式 |
| cmd/storage-datatypes.go | maxio-storage/datatypes.rs | FileInfo 等数据类型 |
| cmd/format-erasure.go | maxio-storage/format.rs | 磁盘格式管理 |
| cmd/iam.go | maxio-iam/system.rs | IAM 核心系统 |
| cmd/iam-store.go | maxio-iam/store.rs + cache.rs | IAM 存储与缓存 |
| cmd/bucket-policy.go | maxio-iam/policy.rs | 策略评估 |
| cmd/sts-handlers.go | maxio-auth/sts.rs | STS 服务 |
| internal/crypto/sse-*.go | maxio-crypto/ | 服务端加密 |
| internal/kms/ | maxio-crypto/ (KMS 集成) | 密钥管理服务 |
| internal/dsync/ | maxio-distributed/dsync/ | 分布式锁 |
| internal/grid/ | maxio-distributed/grid/ | Grid RPC 框架 |
| cmd/peer-rest-*.go | maxio-distributed/peer/ | 节点间通信 |
| cmd/notification.go | maxio-notification/system.rs | 事件通知系统 |
| internal/event/target/ | maxio-notification/target/ | 通知目标实现 |
| cmd/bucket-lifecycle.go | maxio-lifecycle/ | 生命周期管理 |
| cmd/data-scanner.go | maxio-lifecycle/scanner.rs | 后台数据扫描 |
| cmd/admin-handlers.go | maxio-admin/ | Admin API |
| cmd/metrics-*.go | maxio-admin/handlers/metrics.rs | Prometheus 指标 |
| cmd/bucket-replication.go | maxio-distributed/replication/ | 数据复制 |

---

## 四、分阶段实施计划

### Phase 0: 基础框架搭建 [预计 1-2 周]

**目标**: 建立 Rust workspace 骨架，跑通最小 S3 请求链路。

**交付物**:
- Workspace 结构创建 (所有 crate 骨架)
- maxio-server 启动 HTTP 服务
- maxio-common 基础类型定义
- 能响应一个硬编码的 ListBuckets 请求

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 0.1 | 创建 workspace Cargo.toml，定义所有 crate | 根目录 | 低 |
| 0.2 | 实现 maxio-common: 错误类型、公共类型 | maxio-common | 低 |
| 0.3 | 实现 maxio-server: tokio + hyper HTTP 服务启动 | maxio-server | 低 |
| 0.4 | 实现 maxio-s3-api 骨架: 路由框架 + ListBuckets 硬编码响应 | maxio-s3-api | 中 |
| 0.5 | 集成测试: aws-cli s3 ls 能返回结果 | 测试 | 低 |

**验收标准**:
- `cargo build` 全部通过
- `aws s3 ls --endpoint-url http://localhost:9000` 返回空 bucket 列表
- 所有 crate 骨架存在且可编译

---

### Phase 1: 单机存储引擎 -- 核心读写 [预计 3-4 周]

**目标**: 实现单磁盘存储引擎，支持基本的 PutObject/GetObject/DeleteObject。

**交付物**:
- ObjectLayer trait 定义 (参照 MinIO 40+ 方法，先实现核心 10 个)
- StorageApi trait 定义
- XL 磁盘存储实现 (单磁盘)
- XLv2 元数据格式读写
- 基本的 PutObject / GetObject / DeleteObject / HeadObject
- MakeBucket / ListBuckets / DeleteBucket / HeadBucket

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 1.1 | 定义 ObjectLayer trait (核心方法) | maxio-storage | 高 |
| 1.2 | 定义 StorageApi trait (磁盘操作) | maxio-storage | 中 |
| 1.3 | 实现 FileInfo / ErasureInfo / ObjectPartInfo 数据类型 | maxio-storage | 中 |
| 1.4 | 实现 XLv2 元数据格式 (msgpack 序列化/反序列化) | maxio-storage | 高 |
| 1.5 | 实现 XL 磁盘存储 (文件读写、目录管理) | maxio-storage | 高 |
| 1.6 | 实现 format.json 管理 (磁盘格式初始化) | maxio-storage | 中 |
| 1.7 | 实现单磁盘 ObjectLayer (无纠删码，直接存储) | maxio-storage | 高 |
| 1.8 | 实现 S3 PutObject handler (含 Content-MD5 校验) | maxio-s3-api | 高 |
| 1.9 | 实现 S3 GetObject handler (含 Range 请求) | maxio-s3-api | 高 |
| 1.10 | 实现 S3 DeleteObject / HeadObject handler | maxio-s3-api | 中 |
| 1.11 | 实现 S3 MakeBucket / ListBuckets / DeleteBucket / HeadBucket | maxio-s3-api | 中 |
| 1.12 | 实现 S3 错误码体系 (100+ 错误码) | maxio-s3-api | 中 |
| 1.13 | 集成测试: aws-cli 完整 CRUD 流程 | 测试 | 中 |

**验收标准**:
- `aws s3 mb s3://test-bucket` 创建 bucket 成功
- `aws s3 cp file.txt s3://test-bucket/` 上传成功
- `aws s3 cp s3://test-bucket/file.txt ./` 下载成功且内容一致
- `aws s3 rm s3://test-bucket/file.txt` 删除成功
- 磁盘上可见 xl.meta 文件且格式正确

---

### Phase 2: 认证与签名验证 [预计 2-3 周]

**目标**: 实现完整的 AWS Signature V4 认证链路。

**交付物**:
- AWS Signature V4 签名验证
- AWS Signature V2 兼容
- Presigned URL 支持
- 流式签名 (aws-chunked) 支持
- POST 策略验证
- 基础凭证管理 (root 用户)

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 2.1 | 实现 Signature V4 签名计算与验证 | maxio-auth | 高 |
| 2.2 | 实现 Canonical Request 构建 | maxio-auth | 高 |
| 2.3 | 实现 Signature V2 兼容 | maxio-auth | 中 |
| 2.4 | 实现 Presigned URL 验证 | maxio-auth | 中 |
| 2.5 | 实现流式签名验证 (aws-chunked encoding) | maxio-auth | 高 |
| 2.6 | 实现 POST 策略解析与验证 | maxio-auth | 中 |
| 2.7 | 实现认证中间件 (集成到请求链路) | maxio-s3-api | 中 |
| 2.8 | 实现 root 凭证管理 (环境变量配置) | maxio-iam | 低 |
| 2.9 | 集成测试: 带签名的 aws-cli 操作 | 测试 | 中 |

**验收标准**:
- 配置 MAXIO_ROOT_USER / MAXIO_ROOT_PASSWORD 后，aws-cli 使用对应凭证可正常操作
- 错误凭证返回 403 SignatureDoesNotMatch
- Presigned URL 可正常上传/下载
- aws-chunked 流式上传正常工作

---

### Phase 3: 纠删码存储引擎 [预计 4-5 周]

**目标**: 实现完整的纠删码存储，支持多磁盘数据分布与容错。

**交付物**:
- Reed-Solomon 纠删码编解码
- erasureObjects 实现 (单集合)
- erasureSets 实现 (多集合)
- 对象分布算法 (SipHash)
- Quorum 读写逻辑
- Bitrot 校验 (HighwayHash)

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 3.1 | 集成 reed-solomon-simd，封装编解码接口 | maxio-storage | 中 |
| 3.2 | 实现数据分片: 对象数据按 blockSize (1MB) 切分 | maxio-storage | 中 |
| 3.3 | 实现编码写入: 数据块 + 校验块分布到多磁盘 | maxio-storage | 高 |
| 3.4 | 实现解码读取: 从可用磁盘重建数据 | maxio-storage | 高 |
| 3.5 | 实现 Bitrot 校验 (HighwayHash per shard) | maxio-storage | 中 |
| 3.6 | 实现 erasureObjects: 单集合 ObjectLayer | maxio-storage | 高 |
| 3.7 | 实现 SipHash 对象分布算法 | maxio-storage | 中 |
| 3.8 | 实现 erasureSets: 多集合管理与路由 | maxio-storage | 高 |
| 3.9 | 实现 Quorum 逻辑: 读 quorum = M, 写 quorum = M (或 M+1) | maxio-storage | 高 |
| 3.10 | 更新 XLv2 元数据: ErasureAlgorithm, ErasureM/N, Distribution | maxio-storage | 中 |
| 3.11 | 实现磁盘健康检查与离线标记 | maxio-storage | 中 |
| 3.12 | 集成测试: 多磁盘配置，模拟磁盘故障后仍可读取 | 测试 | 高 |

**验收标准**:
- 配置 4 数据盘 + 2 校验盘，上传/下载正常
- 移除任意 2 块盘后，数据仍可正常读取
- xl.meta 中包含正确的纠删码参数
- Bitrot 校验能检测到数据损坏

---

### Phase 4: 分片上传与对象列举 [预计 2-3 周]

**目标**: 实现 S3 Multipart Upload 和 ListObjects。

**交付物**:
- CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload
- ListMultipartUploads / ListParts
- ListObjectsV1 / ListObjectsV2
- ListObjectVersions
- CopyObject / UploadPartCopy

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 4.1 | 实现分片上传状态管理 (uploadID 生成、临时目录) | maxio-storage | 高 |
| 4.2 | 实现 CreateMultipartUpload handler | maxio-s3-api | 中 |
| 4.3 | 实现 UploadPart handler (含纠删码写入) | maxio-s3-api | 高 |
| 4.4 | 实现 CompleteMultipartUpload (合并 parts、生成最终 xl.meta) | maxio-s3-api + storage | 高 |
| 4.5 | 实现 AbortMultipartUpload (清理临时文件) | maxio-s3-api | 中 |
| 4.6 | 实现 ListMultipartUploads / ListParts | maxio-s3-api | 中 |
| 4.7 | 实现 ListObjectsV1 (Marker 分页) | maxio-s3-api + storage | 高 |
| 4.8 | 实现 ListObjectsV2 (ContinuationToken 分页) | maxio-s3-api | 中 |
| 4.9 | 实现 CopyObject / UploadPartCopy | maxio-s3-api | 中 |
| 4.10 | 集成测试: 大文件分片上传、列举、拷贝 | 测试 | 高 |

**验收标准**:
- `aws s3 cp large-file.bin s3://bucket/` (大文件自动分片上传) 成功
- `aws s3 ls s3://bucket/` 正确列举所有对象
- `aws s3 cp s3://bucket/a s3://bucket/b` 服务端拷贝成功
- 分片上传中断后，AbortMultipartUpload 清理干净


---

### Phase 5: 版本控制与对象锁定 [预计 2 周]

**目标**: 实现 S3 对象版本控制和 WORM 锁定。

**交付物**:
- Bucket 版本控制 (Enabled / Suspended)
- 版本化 PutObject / GetObject / DeleteObject (含 Delete Marker)
- ListObjectVersions
- 对象锁定: Governance 模式 / Compliance 模式
- Legal Hold

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 5.1 | 实现 Bucket 版本控制配置 (Put/GetBucketVersioning) | maxio-s3-api + storage | 中 |
| 5.2 | 修改 PutObject: 版本化写入 (生成 VersionID) | maxio-storage | 高 |
| 5.3 | 修改 GetObject: 支持 versionId 参数 | maxio-storage | 中 |
| 5.4 | 修改 DeleteObject: 版本化删除 (Delete Marker) | maxio-storage | 高 |
| 5.5 | 实现 ListObjectVersions | maxio-s3-api + storage | 高 |
| 5.6 | 实现 XLv2 多版本元数据存储 | maxio-storage | 高 |
| 5.7 | 实现对象锁定配置 (Put/GetObjectLockConfiguration) | maxio-s3-api | 中 |
| 5.8 | 实现 Retention 设置与验证 (Governance/Compliance) | maxio-storage | 高 |
| 5.9 | 实现 Legal Hold (Put/GetObjectLegalHold) | maxio-s3-api | 中 |
| 5.10 | 实现 Governance Bypass 权限检查 | maxio-iam | 中 |
| 5.11 | 集成测试: 版本化操作、锁定保护 | 测试 | 高 |

**验收标准**:
- 启用版本控制后，覆盖写入产生新版本
- 删除操作产生 Delete Marker，旧版本仍可通过 versionId 访问
- Compliance 模式下对象在保留期内无法删除
- Governance 模式下有权限的用户可以 bypass

---

### Phase 6: IAM 完整实现 [预计 3-4 周]

**目标**: 实现完整的 IAM 子系统，包括用户/组/策略/服务账户。

**交付物**:
- IAMSys 核心 (用户/组/策略 CRUD)
- 策略评估引擎 (IAM Policy + Bucket Policy)
- 服务账户管理
- STS: AssumeRole / AssumeRoleWithWebIdentity
- Bucket Policy 管理

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 6.1 | 实现 IAMSys 核心结构 | maxio-iam | 高 |
| 6.2 | 实现 iamCache 内存缓存 | maxio-iam | 中 |
| 6.3 | 实现 IAM 对象存储后端 | maxio-iam | 高 |
| 6.4 | 实现用户 CRUD (CreateUser/DeleteUser/ListUsers) | maxio-iam | 中 |
| 6.5 | 实现组管理 (CreateGroup/AddToGroup/RemoveFromGroup) | maxio-iam | 中 |
| 6.6 | 实现策略管理 (CreatePolicy/AttachPolicy/DetachPolicy) | maxio-iam | 中 |
| 6.7 | 实现 IAM 策略评估引擎 (Action/Resource/Condition 匹配) | maxio-iam | 高 |
| 6.8 | 实现 Bucket Policy 评估 | maxio-iam | 高 |
| 6.9 | 实现服务账户 (Create/Delete/List ServiceAccount) | maxio-iam | 中 |
| 6.10 | 实现 STS AssumeRole | maxio-auth | 高 |
| 6.11 | 实现 STS AssumeRoleWithWebIdentity (OpenID) | maxio-auth | 高 |
| 6.12 | 实现 JWT 令牌生成与验证 | maxio-auth | 中 |
| 6.13 | 集成认证中间件: 请求 -> 签名验证 -> 策略评估 -> 授权 | maxio-s3-api | 高 |
| 6.14 | 集成测试: 多用户、策略限制、服务账户 | 测试 | 高 |

**验收标准**:
- 创建子用户，分配只读策略，验证写操作被拒绝
- 服务账户可正常操作且受父用户策略限制
- STS 临时凭证可正常使用且过期后失效
- Bucket Policy 正确限制匿名/特定用户访问

---

### Phase 7: 服务端加密 [预计 2 周]

**目标**: 实现 S3 服务端加密三种模式。

**交付物**:
- SSE-S3: 服务端管理密钥加密
- SSE-C: 客户端提供密钥加密
- SSE-KMS: 外部 KMS 集成
- 自动加密配置
- 加密元数据管理

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 7.1 | 实现加密密钥派生与管理 | maxio-crypto | 高 |
| 7.2 | 实现 SSE-S3: 服务端密钥加密/解密 | maxio-crypto | 高 |
| 7.3 | 实现 SSE-C: 客户端密钥处理 | maxio-crypto | 中 |
| 7.4 | 实现 SSE-KMS: KES 客户端集成 | maxio-crypto | 高 |
| 7.5 | 实现加密元数据存储 (MetaSys 中的加密信息) | maxio-crypto | 中 |
| 7.6 | 修改 PutObject/GetObject: 透明加密/解密 | maxio-storage | 高 |
| 7.7 | 实现 Bucket 默认加密配置 | maxio-s3-api | 中 |
| 7.8 | 集成测试: 三种加密模式的上传/下载 | 测试 | 高 |

**验收标准**:
- SSE-S3 加密上传后，磁盘上数据为密文
- SSE-C 使用正确密钥可解密，错误密钥返回 403
- 加密对象的 CopyObject 正确处理密钥转换

---

### Phase 8: 分布式集群 -- 节点通信 [预计 4-5 周]

**目标**: 实现多节点集群通信基础设施。

**交付物**:
- 端点解析与节点发现
- Grid RPC 框架 (持久化连接、多路复用、流式通信)
- 分布式锁 (dsync DRWMutex)
- Peer RPC 服务端/客户端
- Storage RPC (远程磁盘操作)
- erasureServerPools 多池架构

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 8.1 | 实现端点解析 (含省略号展开) | maxio-distributed | 高 |
| 8.2 | 实现 Grid Manager: 连接管理、Handler 注册 | maxio-distributed | 高 |
| 8.3 | 实现 Grid Connection: WebSocket 持久连接、多路复用 | maxio-distributed | 高 |
| 8.4 | 实现 Grid Stream: 双向流式通信 | maxio-distributed | 高 |
| 8.5 | 实现 dsync DRWMutex: 基于 quorum 的分布式读写锁 | maxio-distributed | 高 |
| 8.6 | 实现 Lock Server/Client (Lock/RLock/Unlock/Refresh) | maxio-distributed | 高 |
| 8.7 | 实现 Namespace Lock (对象级锁) | maxio-distributed | 中 |
| 8.8 | 实现 Peer RPC Server: 注册 100+ handler | maxio-distributed | 高 |
| 8.9 | 实现 Peer RPC Client: 远程调用封装 | maxio-distributed | 高 |
| 8.10 | 实现 Storage RPC: 远程磁盘 StorageApi 实现 | maxio-distributed | 高 |
| 8.11 | 实现 erasureServerPools: 多池管理 | maxio-storage | 高 |
| 8.12 | 实现 Bootstrap: 节点启动时的对等验证 | maxio-distributed | 中 |
| 8.13 | 集成测试: 多节点集群启动、跨节点读写 | 测试 | 高 |

**验收标准**:
- 4 节点集群正常启动并互相发现
- 跨节点 PutObject/GetObject 正常工作
- 分布式锁在节点故障时正确释放
- 单节点宕机后集群仍可服务 (在 quorum 范围内)

---

### Phase 9: 数据修复与后台扫描 [预计 2-3 周]

**目标**: 实现数据完整性保障机制。

**交付物**:
- 后台数据扫描器 (data-scanner)
- 对象修复 (healing): 检测损坏/缺失分片并重建
- 新磁盘自动修复
- MRF (Minimum Repair Frequency) 队列
- 修复进度追踪

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 9.1 | 实现后台扫描器框架 (定时遍历所有对象) | maxio-lifecycle | 高 |
| 9.2 | 实现对象健康检查 (校验分片完整性) | maxio-storage | 高 |
| 9.3 | 实现 healObject: 从可用分片重建缺失分片 | maxio-distributed | 高 |
| 9.4 | 实现 healingTracker: 修复进度持久化 | maxio-distributed | 中 |
| 9.5 | 实现 healSequence: 修复任务管理 | maxio-distributed | 中 |
| 9.6 | 实现 MRF 队列: 写入失败时的即时修复 | maxio-distributed | 中 |
| 9.7 | 实现新磁盘检测与自动修复触发 | maxio-distributed | 中 |
| 9.8 | 实现 Admin Heal API | maxio-admin | 中 |
| 9.9 | 集成测试: 模拟磁盘损坏，验证自动修复 | 测试 | 高 |

**验收标准**:
- 损坏一个分片后，后台扫描器检测并自动修复
- 替换新磁盘后，数据自动迁移到新磁盘
- Admin API 可查询修复进度
- MRF 队列在写入部分失败时触发即时修复

---

### Phase 10: 事件通知系统 [预计 2-3 周]

**目标**: 实现 S3 事件通知机制。

**交付物**:
- EventNotifier 核心
- Bucket 通知配置 (Put/GetBucketNotification)
- 通知目标: Webhook, Kafka, NATS, AMQP, Redis
- 事件过滤 (前缀/后缀匹配)
- 集群通知同步

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 10.1 | 实现 EventNotifier 核心 (事件路由与分发) | maxio-notification | 高 |
| 10.2 | 实现通知配置解析与验证 | maxio-notification | 中 |
| 10.3 | 实现 Webhook 目标 | maxio-notification | 中 |
| 10.4 | 实现 Kafka 目标 | maxio-notification | 中 |
| 10.5 | 实现 NATS 目标 | maxio-notification | 中 |
| 10.6 | 实现 AMQP 目标 | maxio-notification | 中 |
| 10.7 | 实现 Redis 目标 | maxio-notification | 中 |
| 10.8 | 实现事件过滤 (前缀/后缀/事件类型) | maxio-notification | 中 |
| 10.9 | 实现 S3 通知 API handler | maxio-s3-api | 中 |
| 10.10 | 集成到对象操作: Put/Delete/Copy 触发事件 | maxio-storage | 中 |
| 10.11 | 集成测试: Webhook 接收通知验证 | 测试 | 中 |

**验收标准**:
- 配置 Webhook 通知后，PutObject 触发 HTTP 回调
- 事件过滤正确匹配前缀/后缀
- Kafka/NATS 目标可正常投递消息

---

### Phase 11: 生命周期管理 [预计 2 周]

**目标**: 实现 S3 对象生命周期管理。

**交付物**:
- 生命周期规则配置 (Put/GetBucketLifecycle)
- 过期规则 (Expiration)
- 转换规则 (Transition) -- 框架
- 非当前版本过期
- 后台扫描器集成

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 11.1 | 实现生命周期规则解析 (XML -> Rust 结构) | maxio-lifecycle | 中 |
| 11.2 | 实现规则评估器 (匹配前缀、标签、大小) | maxio-lifecycle | 高 |
| 11.3 | 实现过期动作执行 (删除过期对象) | maxio-lifecycle | 中 |
| 11.4 | 实现非当前版本过期 | maxio-lifecycle | 中 |
| 11.5 | 实现 Delete Marker 清理 | maxio-lifecycle | 中 |
| 11.6 | 集成到后台扫描器 | maxio-lifecycle | 中 |
| 11.7 | 实现 S3 Lifecycle API handler | maxio-s3-api | 中 |
| 11.8 | 集成测试: 配置过期规则，验证对象自动删除 | 测试 | 中 |

**验收标准**:
- 配置 1 天过期规则后，过期对象被后台扫描器自动删除
- 非当前版本过期正确处理
- 孤立 Delete Marker 被自动清理

---

### Phase 12: Admin API 与监控 [预计 2-3 周]

**目标**: 实现管理 API 和 Prometheus 监控。

**交付物**:
- Admin API 路由与认证
- 服务器信息 / 配置管理
- 用户/策略管理 API
- Prometheus 指标暴露
- 健康检查端点

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 12.1 | 实现 Admin API 路由框架 (axum) | maxio-admin | 中 |
| 12.2 | 实现 Admin 认证 (与 IAM 集成) | maxio-admin | 中 |
| 12.3 | 实现 ServerInfo handler | maxio-admin | 中 |
| 12.4 | 实现配置管理 API (Get/Set Config) | maxio-admin | 中 |
| 12.5 | 实现用户/策略管理 API | maxio-admin | 中 |
| 12.6 | 实现 Prometheus 指标收集框架 | maxio-admin | 高 |
| 12.7 | 实现 API 操作指标 (请求数、延迟、错误率) | maxio-admin | 中 |
| 12.8 | 实现存储指标 (磁盘使用、对象数量) | maxio-admin | 中 |
| 12.9 | 实现健康检查端点 (liveness / readiness) | maxio-admin | 低 |
| 12.10 | 集成测试: mc admin 命令兼容性 | 测试 | 中 |

**验收标准**:
- `mc admin info` 返回正确的服务器信息
- Prometheus /metrics 端点返回有效指标
- 健康检查端点正确反映集群状态

---

### Phase 13: 数据复制 [预计 3-4 周]

**目标**: 实现 Bucket 级和站点级数据复制。

**交付物**:
- Bucket 复制配置 (Put/GetBucketReplication)
- ReplicationPool: 异步复制引擎
- 复制状态追踪 (PENDING/COMPLETED/FAILED)
- 站点复制 (SiteReplicationSys)
- 复制指标

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 13.1 | 实现复制规则配置解析 | maxio-distributed | 中 |
| 13.2 | 实现 ReplicationPool: 异步复制工作池 | maxio-distributed | 高 |
| 13.3 | 实现对象复制逻辑 (通过 S3 API 推送到远端) | maxio-distributed | 高 |
| 13.4 | 实现复制状态追踪与重试 | maxio-distributed | 高 |
| 13.5 | 实现 Delete Marker 复制 | maxio-distributed | 中 |
| 13.6 | 实现站点复制框架 (IAM/Policy/Config 同步) | maxio-distributed | 高 |
| 13.7 | 实现复制指标收集 | maxio-distributed | 中 |
| 13.8 | 实现 S3 Replication API handler | maxio-s3-api | 中 |
| 13.9 | 集成测试: 双集群复制验证 | 测试 | 高 |

**验收标准**:
- 配置复制规则后，源 bucket 写入自动复制到目标
- 复制状态可通过 HeadObject 的 x-amz-replication-status 查询
- 复制失败自动重试
- 站点复制正确同步 IAM 和 Bucket 配置

---

### Phase 14: 批量操作与高级特性 [预计 2-3 周]

**目标**: 实现批量操作框架和剩余 S3 特性。

**交付物**:
- 批量操作框架 (Batch Jobs)
- 批量过期 / 批量复制 / 批量密钥轮换
- 对象标签 (Tagging)
- Select Object Content (可选)
- 池管理: 扩容 / 缩容 / 重平衡

**具体任务**:

| 编号 | 任务 | 涉及 Crate | 复杂度 |
|------|------|-----------|--------|
| 14.1 | 实现批量操作框架 (Job 定义、调度、进度追踪) | maxio-admin | 高 |
| 14.2 | 实现批量过期 Job | maxio-admin | 中 |
| 14.3 | 实现批量复制 Job | maxio-admin | 中 |
| 14.4 | 实现批量密钥轮换 Job | maxio-admin | 中 |
| 14.5 | 实现对象标签 API (Put/Get/DeleteObjectTagging) | maxio-s3-api | 中 |
| 14.6 | 实现池扩容 (添加新池) | maxio-storage | 高 |
| 14.7 | 实现池缩容 (Decommission) | maxio-storage | 高 |
| 14.8 | 实现数据重平衡 (Rebalance) | maxio-storage | 高 |
| 14.9 | 集成测试: 批量操作、池管理 | 测试 | 高 |

**验收标准**:
- 批量过期 Job 正确删除匹配对象
- 添加新池后，新数据写入新池
- Decommission 将旧池数据迁移到其他池


---

## 五、核心 Trait 设计 (Rust)

### 5.1 ObjectLayer Trait

对应 MinIO 的 ObjectLayer interface (cmd/object-api-interface.go)，是整个存储引擎的核心抽象。

```rust
#[async_trait]
pub trait ObjectLayer: Send + Sync {
    // -- Bucket 操作 --
    async fn make_bucket(&self, ctx: &Context, bucket: &str, opts: MakeBucketOptions) -> Result<ObjectInfo>;
    async fn get_bucket_info(&self, ctx: &Context, bucket: &str, opts: BucketOptions) -> Result<BucketInfo>;
    async fn list_buckets(&self, ctx: &Context, opts: BucketOptions) -> Result<Vec<BucketInfo>>;
    async fn delete_bucket(&self, ctx: &Context, bucket: &str, opts: DeleteBucketOptions) -> Result<()>;

    // -- Object 操作 --
    async fn get_object_n_info(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<GetObjectReader>;
    async fn get_object_info(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn put_object(
        &self, ctx: &Context, bucket: &str, object: &str, data: PutObjReader, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn copy_object(
        &self, ctx: &Context, src_bucket: &str, src_object: &str,
        dst_bucket: &str, dst_object: &str, src_info: ObjectInfo, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn delete_object(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn delete_objects(
        &self, ctx: &Context, bucket: &str, objects: Vec<ObjectToDelete>, opts: ObjectOptions,
    ) -> Result<Vec<DeletedObject>>;

    // -- 列举 --
    async fn list_objects(
        &self, ctx: &Context, bucket: &str, prefix: &str, marker: &str,
        delimiter: &str, max_keys: i32,
    ) -> Result<ListObjectsInfo>;
    async fn list_objects_v2(
        &self, ctx: &Context, bucket: &str, prefix: &str, continuation_token: &str,
        delimiter: &str, max_keys: i32, fetch_owner: bool, start_after: &str,
    ) -> Result<ListObjectsV2Info>;
    async fn list_object_versions(
        &self, ctx: &Context, bucket: &str, prefix: &str, marker: &str,
        version_marker: &str, delimiter: &str, max_keys: i32,
    ) -> Result<ListObjectVersionsInfo>;

    // -- 分片上传 --
    async fn new_multipart_upload(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<NewMultipartUploadResult>;
    async fn put_object_part(
        &self, ctx: &Context, bucket: &str, object: &str, upload_id: &str,
        part_id: i32, data: PutObjReader, opts: ObjectOptions,
    ) -> Result<PartInfo>;
    async fn complete_multipart_upload(
        &self, ctx: &Context, bucket: &str, object: &str, upload_id: &str,
        parts: Vec<CompletePart>, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn abort_multipart_upload(
        &self, ctx: &Context, bucket: &str, object: &str, upload_id: &str, opts: ObjectOptions,
    ) -> Result<()>;
    async fn list_multipart_uploads(
        &self, ctx: &Context, bucket: &str, prefix: &str, key_marker: &str,
        upload_id_marker: &str, delimiter: &str, max_uploads: i32,
    ) -> Result<ListMultipartsInfo>;
    async fn list_object_parts(
        &self, ctx: &Context, bucket: &str, object: &str, upload_id: &str,
        part_number_marker: i32, max_parts: i32, opts: ObjectOptions,
    ) -> Result<ListPartsInfo>;

    // -- 元数据操作 --
    async fn put_object_metadata(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn put_object_tags(
        &self, ctx: &Context, bucket: &str, object: &str, tags: &str, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn get_object_tags(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<ObjectTags>;
    async fn delete_object_tags(
        &self, ctx: &Context, bucket: &str, object: &str, opts: ObjectOptions,
    ) -> Result<ObjectInfo>;

    // -- 修复 --
    async fn heal_format(&self, ctx: &Context, dry_run: bool) -> Result<HealResultItem>;
    async fn heal_bucket(&self, ctx: &Context, bucket: &str, opts: HealOpts) -> Result<HealResultItem>;
    async fn heal_object(
        &self, ctx: &Context, bucket: &str, object: &str, version_id: &str, opts: HealOpts,
    ) -> Result<HealResultItem>;
    async fn heal_objects(
        &self, ctx: &Context, bucket: &str, prefix: &str, opts: HealOpts,
        heal_fn: HealObjectFn,
    ) -> Result<()>;

    // -- 存储信息 --
    async fn storage_info(&self, ctx: &Context) -> Result<StorageInfo>;
    async fn local_storage_info(&self, ctx: &Context) -> Result<StorageInfo>;
    fn backend_info(&self) -> BackendInfo;
}
```

### 5.2 StorageApi Trait

对应 MinIO 的 StorageAPI interface (cmd/storage-interface.go)，是底层磁盘操作抽象。

```rust
#[async_trait]
pub trait StorageApi: Send + Sync {
    // -- 卷操作 (对应 Bucket 在磁盘上的目录) --
    async fn make_vol(&self, ctx: &Context, volume: &str) -> Result<()>;
    async fn list_vols(&self, ctx: &Context) -> Result<Vec<VolInfo>>;
    async fn stat_vol(&self, ctx: &Context, volume: &str) -> Result<VolInfo>;
    async fn delete_vol(&self, ctx: &Context, volume: &str, force_delete: bool) -> Result<()>;

    // -- 文件操作 --
    async fn create_file(
        &self, ctx: &Context, volume: &str, path: &str, size: i64, reader: &mut dyn AsyncRead,
    ) -> Result<()>;
    async fn read_file_stream(
        &self, ctx: &Context, volume: &str, path: &str, offset: i64, length: i64,
    ) -> Result<Box<dyn AsyncRead>>;
    async fn rename_file(
        &self, ctx: &Context, src_volume: &str, src_path: &str, dst_volume: &str, dst_path: &str,
    ) -> Result<()>;
    async fn delete(&self, ctx: &Context, volume: &str, path: &str, opts: DeleteOptions) -> Result<()>;

    // -- 元数据操作 --
    async fn write_metadata(
        &self, ctx: &Context, orig_volume: &str, path: &str, fi: FileInfo, opts: WriteOptions,
    ) -> Result<()>;
    async fn update_metadata(
        &self, ctx: &Context, volume: &str, path: &str, fi: FileInfo, opts: UpdateOptions,
    ) -> Result<()>;
    async fn read_version(
        &self, ctx: &Context, orig_volume: &str, path: &str, version_id: &str, opts: ReadOptions,
    ) -> Result<FileInfo>;
    async fn read_xl(
        &self, ctx: &Context, volume: &str, path: &str, read_data: bool,
    ) -> Result<RawFileInfo>;

    // -- 扫描 --
    async fn walk_dir(
        &self, ctx: &Context, opts: WalkDirOptions, tx: Sender<MetaCacheEntry>,
    ) -> Result<()>;
    async fn ns_scanner(
        &self, ctx: &Context, cache: &DataUsageCache, updates: Sender<DataUsageEntry>,
        scan_mode: ScanMode, should_sleep: fn() -> bool,
    ) -> Result<DataUsageCache>;

    // -- 磁盘信息 --
    async fn disk_info(&self, ctx: &Context, opts: DiskInfoOptions) -> Result<DiskInfo>;
    fn get_disk_id(&self) -> Result<String>;
    fn set_disk_id(&self, id: String);
    fn healing(&self) -> Option<HealingTracker>;
    fn get_disk_location(&self) -> (pool_idx: usize, set_idx: usize, disk_idx: usize);
    fn close(&self) -> Result<()>;
}
```

### 5.3 核心数据结构

```rust
/// 对象元数据 (对应 MinIO FileInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub volume: String,
    pub name: String,
    pub version_id: String,
    pub is_latest: bool,
    pub deleted: bool,
    pub data_dir: String,          // UUID, 数据目录名

    // 纠删码信息
    pub erasure: ErasureInfo,

    // 对象数据
    pub size: i64,
    pub mod_time: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
    pub parts: Vec<ObjectPartInfo>,

    // 版本控制
    pub num_versions: i32,
    pub successor_mod_time: DateTime<Utc>,

    // 复制状态
    pub replication_state: ReplicationState,

    // 内联数据 (小对象优化)
    pub data: Option<Vec<u8>>,
}

/// 纠删码参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureInfo {
    pub algorithm: String,         // "rs-vandermonde"
    pub data_blocks: usize,        // M
    pub parity_blocks: usize,      // N
    pub block_size: i64,           // 1MB
    pub index: usize,              // 当前磁盘索引 (1-based)
    pub distribution: Vec<usize>,  // 分片分布
    pub checksums: Vec<ChecksumInfo>,
}

/// 分片信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectPartInfo {
    pub number: i32,
    pub etag: String,
    pub size: i64,
    pub actual_size: i64,          // 解压后大小
    pub mod_time: DateTime<Utc>,
    pub checksums: HashMap<String, String>,
}

/// XLv2 元数据格式 (msgpack 序列化)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XlMetaV2 {
    pub versions: Vec<XlMetaV2Version>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum XlMetaV2Version {
    ObjectV2(XlMetaV2Object),
    DeleteMarker(XlMetaV2DeleteMarker),
    LegacyObject(XlMetaV1Object),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XlMetaV2Object {
    pub version_id: [u8; 16],
    pub data_dir: [u8; 16],
    pub erasure_algorithm: u8,     // 1 = ReedSolomon
    pub erasure_m: u8,             // 数据块数
    pub erasure_n: u8,             // 校验块数
    pub erasure_block_size: u32,
    pub erasure_index: u8,
    pub erasure_dist: Vec<u8>,
    pub bitrot_checksum_algo: u8,  // 1 = HighwayHash
    pub part_numbers: Vec<i32>,
    pub part_etags: Vec<String>,
    pub part_sizes: Vec<i64>,
    pub part_actual_sizes: Vec<i64>,
    pub size: i64,
    pub mod_time: i64,             // Unix timestamp
    pub meta_sys: HashMap<String, Vec<u8>>,
    pub meta_user: HashMap<String, String>,
}
```

---

## 六、测试策略

### 6.1 测试层次

| 层次 | 工具 | 覆盖范围 |
|------|------|---------|
| 单元测试 | cargo test | 每个模块的核心逻辑 |
| 集成测试 | cargo test --test | 跨 crate 交互 |
| S3 兼容性测试 | s3-tests (Ceph) | S3 API 兼容性 |
| aws-cli 测试 | aws s3 / aws s3api | 端到端功能验证 |
| mc 测试 | mc (MinIO Client) | MinIO 特有功能验证 |
| 性能测试 | warp / s3bench | 吞吐量与延迟 |
| 故障测试 | 自定义脚本 | 磁盘故障、节点宕机 |

### 6.2 每阶段测试要求

- Phase 0-1: aws-cli 基本 CRUD
- Phase 2: 签名验证正确性 (正确/错误凭证)
- Phase 3: 纠删码容错 (模拟磁盘故障)
- Phase 4: 大文件分片上传 (1GB+)
- Phase 5: 版本化操作完整性
- Phase 6: 多用户权限隔离
- Phase 7: 加密数据不可明文读取
- Phase 8: 多节点集群读写
- Phase 9: 数据损坏自动修复
- Phase 10-14: 各子系统功能验证

### 6.3 持续集成

```
每次提交:
  - cargo fmt --check
  - cargo clippy -- -D warnings
  - cargo test --workspace
  - cargo build --release

每日构建:
  - S3 兼容性测试套件
  - 多节点集群测试
  - 性能基准测试
```

---

## 七、风险与挑战

### 7.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| 纠删码性能不足 | 吞吐量低于 MinIO | 使用 reed-solomon-simd (SIMD 加速)，必要时用 unsafe 优化 |
| XLv2 格式兼容性 | 无法读取 MinIO 数据 | 严格按 MinIO 源码实现 msgpack 序列化 |
| 分布式锁死锁 | 集群不可用 | 实现锁超时、自动刷新、强制解锁机制 |
| Grid RPC 复杂度 | 开发周期过长 | 可先用 tonic (gRPC) 替代，后续优化为自定义协议 |
| S3 API 兼容性 | 客户端不兼容 | 使用 s3-tests 套件持续验证 |
| 内存管理 | 大文件处理 OOM | 流式处理，避免全量加载到内存 |

### 7.2 架构决策点

| 决策 | 选项 A | 选项 B | 建议 |
|------|--------|--------|------|
| S3 协议层 | 使用 s3s crate | 自行实现 | 先用 s3s 快速启动，后续按需替换 |
| 节点间通信 | 自定义 Grid (WebSocket) | gRPC (tonic) | 先用 tonic，Phase 8 后评估是否需要自定义 |
| 元数据存储 | 文件系统 (兼容 MinIO) | 嵌入式 DB (redb) | 文件系统优先 (兼容性)，redb 用于索引加速 |
| 共识协议 | openraft (Raft) | 无共识 (MinIO 模式) | 先无共识 (MinIO 模式)，后续可选 Raft |
| 异步模型 | tokio 多线程 | tokio 单线程 per core | 多线程 (默认)，性能调优时考虑 per-core |

---

## 八、里程碑总览

| 里程碑 | 阶段 | 预计时间 | 核心能力 |
|--------|------|---------|---------|
| M0: 骨架 | Phase 0 | 第 1-2 周 | HTTP 服务启动，空响应 |
| M1: 单机存储 | Phase 1-2 | 第 3-8 周 | 单磁盘 CRUD + 认证 |
| M2: 纠删码 | Phase 3-4 | 第 9-16 周 | 多磁盘纠删码 + 分片上传 |
| M3: 企业特性 | Phase 5-7 | 第 17-24 周 | 版本控制 + IAM + 加密 |
| M4: 分布式 | Phase 8-9 | 第 25-33 周 | 多节点集群 + 数据修复 |
| M5: 生态完善 | Phase 10-14 | 第 34-45 周 | 通知 + 生命周期 + Admin + 复制 |

**总预计工期: 约 10-12 个月** (单人全职开发)

---

## 九、参考资源

### 9.1 MinIO 源码关键文件 (按重要性排序)

1. cmd/object-api-interface.go -- ObjectLayer 接口定义 (最重要)
2. cmd/storage-interface.go -- StorageAPI 接口定义
3. cmd/erasure-object.go -- 纠删码对象操作核心
4. cmd/xl-storage-format-v2.go -- XLv2 元数据格式
5. cmd/erasure-coding.go -- Reed-Solomon 编解码
6. cmd/api-router.go -- S3 API 路由注册
7. cmd/object-handlers.go -- S3 对象操作处理器
8. cmd/signature-v4.go -- Signature V4 验证
9. cmd/iam.go -- IAM 核心系统
10. cmd/erasure-server-pool.go -- 多池架构

### 9.2 外部参考

- AWS S3 API Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/
- s3s crate: https://github.com/Nugine/s3s
- Garage (Rust S3): https://github.com/deuxfleurs-org/garage
- reed-solomon-simd: https://crates.io/crates/reed-solomon-simd
- MinIO 设计文档: https://min.io/docs/minio/linux/operations/concepts.html

---

## 十、附录: MinIO 代码规模参考

| 子系统 | Go 文件数 | 估计代码行数 | Rust 预估行数 |
|--------|----------|-------------|-------------|
| S3 API 层 (handlers + router) | ~30 | ~25,000 | ~20,000 |
| 存储引擎 (erasure + xl) | ~25 | ~20,000 | ~18,000 |
| IAM + Auth | ~15 | ~15,000 | ~12,000 |
| 分布式 (grid + dsync + peer) | ~20 | ~15,000 | ~12,000 |
| 通知 + 生命周期 | ~15 | ~8,000 | ~6,000 |
| Admin API + 监控 | ~40 | ~20,000 | ~15,000 |
| 加密 (crypto + kms) | ~15 | ~5,000 | ~4,000 |
| 公共工具 | ~20 | ~8,000 | ~6,000 |
| **合计** | **~180** | **~116,000** | **~93,000** |

> 注: Rust 预估行数通常比 Go 少 15-20%，因为 Rust 的类型系统和 trait 可以减少样板代码，
> 但错误处理和生命周期标注会增加部分代码量。

