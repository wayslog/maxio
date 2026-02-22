# Admin API Operations Analysis

This document details all Admin API operations implemented in MinIO.

## Server Management (admin-handlers.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| ServerInfo | GET /admin/v3/info | Get server information |
| StorageInfo | GET /admin/v3/storageinfo | Get storage statistics |
| DataUsageInfo | GET /admin/v3/datausageinfo | Get data usage metrics |
| AccountInfo | GET /admin/v3/accountinfo | Get account information |
| Heal | POST /admin/v3/heal | Trigger healing operation |
| Profiling | POST /admin/v3/profiling | CPU/memory profiling |
| Trace | GET /admin/v3/trace | Real-time request tracing |
| ConsoleLog | GET /admin/v3/log | Stream console logs |
| ServerUpdate | POST /admin/v3/update | Update server binary |
| ServiceRestart | POST /admin/v3/service | Restart service |
| ServiceStop | POST /admin/v3/service | Stop service |
| ServiceFreeze | POST /admin/v3/service | Freeze service |

## IAM User Management (admin-handlers-users.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| AddUser | PUT /admin/v3/add-user | Create IAM user |
| SetUserStatus | PUT /admin/v3/set-user-status | Enable/disable user |
| ListUsers | GET /admin/v3/list-users | List all users |
| GetUserInfo | GET /admin/v3/user-info | Get user details |
| RemoveUser | DELETE /admin/v3/remove-user | Delete user |
| AddServiceAccount | PUT /admin/v3/add-service-account | Create service account |
| UpdateServiceAccount | POST /admin/v3/update-service-account | Update service account |
| ListServiceAccounts | GET /admin/v3/list-service-accounts | List service accounts |
| DeleteServiceAccount | DELETE /admin/v3/delete-service-account | Delete service account |
| InfoServiceAccount | GET /admin/v3/info-service-account | Get service account info |

## Group Management (admin-handlers-users.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| AddGroup | PUT /admin/v3/add-group | Create group |
| GetGroup | GET /admin/v3/group | Get group info |
| ListGroups | GET /admin/v3/groups | List all groups |
| SetGroupStatus | PUT /admin/v3/set-group-status | Enable/disable group |
| UpdateGroupMembers | PUT /admin/v3/update-group-members | Add/remove members |

## Policy Management (admin-handlers-users.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| AddCannedPolicy | PUT /admin/v3/add-canned-policy | Create policy |
| SetPolicy | PUT /admin/v3/set-user-or-group-policy | Attach policy |
| ListCannedPolicies | GET /admin/v3/list-canned-policies | List policies |
| RemoveCannedPolicy | DELETE /admin/v3/remove-canned-policy | Delete policy |
| InfoCannedPolicy | GET /admin/v3/info-canned-policy | Get policy details |
| ListPolicyMappingEntities | GET /admin/v3/idp/builtin/policy-entities | List policy mappings |

## Configuration Management (admin-handlers-config-kv.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| GetConfig | GET /admin/v3/config | Get configuration |
| SetConfig | PUT /admin/v3/config | Set configuration |
| DelConfigKV | DELETE /admin/v3/del-config-kv | Delete config key |
| GetConfigKV | GET /admin/v3/get-config-kv | Get config key |
| SetConfigKV | PUT /admin/v3/set-config-kv | Set config key |
| ListConfigHistoryKV | GET /admin/v3/list-config-history-kv | List config history |
| RestoreConfigHistoryKV | PUT /admin/v3/restore-config-history-kv | Restore config |

## LDAP Integration (admin-handlers-idp-ldap.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| AddLDAPIDPConfig | PUT /admin/v3/idp/ldap | Add LDAP config |
| UpdateLDAPIDPConfig | POST /admin/v3/idp/ldap | Update LDAP config |
| GetLDAPIDPConfig | GET /admin/v3/idp/ldap | Get LDAP config |
| DeleteLDAPIDPConfig | DELETE /admin/v3/idp/ldap | Delete LDAP config |
| ListLDAPIDPConfigs | GET /admin/v3/idp/ldap/list | List LDAP configs |
| GetLDAPPolicyEntities | GET /admin/v3/idp/ldap/policy-entities | Get LDAP policy mappings |
| AttachPolicyLDAP | PUT /admin/v3/idp/ldap/policy | Attach policy to LDAP entity |

## OpenID Connect (admin-handlers-idp-openid.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| AddOpenIDConfig | PUT /admin/v3/idp/openid | Add OIDC config |
| UpdateOpenIDConfig | POST /admin/v3/idp/openid | Update OIDC config |
| GetOpenIDConfig | GET /admin/v3/idp/openid | Get OIDC config |
| DeleteOpenIDConfig | DELETE /admin/v3/idp/openid | Delete OIDC config |
| ListOpenIDConfigs | GET /admin/v3/idp/openid/list | List OIDC configs |

## Site Replication (admin-handlers-site-replication.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| SiteReplicationAdd | PUT /admin/v3/site-replication/add | Add replication peer |
| SiteReplicationRemove | PUT /admin/v3/site-replication/remove | Remove peer |
| SiteReplicationInfo | GET /admin/v3/site-replication/info | Get replication info |
| SiteReplicationStatus | GET /admin/v3/site-replication/status | Get replication status |
| SiteReplicationEdit | PUT /admin/v3/site-replication/edit | Edit peer config |
| SiteReplicationResync | POST /admin/v3/site-replication/resync | Trigger resync |

## Pool Management (admin-handlers-pools.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| ListPools | GET /admin/v3/pools/list | List storage pools |
| PoolStatus | GET /admin/v3/pools/status | Get pool status |
| DecommissionPool | POST /admin/v3/pools/decommission | Start decommission |
| CancelDecommission | POST /admin/v3/pools/cancel | Cancel decommission |

## Healing Operations (admin-heal-ops.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| HealStart | POST /admin/v3/heal | Start healing |
| HealStatus | GET /admin/v3/heal | Get healing status |
| BackgroundHealStatus | GET /admin/v3/background-heal/status | Background heal status |

## Bucket Admin Operations (admin-bucket-handlers.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| GetBucketQuota | GET /admin/v3/get-bucket-quota | Get bucket quota |
| SetBucketQuota | PUT /admin/v3/set-bucket-quota | Set bucket quota |
| SetRemoteTarget | PUT /admin/v3/set-remote-target | Set replication target |
| ListRemoteTargets | GET /admin/v3/list-remote-targets | List replication targets |
| RemoveRemoteTarget | DELETE /admin/v3/remove-remote-target | Remove target |
| GetBucketBandwidth | GET /admin/v3/get-bucket-bandwidth | Get bandwidth limit |
| SetBucketBandwidth | PUT /admin/v3/set-bucket-bandwidth | Set bandwidth limit |

## Tier Management (tier-handlers.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| AddTier | PUT /admin/v3/tier | Add tiering target |
| ListTiers | GET /admin/v3/tier | List tiers |
| EditTier | POST /admin/v3/tier | Edit tier config |
| RemoveTier | DELETE /admin/v3/tier | Remove tier |
| VerifyTier | GET /admin/v3/tier/verify | Verify tier connectivity |

## Batch Jobs (batch-handlers.go)

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| StartBatchJob | POST /admin/v3/start-job | Start batch job |
| ListBatchJobs | GET /admin/v3/list-jobs | List batch jobs |
| DescribeBatchJob | GET /admin/v3/describe-job | Get job details |
| CancelBatchJob | DELETE /admin/v3/cancel-job | Cancel job |

### Batch Job Types
- replicate: Bulk object replication
- expire: Bulk object expiration
- keyrotate: Bulk encryption key rotation
