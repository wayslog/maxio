#[cfg(test)]
mod tests {
    use crate::tiering::{TierClient, TierConfig, TierRule, TierRules, TierTarget};
    use bytes::Bytes;
    use chrono::{Duration, Utc};

    mod azure_tier {
        use super::*;

        #[tokio::test]
        #[ignore] // Requires real Azure credentials
        async fn test_azure_put_object() {
            let config = TierConfig {
                name: "azure-tier".to_string(),
                target: TierTarget::Azure {
                    account_name: "testaccount".to_string(),
                    account_key: "dGVzdGtleQ==".to_string(),
                    container: "testcontainer".to_string(),
                    endpoint: None,
                },
                prefix: "tier/".to_string(),
            };

            let client = TierClient::new(config).unwrap();
            let data = Bytes::from("test data for azure");
            
            let result = client.put_object("test-key", data).await;
            assert!(result.is_ok(), "Azure PUT should succeed");
        }

        #[tokio::test]
        #[ignore] // Requires real Azure credentials
        async fn test_azure_get_object() {
            let config = TierConfig {
                name: "azure-tier".to_string(),
                target: TierTarget::Azure {
                    account_name: "testaccount".to_string(),
                    account_key: "dGVzdGtleQ==".to_string(),
                    container: "testcontainer".to_string(),
                    endpoint: None,
                },
                prefix: "tier/".to_string(),
            };

            let client = TierClient::new(config).unwrap();
            
            let result = client.get_object("test-key").await;
            assert!(result.is_ok(), "Azure GET should succeed");
        }

        #[tokio::test]
        async fn test_azure_delete_object() {
            let config = TierConfig {
                name: "azure-tier".to_string(),
                target: TierTarget::Azure {
                    account_name: "testaccount".to_string(),
                    account_key: "dGVzdGtleQ==".to_string(),
                    container: "testcontainer".to_string(),
                    endpoint: None,
                },
                prefix: "tier/".to_string(),
            };

            let client = TierClient::new(config).unwrap();
            
            let result = client.delete_object("test-key").await;
            assert!(result.is_ok(), "Azure DELETE should succeed");
        }

        #[test]
        fn test_azure_tier_config() {
            let config = TierConfig {
                name: "azure-tier".to_string(),
                target: TierTarget::Azure {
                    account_name: "testaccount".to_string(),
                    account_key: "dGVzdGtleQ==".to_string(),
                    container: "testcontainer".to_string(),
                    endpoint: Some("custom.blob.endpoint.com".to_string()),
                },
                prefix: "tier/".to_string(),
            };

            let client = TierClient::new(config);
            assert!(client.is_ok(), "Azure tier client should be created");
            assert_eq!(client.unwrap().tier_name(), "azure-tier");
        }
    }

    mod gcs_tier {
        use super::*;

        #[tokio::test]
        #[ignore] // Requires real GCS credentials
        async fn test_gcs_put_object() {
            let config = TierConfig {
                name: "gcs-tier".to_string(),
                target: TierTarget::Gcs {
                    bucket: "test-bucket".to_string(),
                    credentials_json: r#"{"type":"service_account"}"#.to_string(),
                    prefix: "tier/".to_string(),
                },
                prefix: "".to_string(),
            };

            let client = TierClient::new(config).unwrap();
            let data = Bytes::from("test data for gcs");
            
            let result = client.put_object("test-key", data).await;
            assert!(result.is_ok(), "GCS PUT should succeed");
        }

        #[tokio::test]
        #[ignore] // Requires real GCS credentials
        async fn test_gcs_get_object() {
            let config = TierConfig {
                name: "gcs-tier".to_string(),
                target: TierTarget::Gcs {
                    bucket: "test-bucket".to_string(),
                    credentials_json: r#"{"type":"service_account"}"#.to_string(),
                    prefix: "tier/".to_string(),
                },
                prefix: "".to_string(),
            };

            let client = TierClient::new(config).unwrap();
            
            let result = client.get_object("test-key").await;
            assert!(result.is_ok(), "GCS GET should succeed");
        }

        #[tokio::test]
        async fn test_gcs_delete_object() {
            let config = TierConfig {
                name: "gcs-tier".to_string(),
                target: TierTarget::Gcs {
                    bucket: "test-bucket".to_string(),
                    credentials_json: r#"{"type":"service_account"}"#.to_string(),
                    prefix: "tier/".to_string(),
                },
                prefix: "".to_string(),
            };

            let client = TierClient::new(config).unwrap();
            
            let result = client.delete_object("test-key").await;
            assert!(result.is_ok(), "GCS DELETE should succeed");
        }

        #[test]
        fn test_gcs_tier_config() {
            let config = TierConfig {
                name: "gcs-tier".to_string(),
                target: TierTarget::Gcs {
                    bucket: "test-bucket".to_string(),
                    credentials_json: r#"{"type":"service_account"}"#.to_string(),
                    prefix: "tier/".to_string(),
                },
                prefix: "".to_string(),
            };

            let client = TierClient::new(config);
            assert!(client.is_ok(), "GCS tier client should be created");
            assert_eq!(client.unwrap().tier_name(), "gcs-tier");
        }
    }

    mod s3_tier {
        use super::*;

        #[tokio::test]
        async fn test_s3_tier_config() {
            let config = TierConfig {
                name: "s3-tier".to_string(),
                target: TierTarget::S3 {
                    endpoint: "s3.amazonaws.com".to_string(),
                    bucket: "test-bucket".to_string(),
                    region: "us-east-1".to_string(),
                    access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
                    use_ssl: true,
                },
                prefix: "tier/".to_string(),
            };

            let client = TierClient::new(config);
            assert!(client.is_ok(), "S3 tier client should be created");
            assert_eq!(client.unwrap().tier_name(), "s3-tier");
        }
    }

    mod tier_rules {
        use super::*;

        #[test]
        fn test_add_rule() {
            let mut rules = TierRules::default();
            let rule = TierRule {
                id: "rule1".to_string(),
                tier_name: "cold-tier".to_string(),
                prefix: "logs/".to_string(),
                tags: vec![],
                transition_days: 30,
                enabled: true,
            };

            rules.add_rule(rule);
            assert_eq!(rules.rules.len(), 1);
        }

        #[test]
        fn test_remove_rule() {
            let mut rules = TierRules::default();
            rules.add_rule(TierRule {
                id: "rule1".to_string(),
                tier_name: "cold-tier".to_string(),
                prefix: "".to_string(),
                tags: vec![],
                transition_days: 30,
                enabled: true,
            });

            rules.remove_rule("rule1");
            assert_eq!(rules.rules.len(), 0);
        }

        #[test]
        fn test_matching_tier_by_age() {
            let mut rules = TierRules::default();
            rules.add_rule(TierRule {
                id: "rule1".to_string(),
                tier_name: "cold-tier".to_string(),
                prefix: "".to_string(),
                tags: vec![],
                transition_days: 30,
                enabled: true,
            });

            // Object created 31 days ago should match
            let old_date = Utc::now() - Duration::days(31);
            assert_eq!(rules.matching_tier("any-key", old_date), Some("cold-tier"));

            // Object created today should not match
            let new_date = Utc::now();
            assert_eq!(rules.matching_tier("any-key", new_date), None);
        }

        #[test]
        fn test_matching_tier_by_prefix() {
            let mut rules = TierRules::default();
            rules.add_rule(TierRule {
                id: "rule1".to_string(),
                tier_name: "archive-tier".to_string(),
                prefix: "archive/".to_string(),
                tags: vec![],
                transition_days: 0,
                enabled: true,
            });

            let date = Utc::now();
            assert_eq!(rules.matching_tier("archive/file.txt", date), Some("archive-tier"));
            assert_eq!(rules.matching_tier("other/file.txt", date), None);
        }

        #[test]
        fn test_disabled_rule_not_matched() {
            let mut rules = TierRules::default();
            rules.add_rule(TierRule {
                id: "rule1".to_string(),
                tier_name: "cold-tier".to_string(),
                prefix: "".to_string(),
                tags: vec![],
                transition_days: 0,
                enabled: false,
            });

            let date = Utc::now();
            assert_eq!(rules.matching_tier("any-key", date), None);
        }
    }
}
