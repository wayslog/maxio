#[cfg(test)]
mod tests {
    use crate::audit::{AuditConfig, AuditEvent, AuditEventType, AuditLogger};
    use crate::audit::config::AuditTarget;

    #[test]
    fn test_audit_event_types() {
        let types = [
            AuditEventType::GetObject,
            AuditEventType::PutObject,
            AuditEventType::DeleteObject,
            AuditEventType::ListObjects,
            AuditEventType::CreateBucket,
            AuditEventType::DeleteBucket,
        ];

        for t in &types {
            assert!(matches!(
                t,
                AuditEventType::GetObject
                    | AuditEventType::PutObject
                    | AuditEventType::DeleteObject
                    | AuditEventType::ListObjects
                    | AuditEventType::CreateBucket
                    | AuditEventType::DeleteBucket
            ));
        }
    }

    #[test]
    fn test_audit_event_creation() {
        let event = AuditEvent::new(
            AuditEventType::GetObject,
            "req-123".to_string(),
            "192.168.1.1".to_string(),
        );

        assert_eq!(event.event_type, AuditEventType::GetObject);
        assert_eq!(event.request_id, "req-123");
        assert_eq!(event.source_ip, "192.168.1.1");
        assert_eq!(event.status_code, 200);
        assert!(event.bucket.is_none());
        assert!(event.object.is_none());
    }

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::new(
            AuditEventType::PutObject,
            "req-456".to_string(),
            "10.0.0.1".to_string(),
        )
        .with_bucket("my-bucket")
        .with_object("my-key")
        .with_access_key("AKIAEXAMPLE")
        .with_status(201);

        assert_eq!(event.bucket, Some("my-bucket".to_string()));
        assert_eq!(event.object, Some("my-key".to_string()));
        assert_eq!(event.access_key, Some("AKIAEXAMPLE".to_string()));
        assert_eq!(event.status_code, 201);
    }

    #[test]
    fn test_audit_event_with_error() {
        let event = AuditEvent::new(
            AuditEventType::DeleteObject,
            "req-789".to_string(),
            "172.16.0.1".to_string(),
        )
        .with_status(404)
        .with_error("Object not found");

        assert_eq!(event.status_code, 404);
        assert_eq!(event.error_message, Some("Object not found".to_string()));
    }

    #[test]
    fn test_audit_config_default() {
        let config = AuditConfig::default();
        assert!(!config.enabled);
        assert!(config.targets.is_empty());
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_audit_config_enabled_without_targets() {
        let config = AuditConfig {
            enabled: true,
            targets: vec![],
        };
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_audit_config_enabled_with_targets() {
        let config = AuditConfig {
            enabled: true,
            targets: vec![AuditTarget::Console],
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_audit_logger_disabled() {
        let config = AuditConfig::default();
        let logger = AuditLogger::new(config);
        assert!(!logger.is_enabled());
    }

    #[test]
    fn test_audit_logger_enabled() {
        let config = AuditConfig {
            enabled: true,
            targets: vec![AuditTarget::Console],
        };
        let logger = AuditLogger::new(config);
        assert!(logger.is_enabled());
    }

    #[tokio::test]
    async fn test_audit_logger_start() {
        let config = AuditConfig {
            enabled: true,
            targets: vec![AuditTarget::Console],
        };
        let mut logger = AuditLogger::new(config);
        let result = logger.start().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_audit_logger_log_event() {
        let config = AuditConfig {
            enabled: true,
            targets: vec![AuditTarget::Console],
        };
        let mut logger = AuditLogger::new(config);
        logger.start().await.unwrap();

        let event = AuditEvent::new(
            AuditEventType::GetObject,
            "test-req".to_string(),
            "127.0.0.1".to_string(),
        );

        let result = logger.log(event).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_file_audit_target() {
        let target = AuditTarget::File {
            path: "/var/log/audit.log".to_string(),
        };
        assert!(matches!(target, AuditTarget::File { .. }));
    }

    #[test]
    fn test_webhook_audit_target() {
        let target = AuditTarget::Webhook {
            endpoint: "https://audit.example.com/events".to_string(),
            auth_token: Some("secret-token".to_string()),
        };
        assert!(matches!(target, AuditTarget::Webhook { .. }));
    }

    #[test]
    fn test_kafka_audit_target() {
        let target = AuditTarget::Kafka {
            brokers: vec!["kafka1:9092".to_string(), "kafka2:9092".to_string()],
            topic: "audit-events".to_string(),
        };
        assert!(matches!(target, AuditTarget::Kafka { .. }));
    }
}
