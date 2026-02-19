use std::time::SystemTime;

pub fn generate_request_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:X}", nanos)
}

pub fn generate_host_id() -> String {
    "maxio-deployment-id".to_string()
}
