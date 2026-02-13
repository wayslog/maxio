use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename = "NotificationConfiguration")]
pub struct NotificationConfiguration {
    #[serde(rename = "QueueConfiguration", default)]
    pub queue_configurations: Vec<QueueConfiguration>,
    #[serde(rename = "TopicConfiguration", default)]
    pub topic_configurations: Vec<TopicConfiguration>,
    #[serde(rename = "CloudFunctionConfiguration", default)]
    pub lambda_configurations: Vec<LambdaConfiguration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfiguration {
    #[serde(rename = "Id", default)]
    pub id: String,
    #[serde(rename = "Queue")]
    pub queue_arn: String,
    #[serde(rename = "Event", default)]
    pub events: Vec<String>,
    #[serde(
        rename = "Filter",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_filter_rules",
        serialize_with = "serialize_filter_rules"
    )]
    pub filter: Option<FilterRules>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfiguration {
    #[serde(rename = "Id", default)]
    pub id: String,
    #[serde(rename = "Topic")]
    pub topic_arn: String,
    #[serde(rename = "Event", default)]
    pub events: Vec<String>,
    #[serde(
        rename = "Filter",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_filter_rules",
        serialize_with = "serialize_filter_rules"
    )]
    pub filter: Option<FilterRules>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaConfiguration {
    #[serde(rename = "Id", default)]
    pub id: String,
    #[serde(rename = "CloudFunction")]
    pub lambda_arn: String,
    #[serde(rename = "Event", default)]
    pub events: Vec<String>,
    #[serde(
        rename = "Filter",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_filter_rules",
        serialize_with = "serialize_filter_rules"
    )]
    pub filter: Option<FilterRules>,
}

#[derive(Debug, Clone, Default)]
pub struct FilterRules {
    pub prefix: Option<String>,
    pub suffix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Event {
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "eventTime")]
    pub event_time: String,
    #[serde(rename = "eventName")]
    pub event_name: String,
    pub bucket: BucketInfo,
    pub object: ObjectInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub key: String,
    pub size: i64,
    #[serde(rename = "eTag")]
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XmlFilter {
    #[serde(rename = "S3Key")]
    s3_key: XmlS3Key,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XmlS3Key {
    #[serde(rename = "FilterRule", default)]
    filter_rule: Vec<XmlFilterRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XmlFilterRule {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Value")]
    value: String,
}

fn serialize_filter_rules<S>(value: &Option<FilterRules>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(value) => {
            let mut rules = Vec::new();
            if let Some(prefix) = value.prefix.as_ref() {
                rules.push(XmlFilterRule {
                    name: "prefix".to_string(),
                    value: prefix.clone(),
                });
            }
            if let Some(suffix) = value.suffix.as_ref() {
                rules.push(XmlFilterRule {
                    name: "suffix".to_string(),
                    value: suffix.clone(),
                });
            }

            Some(XmlFilter {
                s3_key: XmlS3Key { filter_rule: rules },
            })
            .serialize(serializer)
        }
    }
}

fn deserialize_filter_rules<'de, D>(deserializer: D) -> Result<Option<FilterRules>, D::Error>
where
    D: Deserializer<'de>,
{
    let filter = Option::<XmlFilter>::deserialize(deserializer)?;
    let Some(filter) = filter else {
        return Ok(None);
    };

    let mut prefix = None;
    let mut suffix = None;
    for rule in filter.s3_key.filter_rule {
        match rule.name.to_ascii_lowercase().as_str() {
            "prefix" => prefix = Some(rule.value),
            "suffix" => suffix = Some(rule.value),
            _ => {}
        }
    }

    if prefix.is_none() && suffix.is_none() {
        return Ok(None);
    }

    Ok(Some(FilterRules { prefix, suffix }))
}
