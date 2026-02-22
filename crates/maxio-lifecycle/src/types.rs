use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Action represents lifecycle actions that can be taken on objects
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    None,
    Delete,
    DeleteVersion,
    Transition,
    TransitionVersion,
    DeleteRestored,
    DeleteRestoredVersion,
    DeleteAllVersions,
    DelMarkerDeleteAllVersions,
}

impl Action {
    pub fn is_delete(&self) -> bool {
        matches!(
            self,
            Action::Delete
                | Action::DeleteVersion
                | Action::DeleteRestored
                | Action::DeleteRestoredVersion
                | Action::DeleteAllVersions
                | Action::DelMarkerDeleteAllVersions
        )
    }

    pub fn is_delete_restored(&self) -> bool {
        matches!(self, Action::DeleteRestored | Action::DeleteRestoredVersion)
    }

    pub fn is_delete_versioned(&self) -> bool {
        matches!(self, Action::DeleteVersion | Action::DeleteRestoredVersion)
    }

    pub fn is_delete_all(&self) -> bool {
        matches!(
            self,
            Action::DeleteAllVersions | Action::DelMarkerDeleteAllVersions
        )
    }

    pub fn is_transition(&self) -> bool {
        matches!(self, Action::Transition | Action::TransitionVersion)
    }
}

impl Default for Action {
    fn default() -> Self {
        Action::None
    }
}

// Event contains a lifecycle action with associated info
#[derive(Debug, Clone, Default)]
pub struct LifecycleEvent {
    pub action: Action,
    pub rule_id: String,
    pub due: Option<DateTime<Utc>>,
    pub noncurrent_days: i32,
    pub newer_noncurrent_versions: i32,
    pub storage_class: String,
}

// Transition status constants
pub const TRANSITION_COMPLETE: &str = "complete";
pub const TRANSITION_PENDING: &str = "pending";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename = "LifecycleConfiguration")]
pub struct LifecycleConfiguration {
    #[serde(rename = "Rule", default)]
    pub rules: Vec<LifecycleRule>,
    #[serde(rename = "ExpiryUpdatedAt", skip_serializing_if = "Option::is_none")]
    pub expiry_updated_at: Option<DateTime<Utc>>,
}

impl LifecycleConfiguration {
    pub fn has_transition(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.transition.as_ref().is_some_and(|t| t.is_enabled()))
    }

    pub fn has_expiry(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.expiration.is_some() || rule.noncurrent_version_expiration.is_some())
    }

    pub fn has_active_rules(&self, prefix: &str) -> bool {
        if self.rules.is_empty() {
            return false;
        }

        for rule in &self.rules {
            if rule.status != RuleStatus::Enabled {
                continue;
            }

            let rule_prefix = rule.get_prefix();
            if !prefix.is_empty() && !rule_prefix.is_empty() {
                if !prefix.starts_with(&rule_prefix) && !rule_prefix.starts_with(prefix) {
                    continue;
                }
            }

            if rule
                .noncurrent_version_expiration
                .as_ref()
                .is_some_and(|e| e.noncurrent_days > 0)
            {
                return true;
            }

            if rule
                .noncurrent_version_expiration
                .as_ref()
                .is_some_and(|e| e.newer_noncurrent_versions > 0)
            {
                return true;
            }

            if rule
                .noncurrent_version_transition
                .as_ref()
                .is_some_and(|t| !t.is_null())
            {
                return true;
            }

            if let Some(exp) = &rule.expiration {
                if exp.date.is_some_and(|d| d < Utc::now()) {
                    return true;
                }
                if exp.days.is_some() {
                    return true;
                }
                if exp.expired_object_delete_marker == Some(true) {
                    return true;
                }
            }

            if let Some(trans) = &rule.transition {
                if trans.date.is_some_and(|d| d < Utc::now()) {
                    return true;
                }
                if trans.days.is_some() {
                    return true;
                }
            }
        }

        false
    }

    pub fn filter_rules(&self, obj: &ObjectOpts) -> Vec<&LifecycleRule> {
        if obj.name.is_empty() {
            return vec![];
        }

        self.rules
            .iter()
            .filter(|rule| {
                if rule.status != RuleStatus::Enabled {
                    return false;
                }
                if !obj.name.starts_with(&rule.get_prefix()) {
                    return false;
                }
                if let Some(filter) = &rule.filter {
                    if !filter.test_tags(&obj.user_tags) {
                        return false;
                    }
                    if !obj.delete_marker && !filter.by_size(obj.size) {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    pub fn eval(&self, obj: &ObjectOpts) -> LifecycleEvent {
        self.eval_at(obj, Utc::now(), 0)
    }

    pub fn eval_at(
        &self,
        obj: &ObjectOpts,
        now: DateTime<Utc>,
        remaining_versions: i32,
    ) -> LifecycleEvent {
        let mut events = Vec::new();

        if obj.mod_time.is_none() {
            return LifecycleEvent::default();
        }

        // Handle expiry of restored objects
        if let Some(restore_expires) = obj.restore_expires {
            if now > restore_expires {
                let action = if obj.is_latest {
                    Action::DeleteRestored
                } else {
                    Action::DeleteRestoredVersion
                };
                events.push(LifecycleEvent {
                    action,
                    due: Some(now),
                    ..Default::default()
                });
            }
        }

        for rule in self.filter_rules(obj) {
            // Expired object delete marker handling
            if obj.expired_object_delete_marker() {
                if let Some(exp) = &rule.expiration {
                    if exp.expired_object_delete_marker == Some(true) {
                        events.push(LifecycleEvent {
                            action: Action::DeleteVersion,
                            rule_id: rule.id.clone(),
                            due: Some(now),
                            ..Default::default()
                        });
                        break;
                    }

                    if let Some(days) = exp.days {
                        if let Some(mod_time) = obj.mod_time {
                            let expected_expiry = expected_expiry_time(mod_time, days);
                            if now > expected_expiry {
                                events.push(LifecycleEvent {
                                    action: Action::DeleteVersion,
                                    rule_id: rule.id.clone(),
                                    due: Some(expected_expiry),
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                }
            }

            // DelMarkerExpiration
            if obj.is_latest && obj.delete_marker {
                if let Some(del_marker_exp) = &rule.del_marker_expiration {
                    if let Some((due, ok)) = del_marker_exp.next_due(obj) {
                        if ok && now > due {
                            events.push(LifecycleEvent {
                                action: Action::DelMarkerDeleteAllVersions,
                                rule_id: rule.id.clone(),
                                due: Some(due),
                                ..Default::default()
                            });
                        }
                    }
                }
                continue;
            }

            // NoncurrentVersionExpiration
            if !obj.is_latest {
                if let Some(noncurrent_exp) = &rule.noncurrent_version_expiration {
                    let retained_enough = noncurrent_exp.newer_noncurrent_versions == 0
                        || remaining_versions >= noncurrent_exp.newer_noncurrent_versions;

                    if let Some(successor_mod_time) = obj.successor_mod_time {
                        let expected_expiry = expected_expiry_time(
                            successor_mod_time,
                            noncurrent_exp.noncurrent_days,
                        );
                        let old_enough = now > expected_expiry;

                        if retained_enough && old_enough {
                            events.push(LifecycleEvent {
                                action: Action::DeleteVersion,
                                rule_id: rule.id.clone(),
                                due: Some(expected_expiry),
                                ..Default::default()
                            });
                        }
                    }
                }

                // NoncurrentVersionTransition
                if let Some(noncurrent_trans) = &rule.noncurrent_version_transition {
                    if !obj.delete_marker && obj.transition_status != TRANSITION_COMPLETE {
                        if let Some((due, ok)) = noncurrent_trans.next_due(obj) {
                            if ok && now > due {
                                events.push(LifecycleEvent {
                                    action: Action::TransitionVersion,
                                    rule_id: rule.id.clone(),
                                    due: Some(due),
                                    storage_class: noncurrent_trans.storage_class.clone(),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
            }

            // Current version expiration and transition
            if obj.is_latest && !obj.delete_marker {
                if let Some(exp) = &rule.expiration {
                    if let Some(date) = exp.date {
                        if now > date {
                            let action = if exp.delete_all == Some(true) {
                                Action::DeleteAllVersions
                            } else {
                                Action::Delete
                            };
                            events.push(LifecycleEvent {
                                action,
                                rule_id: rule.id.clone(),
                                due: Some(date),
                                ..Default::default()
                            });
                        }
                    } else if let Some(days) = exp.days {
                        if let Some(mod_time) = obj.mod_time {
                            let expected_expiry = expected_expiry_time(mod_time, days);
                            if now > expected_expiry {
                                let action = if exp.delete_all == Some(true) {
                                    Action::DeleteAllVersions
                                } else {
                                    Action::Delete
                                };
                                events.push(LifecycleEvent {
                                    action,
                                    rule_id: rule.id.clone(),
                                    due: Some(expected_expiry),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }

                // Transition
                if obj.transition_status != TRANSITION_COMPLETE {
                    if let Some(trans) = &rule.transition {
                        if let Some((due, ok)) = trans.next_due(obj) {
                            if ok && now > due {
                                events.push(LifecycleEvent {
                                    action: Action::Transition,
                                    rule_id: rule.id.clone(),
                                    due: Some(due),
                                    storage_class: trans.storage_class.clone(),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                }
            }
        }

        if events.is_empty() {
            return LifecycleEvent::default();
        }

        // Sort events: prefer expiration over transition, prefer earlier due dates
        events.sort_by(|a, b| {
            let a_due = a.due.unwrap_or(now);
            let b_due = b.due.unwrap_or(now);

            if now > a_due && now > b_due || a_due == b_due {
                // Prefer delete actions over transitions
                if a.action.is_delete() && !b.action.is_delete() {
                    return std::cmp::Ordering::Less;
                }
                if !a.action.is_delete() && b.action.is_delete() {
                    return std::cmp::Ordering::Greater;
                }
            }

            a_due.cmp(&b_due)
        });

        events.into_iter().next().unwrap_or_default()
    }

    pub fn noncurrent_versions_expiration_limit(&self, obj: &ObjectOpts) -> LifecycleEvent {
        for rule in self.filter_rules(obj) {
            if let Some(noncurrent_exp) = &rule.noncurrent_version_expiration {
                if noncurrent_exp.newer_noncurrent_versions > 0 {
                    return LifecycleEvent {
                        action: Action::DeleteVersion,
                        rule_id: rule.id.clone(),
                        noncurrent_days: noncurrent_exp.noncurrent_days,
                        newer_noncurrent_versions: noncurrent_exp.newer_noncurrent_versions,
                        ..Default::default()
                    };
                }
            }
        }
        LifecycleEvent::default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Status")]
    pub status: RuleStatus,
    #[serde(rename = "Filter", default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<LifecycleFilter>,
    #[serde(rename = "Prefix", default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(
        rename = "Expiration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration: Option<Expiration>,
    #[serde(
        rename = "Transition",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub transition: Option<Transition>,
    #[serde(
        rename = "NoncurrentVersionExpiration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
    #[serde(
        rename = "NoncurrentVersionTransition",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub noncurrent_version_transition: Option<NoncurrentVersionTransition>,
    #[serde(
        rename = "DelMarkerExpiration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub del_marker_expiration: Option<DelMarkerExpiration>,
}

impl LifecycleRule {
    pub fn get_prefix(&self) -> String {
        if let Some(filter) = &self.filter {
            if let Some(prefix) = &filter.prefix {
                return prefix.clone();
            }
        }
        self.prefix.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    #[serde(rename = "Enabled")]
    Enabled,
    #[serde(rename = "Disabled")]
    Disabled,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleFilter {
    #[serde(rename = "Prefix", default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Tag", default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<Tag>,
    #[serde(rename = "And", default, skip_serializing_if = "Option::is_none")]
    pub and: Option<AndOperator>,
    #[serde(
        rename = "ObjectSizeGreaterThan",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub object_size_greater_than: Option<i64>,
    #[serde(
        rename = "ObjectSizeLessThan",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub object_size_less_than: Option<i64>,
}

impl LifecycleFilter {
    pub fn test_tags(&self, user_tags: &str) -> bool {
        if user_tags.is_empty() {
            return self.tag.is_none() && self.and.as_ref().map_or(true, |a| a.tags.is_empty());
        }

        let tags: HashMap<String, String> = user_tags
            .split('&')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, '=');
                Some((parts.next()?.to_string(), parts.next()?.to_string()))
            })
            .collect();

        if let Some(tag) = &self.tag {
            if tags.get(&tag.key) != Some(&tag.value) {
                return false;
            }
        }

        if let Some(and) = &self.and {
            for tag in &and.tags {
                if tags.get(&tag.key) != Some(&tag.value) {
                    return false;
                }
            }
        }

        true
    }

    pub fn by_size(&self, size: i64) -> bool {
        if let Some(min) = self.object_size_greater_than {
            if size <= min {
                return false;
            }
        }
        if let Some(max) = self.object_size_less_than {
            if size >= max {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AndOperator {
    #[serde(rename = "Prefix", default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Tags", default)]
    pub tags: Vec<Tag>,
    #[serde(
        rename = "ObjectSizeGreaterThan",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub object_size_greater_than: Option<i64>,
    #[serde(
        rename = "ObjectSizeLessThan",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub object_size_less_than: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Expiration {
    #[serde(rename = "Days", default, skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    #[serde(rename = "Date", default, skip_serializing_if = "Option::is_none")]
    pub date: Option<DateTime<Utc>>,
    #[serde(
        rename = "ExpiredObjectDeleteMarker",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expired_object_delete_marker: Option<bool>,
    #[serde(
        rename = "ExpiredObjectAllVersions",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub delete_all: Option<bool>,
}

impl Expiration {
    pub fn is_null(&self) -> bool {
        self.days.is_none() && self.date.is_none() && self.expired_object_delete_marker.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Transition {
    #[serde(rename = "Days", default, skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
    #[serde(rename = "Date", default, skip_serializing_if = "Option::is_none")]
    pub date: Option<DateTime<Utc>>,
    #[serde(rename = "StorageClass", default)]
    pub storage_class: String,
}

impl Transition {
    pub fn is_enabled(&self) -> bool {
        !self.is_null()
    }

    pub fn is_null(&self) -> bool {
        self.days.is_none() && self.date.is_none()
    }

    pub fn next_due(&self, obj: &ObjectOpts) -> Option<(DateTime<Utc>, bool)> {
        if let Some(date) = self.date {
            return Some((date, true));
        }
        if let Some(days) = self.days {
            if let Some(mod_time) = obj.mod_time {
                let due = expected_expiry_time(mod_time, days);
                return Some((due, true));
            }
        }
        None
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    #[serde(rename = "NoncurrentDays", default)]
    pub noncurrent_days: i32,
    #[serde(rename = "NewerNoncurrentVersions", default)]
    pub newer_noncurrent_versions: i32,
}

impl NoncurrentVersionExpiration {
    pub fn is_null(&self) -> bool {
        self.noncurrent_days == 0 && self.newer_noncurrent_versions == 0
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NoncurrentVersionTransition {
    #[serde(rename = "NoncurrentDays", default)]
    pub noncurrent_days: i32,
    #[serde(rename = "StorageClass", default)]
    pub storage_class: String,
}

impl NoncurrentVersionTransition {
    pub fn is_null(&self) -> bool {
        self.noncurrent_days == 0 && self.storage_class.is_empty()
    }

    pub fn next_due(&self, obj: &ObjectOpts) -> Option<(DateTime<Utc>, bool)> {
        if self.noncurrent_days == 0 {
            return None;
        }
        if let Some(successor_mod_time) = obj.successor_mod_time {
            let due = expected_expiry_time(successor_mod_time, self.noncurrent_days);
            return Some((due, true));
        }
        None
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DelMarkerExpiration {
    #[serde(rename = "Days", default, skip_serializing_if = "Option::is_none")]
    pub days: Option<i32>,
}

impl DelMarkerExpiration {
    pub fn is_empty(&self) -> bool {
        self.days.is_none() || self.days == Some(0)
    }

    pub fn next_due(&self, obj: &ObjectOpts) -> Option<(DateTime<Utc>, bool)> {
        if let Some(days) = self.days {
            if days > 0 {
                if let Some(mod_time) = obj.mod_time {
                    let due = expected_expiry_time(mod_time, days);
                    return Some((due, true));
                }
            }
        }
        None
    }
}

// ObjectOpts provides information to deduce lifecycle actions
#[derive(Debug, Clone, Default)]
pub struct ObjectOpts {
    pub name: String,
    pub user_tags: String,
    pub mod_time: Option<DateTime<Utc>>,
    pub size: i64,
    pub version_id: String,
    pub is_latest: bool,
    pub delete_marker: bool,
    pub num_versions: i32,
    pub successor_mod_time: Option<DateTime<Utc>>,
    pub transition_status: String,
    pub restore_ongoing: bool,
    pub restore_expires: Option<DateTime<Utc>>,
}

impl ObjectOpts {
    pub fn expired_object_delete_marker(&self) -> bool {
        self.delete_marker && self.num_versions == 1
    }
}

// Calculate expected expiry time based on modification time and days
pub fn expected_expiry_time(mod_time: DateTime<Utc>, days: i32) -> DateTime<Utc> {
    if days == 0 {
        return mod_time;
    }
    let duration = chrono::Duration::days(i64::from(days) + 1);
    let t = mod_time + duration;
    // Truncate to midnight
    t.date_naive()
        .and_hms_opt(0, 0, 0)
        .map(|dt| DateTime::from_naive_utc_and_offset(dt, Utc))
        .unwrap_or(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_is_delete() {
        assert!(Action::Delete.is_delete());
        assert!(Action::DeleteVersion.is_delete());
        assert!(Action::DeleteAllVersions.is_delete());
        assert!(!Action::Transition.is_delete());
        assert!(!Action::None.is_delete());
    }

    #[test]
    fn test_filter_test_tags() {
        let filter = LifecycleFilter {
            tag: Some(Tag {
                key: "env".to_string(),
                value: "prod".to_string(),
            }),
            ..Default::default()
        };

        assert!(filter.test_tags("env=prod"));
        assert!(filter.test_tags("env=prod&team=backend"));
        assert!(!filter.test_tags("env=dev"));
        assert!(!filter.test_tags(""));
    }

    #[test]
    fn test_filter_by_size() {
        let filter = LifecycleFilter {
            object_size_greater_than: Some(100),
            object_size_less_than: Some(1000),
            ..Default::default()
        };

        assert!(filter.by_size(500));
        assert!(!filter.by_size(50));
        assert!(!filter.by_size(1500));
    }

    #[test]
    fn test_expected_expiry_time() {
        let mod_time = DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let expiry = expected_expiry_time(mod_time, 1);
        // Should be midnight on Jan 17 (mod_time + 2 days, truncated to midnight)
        assert_eq!(expiry.date_naive().to_string(), "2024-01-17");
        assert_eq!(expiry.time().to_string(), "00:00:00");
    }

    #[test]
    fn test_lifecycle_config_has_active_rules() {
        let config = LifecycleConfiguration {
            rules: vec![LifecycleRule {
                id: "rule1".to_string(),
                status: RuleStatus::Enabled,
                filter: Some(LifecycleFilter {
                    prefix: Some("logs/".to_string()),
                    ..Default::default()
                }),
                expiration: Some(Expiration {
                    days: Some(30),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(config.has_active_rules("logs/"));
        assert!(config.has_active_rules("logs/2024/"));
        assert!(!config.has_active_rules("data/"));
    }
}

impl Default for LifecycleRule {
    fn default() -> Self {
        Self {
            id: String::new(),
            status: RuleStatus::Disabled,
            filter: None,
            prefix: None,
            expiration: None,
            transition: None,
            noncurrent_version_expiration: None,
            noncurrent_version_transition: None,
            del_marker_expiration: None,
        }
    }
}
