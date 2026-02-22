use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierRules {
    pub rules: Vec<TierRule>,
}

impl Default for TierRules {
    fn default() -> Self {
        Self { rules: Vec::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierRule {
    pub id: String,
    pub tier_name: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub tags: Vec<(String, String)>,
    pub transition_days: u32,
    #[serde(default)]
    pub enabled: bool,
}

impl TierRules {
    pub fn add_rule(&mut self, rule: TierRule) {
        if !self.rules.iter().any(|r| r.id == rule.id) {
            self.rules.push(rule);
        }
    }

    pub fn remove_rule(&mut self, id: &str) {
        self.rules.retain(|r| r.id != id);
    }

    pub fn matching_tier(&self, key: &str, created: DateTime<Utc>) -> Option<&str> {
        let now = Utc::now();
        let age_days = (now - created).num_days() as u32;

        for rule in &self.rules {
            if !rule.enabled {
                continue;
            }

            if !rule.prefix.is_empty() && !key.starts_with(&rule.prefix) {
                continue;
            }

            if age_days >= rule.transition_days {
                return Some(&rule.tier_name);
            }
        }

        None
    }

    pub fn enabled_rules(&self) -> impl Iterator<Item = &TierRule> {
        self.rules.iter().filter(|r| r.enabled)
    }
}
