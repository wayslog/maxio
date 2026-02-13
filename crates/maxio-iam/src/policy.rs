use crate::types::{Effect, Policy};

pub fn evaluate_policy(policies: &[Policy], action: &str, resource: &str) -> bool {
    let mut allow = false;

    for statement in policies.iter().flat_map(|p| p.statements.iter()) {
        if !matches_any(&statement.actions, action) || !matches_any(&statement.resources, resource)
        {
            continue;
        }

        if statement.effect == Effect::Deny {
            return false;
        }

        allow = true;
    }

    allow
}

fn matches_any(patterns: &[String], value: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| wildcard_match(pattern, value))
}

fn wildcard_match(pattern: &str, input: &str) -> bool {
    let pattern = pattern.as_bytes();
    let input = input.as_bytes();

    let mut p = 0;
    let mut i = 0;
    let mut star_idx: Option<usize> = None;
    let mut match_idx = 0;

    while i < input.len() {
        if p < pattern.len() && (pattern[p] == input[i] || pattern[p] == b'*') {
            if pattern[p] == b'*' {
                star_idx = Some(p);
                match_idx = i;
                p += 1;
            } else {
                p += 1;
                i += 1;
            }
        } else if let Some(star) = star_idx {
            p = star + 1;
            match_idx += 1;
            i = match_idx;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

#[cfg(test)]
mod tests {
    use crate::types::{Effect, Policy, PolicyStatement};

    use super::evaluate_policy;

    #[test]
    fn deny_precedes_allow() {
        let policies = vec![Policy {
            name: "test".to_string(),
            version: "2012-10-17".to_string(),
            statements: vec![
                PolicyStatement {
                    effect: Effect::Allow,
                    actions: vec!["s3:*".to_string()],
                    resources: vec!["arn:aws:s3:::mybucket/*".to_string()],
                },
                PolicyStatement {
                    effect: Effect::Deny,
                    actions: vec!["s3:DeleteObject".to_string()],
                    resources: vec!["arn:aws:s3:::mybucket/private/*".to_string()],
                },
            ],
        }];

        assert!(!evaluate_policy(
            &policies,
            "s3:DeleteObject",
            "arn:aws:s3:::mybucket/private/key"
        ));
    }

    #[test]
    fn wildcard_action_resource_work() {
        let policies = vec![Policy {
            name: "readonly".to_string(),
            version: "2012-10-17".to_string(),
            statements: vec![PolicyStatement {
                effect: Effect::Allow,
                actions: vec!["s3:Get*".to_string(), "s3:ListBucket".to_string()],
                resources: vec![
                    "arn:aws:s3:::mybucket/*".to_string(),
                    "arn:aws:s3:::mybucket".to_string(),
                ],
            }],
        }];

        assert!(evaluate_policy(
            &policies,
            "s3:GetObject",
            "arn:aws:s3:::mybucket/key"
        ));
        assert!(!evaluate_policy(
            &policies,
            "s3:PutObject",
            "arn:aws:s3:::mybucket/key"
        ));
    }
}
