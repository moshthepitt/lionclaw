use crate::kernel::skills::SkillRecord;

#[derive(Debug, Default)]
pub struct SkillSelector;

impl SkillSelector {
    pub fn new() -> Self {
        Self
    }

    pub fn select(&self, user_text: &str, enabled_skills: &[SkillRecord]) -> Vec<String> {
        let lowered = user_text.to_lowercase();
        let tokens = lowered
            .split_whitespace()
            .map(|token| token.trim_matches(|c: char| !c.is_ascii_alphanumeric()))
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>();

        let mut scored = enabled_skills
            .iter()
            .map(|skill| {
                let name = skill.name.to_lowercase();
                let description = skill.description.to_lowercase();

                let mut score = 0i32;
                for token in &tokens {
                    if name.contains(token) {
                        score += 4;
                    }
                    if description.contains(token) {
                        score += 2;
                    }
                }

                if lowered.contains(&name) {
                    score += 6;
                }

                (skill.skill_id.clone(), score)
            })
            .filter(|(_, score)| *score > 0)
            .collect::<Vec<_>>();

        scored.sort_by(|a, b| b.1.cmp(&a.1));
        scored.into_iter().take(3).map(|(id, _)| id).collect()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::SkillSelector;
    use crate::kernel::skills::SkillRecord;

    #[test]
    fn selects_skill_by_keyword_overlap() {
        let selector = SkillSelector::new();
        let skills = vec![SkillRecord {
            skill_id: "email-123".to_string(),
            name: "email-helper".to_string(),
            description: "Draft and refine email replies".to_string(),
            source: "local".to_string(),
            reference: None,
            hash: "hash".to_string(),
            snapshot_path: None,
            skill_md: None,
            enabled: true,
            installed_at: Utc::now(),
        }];

        let selected = selector.select("help me draft this email", &skills);
        assert_eq!(selected, vec!["email-123".to_string()]);
    }
}
