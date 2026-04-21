use std::time::Duration;

const INTERACTIVE_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const INTERACTIVE_HARD_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const DAEMON_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const DAEMON_HARD_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
const MIN_DERIVED_IDLE_TIMEOUT: Duration = Duration::from_secs(2 * 60);
const MAX_DERIVED_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeTurnTimeouts {
    pub idle: Duration,
    pub hard: Duration,
}

impl RuntimeTurnTimeouts {
    pub fn interactive() -> Self {
        Self::new(INTERACTIVE_IDLE_TIMEOUT, INTERACTIVE_HARD_TIMEOUT)
    }

    pub fn daemon() -> Self {
        Self::new(DAEMON_IDLE_TIMEOUT, DAEMON_HARD_TIMEOUT)
    }

    pub fn with_hard_timeout(hard_timeout: Duration) -> Self {
        let hard_timeout = normalize_nonzero(hard_timeout);
        let derived_idle = hard_timeout
            .checked_div(12)
            .unwrap_or(hard_timeout)
            .clamp(MIN_DERIVED_IDLE_TIMEOUT, MAX_DERIVED_IDLE_TIMEOUT)
            .min(hard_timeout);
        Self::new(derived_idle, hard_timeout)
    }

    pub fn from_millis(idle_timeout_ms: u64, hard_timeout_ms: u64) -> Self {
        Self::new(
            Duration::from_millis(idle_timeout_ms),
            Duration::from_millis(hard_timeout_ms),
        )
    }

    pub fn new(idle_timeout: Duration, hard_timeout: Duration) -> Self {
        let idle = normalize_nonzero(idle_timeout);
        let hard = normalize_nonzero(hard_timeout).max(idle);
        Self { idle, hard }
    }
}

pub fn parse_duration(raw: &str) -> Result<Duration, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("duration cannot be empty".to_string());
    }

    let split_at = raw
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(raw.len());
    let (digits, suffix) = raw.split_at(split_at);
    if digits.is_empty() {
        return Err(format!("invalid duration '{raw}'"));
    }

    let value = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid duration '{raw}'"))?;
    if value == 0 {
        return Err("duration must be greater than zero".to_string());
    }

    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1_000,
        "ms" | "millisecond" | "milliseconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000,
        other => return Err(format!("unsupported duration unit '{other}' in '{raw}'")),
    };

    value
        .checked_mul(multiplier)
        .map(Duration::from_millis)
        .ok_or_else(|| format!("duration '{raw}' is too large"))
}

pub fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis.is_multiple_of(3_600_000) {
        return format!("{}h", millis / 3_600_000);
    }
    if millis.is_multiple_of(60_000) {
        return format!("{}m", millis / 60_000);
    }
    if millis.is_multiple_of(1_000) {
        return format!("{}s", millis / 1_000);
    }
    format!("{millis}ms")
}

fn normalize_nonzero(duration: Duration) -> Duration {
    if duration.is_zero() {
        Duration::from_millis(1)
    } else {
        duration
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_idle_timeout_from_hard_timeout() {
        assert_eq!(
            RuntimeTurnTimeouts::with_hard_timeout(Duration::from_secs(2 * 60 * 60)),
            RuntimeTurnTimeouts::new(
                Duration::from_secs(10 * 60),
                Duration::from_secs(2 * 60 * 60)
            )
        );
        assert_eq!(
            RuntimeTurnTimeouts::with_hard_timeout(Duration::from_secs(60)),
            RuntimeTurnTimeouts::new(Duration::from_secs(60), Duration::from_secs(60))
        );
        assert_eq!(
            RuntimeTurnTimeouts::with_hard_timeout(Duration::from_secs(30 * 60)),
            RuntimeTurnTimeouts::new(Duration::from_secs(150), Duration::from_secs(30 * 60))
        );
    }

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("5m"), Ok(Duration::from_secs(5 * 60)));
        assert_eq!(parse_duration("2h"), Ok(Duration::from_secs(2 * 60 * 60)));
    }

    #[test]
    fn formats_common_durations() {
        assert_eq!(format_duration(Duration::from_millis(750)), "750ms");
        assert_eq!(format_duration(Duration::from_secs(30)), "30s");
        assert_eq!(format_duration(Duration::from_secs(5 * 60)), "5m");
        assert_eq!(format_duration(Duration::from_secs(2 * 60 * 60)), "2h");
    }
}
