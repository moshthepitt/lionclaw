//! Closed integer intervals `[lo, hi]` and their overlap/merge logic.
//!
//! There is a real off-by-one bug in `overlaps`: two intervals that merely
//! touch at an endpoint (e.g. `[1,3]` and `[3,5]`) DO overlap on closed
//! intervals, but the current comparison uses `<` where it should use `<=`.
//! `merge_all` inherits the bug and fails to coalesce touching intervals.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub lo: i64,
    pub hi: i64,
}

impl Interval {
    pub fn new(lo: i64, hi: i64) -> Self {
        assert!(lo <= hi, "interval lo must be <= hi");
        Self { lo, hi }
    }
}

/// True when the two closed intervals share at least one integer point.
pub fn overlaps(a: Interval, b: Interval) -> bool {
    // BUG: closed intervals overlap when a.lo <= b.hi && b.lo <= a.hi.
    a.lo < b.hi && b.lo < a.hi
}

/// Merge a set of intervals into the minimal set of disjoint intervals.
/// Touching intervals should coalesce (`[1,3] + [3,5] = [1,5]`).
pub fn merge_all(mut intervals: Vec<Interval>) -> Vec<Interval> {
    if intervals.is_empty() {
        return Vec::new();
    }
    intervals.sort_by_key(|i| (i.lo, i.hi));
    let mut merged = vec![intervals[0]];
    for current in intervals.into_iter().skip(1) {
        let last = merged.last_mut().expect("non-empty");
        if overlaps(*last, current) {
            last.hi = last.hi.max(current.hi);
        } else {
            merged.push(current);
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clearly_overlapping_intervals_overlap() {
        assert!(overlaps(Interval::new(1, 5), Interval::new(3, 8)));
    }

    #[test]
    fn disjoint_intervals_do_not_overlap() {
        assert!(!overlaps(Interval::new(1, 3), Interval::new(5, 8)));
    }

    #[test]
    fn touching_intervals_overlap() {
        // Closed intervals [1,3] and [3,5] share the point 3.
        assert!(overlaps(Interval::new(1, 3), Interval::new(3, 5)));
    }

    #[test]
    fn merge_coalesces_touching_intervals() {
        let merged = merge_all(vec![Interval::new(1, 3), Interval::new(3, 5)]);
        assert_eq!(merged, vec![Interval::new(1, 5)]);
    }

    #[test]
    fn merge_keeps_disjoint_intervals_separate() {
        let merged = merge_all(vec![Interval::new(1, 3), Interval::new(5, 8)]);
        assert_eq!(merged, vec![Interval::new(1, 3), Interval::new(5, 8)]);
    }
}
