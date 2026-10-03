//! CPU IDs are Linux logical CPU IDs, not indices into available_parallelism.
use crate::AppError;
use std::{collections::BTreeSet, fs};

/// Parse Linux cpulist syntax, e.g. `2-5,8,10-11`. Reject duplicates and
/// reversed/oversized ranges rather than silently pinning several shards alike.
pub fn parse_cpu_list(text: &str) -> Result<Vec<usize>, AppError> {
    let mut cpus = Vec::new();
    let mut seen = BTreeSet::new();
    if text.trim().is_empty() {
        return Err(AppError::Config("CPU list is empty".into()));
    }
    for part in text.trim().split(',') {
        let part = part.trim();
        let (start, end) = match part.split_once('-') {
            Some((a, b)) => (a.parse::<usize>(), b.parse::<usize>()),
            None => (part.parse::<usize>(), part.parse::<usize>()),
        };
        let start =
            start.map_err(|_| AppError::Config(format!("invalid CPU list item {part:?}")))?;
        let end = end.map_err(|_| AppError::Config(format!("invalid CPU list item {part:?}")))?;
        if start > end || end - start > 65_535 {
            return Err(AppError::Config(format!("invalid CPU range {part:?}")));
        }
        for cpu in start..=end {
            if !seen.insert(cpu) {
                return Err(AppError::Config(format!("duplicate CPU {cpu}")));
            }
            cpus.push(cpu);
        }
    }
    Ok(cpus)
}

/// Respect the calling thread's current affinity/cpuset. Do not assume CPU 0
/// is available inside a container or under taskset.
pub fn allowed_cpus() -> Result<Vec<usize>, AppError> {
    let status = fs::read_to_string("/proc/thread-self/status")
        .map_err(|e| AppError::System(format!("reading calling thread CPU affinity: {e}")))?;
    let cpus = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .ok_or_else(|| {
            AppError::System("Cpus_allowed_list missing from /proc/thread-self/status".into())
        })?;
    parse_cpu_list(cpus)
}

/// Select exactly `shards` distinct allowed CPUs. Explicit order defines shard
/// identity: the first CPU hosts shard 0. Auto mode chooses lowest allowed IDs;
/// it does NOT claim NUMA balancing, physical-core or P-core selection.
pub fn resolve_cpus(
    shards: usize,
    explicit: Option<&[usize]>,
    allowed: &[usize],
) -> Result<Vec<usize>, AppError> {
    if shards == 0 {
        return Err(AppError::Config("shards must be greater than zero".into()));
    }
    let cpus = explicit.map_or_else(
        || allowed.iter().copied().take(shards).collect(),
        |v| v.to_vec(),
    );
    if cpus.len() != shards {
        return Err(AppError::Config(format!(
            "need {shards} distinct CPUs; got {}",
            cpus.len()
        )));
    }
    let mut seen = BTreeSet::new();
    for &cpu in &cpus {
        if !allowed.contains(&cpu) {
            return Err(AppError::Config(format!(
                "CPU {cpu} is outside the calling thread's affinity"
            )));
        }
        if !seen.insert(cpu) {
            return Err(AppError::Config(format!("CPU {cpu} selected twice")));
        }
    }
    Ok(cpus)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_cpuset() {
        assert_eq!(parse_cpu_list("2-4,8").unwrap(), vec![2, 3, 4, 8]);
    }
    #[test]
    fn rejects_bad_lists() {
        for input in ["", "2-1", "1,1", "a", "1-2-3", "1,"] {
            assert!(parse_cpu_list(input).is_err(), "{input}");
        }
    }
    #[test]
    fn honors_sparse_affinity_and_explicit_order() {
        assert_eq!(resolve_cpus(2, None, &[4, 7, 9]).unwrap(), vec![4, 7]);
        assert_eq!(
            resolve_cpus(2, Some(&[9, 4]), &[4, 7, 9]).unwrap(),
            vec![9, 4]
        );
        assert!(resolve_cpus(2, Some(&[4, 4]), &[4, 7]).is_err());
        assert!(resolve_cpus(1, Some(&[0]), &[4, 7]).is_err());
        assert!(resolve_cpus(3, None, &[4, 7]).is_err());
    }
}
