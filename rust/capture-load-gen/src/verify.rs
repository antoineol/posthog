//! Compares the Postgres person graph against the personhog writer's temp
//! tables for a run's prefixed distinct ids. Each sweep keyset-pages the
//! cohort, comparing each page with one full outer join whose per-field diffs
//! are counted in the database, so memory and transfer stay flat at any cohort
//! size. The sweep polls until the graphs agree on a drained cohort or the
//! deadline passes, since the shadow leg trails by the writer's changelog lag.

use std::time::Duration;

use anyhow::{Context, Result};
use metrics::gauge;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Executor, PgPool, Row};

const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
/// Distinct ids compared per page.
const COHORT_CHUNK: i64 = 10_000;
/// So a wedged query cannot outlive the deadline.
const STATEMENT_TIMEOUT_MS: u64 = 30_000;
/// Fraction of the user pool a pass must cover.
const COVERAGE_FLOOR: f64 = 0.95;

pub struct VerifyConfig {
    pub database_url: String,
    pub team_id: i32,
    pub prefix: String,
    pub tmp_person_table: String,
    pub tmp_pdi_table: String,
    pub deadline: Duration,
    pub distinct_ids: u64,
}

/// Per-sweep tallies, summed across pages. Field diffs count only ids present
/// in both graphs; a one-sided id is missing_shadow or extra_shadow instead.
#[derive(Default)]
struct Counts {
    main: i64,
    cohort: i64,
    mismatched: i64,
    missing_shadow: i64,
    extra_shadow: i64,
    uuid: i64,
    properties: i64,
    is_identified: i64,
    created_at: i64,
}

impl Counts {
    fn add_page(&mut self, row: &PgRow) {
        self.main += row.get::<i64, _>("main_count");
        self.cohort += row.get::<i64, _>("cohort");
        self.mismatched += row.get::<i64, _>("mismatched");
        self.missing_shadow += row.get::<i64, _>("missing_shadow");
        self.extra_shadow += row.get::<i64, _>("extra_shadow");
        self.uuid += row.get::<i64, _>("uuid");
        self.properties += row.get::<i64, _>("properties");
        self.is_identified += row.get::<i64, _>("is_identified");
        self.created_at += row.get::<i64, _>("created_at");
    }
}

/// The prefix as a LIKE pattern, escaping metacharacters so a literal % or _
/// cannot widen the cohort.
fn like_pattern(prefix: &str) -> String {
    format!(
        "{}%",
        prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
    )
}

/// The exclusive upper bound of the btree range covering every id under
/// `prefix`: the prefix with its last character's code point incremented. A
/// range on the persons index is sargable where a `LIKE` prefix is not.
fn prefix_upper(prefix: &str) -> String {
    let mut chars: Vec<char> = prefix.chars().collect();
    if let Some(last) = chars.pop() {
        let mut next = last as u32 + 1;
        if (0xD800..=0xDFFF).contains(&next) {
            next = 0xE000; // skip the UTF-16 surrogate gap
        }
        if let Some(c) = char::from_u32(next) {
            chars.push(c);
        }
    }
    chars.into_iter().collect()
}

/// Smallest authoritative cohort a pass may cover.
fn coverage_floor(pool: u64) -> i64 {
    (pool as f64 * COVERAGE_FLOOR).ceil() as i64
}

/// Parity holds once the graphs agree on an authoritative cohort that has
/// stopped growing and reached the coverage floor.
fn is_pass(mismatched: i64, main_count: i64, prev_main: Option<i64>, floor: i64) -> bool {
    mismatched == 0 && prev_main == Some(main_count) && main_count >= floor
}

pub struct Verifier {
    pool: PgPool,
    team_id: i32,
    prefix_like: String,
    prefix_lo: String,
    prefix_hi: String,
    tmp_person_table: String,
    tmp_pdi_table: String,
}

impl Verifier {
    pub async fn connect(cfg: &VerifyConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    conn.execute(
                        format!("SET statement_timeout = {STATEMENT_TIMEOUT_MS}").as_str(),
                    )
                    .await?;
                    Ok(())
                })
            })
            .connect(&cfg.database_url)
            .await
            .context("connecting to the persons database")?;
        Ok(Self {
            pool,
            team_id: cfg.team_id,
            prefix_like: like_pattern(&cfg.prefix),
            prefix_lo: cfg.prefix.clone(),
            prefix_hi: prefix_upper(&cfg.prefix),
            tmp_person_table: cfg.tmp_person_table.clone(),
            tmp_pdi_table: cfg.tmp_pdi_table.clone(),
        })
    }

    /// One page of the comparison: the next [`COHORT_CHUNK`] cohort ids after
    /// `$5`, joined graph to graph and counted by difference kind. The `[$3,$4)`
    /// range keeps every leg an index scan (a prefix `LIKE` is not sargable);
    /// `LIKE $2` holds the cohort exact. Binds $1 team, $2 like, $3 prefix,
    /// $4 prefix upper, $5 cursor, $6 page size.
    fn page_sql(&self) -> String {
        let ids = |pdi: &str| {
            format!(
                "SELECT distinct_id FROM {pdi}
                  WHERE team_id = $1 AND distinct_id >= $3 AND distinct_id < $4
                    AND distinct_id LIKE $2 AND distinct_id > $5 AND is_deleted = false"
            )
        };
        let leg = |pdi: &str, person: &str| {
            format!(
                "SELECT d.distinct_id, p.uuid, p.properties, p.is_identified,
                        (extract(epoch from p.created_at) * 1000)::bigint AS created_at_ms
                   FROM {pdi} d
                   JOIN {person} p ON p.id = d.person_id AND p.team_id = d.team_id
                  WHERE d.team_id = $1 AND d.distinct_id IN (SELECT distinct_id FROM page)
                    AND d.is_deleted = false AND p.is_deleted = false"
            )
        };
        format!(
            "WITH page AS (
                SELECT distinct_id FROM (({main_ids}) UNION ({shadow_ids})) u
                ORDER BY distinct_id LIMIT $6
             ), main AS ({main}), shadow AS ({shadow}), j AS (
                SELECT
                    m.distinct_id IS NULL AS extra_shadow,
                    s.distinct_id IS NULL AS missing_shadow,
                    m.uuid          IS DISTINCT FROM s.uuid          AS uuid_diff,
                    m.properties    IS DISTINCT FROM s.properties    AS properties_diff,
                    m.is_identified IS DISTINCT FROM s.is_identified AS is_identified_diff,
                    m.created_at_ms IS DISTINCT FROM s.created_at_ms AS created_at_diff,
                    coalesce(m.distinct_id, s.distinct_id) AS distinct_id
                FROM main m FULL OUTER JOIN shadow s ON m.distinct_id = s.distinct_id
             )
             SELECT
                count(*) FILTER (WHERE NOT extra_shadow) AS main_count,
                count(*) AS cohort,
                count(*) FILTER (WHERE extra_shadow OR missing_shadow OR uuid_diff
                    OR properties_diff OR is_identified_diff OR created_at_diff) AS mismatched,
                count(*) FILTER (WHERE missing_shadow) AS missing_shadow,
                count(*) FILTER (WHERE extra_shadow) AS extra_shadow,
                count(*) FILTER (WHERE uuid_diff AND NOT missing_shadow AND NOT extra_shadow) AS uuid,
                count(*) FILTER (WHERE properties_diff AND NOT missing_shadow AND NOT extra_shadow) AS properties,
                count(*) FILTER (WHERE is_identified_diff AND NOT missing_shadow AND NOT extra_shadow) AS is_identified,
                count(*) FILTER (WHERE created_at_diff AND NOT missing_shadow AND NOT extra_shadow) AS created_at,
                max(distinct_id) AS last_id
             FROM j",
            main_ids = ids("posthog_persondistinctid"),
            shadow_ids = ids(&self.tmp_pdi_table),
            main = leg("posthog_persondistinctid", "posthog_person"),
            shadow = leg(&self.tmp_pdi_table, &self.tmp_person_table),
        )
    }

    /// One full comparison, paged over the cohort in keyset order.
    async fn sweep(&self) -> Result<Counts> {
        let sql = self.page_sql();
        let mut totals = Counts::default();
        let mut cursor = String::new();
        loop {
            let row = sqlx::query(&sql)
                .bind(self.team_id)
                .bind(&self.prefix_like)
                .bind(&self.prefix_lo)
                .bind(&self.prefix_hi)
                .bind(&cursor)
                .bind(COHORT_CHUNK)
                .fetch_one(&self.pool)
                .await
                .context("comparing a cohort page")?;
            let page_cohort: i64 = row.get("cohort");
            totals.add_page(&row);
            if page_cohort < COHORT_CHUNK {
                break;
            }
            cursor = row.get::<Option<String>, _>("last_id").unwrap_or_default();
        }
        Ok(totals)
    }

    fn export_gauges(&self, counts: &Counts) {
        gauge!("capture_loadgen_parity_cohort_size").set(counts.cohort as f64);
        gauge!("capture_loadgen_parity_authoritative_cohort").set(counts.main as f64);
        gauge!("capture_loadgen_parity_mismatched").set(counts.mismatched as f64);
        gauge!("capture_loadgen_parity_clean").set(if counts.mismatched == 0 { 1.0 } else { 0.0 });
        gauge!("capture_loadgen_parity_last_sweep_timestamp_seconds")
            .set(common_metrics::get_current_timestamp_seconds());
    }

    /// Polls until the graphs agree on a stable, drained cohort or the deadline
    /// passes. A transient query error retries; only a divergence that outlasts
    /// the deadline fails.
    pub async fn run(&self, cfg: &VerifyConfig) -> Result<bool> {
        let floor = coverage_floor(cfg.distinct_ids);
        let deadline = tokio::time::Instant::now() + cfg.deadline;
        let mut prev_main: Option<i64> = None;
        loop {
            let counts = match self.sweep().await {
                Ok(counts) => counts,
                Err(error) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(error).context("sweep failed at the deadline");
                    }
                    tracing::warn!(error = format!("{error:#}"), "sweep failed; retrying");
                    tokio::time::sleep(SWEEP_INTERVAL).await;
                    continue;
                }
            };
            self.export_gauges(&counts);
            tracing::info!(
                cohort = counts.cohort,
                main_count = counts.main,
                mismatched = counts.mismatched,
                "sweep complete"
            );
            if is_pass(counts.mismatched, counts.main, prev_main, floor) {
                tracing::info!(
                    cohort = counts.cohort,
                    "shadow graphs agree on a stable, drained cohort"
                );
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                report_failure(cfg, &counts, floor);
                return Ok(false);
            }
            prev_main = Some(counts.main);
            tokio::time::sleep(SWEEP_INTERVAL).await;
        }
    }
}

fn report_failure(cfg: &VerifyConfig, counts: &Counts, floor: i64) {
    let deadline_secs = cfg.deadline.as_secs();
    if counts.mismatched == 0 {
        tracing::warn!(
            cohort = counts.cohort,
            main_count = counts.main,
            floor,
            deadline_secs,
            "verification expired without a divergence: the authoritative cohort never \
             stabilized at or above the coverage floor; ingestion is likely still draining"
        );
        return;
    }
    tracing::warn!(
        cohort = counts.cohort,
        mismatched = counts.mismatched,
        missing_shadow = counts.missing_shadow,
        extra_shadow = counts.extra_shadow,
        uuid = counts.uuid,
        properties = counts.properties,
        is_identified = counts.is_identified,
        created_at = counts.created_at,
        deadline_secs,
        "shadow parity verification failed; on a run with merge contention, check the \
         merge-drop counters before reading mismatches as backend divergence"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_pattern_escapes_metacharacters_instead_of_replacing_them() {
        assert_eq!(like_pattern("loadgen-"), "loadgen-%");
        assert_eq!(like_pattern("run_7%"), "run\\_7\\%%");
        assert_eq!(like_pattern("a\\b"), "a\\\\b%");
    }

    #[test]
    fn prefix_upper_bounds_every_id_under_the_prefix() {
        assert_eq!(prefix_upper("loadgen-"), "loadgen.");
        assert_eq!(prefix_upper("loadgen-abc-"), "loadgen-abc.");
        assert!("loadgen-abc-user-9" < prefix_upper("loadgen-abc-").as_str());
        assert!("loadgen-abc-user-9" >= "loadgen-abc-");
    }

    #[test]
    fn coverage_floor_is_95_percent_of_the_pool() {
        assert_eq!(coverage_floor(10_000), 9_500);
        assert_eq!(coverage_floor(1), 1);
        assert_eq!(coverage_floor(0), 0);
    }

    #[test]
    fn a_pass_needs_no_mismatch_a_stable_cohort_and_the_floor() {
        assert!(is_pass(0, 100, Some(100), 100));
        assert!(!is_pass(1, 100, Some(100), 100));
        assert!(!is_pass(0, 100, None, 100));
        assert!(!is_pass(0, 100, Some(90), 100));
        assert!(!is_pass(0, 99, Some(99), 100));
    }
}
