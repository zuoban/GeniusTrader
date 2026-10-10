use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Mutex;

const REQUEST_SPACING: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(3 * 24 * 60 * 60);
const RESET_AFTER: Duration = Duration::from_secs(30 * 60);
const WEIGHT_WINDOW: Duration = Duration::from_secs(60);
const DEFAULT_WEIGHT_LIMIT: u64 = 2400;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestCost {
    pub weight: u64,
    pub protected: bool,
}
/// USD-M REST weights, verified against Binance's endpoint documentation.
/// New orders use the separate account order quota; the IP weight is zero.
pub(crate) fn request_cost(
    method: &Method,
    path: &str,
    params: &[(String, String)],
) -> RequestCost {
    let parameter = |name: &str| {
        params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    let single = parameter("symbol").is_some_and(|symbol| !symbol.is_empty());
    let weight = match (method.as_str(), path) {
        ("GET", "/fapi/v1/time" | "/fapi/v1/exchangeInfo") => 1,
        ("GET", "/fapi/v1/ticker/24hr" | "/fapi/v1/openOrders") => {
            if single {
                1
            } else {
                40
            }
        }
        ("GET", "/fapi/v1/ticker/bookTicker") => {
            if single {
                2
            } else {
                5
            }
        }
        ("GET", "/fapi/v1/assetIndex") => {
            if single {
                1
            } else {
                10
            }
        }
        ("GET", "/fapi/v1/klines") => match parameter("limit").map(str::parse::<u64>) {
            None => 5, // Binance's default limit is 500.
            Some(Ok(1..=99)) => 1,
            Some(Ok(100..=499)) => 2,
            Some(Ok(500..=1000)) => 5,
            _ => 10,
        },
        (
            "GET",
            "/fapi/v2/account"
            | "/fapi/v3/balance"
            | "/fapi/v3/positionRisk"
            | "/fapi/v1/userTrades",
        ) => 5,
        ("GET", "/fapi/v1/positionSide/dual" | "/fapi/v1/income") => 30,
        ("GET", "/fapi/v1/commissionRate") => 20,
        ("POST", "/fapi/v1/order") => 0,
        ("GET" | "DELETE", "/fapi/v1/order")
        | ("POST", "/fapi/v1/leverage" | "/fapi/v1/positionSide/dual") => 1,
        _ => 40, // Conservative fallback for any future, unmapped endpoint.
    };
    let protected = matches!(
        (method.as_str(), path),
        (
            "GET",
            "/fapi/v1/time"
                | "/fapi/v2/account"
                | "/fapi/v3/balance"
                | "/fapi/v3/positionRisk"
                | "/fapi/v1/openOrders"
                | "/fapi/v1/order"
                | "/fapi/v1/userTrades"
        ) | ("DELETE", "/fapi/v1/order")
    ) || (method == Method::POST
        && path == "/fapi/v1/order"
        && parameter("reduceOnly") == Some("true"));
    RequestCost { weight, protected }
}

#[derive(Debug, Clone, Serialize)]
pub struct RateLimitSnapshot {
    pub weight_limit: u64,
    pub regular_weight_limit: u64,
    pub estimated_used_weight: u64,
    pub remaining_weight: u64,
    pub regular_remaining_weight: u64,
    pub server_reported_used_weight: Option<u64>,
    pub next_weight_release_in_ms: u64,
    pub cooldown_remaining_ms: u64,
    pub last_limit_status: Option<u16>,
    pub last_limit_code: Option<i64>,
    pub admitted_requests: u64,
    pub budget_rejections: u64,
    pub cooldown_rejections: u64,
    pub rate_limit_responses: u64,
}

struct Charge {
    at: Instant,
    weight: u64,
    cumulative: u64,
}
struct WeightReport {
    at: Instant,
    used: u64,
    cumulative_at_start: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AdmissionError {
    Cooldown(Cooldown),
    Budget(Duration),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Cooldown {
    pub status: u16,
    pub code: i64,
    pub remaining: Duration,
}
struct Block {
    until: Instant,
    status: u16,
    code: i64,
}
pub(crate) struct RequestThrottle {
    pub(crate) dispatch: Arc<Mutex<()>>,
    next: Instant,
    blocked: Option<Block>,
    last_limit: Option<Instant>,
    strikes: u32,
    weight_limit: u64,
    charges: VecDeque<Charge>,
    reports: VecDeque<WeightReport>,
    cumulative_weight: u64,
    admitted_requests: u64,
    budget_rejections: u64,
    cooldown_rejections: u64,
    rate_limit_responses: u64,
}
impl RequestThrottle {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            dispatch: Arc::new(Mutex::new(())),
            next: now,
            blocked: None,
            last_limit: None,
            strikes: 0,
            weight_limit: DEFAULT_WEIGHT_LIMIT,
            charges: VecDeque::new(),
            reports: VecDeque::new(),
            cumulative_weight: 0,
            admitted_requests: 0,
            budget_rejections: 0,
            cooldown_rejections: 0,
            rate_limit_responses: 0,
        }
    }
    /// None grants a slot; Some asks the caller to sleep without holding the mutex.
    pub(crate) fn reserve(
        &mut self,
        now: Instant,
        cost: RequestCost,
    ) -> Result<Option<Duration>, AdmissionError> {
        self.prune(now);
        if let Some(block) = &self.blocked
            && now < block.until
        {
            self.cooldown_rejections = self.cooldown_rejections.saturating_add(1);
            return Err(AdmissionError::Cooldown(Cooldown {
                status: block.status,
                code: block.code,
                remaining: block.until - now,
            }));
        }
        let cap = if cost.protected {
            self.weight_limit
        } else {
            self.regular_limit()
        };
        if (cost.weight > 0 || !cost.protected)
            && self.used_at(now).saturating_add(cost.weight) > cap
        {
            self.budget_rejections = self.budget_rejections.saturating_add(1);
            return Err(AdmissionError::Budget(self.budget_wait(
                now,
                cost.weight,
                cap,
            )));
        }
        let wait = self.next.saturating_duration_since(now);
        if !wait.is_zero() {
            return Ok(Some(wait));
        }
        self.next = now + REQUEST_SPACING;
        self.cumulative_weight = self.cumulative_weight.saturating_add(cost.weight);
        self.charges.push_back(Charge {
            at: now,
            weight: cost.weight,
            cumulative: self.cumulative_weight,
        });
        self.admitted_requests = self.admitted_requests.saturating_add(1);
        Ok(None)
    }
    fn regular_limit(&self) -> u64 {
        self.weight_limit - self.weight_limit.div_ceil(10)
    }
    fn prune(&mut self, now: Instant) {
        while self
            .charges
            .front()
            .is_some_and(|entry| now.saturating_duration_since(entry.at) >= WEIGHT_WINDOW)
        {
            self.charges.pop_front();
        }
        while self
            .reports
            .front()
            .is_some_and(|entry| now.saturating_duration_since(entry.at) >= WEIGHT_WINDOW)
        {
            self.reports.pop_front();
        }
    }
    fn used_at(&self, now: Instant) -> u64 {
        let local = self
            .charges
            .iter()
            .filter(|entry| now.saturating_duration_since(entry.at) < WEIGHT_WINDOW)
            .fold(0u64, |sum, entry| sum.saturating_add(entry.weight));
        self.reports
            .iter()
            .filter(|entry| now.saturating_duration_since(entry.at) < WEIGHT_WINDOW)
            .map(|entry| {
                entry.used.saturating_add(
                    self.cumulative_weight
                        .saturating_sub(entry.cumulative_at_start),
                )
            })
            .fold(local, u64::max)
    }
    fn releases(&self) -> impl Iterator<Item = Instant> + '_ {
        self.charges
            .iter()
            .filter(|entry| entry.weight > 0)
            .map(|entry| entry.at + WEIGHT_WINDOW)
            .chain(self.reports.iter().map(|entry| entry.at + WEIGHT_WINDOW))
    }
    fn budget_wait(&self, now: Instant, weight: u64, cap: u64) -> Duration {
        let Some(available) = cap.checked_sub(weight) else {
            return WEIGHT_WINDOW;
        };
        let mut local = self
            .charges
            .iter()
            .fold(0u64, |sum, entry| sum.saturating_add(entry.weight));
        let mut ready = now;
        for entry in &self.charges {
            if local <= available {
                break;
            }
            local = local.saturating_sub(entry.weight);
            ready = entry.at + WEIGHT_WINDOW;
        }
        // Every report whose upper bound exceeds the cap must expire. This is a
        // linear scan, so a depleted budget does not make admission quadratic.
        for report in &self.reports {
            if report.used.saturating_add(
                self.cumulative_weight
                    .saturating_sub(report.cumulative_at_start),
            ) > available
            {
                ready = ready.max(report.at + WEIGHT_WINDOW);
            }
        }
        ready.saturating_duration_since(now)
    }
    pub(crate) fn observe_weight(&mut self, started: Instant, now: Instant, used: u64) {
        self.prune(now);
        // Keep local in-flight charges after this request's admission. A later or
        // lower response cannot erase reservations or an older high usage report.
        let cumulative_at_start = self
            .charges
            .iter()
            .find(|entry| entry.at == started)
            .map(|entry| entry.cumulative)
            .unwrap_or(self.cumulative_weight);
        self.reports.push_back(WeightReport {
            at: now,
            used,
            cumulative_at_start,
        });
    }
    pub(crate) fn configure_weight_limit(&mut self, info: &Value) {
        // Only the one-minute IP quota corresponds to the header tracked here.
        // Missing/malformed rules keep the existing conservative fallback.
        if let Some(limit) = info["rateLimits"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|rule| {
                rule["rateLimitType"] == "REQUEST_WEIGHT"
                    && rule["interval"] == "MINUTE"
                    && rule["intervalNum"] == 1
            })
            .filter_map(|rule| rule["limit"].as_u64().filter(|&limit| limit > 0))
            .min()
        {
            self.weight_limit = limit;
        }
    }
    pub(crate) fn snapshot(&mut self, now: Instant) -> RateLimitSnapshot {
        self.prune(now);
        let used = self.used_at(now);
        RateLimitSnapshot {
            weight_limit: self.weight_limit,
            regular_weight_limit: self.regular_limit(),
            estimated_used_weight: used,
            remaining_weight: self.weight_limit.saturating_sub(used),
            regular_remaining_weight: self.regular_limit().saturating_sub(used),
            server_reported_used_weight: self.reports.back().map(|report| report.used),
            next_weight_release_in_ms: self
                .releases()
                .min()
                .map(|release| release.saturating_duration_since(now).as_millis() as u64)
                .unwrap_or(0),
            cooldown_remaining_ms: self
                .blocked
                .as_ref()
                .map(|block| block.until.saturating_duration_since(now).as_millis() as u64)
                .unwrap_or(0),
            last_limit_status: self.blocked.as_ref().map(|block| block.status),
            last_limit_code: self.blocked.as_ref().map(|block| block.code),
            admitted_requests: self.admitted_requests,
            budget_rejections: self.budget_rejections,
            cooldown_rejections: self.cooldown_rejections,
            rate_limit_responses: self.rate_limit_responses,
        }
    }
    pub(crate) fn penalize(
        &mut self,
        started: Instant,
        now: Instant,
        status: u16,
        code: i64,
        retry_after: Option<Duration>,
    ) -> Duration {
        self.rate_limit_responses = self.rate_limit_responses.saturating_add(1);
        if self
            .blocked
            .as_ref()
            .is_some_and(|block| now.saturating_duration_since(block.until) >= RESET_AFTER)
        {
            self.last_limit = None;
            self.strikes = 0;
        }
        // Several requests already in flight can report the same limit. Escalate only
        // when a request was dispatched after the preceding limit was observed.
        if self.last_limit.is_none_or(|last| started >= last) {
            self.strikes = self.strikes.saturating_add(1);
        }
        self.last_limit = Some(now);
        let base = if status == 418 { 120u64 } else { 60 };
        let cap = if status == 418 {
            MAX_BACKOFF.as_secs()
        } else {
            15 * 60
        };
        let fallback = Duration::from_secs(
            base.saturating_mul(1u64 << self.strikes.saturating_sub(1).min(16))
                .min(cap),
        );
        let delay = retry_after.unwrap_or(fallback).max(Duration::from_secs(1));
        let until = (now + delay).max(
            self.blocked
                .as_ref()
                .map(|block| block.until)
                .unwrap_or(now),
        );
        self.blocked = Some(Block {
            until,
            status,
            code,
        });
        until - now
    }
}

/// Keep the gate alive across client replacement and credential checks on the same host.
pub(crate) fn shared_throttle(origin: &str) -> Arc<Mutex<RequestThrottle>> {
    static GATES: OnceLock<StdMutex<BTreeMap<String, Arc<Mutex<RequestThrottle>>>>> =
        OnceLock::new();
    GATES
        .get_or_init(|| StdMutex::new(BTreeMap::new()))
        .lock()
        .unwrap()
        .entry(origin.to_owned())
        .or_insert_with(|| Arc::new(Mutex::new(RequestThrottle::new(Instant::now()))))
        .clone()
}

pub(crate) fn retry_after(value: Option<&str>, now: SystemTime) -> Option<Duration> {
    let value = value?.trim();
    let delay = if let Ok(seconds) = value.parse::<u64>() {
        Duration::from_secs(seconds)
    } else {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(now)
            .unwrap_or_default()
    };
    (delay <= MAX_BACKOFF).then_some(delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cost(weight: u64, protected: bool) -> RequestCost {
        RequestCost { weight, protected }
    }
    #[test]
    fn weights_follow_symbol_and_kline_limit_boundaries() {
        for (path, all, single) in [
            ("/fapi/v1/ticker/24hr", 40, 1),
            ("/fapi/v1/openOrders", 40, 1),
            ("/fapi/v1/ticker/bookTicker", 5, 2),
            ("/fapi/v1/assetIndex", 10, 1),
        ] {
            assert_eq!(request_cost(&Method::GET, path, &[]).weight, all);
            assert_eq!(
                request_cost(&Method::GET, path, &[("symbol".into(), "BTCUSDT".into())]).weight,
                single
            );
            assert_eq!(
                request_cost(&Method::GET, path, &[("symbol".into(), "".into())]).weight,
                all
            );
        }
        for (limit, weight) in [
            ("1", 1),
            ("99", 1),
            ("100", 2),
            ("499", 2),
            ("500", 5),
            ("1000", 5),
            ("1001", 10),
            ("invalid", 10),
            ("0", 10),
        ] {
            assert_eq!(
                request_cost(
                    &Method::GET,
                    "/fapi/v1/klines",
                    &[("limit".into(), limit.into())]
                )
                .weight,
                weight
            );
        }
        assert_eq!(request_cost(&Method::GET, "/fapi/v1/klines", &[]).weight, 5);
        assert_eq!(
            request_cost(&Method::GET, "/fapi/v1/commissionRate", &[]).weight,
            20
        );
        assert_eq!(
            request_cost(&Method::GET, "/fapi/v1/income", &[]).weight,
            30
        );
        assert_eq!(
            request_cost(&Method::GET, "/fapi/v1/positionSide/dual", &[]).weight,
            30
        );
        assert_eq!(
            request_cost(&Method::POST, "/fapi/v1/positionSide/dual", &[]).weight,
            1
        );
        assert_eq!(
            request_cost(&Method::GET, "/future/unmapped", &[]).weight,
            40
        );
        assert!(request_cost(&Method::GET, "/fapi/v1/order", &[]).protected);
        assert!(request_cost(&Method::DELETE, "/fapi/v1/order", &[]).protected);
        let entry = request_cost(&Method::POST, "/fapi/v1/order", &[]);
        assert_eq!(entry.weight, 0);
        assert!(!entry.protected);
        assert!(
            request_cost(
                &Method::POST,
                "/fapi/v1/order",
                &[("reduceOnly".into(), "true".into())]
            )
            .protected
        );
    }
    #[test]
    fn rolling_budget_preserves_headroom_and_never_charges_rejected_or_waiting_requests() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        gate.weight_limit = 100;
        assert!(gate.reserve(now, cost(80, false)).unwrap().is_none());
        assert_eq!(
            gate.reserve(now, cost(10, false)).unwrap(),
            Some(REQUEST_SPACING)
        );
        let second = now + REQUEST_SPACING;
        assert!(gate.reserve(second, cost(10, false)).unwrap().is_none());
        let third = second + REQUEST_SPACING;
        let AdmissionError::Budget(wait) = gate.reserve(third, cost(1, false)).unwrap_err() else {
            panic!("expected weight budget");
        };
        assert_eq!(wait, WEIGHT_WINDOW - 2 * REQUEST_SPACING);
        assert!(gate.reserve(third, cost(10, true)).unwrap().is_none());
        assert!(matches!(
            gate.reserve(third + REQUEST_SPACING, cost(1, true)),
            Err(AdmissionError::Budget(_))
        ));
        // Zero-IP-weight reduce-only orders still work when the IP budget is full.
        assert!(
            gate.reserve(third + REQUEST_SPACING, cost(0, true))
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            gate.reserve(third + 2 * REQUEST_SPACING, cost(0, false)),
            Err(AdmissionError::Budget(_))
        ));
        let snapshot = gate.snapshot(third + 2 * REQUEST_SPACING);
        assert_eq!(snapshot.estimated_used_weight, 100);
        assert_eq!(snapshot.regular_remaining_weight, 0);
        assert_eq!(snapshot.remaining_weight, 0);
        assert_eq!(snapshot.admitted_requests, 4);
        assert_eq!(snapshot.budget_rejections, 3);
        assert!(
            gate.reserve(now + WEIGHT_WINDOW, cost(70, false))
                .unwrap()
                .is_none()
        );
        assert_eq!(gate.snapshot(now + WEIGHT_WINDOW).estimated_used_weight, 90);
        assert_eq!(
            gate.snapshot(now + 2 * WEIGHT_WINDOW).estimated_used_weight,
            0
        );
        assert!(gate.charges.is_empty());
    }
    #[test]
    fn lower_or_out_of_order_headers_do_not_erase_in_flight_weight_and_expire() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        gate.weight_limit = 100;
        gate.reserve(now, cost(5, false)).unwrap();
        let second = now + REQUEST_SPACING;
        gate.reserve(second, cost(10, false)).unwrap();
        let observed = now + 2 * REQUEST_SPACING;
        gate.observe_weight(now, observed, 80);
        let current = observed + REQUEST_SPACING;
        gate.observe_weight(second, current, 1);
        assert_eq!(gate.snapshot(current).estimated_used_weight, 90);
        assert!(matches!(
            gate.reserve(current, cost(1, false)),
            Err(AdmissionError::Budget(_))
        ));
        gate.reserve(current, cost(10, true)).unwrap();
        assert_eq!(gate.snapshot(current).estimated_used_weight, 100);
        let AdmissionError::Budget(wait) = gate.reserve(current, cost(1, true)).unwrap_err() else {
            panic!("expected weight budget");
        };
        assert_eq!(wait, WEIGHT_WINDOW - REQUEST_SPACING);
        let expired = observed + WEIGHT_WINDOW + REQUEST_SPACING;
        let snapshot = gate.snapshot(expired);
        assert_eq!(snapshot.estimated_used_weight, 0);
        assert_eq!(snapshot.server_reported_used_weight, None);
        assert_eq!(snapshot.remaining_weight, 100);
        assert!(gate.reports.is_empty());
    }
    #[test]
    fn dynamic_quota_changes_preserve_usage_and_ignore_unrelated_or_invalid_rules() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        gate.reserve(now, cost(90, false)).unwrap();
        gate.configure_weight_limit(&serde_json::json!({"rateLimits":[
            {"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":2,"limit":1},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":100},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":80}
        ]}));
        gate.configure_weight_limit(&serde_json::json!({"rateLimits":[
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":0},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":"2400"}
        ]}));
        gate.configure_weight_limit(&serde_json::json!({}));
        let snapshot = gate.snapshot(now);
        assert_eq!(snapshot.weight_limit, 80);
        assert_eq!(snapshot.regular_weight_limit, 72);
        assert_eq!(snapshot.estimated_used_weight, 90);
        assert_eq!(snapshot.remaining_weight, 0);
        assert!(matches!(
            gate.reserve(now + REQUEST_SPACING, cost(1, true)),
            Err(AdmissionError::Budget(_))
        ));
        assert!(
            gate.reserve(now + REQUEST_SPACING, cost(0, true))
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn monitoring_retains_limit_reason_after_cooldown_and_serializes_only_public_state() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        gate.reserve(now, cost(5, false)).unwrap();
        gate.penalize(now, now, 429, -1003, Some(Duration::from_secs(2)));
        assert!(matches!(
            gate.reserve(now, cost(0, true)),
            Err(AdmissionError::Cooldown(_))
        ));
        let active = gate.snapshot(now + Duration::from_millis(500));
        assert_eq!(active.cooldown_remaining_ms, 1500);
        assert_eq!(active.rate_limit_responses, 1);
        assert_eq!(active.cooldown_rejections, 1);
        let ended = gate.snapshot(now + Duration::from_secs(3));
        assert_eq!(ended.cooldown_remaining_ms, 0);
        assert_eq!(ended.last_limit_status, Some(429));
        assert_eq!(ended.last_limit_code, Some(-1003));
        let json = serde_json::to_value(ended).unwrap();
        assert_eq!(json["estimated_used_weight"], 5);
        assert!(json.get("charges").is_none());
        assert!(json.get("dispatch").is_none());
    }
    #[test]
    fn cooldown_blocks_queued_work_extends_and_then_resumes_spacing() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        assert!(gate.reserve(now, cost(1, false)).unwrap().is_none());
        assert_eq!(
            gate.reserve(now, cost(1, false)).unwrap(),
            Some(REQUEST_SPACING)
        );
        let limited_at = now + Duration::from_millis(50);
        assert_eq!(
            gate.penalize(now, limited_at, 429, -1003, Some(Duration::from_secs(2))),
            Duration::from_secs(2)
        );
        let AdmissionError::Cooldown(error) = gate
            .reserve(now + REQUEST_SPACING, cost(1, false))
            .unwrap_err()
        else {
            panic!("expected cooldown");
        };
        assert_eq!(error.status, 429);
        assert_eq!(error.remaining, Duration::from_millis(1950));
        // A late response must not shorten an existing longer cooldown.
        gate.penalize(now, limited_at, 429, -1003, Some(Duration::from_secs(1)));
        let end = limited_at + Duration::from_secs(2);
        assert!(gate.reserve(end, cost(1, false)).unwrap().is_none());
        assert_eq!(
            gate.reserve(end, cost(1, false)).unwrap(),
            Some(REQUEST_SPACING)
        );
        assert_eq!(
            gate.penalize(end, end, 429, -1003, None),
            Duration::from_secs(120)
        );
    }
    #[test]
    fn concurrent_limit_responses_share_a_strike_and_bans_have_a_longer_fallback() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        for offset in [1, 2, 3] {
            assert_eq!(
                gate.penalize(now, now + Duration::from_millis(offset), 429, -1003, None),
                Duration::from_secs(60)
            );
        }
        let later = now + Duration::from_secs(61);
        assert_eq!(
            gate.penalize(later, later, 418, -1003, None),
            Duration::from_secs(240)
        );
        let reset = later + Duration::from_secs(240) + RESET_AFTER;
        assert_eq!(
            gate.penalize(reset, reset, 429, -1003, None),
            Duration::from_secs(60)
        );
    }
    #[test]
    fn a_long_ban_does_not_reset_escalation_until_the_gate_has_been_idle() {
        let now = Instant::now();
        let mut gate = RequestThrottle::new(now);
        gate.penalize(now, now, 418, -1003, Some(Duration::from_secs(3600)));
        let end = now + Duration::from_secs(3600);
        assert!(gate.reserve(end, cost(1, false)).unwrap().is_none());
        assert_eq!(
            gate.penalize(end, end, 418, -1003, None),
            Duration::from_secs(240)
        );
    }
    #[test]
    fn retry_after_supports_seconds_and_http_dates_but_rejects_invalid_values() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert_eq!(retry_after(Some(" 2 "), now), Some(Duration::from_secs(2)));
        let date = httpdate::fmt_http_date(now + Duration::from_secs(60));
        assert_eq!(retry_after(Some(&date), now), Some(Duration::from_secs(60)));
        for value in [
            None,
            Some("invalid"),
            Some("-1"),
            Some("18446744073709551615"),
        ] {
            assert_eq!(retry_after(value, now), None);
        }
    }
}
