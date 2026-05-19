use arc_swap::ArcSwap;
use dashmap::DashMap;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use parking_lot::Mutex;
use rustc_hash::FxHasher;
use serde::Serialize;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
 
type FxBuildHasher = BuildHasherDefault<FxHasher>;

pub enum TypedValue<'a> {
    Null,
    Float(f64),
    Int(i64),
    Str(&'a str),
}

pub struct ColumnStats {
    pub total_count: AtomicU64,
    pub null_count: AtomicU64,
    pub min_val_bits: AtomicU64,
    pub max_val_bits: AtomicU64,
    pub hll: Mutex<HyperLogLogPlus<u64, RandomState>>,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            total_count: AtomicU64::new(0),
            null_count: AtomicU64::new(0),
            min_val_bits: AtomicU64::new(f64::INFINITY.to_bits()),
            max_val_bits: AtomicU64::new(f64::NEG_INFINITY.to_bits()),
            hll: Mutex::new(HyperLogLogPlus::new(14, RandomState::new()).unwrap()),
        }
    }
}

impl ColumnStats {
    pub fn update(&self, val: &TypedValue) {
        self.total_count.fetch_add(1, Ordering::Relaxed);

        match val {
            TypedValue::Null => {
                self.null_count.fetch_add(1, Ordering::Relaxed);
            }
            TypedValue::Float(_) | TypedValue::Int(_) => {
                let f_num = match val {
                    TypedValue::Int(i) => *i as f64,
                    TypedValue::Float(f) => *f,
                    _ => unreachable!(),
                };

                if f_num.is_nan() {
                    return;
                }

                self.min_val_bits.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current_bits| {
                    let current_val = f64::from_bits(current_bits);
                    if f_num < current_val { Some(f_num.to_bits()) } else { None }
                }).ok();

                self.max_val_bits.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current_bits| {
                    let current_val = f64::from_bits(current_bits);
                    if f_num > current_val { Some(f_num.to_bits()) } else { None }
                }).ok();

                let mut hll_guard = self.hll.lock();
                hll_guard.insert(&f_num.to_bits());
            }
            TypedValue::Str(s) => {
                let mut hasher = FxHasher::default();
                s.hash(&mut hasher);
                let hash_val = hasher.finish();

                let mut hll_guard = self.hll.lock();
                hll_guard.insert(&hash_val);
            }
        }
    }
}

#[derive(Serialize)]
pub struct ColumnStatsSnapshot {
    pub total_count: u64,
    pub null_count: u64,
    pub min_val: Option<f64>,
    pub max_val: Option<f64>,
    pub estimated_cardinality: usize,
}

pub struct DalgaEngine {
    active_stats: ArcSwap<DashMap<String, Arc<ColumnStats>, FxBuildHasher>>,
    pub total_records_processed: AtomicU64,
}

impl DalgaEngine {
    pub fn new() -> Self {
        Self {
            active_stats: ArcSwap::from_pointee(DashMap::with_hasher(FxBuildHasher::default())),
            total_records_processed: AtomicU64::new(0),
        }
    }

    pub fn process_batch(&self, rows: &[rustc_hash::FxHashMap<String, TypedValue>]) {
        self.total_records_processed.fetch_add(rows.len() as u64, Ordering::Relaxed);
        let current_map = self.active_stats.load();

        for row in rows {
            for (col_name, val) in row {
                let col_stat = if let Some(stat) = current_map.get(col_name) {
                    Arc::clone(stat.value())
                } else {
                    Arc::clone(
                        current_map
                            .entry(col_name.clone())
                            .or_insert_with(|| Arc::new(ColumnStats::default()))
                            .value()
                    )
                };

                col_stat.update(val);
            }
        }
    }

    pub fn flush_and_reset(&self) -> String {
        let new_map: Arc<DashMap<String, Arc<ColumnStats>, FxBuildHasher>> =
            Arc::new(DashMap::with_hasher(FxBuildHasher::default()));
            
        let old_map = self.active_stats.swap(new_map);
        let _ = self.total_records_processed.swap(0, Ordering::Relaxed);

        let mut snapshot_map = rustc_hash::FxHashMap::default();

        for kv in old_map.iter() {
            let col_stat = kv.value();
            let min_val = f64::from_bits(col_stat.min_val_bits.load(Ordering::Relaxed));
            let max_val = f64::from_bits(col_stat.max_val_bits.load(Ordering::Relaxed));

            snapshot_map.insert(
                kv.key().clone(),
                ColumnStatsSnapshot {
                    total_count: col_stat.total_count.load(Ordering::Relaxed),
                    null_count: col_stat.null_count.load(Ordering::Relaxed),
                    min_val: if min_val == f64::INFINITY { None } else { Some(min_val) },
                    max_val: if max_val == f64::NEG_INFINITY { None } else { Some(max_val) },
                    estimated_cardinality: col_stat.hll.lock().count().round() as usize,
                },
            );
        }

        serde_json::to_string(&snapshot_map).unwrap_or_else(|_| "{}".to_string())
    }
}
